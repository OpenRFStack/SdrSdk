/*
========================================================================
Project: OpenRFStack
Author:  Brendan Michaud
Year:    2026
Part of OpenRFStack (https://github.com/OpenRFStack)

Licensed under the Personal Use License.
Do not use for commercial, organizational, or military purposes.
Contact author for permission: https://github.com/OpenRFStack
========================================================================
*/
#include "sdrsdk/SdrClient.hpp"

#include <nlohmann/json.hpp>
#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/container.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/receiver.hpp>
#include <proton/receiver_options.hpp>
#include <proton/reconnect_options.hpp>
#include <proton/sender.hpp>
#include <proton/sender_options.hpp>
#include <proton/source_options.hpp>
#include <proton/symbol.hpp>
#include <proton/target_options.hpp>
#include <proton/transport.hpp>
#include <proton/work_queue.hpp>
#include <sdr/MessageCodec.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <iomanip>
#include <mutex>
#include <random>
#include <sstream>
#include <thread>
#include <unordered_map>

using json = nlohmann::json;
using namespace std::chrono;

static std::string make_uuid()
{
    static std::mt19937_64                  rng(std::random_device{}());
    static std::mutex                       rng_mu;
    std::uniform_int_distribution<uint64_t> d;
    uint64_t                                a, b;
    {
        std::lock_guard<std::mutex> lk(rng_mu);
        a = d(rng);
        b = d(rng);
    }
    std::ostringstream s;
    s << std::hex << std::setfill('0') << std::setw(8) << (a >> 32) << '-' << std::setw(4)
      << ((a >> 16) & 0xffff) << '-' << std::setw(4) << (a & 0xffff) << '-' << std::setw(4)
      << (b >> 48) << '-' << std::setw(12) << (b & 0xffff'ffff'ffff);
    return s.str();
}

// Requests are encoded and responses decoded by sdr::MessageCodec, the same
// code the controller uses: the hand-rolled encoder here sent "TASK_REQUEST"
// for triggered captures (the controller ignored the trigger), dropped
// coherency_group / devices / dest_ports / gains / device constraints, read
// stream addresses from "dest_ip" (the controller sends "udp_ip") and mapped
// 3 of the reject codes.

// ── AMQP implementation ───────────────────────────────────────────────────────

namespace sdr
{

struct SdrClient::Impl : proton::messaging_handler
{
    Config cfg;

    proton::container  container_{*this};
    std::thread        loop_thread_;
    proton::connection conn_;
    proton::sender     sender_;
    proton::receiver   receiver_;
    std::atomic<bool>  connected_{false};
    // Replies come to a private dynamic queue (reply_to on every request).
    // The shared response queue let concurrent clients take each other's
    // replies (2 clients x 20 requests: 1 lost; triggered() got none).
    std::string                      reply_addr_;  // proton thread only
    std::atomic<proton::work_queue*> wq_{nullptr};

    std::mutex              conn_mu_;
    std::condition_variable conn_cv_;

    std::mutex              out_mu_;
    std::deque<std::string> out_q_;

    struct Waiter
    {
        std::mutex              mu;
        std::condition_variable cv;
        std::string             body;
        bool                    done = false;
    };

    std::mutex                                               waiters_mu_;
    std::unordered_map<std::string, std::shared_ptr<Waiter>> waiters_;

    void on_container_start(proton::container& c) override
    {
        proton::connection_options co;
        // Without sasl_allowed_mechs("PLAIN"), proton never negotiates
        // credentials onto the wire — Artemis rejects (AMQ229031) even though
        // user/password are set. Same fix applied across all repos.
        if (!cfg.user.empty())
        {
            co.sasl_allowed_mechs("PLAIN");
            co.sasl_allow_insecure_mechs(true);
            co.user(cfg.user);
        }
        else
        {
            co.sasl_allowed_mechs("ANONYMOUS");
        }
        if (!cfg.password.empty())
            co.password(cfg.password);
        // Without this, a connection attempt that loses the race with a
        // still-starting broker is permanent -- every user app linking this
        // SDK would silently never connect after one bad boot timing.
        proton::reconnect_options ropts;
        ropts.delay(proton::duration(2000));
        ropts.max_delay(proton::duration(30'000));
        ropts.max_attempts(0);
        co.reconnect(ropts);
        conn_ = c.connect(cfg.broker, co);
        sender_ =
            conn_.open_sender(cfg.req_queue,
                              proton::sender_options().target(proton::target_options().capabilities(
                                  {proton::symbol("queue")})));
        receiver_ = conn_.open_receiver(
            "", proton::receiver_options().source(proton::source_options().dynamic(true)));
    }

    // Also after a reconnect: proton restores the link, the broker assigns a
    // fresh dynamic address.
    void on_receiver_open(proton::receiver& r) override
    {
        reply_addr_ = r.source().address();
        wq_.store(&r.work_queue());
        flush();
        if (!connected_.exchange(true))
            conn_cv_.notify_all();
    }

    void flush()
    {
        std::lock_guard<std::mutex> g(out_mu_);
        while (!out_q_.empty() && !reply_addr_.empty() && sender_.credit() > 0)
        {
            proton::message m;
            m.body(out_q_.front());
            m.content_type("application/json");
            m.reply_to(reply_addr_);
            sender_.send(m);
            out_q_.pop_front();
        }
    }

    void on_sendable(proton::sender&) override
    {
        flush();
    }

    void on_connection_open(proton::connection&) override
    {
        flush();
    }

    void on_message(proton::delivery&, proton::message& msg) override
    {
        try
        {
            std::string body;
            auto        v = msg.body();
            if (v.type() == proton::STRING)
                body = proton::get<std::string>(v);
            else if (v.type() == proton::BINARY)
            {
                auto b = proton::get<proton::binary>(v);
                body.assign(b.begin(), b.end());
            }
            else
                return;

            auto                        j   = json::parse(body);
            auto                        rid = j.value("request_id", std::string{});
            std::lock_guard<std::mutex> g(waiters_mu_);
            auto                        it = waiters_.find(rid);
            if (it != waiters_.end())
            {
                std::lock_guard<std::mutex> wg(it->second->mu);
                it->second->body = std::move(body);
                it->second->done = true;
                it->second->cv.notify_one();
            }
        }
        catch (...)
        {
        }
    }

    void on_connection_error(proton::connection&) override
    {
        connected_ = false;
    }

    void on_transport_error(proton::transport&) override
    {
        connected_ = false;
        reply_addr_.clear();  // a reconnect brings a new one
    }

    void enqueue(std::string body)
    {
        {
            std::lock_guard<std::mutex> g(out_mu_);
            out_q_.push_back(std::move(body));
        }
        // add() wakes the connection; a scheduled task does not, so the send
        // could wait for the next inbound frame (same fault as the RM's RM-87).
        if (auto* wq = wq_.load())
            wq->add(
                [this]
                {
                    flush();
                });
    }

    std::string rpc(std::string body, const std::string& rid, int timeout_ms)
    {
        auto        waiter = std::make_shared<Waiter>();
        {
            std::lock_guard<std::mutex> g(waiters_mu_);
            waiters_[rid] = waiter;
        }
        enqueue(std::move(body));
        auto deadline = steady_clock::now() + milliseconds(timeout_ms);
        {
            std::unique_lock<std::mutex> lk(waiter->mu);
            waiter->cv.wait_until(lk,
                                  deadline,
                                  [&]
                                  {
                                      return waiter->done;
                                  });
        }
        {
            std::lock_guard<std::mutex> g(waiters_mu_);
            waiters_.erase(rid);
        }
        if (!waiter->done)
            return {};
        return waiter->body;
    }
};

// ── SdrClient public API ──────────────────────────────────────────────────────

SdrClient::SdrClient(const std::string& broker,
                     const std::string& user,
                     const std::string& password,
                     const std::string& dest_ip)
    : SdrClient(Config{broker,
                       user,
                       password,
                       "sdr.task.request",
                       "sdr.task.response",
                       dest_ip,
                       sdrunit::s(20.0)})
{
}

SdrClient::SdrClient(Config cfg)
    : impl_(std::make_unique<Impl>())
    , cfg_(std::move(cfg))
{
    impl_->cfg = cfg_;
}

SdrClient::~SdrClient()
{
    disconnect();
}

void SdrClient::connect()
{
    impl_->loop_thread_ = std::thread(
        [this]
        {
            impl_->container_.run();
        });
    std::unique_lock<std::mutex> lk(impl_->conn_mu_);
    if (!impl_->conn_cv_.wait_for(
            lk,
            milliseconds(static_cast<int>(cfg_.timeout.in(au::milli(au::seconds)))),
            [this]
            {
                return impl_->connected_.load();
            }))
        throw SdrError("Cannot connect to broker at " + cfg_.broker);
}

void SdrClient::disconnect()
{
    if (impl_->loop_thread_.joinable())
    {
        impl_->connected_.store(false);
        impl_->container_.stop();
        impl_->loop_thread_.join();
    }
}

bool SdrClient::isConnected() const
{
    return impl_->connected_.load();
}

TaskResponse SdrClient::submit(const TaskRequest& req_in)
{
    TaskRequest req = req_in;
    if (req.request_id.empty())
        req.request_id = make_uuid();
    if (req.streaming.dest_ip.empty())
        req.streaming.dest_ip = cfg_.dest_ip;
    const auto body = impl_->rpc(MessageCodec::encodeTaskRequest(req),
                                 req.request_id,
                                 static_cast<int>(cfg_.timeout.in(au::milli(au::seconds))));
    if (body.empty())
        throw SdrError("No response from controller (timeout or disconnect)");
    auto resp = MessageCodec::decodeTaskResponse(body);
    if (!resp)
        throw SdrError("Malformed response from controller");
    return *resp;
}

// ── Convenience methods ───────────────────────────────────────────────────────

TaskResponse SdrClient::narrowband(au::QuantityD<au::Hertz>   cf,
                                   au::QuantityD<au::Hertz>   bw,
                                   au::QuantityD<au::Hertz>   sr,
                                   au::QuantityD<au::Seconds> duration)
{
    double      cf_hz  = cf.in(au::hertz);
    double      bw_hz  = bw.in(au::hertz);
    double      sr_hz  = sr.in(au::hertz);
    int         dur_ms = static_cast<int>(duration.in(au::milli(au::seconds)) + 0.5);
    TaskRequest req;
    req.request_id    = make_uuid();
    req.task_type     = TaskType::NARROWBAND;
    req.schedule_mode = ScheduleMode::IMMEDIATE;
    req.duration_ms   = dur_ms;
    req.rank          = 2;
    req.rf            = {cf_hz, bw_hz, sr_hz, 1};
    req.streaming     = {cfg_.dest_ip};
    return submit(req);
}

TaskResponse SdrClient::wideband(au::QuantityD<au::Hertz>   cf,
                                 au::QuantityD<au::Hertz>   bw,
                                 au::QuantityD<au::Hertz>   sr,
                                 au::QuantityD<au::Seconds> duration)
{
    double      cf_hz  = cf.in(au::hertz);
    double      bw_hz  = bw.in(au::hertz);
    double      sr_hz  = sr.in(au::hertz);
    int         dur_ms = static_cast<int>(duration.in(au::milli(au::seconds)) + 0.5);
    TaskRequest req;
    req.request_id    = make_uuid();
    req.task_type     = TaskType::WIDEBAND;
    req.schedule_mode = ScheduleMode::IMMEDIATE;
    req.duration_ms   = dur_ms;
    req.rank          = 2;
    req.rf            = {cf_hz, bw_hz, sr_hz, 1};
    req.streaming     = {cfg_.dest_ip};
    req.wb_params     = WidebandParams{true, -60.0, 2048};
    return submit(req);
}

TaskResponse SdrClient::triggered(au::QuantityD<au::Hertz>   cf,
                                  au::QuantityD<au::Hertz>   bw,
                                  au::QuantityD<au::Hertz>   sr,
                                  double                     thr,
                                  au::QuantityD<au::Seconds> post_trigger,
                                  int                        max_cap,
                                  au::QuantityD<au::Seconds> duration)
{
    double      cf_hz   = cf.in(au::hertz);
    double      bw_hz   = bw.in(au::hertz);
    double      sr_hz   = sr.in(au::hertz);
    int         post_ms = static_cast<int>(post_trigger.in(au::milli(au::seconds)) + 0.5);
    int         dur_ms  = static_cast<int>(duration.in(au::milli(au::seconds)) + 0.5);
    TaskRequest req;
    req.request_id     = make_uuid();
    req.task_type      = TaskType::TRIGGERED;
    req.schedule_mode  = ScheduleMode::IMMEDIATE;
    req.duration_ms    = dur_ms;
    req.rank           = 2;
    req.rf             = {cf_hz, bw_hz, sr_hz, 1};
    req.streaming      = {cfg_.dest_ip};
    req.trigger_params = TriggerParams{"POWER_THRESHOLD", thr, 50, post_ms, max_cap};
    return submit(req);
}

TaskResponse SdrClient::scan(const std::vector<ScanEntry>& entries,
                             bool                          repeat,
                             au::QuantityD<au::Seconds>    duration)
{
    int         dur_ms = static_cast<int>(duration.in(au::milli(au::seconds)) + 0.5);
    TaskRequest req;
    req.request_id    = make_uuid();
    req.task_type     = TaskType::SCAN;
    req.schedule_mode = ScheduleMode::IMMEDIATE;
    req.duration_ms   = dur_ms;
    req.rank          = 2;
    if (!entries.empty())
        req.rf = {
            entries[0].center_freq_hz, entries[0].bandwidth_hz, entries[0].sample_rate_sps, 1};
    req.streaming   = {cfg_.dest_ip};
    req.scan_params = ScanParams{repeat, entries};
    return submit(req);
}

TaskResponse SdrClient::snapshot(au::QuantityD<au::Hertz> cf,
                                 au::QuantityD<au::Hertz> bw,
                                 au::QuantityD<au::Hertz> sr,
                                 int                      fft_size,
                                 int                      n_avg)
{
    double      cf_hz = cf.in(au::hertz);
    double      bw_hz = bw.in(au::hertz);
    double      sr_hz = sr.in(au::hertz);
    TaskRequest req;
    req.request_id      = make_uuid();
    req.task_type       = TaskType::SNAPSHOT;
    req.schedule_mode   = ScheduleMode::IMMEDIATE;
    req.rank            = 2;
    req.rf              = {cf_hz, bw_hz, sr_hz, 1};
    req.snapshot_params = SnapshotParams{cf_hz, bw_hz, sr_hz, fft_size, n_avg};
    return submit(req);
}

TaskResponse SdrClient::calibration(au::QuantityD<au::Hertz>        cf,
                                    au::QuantityD<au::Hertz>        bw,
                                    au::QuantityD<au::Hertz>        sr,
                                    au::QuantityD<au::Seconds>      duration,
                                    const std::vector<std::string>& devices)
{
    double      cf_hz  = cf.in(au::hertz);
    double      bw_hz  = bw.in(au::hertz);
    double      sr_hz  = sr.in(au::hertz);
    int         dur_ms = static_cast<int>(duration.in(au::milli(au::seconds)) + 0.5);
    TaskRequest req;
    req.request_id    = make_uuid();
    req.task_type     = TaskType::CALIBRATION;
    req.schedule_mode = ScheduleMode::IMMEDIATE;
    req.duration_ms   = dur_ms;
    req.rank          = 2;
    req.rf            = {cf_hz, bw_hz, sr_hz, 1};
    req.streaming     = {cfg_.dest_ip};
    req.cal_params    = CalibrationParams{cf_hz, bw_hz, sr_hz, dur_ms, 1, "", devices};
    return submit(req);
}

void SdrClient::stop(const std::string& task_id, const std::string& reason)
{
    TaskRequest r;
    r.msg_type   = "TASK_STOP";
    r.request_id = make_uuid();
    r.task_id    = task_id;
    r.reason     = reason.empty() ? "client stop" : reason;
    impl_->enqueue(MessageCodec::encodeTaskRequest(r));
}

void SdrClient::cancel(const std::string& task_id, const std::string& reason)
{
    TaskRequest r;
    r.msg_type   = "TASK_CANCEL";
    r.request_id = make_uuid();
    r.task_id    = task_id;
    r.reason     = reason.empty() ? "client cancel" : reason;
    impl_->enqueue(MessageCodec::encodeTaskRequest(r));
}

}  // namespace sdr

/*
========================================================================
End of file — OpenRFStack
Subject to Personal Use License
https://github.com/OpenRFStack
========================================================================
*/
