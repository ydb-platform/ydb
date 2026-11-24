#include "remote_embedder_http.h"

#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/threading/future/async.h>
#include <library/cpp/time_provider/monotonic.h>

#include <util/generic/hash_set.h>
#include <util/generic/singleton.h>
#include <util/generic/scope.h>
#include <util/stream/output.h>

#include <chrono>
#include <condition_variable>
#include <map>
#include <mutex>
#include <thread>

namespace NKnn {
namespace {

constexpr size_t HttpWorkers = 8;
constexpr size_t MaxQueuedRequests = 64;
constexpr size_t MaxQueuedTimers = 128;
constexpr size_t MaxResponseBytes = 16 * 1024 * 1024;

class TResponseTooLarge : public yexception {};

class TBoundedResponse final : public IOutputStream {
public:
    TString ExtractBody() {
        return std::move(Body_);
    }

private:
    void DoWrite(const void* data, size_t size) override {
        if (size > MaxResponseBytes - Body_.size()) {
            ythrow TResponseTooLarge();
        }
        Body_.append(static_cast<const char*>(data), size);
    }

    TString Body_;
};

class TRemoteEmbedderHttpExecutor {
public:
    TRemoteEmbedderHttpExecutor()
    {
        Workers_.Start(HttpWorkers, MaxQueuedRequests);
        Timer_ = std::thread([this] { RunTimers(); });
    }

    ~TRemoteEmbedderHttpExecutor() {
        {
            std::lock_guard lock(Mutex_);
            Stopping_ = true;
            for (auto* connection : ActiveConnections_) {
                try {
                    connection->Shutdown();
                } catch (const std::exception&) {
                    // A peer can close the socket before shutdown reaches it.
                }
            }
        }
        Ready_.notify_one();
        Timer_.join();
        Workers_.Stop();
        for (auto& [_, promise] : Timers_) {
            promise.SetException("RemoteEmbedder: HTTP executor is shutting down");
        }
    }

    NThreading::TFuture<TRemoteEmbedderHttpResponse> Post(TString host, ui16 port, bool useTls, TString request) {
        std::lock_guard lock(Mutex_);
        Y_ENSURE(!Stopping_, "RemoteEmbedder: HTTP executor is shutting down");
        return NThreading::Async(
            [this, host = std::move(host), port, useTls, request = std::move(request)] {
                return PostOnce(host, port, useTls, request);
            }, Workers_);
    }

    NThreading::TFuture<void> Wait(TDuration delay) {
        const auto deadline = NMonotonic::TMonotonic::Now() + delay;
        auto promise = NThreading::NewPromise();
        auto future = promise.GetFuture();
        {
            std::lock_guard lock(Mutex_);
            Y_ENSURE(!Stopping_ && Timers_.size() < MaxQueuedTimers, "RemoteEmbedder: HTTP timer queue is unavailable");
            Timers_.emplace(deadline, std::move(promise));
        }
        Ready_.notify_one();
        return future;
    }

private:
    void EnsureRunning() {
        std::lock_guard lock(Mutex_);
        Y_ENSURE(!Stopping_, "RemoteEmbedder: HTTP executor is shutting down");
    }

    TRemoteEmbedderHttpResponse PostOnce(const TString& host, ui16 port, bool useTls, const TString& request) {
        EnsureRunning();
        using TVerifyCert = TOpenSslClientIO::TOptions::TVerifyCert;
        try {
            // Each attempt owns a fresh connection and cannot implicitly resend a POST.
            NPrivate::THttpConnection connection(host, port, TDuration::Seconds(60), TDuration::Seconds(10),
                useTls, Nothing(), TVerifyCert{host}, false, true);
            {
                std::lock_guard lock(Mutex_);
                Y_ENSURE(!Stopping_, "RemoteEmbedder: HTTP executor is shutting down");
                ActiveConnections_.insert(&connection);
            }
            Y_DEFER {
                std::lock_guard lock(Mutex_);
                ActiveConnections_.erase(&connection);
            };
            connection.Write(request);
            auto& input = *connection.GetHttpInput();
            const auto status = ParseHttpRetCode(input.FirstLine());
            TBoundedResponse response;
            if (status >= 200 && status < 300 && status != HTTP_NO_CONTENT) {
                ui64 contentLength = 0;
                if (input.GetContentLength(contentLength) && contentLength > MaxResponseBytes) {
                    ythrow TResponseTooLarge();
                }
                TransferData(&input, &response);
            }
            return {status, response.ExtractBody()};
        } catch (const TResponseTooLarge&) {
            return {502, {}, true};
        } catch (const std::exception&) {
            EnsureRunning();
            // HTTP diagnostics may contain credentials or input text.
            return {0, {}};
        }
    }

    void RunTimers() {
        std::unique_lock lock(Mutex_);
        while (!Stopping_) {
            if (Timers_.empty()) {
                Ready_.wait(lock);
                continue;
            }
            const auto deadline = Timers_.begin()->first;
            const auto now = NMonotonic::TMonotonic::Now();
            if (now < deadline) {
                const auto delay = Min(deadline - now, TDuration::Hours(1));
                Ready_.wait_for(lock, std::chrono::microseconds(delay.MicroSeconds()));
                continue;
            }
            auto timer = Timers_.extract(Timers_.begin());
            lock.unlock();
            timer.mapped().SetValue();
            lock.lock();
        }
    }

    TThreadPool Workers_;
    std::mutex Mutex_;
    THashSet<NPrivate::THttpConnection*> ActiveConnections_;
    std::condition_variable Ready_;
    std::multimap<NMonotonic::TMonotonic, NThreading::TPromise<void>> Timers_;
    bool Stopping_ = false;
    std::thread Timer_;
};

} // namespace

NThreading::TFuture<TRemoteEmbedderHttpResponse> PostRemoteEmbedderRequest(
    TString host, ui16 port, bool useTls, TString request)
{
    return Singleton<TRemoteEmbedderHttpExecutor>()->Post(std::move(host), port, useTls, std::move(request));
}

NThreading::TFuture<void> WaitBeforeRemoteEmbedderRequest(TDuration delay) {
    return Singleton<TRemoteEmbedderHttpExecutor>()->Wait(delay);
}

} // namespace NKnn
