#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/distributed_mutex.h>

#include <util/system/hostname.h>

namespace NYdb {
namespace NCoordination {
    static constexpr TDuration DEFAULT_TIMEOUT = TDuration::Seconds(10);
    struct TDistributedMutex::TImpl {
        std::mutex localMutex;
        std::stop_source stopSource;
        TSession session;
        TAcquireSemaphoreSettings settings;
        std::string name;
        std::string path;
        TClient& client;
        TDuration clientWait_;
        TSessionSettings MakeSessionSettings() {
            return TSessionSettings()
                .OnStateChanged([this](ESessionState state) {
                    if (state == ESessionState::EXPIRED) {
                        stopSource.request_stop();
                    }
                })
                .OnStopped([this] {
                    stopSource.request_stop();
                });
        }
        void ResetSession() {
            session.Close().GetValueSync();
            auto sessionResult = client.StartSession(path, MakeSessionSettings()).GetValueSync();
            if (!sessionResult.IsSuccess()) {
                throw TYdbLockException("Failed to start session");
            }
            session = sessionResult.GetResult();
        }
        TImpl(TClient& client, std::string_view path, std::string_view name, TDuration timeout)
            : client(client)
            , clientWait_(timeout + DEFAULT_TIMEOUT)
        {
            this->path = std::string(path);
            auto sessionResult = client.StartSession(this->path, MakeSessionSettings()).GetValueSync();
            if (!sessionResult.IsSuccess()) {
                throw TYdbLockException("Failed to start session");
            }
            this->session = sessionResult.GetResult();
            settings = TAcquireSemaphoreSettings()
                .Exclusive()
                .Data(FQDNHostName())
                .Ephemeral()
                .Timeout(timeout);
            this->name = name;
        }
        bool try_lock() noexcept {
            if (localMutex.try_lock()) {
                auto aquireFuture = session.AcquireSemaphore(name, settings);
                if (!aquireFuture.Wait(clientWait_)) {
                    try {
                        ResetSession();
                    } catch (...) {
                    }
                    localMutex.unlock();
                    return false;
                }
                const auto result = aquireFuture.GetValue();
                if (!result.IsSuccess() || !result.GetResult()) {
                    localMutex.unlock();
                    return false;
                }
                return true;
            }
            return false;
        }
        void lock() {
            localMutex.lock();
            auto aquireFuture = session.AcquireSemaphore(name, settings);
            if (!aquireFuture.Wait(clientWait_)) {
                ResetSession();
                localMutex.unlock();
                throw TYdbLockException("Failed to acquire semaphore");
            }
            const auto result = aquireFuture.GetValue();
            if (!result.IsSuccess() || !result.GetResult()) {
                session.Close();
                localMutex.unlock();
                throw TYdbLockException("Failed to acquire semaphore");
            }
        }
        void unlock() noexcept {
            auto releaseFuture = session.ReleaseSemaphore(name);
            if (releaseFuture.Wait(clientWait_)) {
                const auto result = releaseFuture.GetValue();
                if (!result.IsSuccess() || !result.GetResult()) {
                    session.Close();
                }
            } else {
                session.Close();
            }
            localMutex.unlock();
        }
    };
    TDistributedMutex::TDistributedMutex(TClient& client, std::string_view path, std::string_view name, TDuration timeout) {
        impl_ = std::make_unique<TImpl>(client, path, name, timeout);
    }
    void TDistributedMutex::lock() {
        impl_->lock();
    }
    void TDistributedMutex::unlock() noexcept {
        impl_->unlock();
    }
    bool TDistributedMutex::try_lock() noexcept {
        return impl_->try_lock();
    }
    std::stop_token TDistributedMutex::getStopToken() const {
        return impl_->stopSource.get_token();
    }
}
}
