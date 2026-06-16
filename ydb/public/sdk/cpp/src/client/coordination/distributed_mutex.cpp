#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/distributed_mutex.h>

#include <util/system/hostname.h>

#include <memory>

namespace NYdb {
namespace NCoordination {
    static constexpr TDuration CLIENT_WAIT_MARGIN = TDuration::Seconds(10);
    struct TDistributedMutex::TImpl {
        std::mutex localMutex;
        std::shared_ptr<std::stop_source> stopSource = std::make_shared<std::stop_source>();
        TSession session;
        TAcquireSemaphoreSettings settings;
        std::string name;
        std::string path;
        TClient client;
        TDuration clientWait_;
        TSessionSettings MakeSessionSettings() {
            auto stopSource = std::atomic_load(&this->stopSource);
            return TSessionSettings()
                .OnStateChanged([stopSource](ESessionState state) {
                    if (state == ESessionState::EXPIRED) {
                        stopSource->request_stop();
                    }
                })
                .OnStopped([stopSource]() {
                    stopSource->request_stop();
                });
        }
        void ResetSession() {
            std::atomic_store(&stopSource, std::make_shared<std::stop_source>());
            session.Close().GetValueSync();
            auto sessionResult = client.StartSession(path, MakeSessionSettings()).GetValueSync();
            if (!sessionResult.IsSuccess()) {
                throw TYdbLockException("Failed to start session");
            }
            session = sessionResult.GetResult();
        }
        void ResetSessionNoexcept() noexcept {
            try {
                ResetSession();
            } catch (...) {
            }
        }
        ~TImpl() {
            session.Close().GetValueSync();
        }
        TImpl(TClient& client, std::string_view path, std::string_view name, TDuration timeout)
            : client(client)
            , clientWait_(timeout + CLIENT_WAIT_MARGIN)
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
                auto acquireFuture = session.AcquireSemaphore(name, settings);
                if (!acquireFuture.Wait(clientWait_)) {
                    ResetSessionNoexcept();
                    localMutex.unlock();
                    return false;
                }
                const auto result = acquireFuture.GetValue();
                if (!result.IsSuccess()) {
                    ResetSessionNoexcept();
                    localMutex.unlock();
                    return false;
                }
                if (!result.GetResult()) {
                    localMutex.unlock();
                    return false;
                }
                return true;
            }
            return false;
        }
        void lock() {
            localMutex.lock();
            auto acquireFuture = session.AcquireSemaphore(name, settings);
            if (!acquireFuture.Wait(clientWait_)) {
                ResetSession();
                localMutex.unlock();
                throw TYdbLockException("Failed to acquire semaphore");
            }
            const auto result = acquireFuture.GetValue();
            if (!result.IsSuccess()) {
                ResetSession();
                localMutex.unlock();
                throw TYdbLockException("Failed to acquire semaphore");
            }
            if (!result.GetResult()) {
                localMutex.unlock();
                throw TYdbLockException("Failed to acquire semaphore");
            }
        }
        void unlock() noexcept {
            auto releaseFuture = session.ReleaseSemaphore(name);
            if (releaseFuture.Wait(clientWait_)) {
                const auto result = releaseFuture.GetValue();
                if (!result.IsSuccess() || !result.GetResult()) {
                    ResetSessionNoexcept();
                }
            } else {
                ResetSessionNoexcept();
            }
            localMutex.unlock();
        }
    };
    TDistributedMutex::TDistributedMutex(TClient& client, std::string_view path, std::string_view name, TDuration timeout) {
        impl_ = std::make_unique<TImpl>(client, path, name, timeout);
    }
    TDistributedMutex::~TDistributedMutex() = default;
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
        return std::atomic_load(&impl_->stopSource)->get_token();
    }
}
}
