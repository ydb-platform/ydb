#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/distributed_lock.h>

#include <util/system/hostname.h>

#include <atomic>

namespace NYdb {
namespace NCoordination {
    struct TDistributedLock::TImpl {
        std::stop_source stopSource;
        std::atomic<bool> suppressLockLossNotify{false};
        std::atomic<bool> sessionReady_{false};
        TSession session;
        TAcquireSemaphoreSettings settings;
        std::string name;
        std::string path;
        TClient client;
        TDuration timeout_;
        TSessionSettings MakeSessionSettings() {
            return TSessionSettings()
                .Timeout(timeout_)
                .OnStateChanged([this](ESessionState state) {
                    if (state == ESessionState::EXPIRED) {
                        sessionReady_.store(false);
                        if (!suppressLockLossNotify.load()) {
                            stopSource.request_stop();
                        }
                    }
                })
                .OnStopped([this] {
                    sessionReady_.store(false);
                    if (!suppressLockLossNotify.load()) {
                        stopSource.request_stop();
                    }
                });
        }
        bool TryStartSession() noexcept {
            try {
                auto sessionResult = client.StartSession(path, MakeSessionSettings()).GetValueSync();
                if (!sessionResult.IsSuccess()) {
                    sessionReady_.store(false);
                    return false;
                }
                session = sessionResult.GetResult();
                sessionReady_.store(true);
                return true;
            } catch (...) {
                sessionReady_.store(false);
                return false;
            }
        }
        bool EnsureSession() noexcept {
            return sessionReady_.load() || TryStartSession();
        }
        bool ResetSession(bool lockLost = false) noexcept {
            if (lockLost) {
                stopSource.request_stop();
            }
            suppressLockLossNotify.store(true);
            sessionReady_.store(false);
            try {
                session.Close().GetValueSync();
            } catch (...) {
                suppressLockLossNotify.store(false);
                return false;
            }
            if (!TryStartSession()) {
                suppressLockLossNotify.store(false);
                return false;
            }
            suppressLockLossNotify.store(false);
            return true;
        }
        ~TImpl() {
            suppressLockLossNotify.store(true);
            try {
                session.Close().GetValueSync();
            } catch (...) {}
        }
        TImpl(TClient& client, std::string_view path, std::string_view name, TDuration timeout)
            : name(name)
            , path(path)
            , client(client)
            , timeout_(timeout)
        {
            settings = TAcquireSemaphoreSettings()
                .Exclusive()
                .Data(FQDNHostName())
                .Ephemeral()
                .Timeout(timeout_);
            if (!TryStartSession()) {
                throw TYdbLockException("Failed to start session");
            }
        }
        bool try_lock() noexcept try {
            if (!EnsureSession()) {
                return false;
            }
            auto acquireFuture = session.AcquireSemaphore(name, settings);
            if (!acquireFuture.Wait(timeout_)) {
                ResetSession();
                return false;
            }
            const auto result = acquireFuture.GetValue();
            if (!result.IsSuccess()) {
                ResetSession();
                return false;
            }
            if (!result.GetResult()) {
                return false;
            }
            stopSource = std::stop_source{};
            return true;
        } catch (...) {
            return false;
        }
        void lock() {
            if (!EnsureSession()) {
                throw TYdbLockException("Failed to start session");
            }
            auto acquireFuture = session.AcquireSemaphore(name, settings);
            if (!acquireFuture.Wait(timeout_)) {
                ResetSession();
                throw TYdbLockException("Failed to acquire semaphore");
            }
            const auto result = acquireFuture.GetValue();
            if (!result.IsSuccess()) {
                ResetSession();
                throw TYdbLockException("Failed to acquire semaphore");
            }
            if (!result.GetResult()) {
                throw TYdbLockException("Failed to acquire semaphore");
            }
            stopSource = std::stop_source{};
        }
        void unlock() noexcept try {
            auto releaseFuture = session.ReleaseSemaphore(name);
            if (releaseFuture.Wait(timeout_)) {
                const auto result = releaseFuture.GetValue();
                if (!result.IsSuccess() || !result.GetResult()) {
                    ResetSession(true);
                }
            } else {
                ResetSession(true);
            }
        } catch (...) {
            ResetSession(true);
        }
    };
    TDistributedLock::TDistributedLock(TClient& client, std::string_view path, std::string_view name, TDuration timeout) {
        impl_ = std::make_unique<TImpl>(client, path, name, timeout);
    }
    TDistributedLock::~TDistributedLock() = default;
    void TDistributedLock::lock() {
        impl_->lock();
    }
    void TDistributedLock::unlock() noexcept {
        impl_->unlock();
    }
    void TDistributedLock::Acquire() {
        impl_->lock();
    }
    void TDistributedLock::Release() noexcept {
        impl_->unlock();
    }
    bool TDistributedLock::try_lock() noexcept {
        return impl_->try_lock();
    }
    std::stop_token TDistributedLock::getStopToken() const {
        return impl_->stopSource.get_token();
    }
}
}
