#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/distributed_lock.h>

#include <util/system/hostname.h>

#include <mutex>

namespace NYdb {
namespace NCoordination {
    struct TDistributedLock::TImpl {
        struct TLockState {
            std::stop_token GetStopToken() const {
                std::lock_guard guard(Mutex);
                return StopSource.get_token();
            }

            bool TryBeginHold() {
                std::lock_guard guard(Mutex);
                if (SessionLost) {
                    return false;
                }
                StopSource = std::stop_source{};
                Holding = true;
                return true;
            }

            void EndHold() {
                std::lock_guard guard(Mutex);
                Holding = false;
            }

            void RequestStopIfHolding() {
                std::lock_guard guard(Mutex);
                if (Holding) {
                    StopSource.request_stop();
                }
            }

            void NotifySessionLost() {
                std::lock_guard guard(Mutex);
                SessionLost = true;
                if (Holding) {
                    StopSource.request_stop();
                }
            }

            mutable std::mutex Mutex;
            std::stop_source StopSource;
            bool Holding = false;
            bool SessionLost = false;
        };

        TSession Session;
        TAcquireSemaphoreSettings Settings;
        std::string Name;
        TDuration Timeout;
        std::shared_ptr<TLockState> LockState;
        std::shared_ptr<void> SessionLostSubscription;

        bool CancelAcquire() noexcept try {
            auto releaseFuture = Session.ReleaseSemaphore(Name);
            if (!releaseFuture.Wait(Timeout)) {
                return false;
            }
            const auto result = releaseFuture.GetValue();
            return result.IsSuccess();
        } catch (...) {
            return false;
        }

        bool IsSessionLost() {
            return Session.GetSessionState() == ESessionState::EXPIRED ||
                Session.GetConnectionState() == EConnectionState::STOPPED;
        }

        bool TryBeginHold() {
            if (IsSessionLost()) {
                LockState->NotifySessionLost();
                return false;
            }
            if (!LockState->TryBeginHold()) {
                return false;
            }
            if (IsSessionLost()) {
                LockState->NotifySessionLost();
                LockState->EndHold();
                return false;
            }
            return true;
        }

        TImpl(TSession session, const TDistributedLockSettings& lockSettings)
            : Session(std::move(session))
            , Name(lockSettings.Name_)
            , Timeout(lockSettings.Timeout_)
            , LockState(std::make_shared<TLockState>())
        {
            if (!Session) {
                throw TYdbLockException("Session is not initialized");
            }

            Settings = TAcquireSemaphoreSettings()
                .Exclusive()
                .Data(FQDNHostName())
                .Ephemeral()
                .Timeout(Timeout);

            std::weak_ptr<TLockState> weakLockState = LockState;
            SessionLostSubscription = Session.SubscribeSessionLost([weakLockState] {
                if (auto lockState = weakLockState.lock()) {
                    lockState->NotifySessionLost();
                }
            });
        }

        bool try_lock() noexcept try {
            auto acquireFuture = Session.AcquireSemaphore(Name, Settings);
            if (!acquireFuture.Wait(Timeout)) {
                CancelAcquire();
                return false;
            }
            const auto result = acquireFuture.GetValue();
            if (!result.IsSuccess()) {
                return false;
            }
            if (!result.GetResult()) {
                return false;
            }
            if (!TryBeginHold()) {
                return false;
            }
            return true;
        } catch (...) {
            return false;
        }

        void lock() {
            auto acquireFuture = Session.AcquireSemaphore(Name, Settings);
            if (!acquireFuture.Wait(Timeout)) {
                CancelAcquire();
                throw TYdbLockException("Failed to acquire semaphore");
            }
            const auto result = acquireFuture.GetValue();
            if (!result.IsSuccess()) {
                throw TYdbLockException("Failed to acquire semaphore");
            }
            if (!result.GetResult()) {
                throw TYdbLockException("Failed to acquire semaphore");
            }
            if (!TryBeginHold()) {
                throw TYdbLockException("Failed to acquire semaphore");
            }
        }

        void unlock() noexcept try {
            auto releaseFuture = Session.ReleaseSemaphore(Name);
            if (releaseFuture.Wait(Timeout)) {
                const auto result = releaseFuture.GetValue();
                if (result.IsSuccess() && result.GetResult()) {
                    LockState->EndHold();
                    return;
                }
            }

            LockState->RequestStopIfHolding();
            LockState->EndHold();
        } catch (...) {
            LockState->RequestStopIfHolding();
            LockState->EndHold();
        }
    };

    TDistributedLock::TDistributedLock(TSession session, const TDistributedLockSettings& settings) {
        impl_ = std::make_unique<TImpl>(std::move(session), settings);
    }

    TDistributedLock TSession::CreateDistributedLock(const TDistributedLockSettings& settings) {
        return TDistributedLock(*this, settings);
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
        return impl_->LockState->GetStopToken();
    }
}
}
