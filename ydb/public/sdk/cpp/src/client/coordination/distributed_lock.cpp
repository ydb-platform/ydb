#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/distributed_lock.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/session_pool.h>

#include <util/system/hostname.h>

#include <functional>
#include <memory>
#include <mutex>
#include <utility>

namespace NYdb {
namespace NCoordination {

namespace {

constexpr double SERVER_ACQUIRE_TIMEOUT_FRACTION = 0.8;

class TLockLossState {
public:
    std::stop_token GetStopToken() const {
        std::lock_guard guard(Lock_);
        return StopSource_.get_token();
    }

    void Reset() {
        std::lock_guard guard(Lock_);
        StopSource_ = std::stop_source{};
    }

    void RequestStop() {
        std::lock_guard guard(Lock_);
        StopSource_.request_stop();
    }

private:
    mutable std::mutex Lock_;
    std::stop_source StopSource_;
};

} // namespace

struct TDistributedLock::TImpl {
    enum class EAcquireResult {
        Acquired,
        NotAcquired,
        NoSession,
        Failed,
    };

    TImpl(TClient& client, const TDistributedLockSettings& lockSettings)
        : TImpl(
            client.CreateSessionPool(
                lockSettings.Path_,
                TCoordinationSessionPoolSettings()
                    .PoolSize(1)
                    .SessionSettings(TSessionSettings().Timeout(lockSettings.SessionTimeout_))),
            lockSettings)
    {
    }

    TImpl(TCoordinationSessionPool pool, const TDistributedLockSettings& lockSettings)
        : Pool_(std::move(pool))
        , Name_(lockSettings.Name_)
        , Timeout_(lockSettings.Timeout_)
        , LockLossState_(std::make_shared<TLockLossState>())
    {
    }

    ~TImpl() {
        if (Locked_) {
            LockLossState_->RequestStop();
        }
        Locked_ ? ReplaceSession() : ReturnSession();
    }

    std::stop_token GetStopToken() const {
        return LockLossState_->GetStopToken();
    }

    bool try_lock() noexcept {
        return TryAcquire(Min(TDuration::MilliSeconds(100), Timeout_)) == EAcquireResult::Acquired;
    }

    void lock() {
        const auto deadline = TInstant::Now() + Timeout_;
        while (true) {
            const auto remaining = deadline - TInstant::Now();
            if (remaining <= TDuration::Zero()) {
                throw TYdbLockException("Failed to acquire semaphore");
            }

            const auto acquireTimeout = Min(ServerAcquireTimeout(), remaining);
            switch (TryAcquire(acquireTimeout)) {
                case EAcquireResult::Acquired:
                    return;
                case EAcquireResult::NotAcquired:
                case EAcquireResult::NoSession:
                    break;
                case EAcquireResult::Failed:
                    throw TYdbLockException("Failed to acquire semaphore");
            }

            Sleep(TDuration::MilliSeconds(1));
        }
    }

    void unlock() noexcept {
        if (!Session_) {
            Locked_ = false;
            return;
        }

        bool releaseFailed = false;
        try {
            auto releaseFuture = Session_.ReleaseSemaphore(Name_);
            if (releaseFuture.Wait(Timeout_)) {
                const auto result = releaseFuture.GetValue();
                releaseFailed = !result.IsSuccess() || !result.GetResult();
            } else {
                releaseFailed = true;
            }
        } catch (...) {
            releaseFailed = true;
        }

        if (releaseFailed) {
            LockLossState_->RequestStop();
            ReplaceSession();
        } else {
            ReturnSession();
        }
        Locked_ = false;
    }

private:
    std::function<void()> MakeLockLossCallback() const {
        std::weak_ptr<TLockLossState> weakLockLoss = LockLossState_;
        return [weakLockLoss] {
            if (auto lockLoss = weakLockLoss.lock()) {
                lockLoss->RequestStop();
            }
        };
    }

    TAcquireSemaphoreSettings MakeAcquireSettings(TDuration acquireTimeout) const {
        return TAcquireSemaphoreSettings()
            .Exclusive()
            .Data(FQDNHostName())
            .Ephemeral()
            .Timeout(acquireTimeout);
    }

    TDuration ServerAcquireTimeout() const {
        return Timeout_ * SERVER_ACQUIRE_TIMEOUT_FRACTION;
    }

    bool EnsureSession() noexcept {
        if (Session_) {
            return true;
        }

        try {
            auto session = Pool_.GetAny(MakeLockLossCallback());
            if (!session) {
                return false;
            }
            Session_ = std::move(*session);
            return true;
        } catch (const TYdbException&) {
            return false;
        }
    }

    void ReturnSession() noexcept {
        ReleaseSession(false);
    }

    void ReplaceSession() noexcept {
        ReleaseSession(true);
    }

    void ReleaseSession(bool replace) noexcept {
        if (Session_) {
            try {
                if (replace) {
                    Pool_.Replace(std::move(Session_));
                } else {
                    Pool_.Return(std::move(Session_));
                }
            } catch (...) {
            }
            Session_ = {};
        }
    }

    EAcquireResult TryAcquire(TDuration acquireTimeout) noexcept try {
        if (!EnsureSession()) {
            return EAcquireResult::NoSession;
        }

        LockLossState_->Reset();
        auto acquireFuture = Session_.AcquireSemaphore(Name_, MakeAcquireSettings(acquireTimeout));
        if (!acquireFuture.Wait(acquireTimeout)) {
            ReplaceSession();
            return EAcquireResult::Failed;
        }

        const auto result = acquireFuture.GetValue();
        if (!result.IsSuccess()) {
            ReplaceSession();
            return EAcquireResult::Failed;
        }

        if (!result.GetResult()) {
            ReturnSession();
            return EAcquireResult::NotAcquired;
        }

        Locked_ = true;
        return EAcquireResult::Acquired;
    } catch (...) {
        ReplaceSession();
        return EAcquireResult::Failed;
    }

private:
    TCoordinationSessionPool Pool_;
    TSession Session_;
    std::string Name_;
    TDuration Timeout_;
    std::shared_ptr<TLockLossState> LockLossState_;
    bool Locked_ = false;
};

TDistributedLock::TDistributedLock(TClient& client, const TDistributedLockSettings& settings)
    : TDistributedLock(std::make_unique<TImpl>(client, settings))
{
}

TDistributedLock::TDistributedLock(TCoordinationSessionPool pool, const TDistributedLockSettings& settings)
    : TDistributedLock(std::make_unique<TImpl>(std::move(pool), settings))
{
}

TDistributedLock::TDistributedLock(std::unique_ptr<TImpl> impl)
    : Impl_(std::move(impl))
{
}

TDistributedLock TClient::CreateDistributedLock(const TDistributedLockSettings& settings)
{
    return TDistributedLock(*this, settings);
}

TDistributedLock::~TDistributedLock() = default;

void TDistributedLock::lock() {
    Impl_->lock();
}

void TDistributedLock::unlock() noexcept {
    Impl_->unlock();
}

void TDistributedLock::Acquire() {
    Impl_->lock();
}

void TDistributedLock::Release() noexcept {
    Impl_->unlock();
}

bool TDistributedLock::try_lock() noexcept {
    return Impl_->try_lock();
}

std::stop_token TDistributedLock::GetStopToken() const {
    return Impl_->GetStopToken();
}

} // namespace NCoordination
} // namespace NYdb
