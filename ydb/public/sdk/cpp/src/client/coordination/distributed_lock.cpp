#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/distributed_lock.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/session_pool.h>

#include <util/system/hostname.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>

namespace NYdb {
namespace NCoordination {

namespace {

void CloseSession(TSession& session) noexcept {
    try {
        if (session) {
            session.Close().GetValueSync();
        }
    } catch (...) {
    }
    session = {};
}

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

    void SetSuppress(bool suppress) {
        std::lock_guard guard(Lock_);
        Suppress_ = suppress;
    }

    void RequestStop() {
        std::lock_guard guard(Lock_);
        if (!Suppress_) {
            StopSource_.request_stop();
        }
    }

    void ForceStop() {
        std::lock_guard guard(Lock_);
        StopSource_.request_stop();
    }

private:
    mutable std::mutex Lock_;
    std::stop_source StopSource_;
    bool Suppress_ = false;
};

class TSuppressLockLoss {
public:
    explicit TSuppressLockLoss(const std::shared_ptr<TLockLossState>& state)
        : State_(state)
    {
        State_->SetSuppress(true);
    }

    ~TSuppressLockLoss() {
        State_->SetSuppress(false);
    }

private:
    std::shared_ptr<TLockLossState> State_;
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
        : Client_(client)
        , Name_(lockSettings.Name_)
        , Path_(lockSettings.Path_)
        , Timeout_(lockSettings.Timeout_)
        , DirectSessionReady_(std::make_shared<std::atomic<bool>>(false))
        , LockLossState_(std::make_shared<TLockLossState>())
    {
        if (!TryStartDirectSession()) {
            throw TYdbLockException("Failed to start session");
        }
    }

    TImpl(TCoordinationSessionPool pool, const TDistributedLockSettings& lockSettings)
        : Pool_(std::move(pool))
        , Name_(lockSettings.Name_)
        , Timeout_(lockSettings.Timeout_)
        , LockLossState_(std::make_shared<TLockLossState>())
    {
    }

    ~TImpl() {
        if (Pool_) {
            if (Session_) {
                if (Locked_) {
                    Pool_->Replace(std::move(Session_));
                } else {
                    Pool_->Return(std::move(Session_));
                }
            }
            return;
        }

        TSuppressLockLoss suppress(LockLossState_);
        CloseSession(Session_);
    }

    std::stop_token GetStopToken() const {
        return LockLossState_->GetStopToken();
    }

    bool try_lock() noexcept {
        return TryAcquire(TDuration::Zero()) == EAcquireResult::Acquired;
    }

    void lock() {
        switch (TryAcquire(Timeout_)) {
            case EAcquireResult::Acquired:
                return;
            case EAcquireResult::NoSession:
                throw TYdbLockException("Failed to start session");
            case EAcquireResult::NotAcquired:
            case EAcquireResult::Failed:
                throw TYdbLockException("Failed to acquire semaphore");
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
            LockLossState_->ForceStop();
            ReplaceCurrentSession();
        } else {
            ReturnPooledSessionIfNeeded();
        }
        Locked_ = false;
    }

private:
    TSessionSettings MakeDirectSessionSettings() {
        std::weak_ptr<TLockLossState> weakLockLoss = LockLossState_;
        auto sessionReady = DirectSessionReady_;
        return TSessionSettings()
            .Timeout(Timeout_)
            .OnStateChanged([weakLockLoss, sessionReady](ESessionState state) {
                if (state == ESessionState::EXPIRED) {
                    sessionReady->store(false);
                    if (auto lockLoss = weakLockLoss.lock()) {
                        lockLoss->RequestStop();
                    }
                }
            })
            .OnStopped([weakLockLoss, sessionReady] {
                sessionReady->store(false);
                if (auto lockLoss = weakLockLoss.lock()) {
                    lockLoss->RequestStop();
                }
            });
    }

    bool TryStartDirectSession() noexcept {
        try {
            auto sessionResult = Client_->StartSession(Path_, MakeDirectSessionSettings()).GetValueSync();
            if (!sessionResult.IsSuccess()) {
                DirectSessionReady_->store(false);
                return false;
            }
            Session_ = sessionResult.GetResult();
            DirectSessionReady_->store(true);
            return true;
        } catch (...) {
            DirectSessionReady_->store(false);
            return false;
        }
    }

    bool CheckoutPooledSession() noexcept {
        std::weak_ptr<TLockLossState> weakLockLoss = LockLossState_;
        auto maybeSession = Pool_->GetAny([weakLockLoss] {
            if (auto lockLoss = weakLockLoss.lock()) {
                lockLoss->RequestStop();
            }
        });
        if (!maybeSession) {
            Session_ = {};
            return false;
        }
        Session_ = std::move(*maybeSession);
        return true;
    }

    bool EnsureSession() noexcept {
        if (Pool_) {
            return bool(Session_) || CheckoutPooledSession();
        }
        return DirectSessionReady_->load() || TryStartDirectSession();
    }

    bool ResetDirectSession(bool lockLost) noexcept {
        if (lockLost) {
            LockLossState_->ForceStop();
        }

        TSuppressLockLoss suppress(LockLossState_);
        DirectSessionReady_->store(false);
        CloseSession(Session_);
        return TryStartDirectSession();
    }

    TAcquireSemaphoreSettings MakeAcquireSettings(TDuration acquireTimeout) const {
        return TAcquireSemaphoreSettings()
            .Exclusive()
            .Data(FQDNHostName())
            .Ephemeral()
            .Timeout(acquireTimeout);
    }

    void ReplaceCurrentSession() noexcept {
        if (Pool_) {
            Pool_->Replace(std::move(Session_));
            Session_ = {};
            return;
        }
        ResetDirectSession(false);
    }

    void ReturnPooledSessionIfNeeded() noexcept {
        if (Pool_ && Session_) {
            Pool_->Return(std::move(Session_));
            Session_ = {};
        }
    }

    EAcquireResult TryAcquire(TDuration acquireTimeout) noexcept try {
        if (!EnsureSession()) {
            return EAcquireResult::NoSession;
        }

        LockLossState_->Reset();
        auto acquireFuture = Session_.AcquireSemaphore(Name_, MakeAcquireSettings(acquireTimeout));
        if (!acquireFuture.Wait(Timeout_)) {
            ReplaceCurrentSession();
            return EAcquireResult::Failed;
        }

        const auto result = acquireFuture.GetValue();
        if (!result.IsSuccess()) {
            ReplaceCurrentSession();
            return EAcquireResult::Failed;
        }

        if (!result.GetResult()) {
            ReturnPooledSessionIfNeeded();
            return EAcquireResult::NotAcquired;
        }

        Locked_ = true;
        return EAcquireResult::Acquired;
    } catch (...) {
        ReplaceCurrentSession();
        return EAcquireResult::Failed;
    }

private:
    std::optional<TClient> Client_;
    std::optional<TCoordinationSessionPool> Pool_;
    TSession Session_;
    std::string Name_;
    std::string Path_;
    TDuration Timeout_;
    std::shared_ptr<std::atomic<bool>> DirectSessionReady_;
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
