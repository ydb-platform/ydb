#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/session_pool.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>

#include <util/system/thread.h>

#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>

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

class TPooledSessionState {
public:
    bool SetOnLost(TCoordinationSessionPool::TSessionLostCallback onLost) {
        std::lock_guard guard(Lock_);
        if (Lost_) {
            return false;
        }
        OnLost_ = std::move(onLost);
        return true;
    }

    bool ClearOnLost() {
        std::lock_guard guard(Lock_);
        OnLost_ = {};
        return Lost_;
    }

    bool IsLost() const {
        std::lock_guard guard(Lock_);
        return Lost_;
    }

    void NotifyLost() {
        TCoordinationSessionPool::TSessionLostCallback onLost;
        {
            std::lock_guard guard(Lock_);
            if (Lost_) {
                return;
            }
            Lost_ = true;
            onLost = std::move(OnLost_);
            OnLost_ = {};
        }
        if (onLost) {
            onLost();
        }
    }

private:
    mutable std::mutex Lock_;
    TCoordinationSessionPool::TSessionLostCallback OnLost_;
    bool Lost_ = false;
};

struct TPooledSession {
    TSession Session;
    std::shared_ptr<TPooledSessionState> State;
};

} // namespace

struct TCoordinationSessionPool::TImpl {
    TImpl(TClient client, std::string path, TCoordinationSessionPoolSettings settings)
        : Client_(std::move(client))
        , Path_(std::move(path))
        , Settings_(std::move(settings))
    {
        if (Settings_.PoolSize_ == 0) {
            throw TYdbException("Session pool size must be positive");
        }

        for (size_t i = 0; i < Settings_.PoolSize_; ++i) {
            auto session = StartSessionWithRetry();
            if (!session) {
                CloseAll();
                throw TYdbException("Failed to start session");
            }
            Sessions_.push_back(std::move(*session));
        }
    }

    ~TImpl() {
        CloseAll();
    }

    std::optional<TSession> GetAny(TSessionLostCallback onLost) {
        if (auto pooled = PopIdleSession()) {
            if (pooled->State->IsLost() || !pooled->State->SetOnLost(onLost)) {
                RefreshPooledSession(*pooled, onLost);
            }

            auto session = pooled->Session;
            {
                std::lock_guard guard(Lock_);
                CheckedOut_[session.GetSessionId()] = std::move(*pooled);
            }
            return session;
        }

        return std::nullopt;
    }

    void Return(TSession session) noexcept {
        auto pooled = TakeCheckedOut(session);
        if (!pooled) {
            return;
        }

        const bool lost = pooled->State->ClearOnLost();
        if (lost) {
            CloseSession(pooled->Session);
            pooled = StartSessionWithRetry();
            if (!pooled) {
                return;
            }
        }

        std::lock_guard guard(Lock_);
        Sessions_.push_back(std::move(*pooled));
    }

    bool Replace(TSession session) noexcept {
        auto pooled = TakeCheckedOut(session);
        if (!pooled) {
            return false;
        }

        pooled->State->ClearOnLost();
        CloseSession(pooled->Session);

        auto replacement = StartSessionWithRetry();
        if (!replacement) {
            return false;
        }

        std::lock_guard guard(Lock_);
        Sessions_.push_back(std::move(*replacement));
        return true;
    }

    size_t Size() const noexcept {
        std::lock_guard guard(Lock_);
        return Sessions_.size();
    }

private:
    std::optional<TPooledSession> PopIdleSession() noexcept {
        std::lock_guard guard(Lock_);
        if (Sessions_.empty()) {
            return std::nullopt;
        }

        auto session = std::move(Sessions_.front());
        Sessions_.pop_front();
        return session;
    }

    std::optional<TPooledSession> TakeCheckedOut(TSession session) noexcept {
        if (!session) {
            return std::nullopt;
        }

        std::lock_guard guard(Lock_);
        auto it = CheckedOut_.find(session.GetSessionId());
        if (it == CheckedOut_.end()) {
            return std::nullopt;
        }

        auto pooled = std::move(it->second);
        CheckedOut_.erase(it);
        return pooled;
    }

    void RestoreIdleSession(TPooledSession&& pooled) {
        std::lock_guard guard(Lock_);
        Sessions_.push_back(std::move(pooled));
    }

    TDuration GetStartSessionRetryDeadline() const {
        const auto& settings = Settings_.SessionSettings_;
        if (settings.Timeout_ == TDuration::Max()) {
            return TDuration::Max();
        }
        return settings.Timeout_ * settings.ReconnectSessionTimeoutMultiplier_;
    }

    std::optional<TPooledSession> StartSessionOnce() noexcept {
        auto state = std::make_shared<TPooledSessionState>();
        auto settings = Settings_.SessionSettings_;
        auto userOnStateChanged = settings.OnStateChanged_;
        auto userOnStopped = settings.OnStopped_;
        settings
            .OnStateChanged([state, userOnStateChanged = std::move(userOnStateChanged)](ESessionState sessionState) {
                if (sessionState == ESessionState::EXPIRED) {
                    state->NotifyLost();
                }
                if (userOnStateChanged) {
                    userOnStateChanged(sessionState);
                }
            })
            .OnStopped([state, userOnStopped = std::move(userOnStopped)] {
                state->NotifyLost();
                if (userOnStopped) {
                    userOnStopped();
                }
            });

        try {
            auto result = Client_.StartSession(Path_, settings).GetValueSync();
            if (!result.IsSuccess()) {
                return std::nullopt;
            }
            return TPooledSession{
                .Session = result.GetResult(),
                .State = std::move(state),
            };
        } catch (...) {
            return std::nullopt;
        }
    }

    std::optional<TPooledSession> StartSessionWithRetry() {
        const auto& settings = Settings_.SessionSettings_;
        const TDuration retryDeadline = GetStartSessionRetryDeadline();
        const TInstant deadline = retryDeadline == TDuration::Max()
            ? TInstant::Max()
            : TInstant::Now() + retryDeadline;
        TDuration backoff = settings.ReconnectBackoffDelay_;

        while (true) {
            if (auto session = StartSessionOnce()) {
                return session;
            }

            if (deadline != TInstant::Max() && TInstant::Now() >= deadline) {
                return std::nullopt;
            }

            TDuration sleepDuration = backoff;
            if (deadline != TInstant::Max()) {
                const TDuration remaining = deadline - TInstant::Now();
                if (remaining <= TDuration::Zero()) {
                    return std::nullopt;
                }
                sleepDuration = Min(sleepDuration, remaining);
            }
            Sleep(sleepDuration);

            if (settings.Timeout_ != TDuration::Max()) {
                backoff = Min(
                    backoff * settings.ReconnectBackoffMultiplier_,
                    settings.Timeout_);
            } else {
                backoff = backoff * settings.ReconnectBackoffMultiplier_;
            }
        }
    }

    void RefreshPooledSession(TPooledSession& pooled, TSessionLostCallback& onLost) {
        CloseSession(pooled.Session);

        auto replacement = StartSessionWithRetry();
        if (!replacement) {
            RestoreIdleSession(std::move(pooled));
            throw TYdbException("Failed to start session");
        }

        pooled = std::move(*replacement);
        if (!pooled.State->SetOnLost(onLost)) {
            CloseSession(pooled.Session);
            RestoreIdleSession(std::move(pooled));
            throw TYdbException("Failed to start session");
        }
    }

    void CloseAll() noexcept {
        std::deque<TPooledSession> sessions;
        std::unordered_map<uint64_t, TPooledSession> checkedOut;
        {
            std::lock_guard guard(Lock_);
            sessions.swap(Sessions_);
            checkedOut.swap(CheckedOut_);
        }

        for (auto& session : sessions) {
            session.State->ClearOnLost();
            CloseSession(session.Session);
        }
        for (auto& [_, session] : checkedOut) {
            session.State->ClearOnLost();
            CloseSession(session.Session);
        }
    }

private:
    TClient Client_;
    std::string Path_;
    TCoordinationSessionPoolSettings Settings_;
    mutable std::mutex Lock_;
    std::deque<TPooledSession> Sessions_;
    std::unordered_map<uint64_t, TPooledSession> CheckedOut_;
};

TCoordinationSessionPool::TCoordinationSessionPool() = default;

TCoordinationSessionPool::TCoordinationSessionPool(std::shared_ptr<TImpl> impl)
    : Impl_(std::move(impl))
{
}

TCoordinationSessionPool::~TCoordinationSessionPool() = default;

std::optional<TSession> TCoordinationSessionPool::GetAny(TSessionLostCallback onLost) {
    if (!Impl_) {
        return std::nullopt;
    }
    return Impl_->GetAny(std::move(onLost));
}

void TCoordinationSessionPool::Return(TSession session) {
    if (Impl_) {
        Impl_->Return(std::move(session));
    }
}

bool TCoordinationSessionPool::Replace(TSession session) {
    if (!Impl_) {
        return false;
    }
    return Impl_->Replace(std::move(session));
}

size_t TCoordinationSessionPool::Size() const {
    if (!Impl_) {
        return 0;
    }
    return Impl_->Size();
}

TCoordinationSessionPool TClient::CreateSessionPool(
    const std::string& path,
    const TCoordinationSessionPoolSettings& settings)
{
    return TCoordinationSessionPool(std::make_shared<TCoordinationSessionPool::TImpl>(*this, path, settings));
}

TDistributedLock TCoordinationSessionPool::CreateDistributedLock(const TDistributedLockSettings& settings) {
    if (!Impl_) {
        throw TYdbException("Session pool is not initialized");
    }
    return TDistributedLock(*this, settings);
}

} // namespace NCoordination
} // namespace NYdb
