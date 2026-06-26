#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/session_pool.h>

#include "distributed_lock_internal.h"

#include <deque>
#include <memory>
#include <mutex>
#include <optional>

namespace NYdb {
namespace NCoordination {

namespace {

using NPrivate::ISessionSource;
using NPrivate::TPooledSessionState;
using NPrivate::TSessionLease;

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
            throw TYdbLockException("Session pool size must be positive");
        }

        for (size_t i = 0; i < Settings_.PoolSize_; ++i) {
            auto session = StartSession();
            if (!session) {
                CloseAll();
                throw TYdbLockException("Failed to start session");
            }
            Sessions_.push_back(std::move(*session));
        }
    }

    ~TImpl() {
        CloseAll();
    }

    bool Checkout(TSessionLease& lease, std::function<void()> onLost) noexcept {
        std::lock_guard guard(Lock_);
        if (Sessions_.empty()) {
            lease = {};
            return false;
        }

        auto session = std::move(Sessions_.front());
        Sessions_.pop_front();
        session.State->SetOnLost(std::move(onLost));
        lease.Session = std::move(session.Session);
        lease.PooledState = std::move(session.State);
        return true;
    }

    bool Replace(TSessionLease& lease, std::function<void()> onLost) noexcept {
        NPrivate::CloseSession(lease.Session);
        auto session = StartSession();
        if (!session) {
            lease = {};
            return false;
        }

        session->State->SetOnLost(std::move(onLost));
        lease.Session = std::move(session->Session);
        lease.PooledState = std::move(session->State);
        return true;
    }

    void Checkin(TSessionLease&& lease, bool close) noexcept {
        if (lease.PooledState) {
            lease.PooledState->ClearOnLost();
        }

        if (close) {
            NPrivate::CloseSession(lease.Session);
            return;
        }

        if (!lease.Session) {
            return;
        }

        std::lock_guard guard(Lock_);
        Sessions_.push_back(TPooledSession{
            .Session = std::move(lease.Session),
            .State = std::move(lease.PooledState),
        });
    }

private:
    std::optional<TPooledSession> StartSession() noexcept {
        auto state = std::make_shared<TPooledSessionState>();
        auto settings = Settings_.SessionSettings_;
        auto userOnStateChanged = settings.OnStateChanged_;
        auto userOnStopped = settings.OnStopped_;
        settings
            .OnStateChanged([state, userOnStateChanged = std::move(userOnStateChanged)](ESessionState sessionState) {
                if (userOnStateChanged) {
                    userOnStateChanged(sessionState);
                }
                if (sessionState == ESessionState::EXPIRED) {
                    state->NotifyLost();
                }
            })
            .OnStopped([state, userOnStopped = std::move(userOnStopped)] {
                if (userOnStopped) {
                    userOnStopped();
                }
                state->NotifyLost();
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

    void CloseAll() noexcept {
        std::deque<TPooledSession> sessions;
        {
            std::lock_guard guard(Lock_);
            sessions.swap(Sessions_);
        }
        for (auto& session : sessions) {
            session.State->ClearOnLost();
            NPrivate::CloseSession(session.Session);
        }
    }

private:
    TClient Client_;
    std::string Path_;
    TCoordinationSessionPoolSettings Settings_;
    std::mutex Lock_;
    std::deque<TPooledSession> Sessions_;
};

TCoordinationSessionPool::TCoordinationSessionPool() = default;

TCoordinationSessionPool::TCoordinationSessionPool(std::shared_ptr<TImpl> impl)
    : Impl_(std::move(impl))
{
}

TCoordinationSessionPool::~TCoordinationSessionPool() = default;

TCoordinationSessionPool TClient::CreateSessionPool(
    const std::string& path,
    const TCoordinationSessionPoolSettings& settings)
{
    return TCoordinationSessionPool(std::make_shared<TCoordinationSessionPool::TImpl>(*this, path, settings));
}

TDistributedLock TCoordinationSessionPool::CreateDistributedLock(const TDistributedLockSettings& settings) {
    if (!Impl_) {
        throw TYdbLockException("Session pool is not initialized");
    }

    class TPoolSessionSource final : public ISessionSource {
    public:
        explicit TPoolSessionSource(std::shared_ptr<TCoordinationSessionPool::TImpl> pool)
            : Pool_(std::move(pool))
        {
        }

        bool Checkout(TSessionLease& lease, std::function<void()> onLost) noexcept override {
            return Pool_->Checkout(lease, std::move(onLost));
        }

        bool Replace(TSessionLease& lease, std::function<void()> onLost) noexcept override {
            return Pool_->Replace(lease, std::move(onLost));
        }

        void Checkin(TSessionLease&& lease, bool close) noexcept override {
            Pool_->Checkin(std::move(lease), close);
        }

        bool IsPersistent() const noexcept override {
            return false;
        }

    private:
        std::shared_ptr<TCoordinationSessionPool::TImpl> Pool_;
    };

    return TDistributedLock(std::make_unique<TDistributedLock::TImpl>(
        std::make_unique<TPoolSessionSource>(Impl_),
        settings,
        false));
}

} // namespace NCoordination
} // namespace NYdb
