#include "session_pool.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/plain_status/status.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>
#include <ydb/public/sdk/cpp/client/ydb_types/operation/operation.h>

#include <util/random/random.h>

namespace NYdb {
namespace NSessionPool {

using namespace NThreading;

constexpr ui64 KEEP_ALIVE_RANDOM_FRACTION = 4;
static const TStatus CLIENT_RESOURCE_EXHAUSTED_ACTIVE_SESSION_LIMIT = TStatus(
    TPlainStatus(
        EStatus::CLIENT_RESOURCE_EXHAUSTED,
            "Active sessions limit exceeded"
        )
    );

TStatus GetStatus(const TOperation& operation) {
    return operation.Status();
}

TStatus GetStatus(const TStatus& status) {
    return status;
}

TDuration RandomizeThreshold(TDuration duration) {
    TDuration::TValue value = duration.GetValue();
    if (KEEP_ALIVE_RANDOM_FRACTION) {
        const i64 randomLimit = value / KEEP_ALIVE_RANDOM_FRACTION;
        if (randomLimit < 2)
            return duration;
        value += static_cast<i64>(RandomNumber<ui64>(randomLimit));
    }
    return TDuration::FromValue(value);
}

bool IsSessionCloseRequested(const TStatus& status) {
    const auto& meta = status.GetResponseMetadata();
    auto hints = meta.equal_range(NYdb::YDB_SERVER_HINTS);
    for(auto it = hints.first; it != hints.second; ++it) {
        if (it->second == NYdb::YDB_SESSION_CLOSE) {
            return true;
        }
    }

    return false;
}

TSessionPool::TWaitersQueue::TWaitersQueue(ui32 maxQueueSize, TDuration maxWaitSessionTimeout)
    : MaxQueueSize_(maxQueueSize)
    , MaxWaitSessionTimeout_(maxWaitSessionTimeout)
{
}

bool TSessionPool::TWaitersQueue::TryPush(std::unique_ptr<IGetSessionCtx>& p) {
    if (Waiters_.size() < MaxQueueSize_) {
        Waiters_.insert(std::make_pair(TInstant::Now(), std::move(p)));
        return true;
    }
    return false;
}

std::unique_ptr<IGetSessionCtx> TSessionPool::TWaitersQueue::TryGet() {
    if (Waiters_.empty()) {
        return {};
    }
    auto it = Waiters_.begin();
    auto result = std::move(it->second);
    Waiters_.erase(it);
    return result;
}

void TSessionPool::TWaitersQueue::GetOld(TInstant now, TVector<std::unique_ptr<IGetSessionCtx>>& oldWaiters) {
    auto it = Waiters_.begin();
    while (it != Waiters_.end()) {
        if (now < it->first + MaxWaitSessionTimeout_)
            break;

        oldWaiters.emplace_back(std::move(it->second));

        Waiters_.erase(it++);
    }
}

ui32 TSessionPool::TWaitersQueue::Size() const {
    return Waiters_.size(); 
}


TSessionPool::TSessionPool(ui32 maxActiveSessions)
    : Closed_(false)
    , WaitersQueue_(maxActiveSessions * 10)
    , ActiveSessions_(0)
    , MaxActiveSessions_(maxActiveSessions)
{}

static void CloseAndDeleteSession(std::unique_ptr<TKqpSessionCommon>&& impl,
                                  std::shared_ptr<ISessionClient> client) {
    auto deleter = TKqpSessionCommon::GetSmartDeleter(client);
    TKqpSessionCommon* p = impl.release();
    p->MarkBroken();
    deleter(p);
}

void TSessionPool::ReplySessionToUser(
    TKqpSessionCommon* session,
    std::unique_ptr<IGetSessionCtx> ctx)
{
    Y_ABORT_UNLESS(session->GetState() == TKqpSessionCommon::S_IDLE);
    Y_ABORT_UNLESS(!session->GetTimeInterval());
    session->MarkActive();
    session->SetNeedUpdateActiveCounter(true);
    ctx->ReplySessionToUser(session);
}

void TSessionPool::GetSession(std::unique_ptr<IGetSessionCtx> ctx)
{
    std::unique_ptr<TKqpSessionCommon> sessionImpl;
    enum class TSessionSource {
        Pool,
        Waiter,
        Error
    } sessionSource = TSessionSource::Pool;

    {
        std::lock_guard guard(Mtx_);

        if (MaxActiveSessions_ == 0 || ActiveSessions_ < MaxActiveSessions_) {
            IncrementActiveCounterUnsafe();
        } else if (WaitersQueue_.TryPush(ctx)) {
            sessionSource = TSessionSource::Waiter;
        } else {
            sessionSource = TSessionSource::Error;
        }
        if (!Sessions_.empty()) {
            auto it = std::prev(Sessions_.end());
            sessionImpl = std::move(it->second);
            Sessions_.erase(it);
        }

        UpdateStats();
    }

    if (sessionSource == TSessionSource::Waiter) {
        // Nothing to do here
    } else if (sessionSource == TSessionSource::Error) {
        FakeSessionsCounter_.Inc();
        ctx->ReplyError(CLIENT_RESOURCE_EXHAUSTED_ACTIVE_SESSION_LIMIT);
    } else if (sessionImpl) {
        ReplySessionToUser(sessionImpl.release(), std::move(ctx));
    } else {
        ctx->ReplyNewSession();
    }
}

bool TSessionPool::CheckAndFeedWaiterNewSession(bool active) {
    std::unique_ptr<IGetSessionCtx> getSessionCtx;
    {
        std::lock_guard guard(Mtx_);
        if (Closed_)
            return false;

        if (auto maybeCtx = WaitersQueue_.TryGet()) {
            getSessionCtx = std::move(maybeCtx);
        } else {
            return false;
        }
    }

    if (!active) {
        // Session was IDLE. It means session has been closed during
        // keep-alive activity inside session pool. In this case
        // we must not touch ActiveSession counter.
        // Moreover it is unsafe to recreate the session because session
        // may be closed during balancing or draining routine.
        // But we must feed waiters if we have some otherwise deadlock
        // is possible.
        // The above mentioned conditions is quite rare so
        // we can just return an error. In this case uplevel code should
        // start retry.
        getSessionCtx->ReplyError(CLIENT_RESOURCE_EXHAUSTED_ACTIVE_SESSION_LIMIT);
        return true;
    }

    getSessionCtx->ReplyNewSession();
    return true;
}

bool TSessionPool::ReturnSession(TKqpSessionCommon* impl, bool active) {
    // Do not call ReplySessionToUser under the session pool lock
    std::unique_ptr<IGetSessionCtx> getSessionCtx;
    {
        std::lock_guard guard(Mtx_);
        if (Closed_)
            return false;

        if (auto maybeCtx = WaitersQueue_.TryGet()) {
            getSessionCtx = std::move(maybeCtx);
            if (!active)
                IncrementActiveCounterUnsafe();
        } else {
            Sessions_.emplace(std::make_pair(
                impl->GetTimeToTouchFast(),
                impl));

            if (active) {
                Y_ABORT_UNLESS(ActiveSessions_);
                ActiveSessions_--;
                impl->SetNeedUpdateActiveCounter(false);
            }
        }
        UpdateStats();
    }

    if (getSessionCtx) {
        ReplySessionToUser(impl, std::move(getSessionCtx));
    }

    return true;
}

void TSessionPool::DecrementActiveCounter() {
    std::lock_guard guard(Mtx_);
    Y_ABORT_UNLESS(ActiveSessions_);
    ActiveSessions_--;
    UpdateStats();
}

void TSessionPool::IncrementActiveCounterUnsafe() {
    ActiveSessions_++;
    UpdateStats();
}

void TSessionPool::Drain(std::function<bool(std::unique_ptr<TKqpSessionCommon>&&)> cb, bool close) {
    std::lock_guard guard(Mtx_);
    Closed_ = close;
    for (auto it = Sessions_.begin(); it != Sessions_.end();) {
        const bool cont = cb(std::move(it->second));
        it = Sessions_.erase(it);
        if (!cont)
            break;
    }
    UpdateStats();
}

TPeriodicCb TSessionPool::CreatePeriodicTask(std::weak_ptr<ISessionClient> weakClient,
    TKeepAliveCmd&& cmd, TDeletePredicate&& deletePredicate)
{
    auto periodicCb = [this, weakClient, cmd=std::move(cmd), deletePredicate=std::move(deletePredicate)](NYql::TIssues&&, EStatus status) {
        if (status != EStatus::SUCCESS) {
            return false;
        }

        auto strongClient = weakClient.lock();
        if (!strongClient) {
            // No more clients alive - no need to run periodic,
            // moreover it is unsafe to touch this ptr!
            return false;
        } else {
            auto keepAliveBatchSize = PERIODIC_ACTION_BATCH_SIZE;
            TVector<std::unique_ptr<TKqpSessionCommon>> sessionsToTouch;
            sessionsToTouch.reserve(keepAliveBatchSize);
            TVector<std::unique_ptr<TKqpSessionCommon>> sessionsToDelete;
            sessionsToDelete.reserve(keepAliveBatchSize);
            TVector<std::unique_ptr<IGetSessionCtx>> waitersToReplyError;
            waitersToReplyError.reserve(keepAliveBatchSize);
            const auto now = TInstant::Now();
            {
                std::lock_guard guard(Mtx_);
                {
                    auto& sessions = Sessions_;

                    auto it = sessions.begin();
                    while (it != sessions.end() && keepAliveBatchSize--) {
                        if (now < it->second->GetTimeToTouchFast())
                            break;

                        if (deletePredicate(it->second.get(), sessions.size())) {
                            sessionsToDelete.emplace_back(std::move(it->second));
                        } else {
                            sessionsToTouch.emplace_back(std::move(it->second));
                        }
                        sessions.erase(it++);
                    }
                }

                WaitersQueue_.GetOld(now, waitersToReplyError);

                UpdateStats();
            }

            for (auto& sessionImpl : sessionsToTouch) {
                if (sessionImpl) {
                    Y_ABORT_UNLESS(sessionImpl->GetState() == TKqpSessionCommon::S_IDLE);
                    cmd(sessionImpl.release());
                }
            }

            for (auto& sessionImpl : sessionsToDelete) {
                if (sessionImpl) {
                    Y_ABORT_UNLESS(sessionImpl->GetState() == TKqpSessionCommon::S_IDLE);
                    CloseAndDeleteSession(std::move(sessionImpl), strongClient);
                }
            }

            for (auto& waiter : waitersToReplyError) {
                FakeSessionsCounter_.Inc();
                waiter->ReplyError(CLIENT_RESOURCE_EXHAUSTED_ACTIVE_SESSION_LIMIT);
            }
        }

        return true;
    };
    return periodicCb;
}

i64 TSessionPool::GetActiveSessions() const {
    std::lock_guard guard(Mtx_);
    return ActiveSessions_;
}

i64 TSessionPool::GetActiveSessionsLimit() const {
    return MaxActiveSessions_;
}

i64 TSessionPool::GetCurrentPoolSize() const {
    std::lock_guard guard(Mtx_);
    return Sessions_.size();
}

void TSessionPool::SetStatCollector(NSdkStats::TStatCollector::TSessionPoolStatCollector statCollector) {
    ActiveSessionsCounter_.Set(statCollector.ActiveSessions);
    InPoolSessionsCounter_.Set(statCollector.InPoolSessions);
    FakeSessionsCounter_.Set(statCollector.FakeSessions);
    SessionWaiterCounter_.Set(statCollector.Waiters);
}

void TSessionPool::UpdateStats() {
    ActiveSessionsCounter_.Apply(ActiveSessions_);
    InPoolSessionsCounter_.Apply(Sessions_.size());
    SessionWaiterCounter_.Apply(WaitersQueue_.Size());
}

}
}
