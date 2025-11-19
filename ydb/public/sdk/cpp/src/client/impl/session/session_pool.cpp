#include "session_pool.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/plain_status/status.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/operation/operation.h>

#include <util/random/random.h>

namespace NYdb::inline Dev {
namespace NSessionPool {

using namespace NThreading;

constexpr std::uint64_t KEEP_ALIVE_RANDOM_FRACTION = 4;
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
        const std::int64_t randomLimit = value / KEEP_ALIVE_RANDOM_FRACTION;
        if (randomLimit < 2)
            return duration;
        value += static_cast<std::int64_t>(RandomNumber<std::uint64_t>(randomLimit));
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

TSessionPool::TWaitersQueue::TWaitersQueue(std::uint32_t maxQueueSize)
    : MaxQueueSize_(maxQueueSize)
{
}

IGetSessionCtx* TSessionPool::TWaitersQueue::TryPush(std::unique_ptr<IGetSessionCtx>& p) {
    if (Waiters_.size() < MaxQueueSize_) {
        auto it = Waiters_.insert(std::make_pair(p->GetDeadline(), std::move(p)));
        return it->second.get();
    }
    return nullptr;
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

void TSessionPool::TWaitersQueue::GetOld(TDeadline deadline, std::vector<std::unique_ptr<IGetSessionCtx>>& oldWaiters) {
    auto it = Waiters_.begin();
    while (it != Waiters_.end()) {
        if (deadline < it->first) {
            break;
        }

        oldWaiters.emplace_back(std::move(it->second));

        Waiters_.erase(it++);
    }
}

std::uint32_t TSessionPool::TWaitersQueue::Size() const {
    return Waiters_.size(); 
}


TSessionPool::TSessionPool(std::uint32_t maxActiveSessions)
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
        } else if (auto* ctxPtr = WaitersQueue_.TryPush(ctx)) {
            sessionSource = TSessionSource::Waiter;
            ctxPtr->ScheduleOnDeadlineWaiterCleanup();
        } else {
            sessionSource = TSessionSource::Error;
        }
        if (!Sessions_.empty()) {
            auto it = std::prev(Sessions_.end());
            it->second->UpdateServerCloseHandler(nullptr);
            sessionImpl = std::move(it->second);
            Sessions_.erase(it);
        }

        UpdateStats();
    }

    if (sessionSource == TSessionSource::Waiter) {
        // ctxPtr->ScheduleOnDeadlineWaiterCleanup() is called after TryPush
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

void TSessionPool::ClearOldWaiters() {
    std::lock_guard guard(Mtx_);

    std::vector<std::unique_ptr<IGetSessionCtx>> oldWaiters;
    WaitersQueue_.GetOld(TDeadline::Now(), oldWaiters);

    for (auto& waiter : oldWaiters) {
        FakeSessionsCounter_.Inc();
        waiter->ReplyError(CLIENT_RESOURCE_EXHAUSTED_ACTIVE_SESSION_LIMIT);
    }

    if (!oldWaiters.empty()) {
        UpdateStats();
    }
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
            impl->UpdateServerCloseHandler(this);
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
        it->second->UpdateServerCloseHandler(nullptr);
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
    auto periodicCb = [this, weakClient, cmd=std::move(cmd), deletePredicate=std::move(deletePredicate)](NYdb::NIssue::TIssues&&, EStatus status) {
        if (status != EStatus::SUCCESS) {
            return false;
        }

        auto strongClient = weakClient.lock();
        if (!strongClient) {
            // No more clients alive - no need to run periodic,
            // moreover it is unsafe to touch this ptr!
            return false;
        } else {
            auto sessionCountToProcess = PERIODIC_ACTION_BATCH_SIZE;
            std::vector<std::unique_ptr<TKqpSessionCommon>> sessionsToTouch;
            sessionsToTouch.reserve(sessionCountToProcess);
            std::vector<std::unique_ptr<TKqpSessionCommon>> sessionsToDelete;
            sessionsToDelete.reserve(sessionCountToProcess);
            std::vector<std::unique_ptr<IGetSessionCtx>> waitersToReplyError;
            waitersToReplyError.reserve(sessionCountToProcess);
            const auto now = TDeadline::Now();
            const std::uint64_t nowUs = std::chrono::duration_cast<std::chrono::microseconds>(
                now.GetTimePoint().time_since_epoch()).count();
            {
                std::lock_guard guard(Mtx_);
                {
                    auto& sessions = Sessions_;

                    auto it = sessions.begin();
                    while (it != sessions.end() && sessionCountToProcess--) {
                        if (nowUs < it->second->GetTimeToTouchFast().MicroSeconds()) {
                            break;
                        }

                        if (deletePredicate(it->second.get(), sessions.size())) {
                            it->second->UpdateServerCloseHandler(nullptr);
                            sessionsToDelete.emplace_back(std::move(it->second));
                            sessions.erase(it++);
                        } else if (cmd) {
                            it->second->UpdateServerCloseHandler(nullptr);
                            sessionsToTouch.emplace_back(std::move(it->second));
                            sessions.erase(it++);
                        } else {
                            it++;
                        }
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

std::int64_t TSessionPool::GetActiveSessions() const {
    std::lock_guard guard(Mtx_);
    return ActiveSessions_;
}

std::int64_t TSessionPool::GetActiveSessionsLimit() const {
    return MaxActiveSessions_;
}

std::int64_t TSessionPool::GetCurrentPoolSize() const {
    std::lock_guard guard(Mtx_);
    return Sessions_.size();
}

void TSessionPool::OnCloseSession(const TKqpSessionCommon* s, std::shared_ptr<ISessionClient> client) {
    std::unique_ptr<TKqpSessionCommon> session;
    {
        std::lock_guard guard(Mtx_);
        const auto timeToTouch = s->GetTimeToTouchFast();
        const auto id = s->GetId();
        auto it = Sessions_.find(timeToTouch);
        // Sessions_ is multimap of sessions sorted by scheduled time to run periodic task
        // Scan sessions with same scheduled time to find needed one. In most cases only one session here
        while (it != Sessions_.end() && it->first == timeToTouch) {
            if (id != it->second->GetId()) {
                it++;
                continue;
            }
            session = std::move(it->second);
            Sessions_.erase(it);
            break;
        }
    }

    if (session) {
        Y_ABORT_UNLESS(session->GetState() == TKqpSessionCommon::S_IDLE);
        CloseAndDeleteSession(std::move(session), client);
    }
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
