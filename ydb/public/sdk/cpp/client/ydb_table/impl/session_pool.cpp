#include "session_pool.h"
#include "table_client.h"


namespace NYdb {
namespace NTable {

using namespace NThreading;


TSessionPool::TWaitersQueue::TWaitersQueue(ui32 maxQueueSize, TDuration maxWaitSessionTimeout)
    : MaxQueueSize_(maxQueueSize)
    , MaxWaitSessionTimeout_(maxWaitSessionTimeout)
{
}

bool TSessionPool::TWaitersQueue::TryPush(NThreading::TPromise<TCreateSessionResult>& p) {
    if (Waiters_.size() < MaxQueueSize_) {
        Waiters_.insert(std::make_pair(TInstant::Now(), p));
        return true;
    }
    return false;
}

TMaybe<NThreading::TPromise<TCreateSessionResult>> TSessionPool::TWaitersQueue::TryGet() {
    if (Waiters_.empty()) {
        return {};
    }
    auto it = Waiters_.begin();
    auto result = it->second;
    Waiters_.erase(it);
    return result;
}

void TSessionPool::TWaitersQueue::GetOld(TInstant now, TVector<NThreading::TPromise<TCreateSessionResult>>& oldWaiters) {
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

void TTableClient::TImpl::CloseAndDeleteSession(std::unique_ptr<TSession::TImpl>&& impl,
                                  std::shared_ptr<TTableClient::TImpl> client) {
    std::shared_ptr<TSession::TImpl> deleteSession(
        impl.release(),
        TSession::TImpl::GetSmartDeleter(client));

    deleteSession->MarkBroken();
}

void TSessionPool::CreateFakeSession(
    NThreading::TPromise<TCreateSessionResult>& promise,
    std::shared_ptr<TTableClient::TImpl> client)
{
    TSession session(client, "", "", false);
    // Mark standalone to prevent returning to session pool
    session.SessionImpl_->MarkStandalone();
    TCreateSessionResult val(
        TStatus(
            TPlainStatus(
                EStatus::CLIENT_RESOURCE_EXHAUSTED,
                "Active sessions limit exceeded"
            )
        ),
        std::move(session)
    );

    client->ScheduleTaskUnsafe([promise, val{std::move(val)}]() mutable {
        promise.SetValue(std::move(val));
    }, TDuration());
}

void TSessionPool::MakeSessionPromiseFromSession(
    TSession::TImpl* session,
    NThreading::TPromise<TCreateSessionResult>& promise,
    std::shared_ptr<TTableClient::TImpl> client
) {
    Y_VERIFY(session->GetState() == TSession::TImpl::S_IDLE);
    Y_VERIFY(!session->GetTimeInterval());
    session->MarkActive();
    session->SetNeedUpdateActiveCounter(true);

    TCreateSessionResult val(
        TStatus(TPlainStatus()),
        TSession(
            client,
            std::shared_ptr<TSession::TImpl>(
                session, TSession::TImpl::GetSmartDeleter(client)
            )
        )
    );

    client->ScheduleTaskUnsafe(
        [promise, val{std::move(val)}]() mutable {
            promise.SetValue(std::move(val));
        },
        TDuration()
    );
}

TAsyncCreateSessionResult TSessionPool::GetSession(
    std::shared_ptr<TTableClient::TImpl> client,
    const TCreateSessionSettings& settings)
{
    auto createSessionPromise = NewPromise<TCreateSessionResult>();
    std::unique_ptr<TSession::TImpl> sessionImpl;
    enum class TSessionSource {
        Pool,
        Waiter,
        Error
    } sessionSource = TSessionSource::Pool;

    {
        std::lock_guard guard(Mtx_);

        if (MaxActiveSessions_ == 0 || ActiveSessions_ < MaxActiveSessions_) {
            IncrementActiveCounterUnsafe();
        } else if (WaitersQueue_.TryPush(createSessionPromise)) {
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
        CreateFakeSession(createSessionPromise, client);
    } else if (sessionImpl) {
        MakeSessionPromiseFromSession(sessionImpl.release(), createSessionPromise, client);
    } else {
        const auto& sessionResult = client->CreateSession(settings, false);
        sessionResult.Subscribe(TSession::TImpl::GetSessionInspector(createSessionPromise, client, settings, 0, true));
    }

    return createSessionPromise.GetFuture();
}

bool TSessionPool::CheckAndFeedWaiterNewSession(std::shared_ptr<TTableClient::TImpl> client) {
    NThreading::TPromise<TCreateSessionResult> createSessionPromise;
    {
        std::lock_guard guard(Mtx_);
        if (Closed_)
            return false;

        if (auto prom = WaitersQueue_.TryGet()) {
            createSessionPromise = std::move(*prom);
        } else {
            return false;
        }
    }

    // This code can be called from client dtors. It may be unsafe to
    // execute grpc call directly...
    client->ScheduleTaskUnsafe([createSessionPromise, client]() mutable {
        TCreateSessionSettings settings;
        settings.ClientTimeout(CREATE_SESSION_INTERNAL_TIMEOUT);

        const auto& sessionResult = client->CreateSession(settings, false);
        sessionResult.Subscribe(TSession::TImpl::GetSessionInspector(createSessionPromise, client, settings, 0, true));
    }, TDuration());
    return true;
}

bool TSessionPool::ReturnSession(TSession::TImpl* impl, bool active, std::shared_ptr<TTableClient::TImpl> client) {
    // Do not set promise under the session pool lock
    NThreading::TPromise<TCreateSessionResult> createSessionPromise;
    {
        std::lock_guard guard(Mtx_);
        if (Closed_)
            return false;

        if (auto prom = WaitersQueue_.TryGet()) {
            createSessionPromise = std::move(*prom);
            if (!active)
                IncrementActiveCounterUnsafe();
        } else {
            Sessions_.emplace(std::make_pair(impl->GetTimeToTouchFast(), impl));

            if (active) {
                Y_VERIFY(ActiveSessions_);
                ActiveSessions_--;
                impl->SetNeedUpdateActiveCounter(false);
            }
        }
        UpdateStats();
    }

    if (createSessionPromise.Initialized()) {
        MakeSessionPromiseFromSession(impl, createSessionPromise, client);
    }

    return true;
}

void TSessionPool::DecrementActiveCounter() {
    std::lock_guard guard(Mtx_);
    Y_VERIFY(ActiveSessions_);
    ActiveSessions_--;
    UpdateStats();
}

void TSessionPool::IncrementActiveCounterUnsafe() {
    ActiveSessions_++;
    UpdateStats();
}

void TSessionPool::Drain(std::function<bool(std::unique_ptr<TSession::TImpl>&&)> cb, bool close) {
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

TPeriodicCb TSessionPool::CreatePeriodicTask(std::weak_ptr<TTableClient::TImpl> weakClient,
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
            TVector<std::unique_ptr<TSession::TImpl>> sessionsToTouch;
            sessionsToTouch.reserve(keepAliveBatchSize);
            TVector<std::unique_ptr<TSession::TImpl>> sessionsToDelete;
            sessionsToDelete.reserve(keepAliveBatchSize);
            TVector<NThreading::TPromise<TCreateSessionResult>> waitersToReplyError;
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

                        if (deletePredicate(it->second.get(), strongClient.get(), sessions.size())) {
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
                    Y_VERIFY(sessionImpl->GetState() == TSession::TImpl::S_IDLE);
                    TSession session(strongClient, std::shared_ptr<TSession::TImpl>(
                        sessionImpl.release(),
                        TSession::TImpl::GetSmartDeleter(strongClient)));
                    cmd(session);
                }
            }

            for (auto& sessionImpl : sessionsToDelete) {
                if (sessionImpl) {
                    Y_VERIFY(sessionImpl->GetState() == TSession::TImpl::S_IDLE);
                    TTableClient::TImpl::CloseAndDeleteSession(std::move(sessionImpl), strongClient);
                }
            }

            for (auto& waiter : waitersToReplyError) {
                FakeSessionsCounter_.Inc();
                CreateFakeSession(waiter, strongClient);
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
