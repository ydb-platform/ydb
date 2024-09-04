#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<0>(std::unique_ptr<IEventHandle> ev) {
        switch (ev->GetTypeRewrite()) {
#define XX(TYPE) case TEvBlobStorage::TYPE: return CreateQuery<TEvBlobStorage::TYPE>(std::move(ev));
            ENUMERATE_INCOMING_EVENTS(XX)
#undef XX
        }
        Y_ABORT();
    }

    void TBlobDepotAgent::HandleStorageProxy(TAutoPtr<IEventHandle> ev) {
        bool doForward = false;

        switch (ev->GetTypeRewrite()) {
            case TEvBlobStorage::EvGet:
                doForward = ev->Get<TEvBlobStorage::TEvGet>()->Decommission
                    || ev->Get<TEvBlobStorage::TEvGet>()->PhantomCheck;
                Y_ABORT_UNLESS(!doForward || !ev->Get<TEvBlobStorage::TEvGet>()->MustRestoreFirst);
                break;

            case TEvBlobStorage::EvRange:
                doForward = ev->Get<TEvBlobStorage::TEvRange>()->Decommission;
                Y_ABORT_UNLESS(!doForward || !ev->Get<TEvBlobStorage::TEvRange>()->MustRestoreFirst);
                break;
        }

        if (doForward) {
            if (ProxyId) {
                TActivationContext::Forward(ev, ProxyId);
            } else {
                CreateQuery<0>(std::unique_ptr<IEventHandle>(ev.Release()))->EndWithError(NKikimrProto::ERROR, "proxy has vanished");
            }
            return;
        }

        std::unique_ptr<IEventHandle> p(ev.Release());

        size_t size = 0;

        if (!IsConnected) { // check for queue overflow
            switch (p->GetTypeRewrite()) {
#define XX(TYPE) case TEvBlobStorage::TYPE: size = p->Get<TEvBlobStorage::T##TYPE>()->CalculateSize(); break;
                ENUMERATE_INCOMING_EVENTS(XX)
#undef XX
            }

            if (size + PendingEventBytes > MaxPendingEventBytes) {
                CreateQuery<0>(std::move(p))->EndWithError(NKikimrProto::ERROR, "pending event queue overflow");
                return;
            }
        }

        if (!IsConnected || !PendingEventQ.empty()) {
            PendingEventBytes += size;
            PendingEventQ.push_back(TPendingEvent{std::move(p), size, TMonotonic::Now() + EventExpirationTime});
        } else {
            ProcessStorageEvent(std::move(p));
        }
    }

    void TBlobDepotAgent::HandleAssimilate(TAutoPtr<IEventHandle> ev) {
        TActivationContext::Forward(ev, ProxyId);
    }

    void TBlobDepotAgent::HandlePendingEvent() {
        for (THPTimer timer; !PendingEventQ.empty(); ) {
            TPendingEvent& item = PendingEventQ.front();
            ProcessStorageEvent(std::move(item.Event));
            Y_ABORT_UNLESS(PendingEventBytes >= item.Size);
            PendingEventBytes -= item.Size;
            PendingEventQ.pop_front();
            if (!PendingEventQ.empty() && TDuration::Seconds(timer.Passed()) >= TDuration::MilliSeconds(1)) {
                if (!ProcessPendingEventInFlight) {
                    TActivationContext::Send(new IEventHandle(TEvPrivate::EvProcessPendingEvent, 0, SelfId(), {}, nullptr, 0));
                    ProcessPendingEventInFlight = true;
                }
                break;
            }
        }
    }

    void TBlobDepotAgent::HandleProcessPendingEvent() {
        Y_ABORT_UNLESS(ProcessPendingEventInFlight);
        ProcessPendingEventInFlight = false;
        HandlePendingEvent();
    }

    void TBlobDepotAgent::ClearPendingEventQueue(const TString& reason) {
        for (auto& item : std::exchange(PendingEventQ, {})) {
            Y_ABORT_UNLESS(PendingEventBytes >= item.Size);
            PendingEventBytes -= item.Size;
            CreateQuery<0>(std::move(item.Event))->EndWithError(NKikimrProto::ERROR, reason);
        }
    }

    void TBlobDepotAgent::ProcessStorageEvent(std::unique_ptr<IEventHandle> ev) {
        TQuery *query = CreateQuery<0>(std::move(ev));
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA13, "new query", (AgentId, LogId),
            (QueryId, query->GetQueryId()), (Name, query->GetName()));
        if (!TabletId) {
            query->EndWithError(NKikimrProto::ERROR, "group is in error state");
        } else {
            query->Initiate();
        }
    }

    void TBlobDepotAgent::HandlePendingEventQueueWatchdog() {
        if (!IsConnected) {
            const TMonotonic now = TActivationContext::Monotonic();
            std::deque<TPendingEvent>::iterator it;
            for (it = PendingEventQ.begin(); it != PendingEventQ.end() && it->ExpirationTimestamp <= now; ++it) {
                CreateQuery<0>(std::move(it->Event))->EndWithError(NKikimrProto::ERROR, "pending event queue timeout");
                PendingEventBytes -= it->Size;
            }
            PendingEventQ.erase(PendingEventQ.begin(), it);
        }

        TActivationContext::Schedule(TDuration::Seconds(1), new IEventHandle(TEvPrivate::EvPendingEventQueueWatchdog, 0,
            SelfId(), {}, nullptr, 0));
    }

    void TBlobDepotAgent::Handle(TEvBlobStorage::TEvBunchOfEvents::TPtr ev) {
        ev->Get()->Process(this);
    }

    void TBlobDepotAgent::HandleQueryWatchdog() {
        auto now = TActivationContext::Monotonic();
        for (auto it = QueryWatchdogMap.begin(); it != QueryWatchdogMap.end(); ) {
            const auto& [timestamp, query] = *it++;
            if (timestamp <= now) {
                query->CheckQueryExecutionTime(now);
            } else {
                break;
            }
        }
        TActivationContext::Schedule(TDuration::Seconds(1), new IEventHandle(TEvPrivate::EvQueryWatchdog, 0, SelfId(),
            {}, nullptr, 0));
    }

    TBlobDepotAgent::TQuery::TQuery(TBlobDepotAgent& agent, std::unique_ptr<IEventHandle> event)
        : TRequestSender(agent)
        , Event(std::move(event))
        , QueryId(RandomNumber<ui64>())
        , StartTime(TActivationContext::Monotonic())
        , QueryWatchdogMapIter(agent.QueryWatchdogMap.emplace(StartTime + WatchdogDuration, this))
    {
        agent.ExecutingQueries.PushBack(this);
    }

    TBlobDepotAgent::TQuery::~TQuery() {
        Agent.QueryWatchdogMap.erase(QueryWatchdogMapIter);
    }

    void TBlobDepotAgent::TQuery::CheckQueryExecutionTime(TMonotonic now) {
        const auto prio = std::exchange(WatchdogPriority, NLog::PRI_NOTICE);
        STLOG(prio, BLOB_DEPOT_AGENT, BDA23, "query is still executing", (AgentId, Agent.LogId),
            (QueryId, GetQueryId()), (Duration, now - StartTime));
        auto nh = Agent.QueryWatchdogMap.extract(QueryWatchdogMapIter);
        nh.key() = now + WatchdogDuration;
        QueryWatchdogMapIter = Agent.QueryWatchdogMap.insert(std::move(nh));
        for (const auto& cookie : SubrequestRelays) {
            Y_VERIFY_S(!cookie.expired(), "AgentId# " << Agent.LogId << " QueryId# " << GetQueryId()
                << " subrequest got stuck");
        }
    }

    void TBlobDepotAgent::TQuery::EndWithError(NKikimrProto::EReplyStatus status, const TString& errorReason) {
        STLOG(PRI_INFO, BLOB_DEPOT_AGENT, BDA14, "query ends with error", (AgentId, Agent.LogId),
            (QueryId, GetQueryId()), (Status, status), (ErrorReason, errorReason),
            (Duration, TActivationContext::Monotonic() - StartTime));

        std::unique_ptr<IEventBase> response;
        switch (Event->GetTypeRewrite()) {
#define XX(TYPE) \
            case TEvBlobStorage::TYPE: \
                response = Event->Get<TEvBlobStorage::T##TYPE>()->MakeErrorResponse(status, errorReason, TGroupId::FromValue(Agent.VirtualGroupId)); \
                static_cast<TEvBlobStorage::T##TYPE##Result&>(*response).ExecutionRelay = std::move(ExecutionRelay); \
                break; \
            //

            ENUMERATE_INCOMING_EVENTS(XX)
#undef XX
        }
        Y_ABORT_UNLESS(response);
        Agent.SelfId().Send(Event->Sender, response.release(), 0, Event->Cookie);
        OnDestroy(false);
        DoDestroy();
    }

    void TBlobDepotAgent::TQuery::EndWithSuccess(std::unique_ptr<IEventBase> response) {
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA15, "query ends with success", (AgentId, Agent.LogId),
            (QueryId, GetQueryId()), (Response, response->ToString()), (Duration, TActivationContext::Monotonic() - StartTime));
        switch (response->Type()) {
#define XX(TYPE) \
            case TEvBlobStorage::TYPE##Result: \
                static_cast<TEvBlobStorage::T##TYPE##Result&>(*response).ExecutionRelay = std::move(ExecutionRelay); \
                break; \
            //

            ENUMERATE_INCOMING_EVENTS(XX)
#undef XX
        }
        Agent.SelfId().Send(Event->Sender, response.release(), 0, Event->Cookie);
        OnDestroy(true);
        DoDestroy();
    }

    void TBlobDepotAgent::TQuery::DoDestroy() {
        Y_ABORT_UNLESS(!Destroyed);
        Destroyed = true;
        TIntrusiveListItem<TQuery, TExecutingQueries>::Unlink();
        TIntrusiveListItem<TQuery, TPendingBlockChecks>::Unlink();
        TIntrusiveListItem<TQuery, TPendingId>::Unlink();
        Agent.DeletePendingQueries.PushBack(this);
        TRequestSender::ClearRequestsInFlight();

        if (TDuration duration(TActivationContext::Monotonic() - StartTime); duration >= WatchdogDuration) {
            STLOG(WatchdogPriority, BLOB_DEPOT_AGENT, BDA00, "query execution took too much time",
                (AgentId, Agent.LogId), (QueryId, GetQueryId()), (Duration, duration));
        }
    }

    TString TBlobDepotAgent::TQuery::GetQueryId() const {
        if (!QueryIdString) {
            TStringStream s;
            s << Hex(QueryId);
            if (const ui64 tabletId = GetTabletId()) {
                s << '@' << tabletId;
            }
            QueryIdString = std::move(s.Str());
        }
        return QueryIdString;
    }

    TString TBlobDepotAgent::TQuery::GetName() const {
        switch (Event->GetTypeRewrite()) {
#define XX(TYPE) case TEvBlobStorage::TYPE: return #TYPE;
            ENUMERATE_INCOMING_EVENTS(XX)
#undef XX
        }
        Y_ABORT();
    }

} // NKikimr::NBlobDepot
