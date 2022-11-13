#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<0>(std::unique_ptr<IEventHandle> ev) {
        switch (ev->GetTypeRewrite()) {
#define XX(TYPE) case TEvBlobStorage::TYPE: return CreateQuery<TEvBlobStorage::TYPE>(std::move(ev));
            ENUMERATE_INCOMING_EVENTS(XX)
#undef XX
        }
        Y_FAIL();
    }

    void TBlobDepotAgent::HandleStorageProxy(TAutoPtr<IEventHandle> ev) {
        std::unique_ptr<IEventHandle> p(ev.Release());

        if (!IsConnected || !PendingEventQ.empty()) {
            size_t size = Max<size_t>();
            switch (p->GetTypeRewrite()) {
#define XX(TYPE) case TEvBlobStorage::TYPE: size = p->Get<TEvBlobStorage::T##TYPE>()->CalculateSize(); break;
                ENUMERATE_INCOMING_EVENTS(XX)
#undef XX
            }
            Y_VERIFY(size != Max<size_t>());

            if (size + PendingEventBytes > MaxPendingEventBytes) {
                CreateQuery<0>(std::move(p))->EndWithError(NKikimrProto::ERROR, "pending event queue overflow");
            } else {
                PendingEventBytes += size;
                PendingEventQ.push_back(TPendingEvent{std::move(p), size, TMonotonic::Now() + EventExpirationTime});
            }
        } else {
            ProcessStorageEvent(std::move(p));
        }
    }

    void TBlobDepotAgent::HandlePendingEvent() {
        for (THPTimer timer; !PendingEventQ.empty(); ) {
            TPendingEvent& item = PendingEventQ.front();
            ProcessStorageEvent(std::move(item.Event));
            Y_VERIFY(PendingEventBytes >= item.Size);
            PendingEventBytes -= item.Size;
            PendingEventQ.pop_front();
            Y_VERIFY(!PendingEventQ.empty() || !PendingEventBytes);

            if (TDuration::Seconds(timer.Passed()) >= TDuration::MicroSeconds(100)) {
                TActivationContext::Send(new IEventHandle(TEvPrivate::EvProcessPendingEvent, 0, SelfId(), {}, nullptr, 0));
                break;
            }
        }
    }

    void TBlobDepotAgent::ProcessStorageEvent(std::unique_ptr<IEventHandle> ev) {
        TQuery *query = CreateQuery<0>(std::move(ev));
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA13, "new query", (VirtualGroupId, VirtualGroupId),
            (QueryId, query->GetQueryId()), (Name, query->GetName()));
        if (!TabletId) {
            query->EndWithError(NKikimrProto::ERROR, "group is in error state");
        } else {
            query->Initiate();
        }
    }

    void TBlobDepotAgent::HandlePendingEventQueueWatchdog() {
        const TMonotonic now = TActivationContext::Monotonic();
        std::deque<TPendingEvent>::iterator it;
        for (it = PendingEventQ.begin(); it != PendingEventQ.end() && it->ExpirationTimestamp <= now; ++it) {
            CreateQuery<0>(std::move(it->Event))->EndWithError(NKikimrProto::ERROR, "pending event queue timeout");
            PendingEventBytes -= it->Size;
        }
        PendingEventQ.erase(PendingEventQ.begin(), it);

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
    {}

    TBlobDepotAgent::TQuery::~TQuery() {
        if (TDuration duration(TActivationContext::Monotonic() - StartTime); duration >= WatchdogDuration) {
            STLOG(WatchdogPriority, BLOB_DEPOT_AGENT, BDA00, "query execution took too much time",
                (VirtualGroupId, Agent.VirtualGroupId), (QueryId, GetQueryId()), (Duration, duration));
        }
        Agent.QueryWatchdogMap.erase(QueryWatchdogMapIter);
    }

    void TBlobDepotAgent::TQuery::CheckQueryExecutionTime(TMonotonic now) {
        const auto prio = std::exchange(WatchdogPriority, NLog::PRI_NOTICE);
        STLOG(prio, BLOB_DEPOT_AGENT, BDA23, "query is still executing", (VirtualGroupId, Agent.VirtualGroupId),
            (QueryId, GetQueryId()), (Duration, now - StartTime));
        auto nh = Agent.QueryWatchdogMap.extract(QueryWatchdogMapIter);
        nh.key() = now + WatchdogDuration;
        QueryWatchdogMapIter = Agent.QueryWatchdogMap.insert(std::move(nh));
    }

    void TBlobDepotAgent::TQuery::EndWithError(NKikimrProto::EReplyStatus status, const TString& errorReason) {
        STLOG(PRI_INFO, BLOB_DEPOT_AGENT, BDA14, "query ends with error", (VirtualGroupId, Agent.VirtualGroupId),
            (QueryId, GetQueryId()), (Status, status), (ErrorReason, errorReason),
            (Duration, TActivationContext::Monotonic() - StartTime));

        std::unique_ptr<IEventBase> response;
        switch (Event->GetTypeRewrite()) {
#define XX(TYPE) \
            case TEvBlobStorage::TYPE: \
                response = Event->Get<TEvBlobStorage::T##TYPE>()->MakeErrorResponse(status, errorReason, Agent.VirtualGroupId); \
                break;

            ENUMERATE_INCOMING_EVENTS(XX)
#undef XX
        }
        Y_VERIFY(response);
        Agent.SelfId().Send(Event->Sender, response.release(), 0, Event->Cookie);
        OnDestroy(false);
        delete this;
    }

    void TBlobDepotAgent::TQuery::EndWithSuccess(std::unique_ptr<IEventBase> response) {
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA15, "query ends with success", (VirtualGroupId, Agent.VirtualGroupId),
            (QueryId, GetQueryId()), (Response, response->ToString()), (Duration, TActivationContext::Monotonic() - StartTime));
        Agent.SelfId().Send(Event->Sender, response.release(), 0, Event->Cookie);
        OnDestroy(true);
        delete this;
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
        Y_FAIL();
    }

} // NKikimr::NBlobDepot
