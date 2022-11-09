#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepotAgent::HandleStorageProxy(TAutoPtr<IEventHandle> ev) {
        if (TabletId == Max<ui64>() || !PendingEventQ.empty()) {
            // TODO: memory usage control
            PendingEventQ.emplace_back(ev.Release());
        } else {
            ProcessStorageEvent(std::unique_ptr<IEventHandle>(ev.Release()));
        }
    }

    void TBlobDepotAgent::HandlePendingEvent() {
        THPTimer timer;

        do {
            if (!PendingEventQ.empty()) {
                ProcessStorageEvent(std::move(PendingEventQ.front()));
                PendingEventQ.pop_front();
            } else {
                break;
            }
        } while (TDuration::Seconds(timer.Passed()) <= TDuration::MicroSeconds(100));

        if (!PendingEventQ.empty()) {
            TActivationContext::Send(new IEventHandle(TEvPrivate::EvProcessPendingEvent, 0, SelfId(), {}, nullptr, 0));
        }
    }

    void TBlobDepotAgent::ProcessStorageEvent(std::unique_ptr<IEventHandle> ev) {
        TQuery *query = nullptr;

        switch (ev->GetTypeRewrite()) {
#define XX(TYPE) \
            case TEvBlobStorage::TYPE: query = CreateQuery<TEvBlobStorage::TYPE>(std::move(ev)); break;

            ENUMERATE_INCOMING_EVENTS(XX)
#undef XX
        }

        Y_VERIFY(query);

        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA13, "new query", (VirtualGroupId, VirtualGroupId),
            (QueryId, query->GetQueryId()), (TabletId, query->GetTabletId()), (Name, query->GetName()));
        if (!TabletId) {
            query->EndWithError(NKikimrProto::ERROR, "group is in error state");
        } else {
            query->Initiate();
        }
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
                (VirtualGroupId, Agent.VirtualGroupId), (QueryId, QueryId), (Duration, duration));
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
            (QueryId, QueryId), (Status, status), (ErrorReason, errorReason),
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
            (QueryId, QueryId), (Response, response->ToString()), (Duration, TActivationContext::Monotonic() - StartTime));
        Agent.SelfId().Send(Event->Sender, response.release(), 0, Event->Cookie);
        OnDestroy(true);
        delete this;
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
