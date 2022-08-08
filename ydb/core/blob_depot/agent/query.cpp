#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepotAgent::HandleStorageProxy(TAutoPtr<IEventHandle> ev) {
        if (TabletId == Max<ui64>()) {
            // TODO: memory usage control
            PendingEventQ.emplace_back(ev.Release());
        } else {
            auto *query = CreateQuery(ev);
            STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA13, "new query", (VirtualGroupId, VirtualGroupId),
                (QueryId, query->GetQueryId()), (Name, query->GetName()));
            if (!TabletId) {
                query->EndWithError(NKikimrProto::ERROR, "group is in error state");
            } else {
                query->Initiate();
            }
        }
    }

    void TBlobDepotAgent::Handle(TEvBlobStorage::TEvBunchOfEvents::TPtr ev) {
        ev->Get()->Process(this);
    }

    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery(TAutoPtr<IEventHandle> ev) {
        switch (ev->GetTypeRewrite()) {
#define XX(TYPE) \
            case TEvBlobStorage::TYPE: return CreateQuery<TEvBlobStorage::TYPE>(std::unique_ptr<IEventHandle>(ev.Release()));

            ENUMERATE_INCOMING_EVENTS(XX)
#undef XX
        }
        Y_FAIL();
    }

    void TBlobDepotAgent::TQuery::EndWithError(NKikimrProto::EReplyStatus status, const TString& errorReason) {
        STLOG(PRI_INFO, BLOB_DEPOT_AGENT, BDA14, "query ends with error", (VirtualGroupId, Agent.VirtualGroupId),
            (QueryId, QueryId), (Status, status), (ErrorReason, errorReason));

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
        delete this;
    }

    void TBlobDepotAgent::TQuery::EndWithSuccess(std::unique_ptr<IEventBase> response) {
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA15, "query ends with success", (VirtualGroupId, Agent.VirtualGroupId),
            (QueryId, QueryId), (Response, response->ToString()));
        Agent.SelfId().Send(Event->Sender, response.release(), 0, Event->Cookie);
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
