#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepotAgent::SendToProxy(ui32 groupId, std::unique_ptr<IEventBase> event, TRequestSender *sender,
            TRequestContext::TPtr context) {
        auto executionRelay = std::make_shared<TEvBlobStorage::TExecutionRelay>();

        switch (event->Type()) {
            case TEvBlobStorage::EvPut:
                static_cast<TEvBlobStorage::TEvPut&>(*event).ExecutionRelay = executionRelay;
                break;

            case TEvBlobStorage::EvGet:
                static_cast<TEvBlobStorage::TEvGet&>(*event).ExecutionRelay = executionRelay;
                break;
        }

        const ui64 id = NextOtherRequestId++;
        auto getQueryId = [&] {
            auto *p = dynamic_cast<TQuery*>(sender);
            return p ? std::make_optional(p->GetQueryId()) : std::nullopt;
        };
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA46, "SendToProxy", (AgentId, LogId), (QueryId, getQueryId()),
            (GroupId, groupId), (DecommitGroupId, DecommitGroupId), (Type, event->Type()), (Cookie, id));
        if (groupId != DecommitGroupId) {
            SendToBSProxy(SelfId(), groupId, event.release(), id);
        } else if (ProxyId) {
            Send(ProxyId, event.release(), 0, id);
        } else {
            std::unique_ptr<IEventBase> response;
            switch (const ui32 type = event->Type()) {
                case TEvBlobStorage::EvGet: {
                    response = static_cast<TEvBlobStorage::TEvGet&>(*event).MakeErrorResponse(NKikimrProto::OK,
                        "proxy has vanished", groupId);
                    event.reset(); // drop origin event to prevent extra refcounts on ExecutionRelay
                    auto& r = static_cast<TEvBlobStorage::TEvGetResult&>(*response);
                    for (size_t i = 0; i < r.ResponseSz; ++i) {
                        r.Responses[i].Status = NKikimrProto::NODATA;
                    }
                    r.ExecutionRelay = executionRelay;
                    break;
                }

                default:
                    Y_FAIL("unexpected request type for decommission proxy Type# 0x%08" PRIx32, type);
            }
            Send(SelfId(), response.release(), 0, id);
        }

        RegisterRequest(id, sender, std::move(context), {}, false, std::move(executionRelay));
    }

} // NKikimr::NBlobDepot
