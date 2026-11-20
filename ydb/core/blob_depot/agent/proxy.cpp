#include "agent_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT_AGENT

namespace NKikimr::NBlobDepot {

    void TBlobDepotAgent::SendToProxy(ui32 groupId, std::unique_ptr<IEventBase> event, TQuery *query,
            TRequestContext::TPtr context, NWilson::TTraceId traceId) {
        auto executionRelay = std::make_shared<TEvBlobStorage::TExecutionRelay>();
        auto& request = dynamic_cast<TEvBlobStorage::TEvRequestCommon&>(*event);
        request.ExecutionRelay = executionRelay;
        // A backing read or write belongs to this query. The tablet issues its own bridge requests for shared
        // decommission metadata lookups and reports those operations independently.
        request.IsDSProxySubrequest = query->ShouldSkipBridgeAccountingForSubrequests();

        const ui64 id = NextOtherRequestId++;
        YDB_LOG_DEBUG("SendToProxy",
            {"marker", "BDA46"},
            {"agentId", LogId},
            {"queryId", query->GetQueryId()},
            {"groupId", groupId},
            {"decommitGroupId", DecommitGroupId},
            {"type", event->Type()},
            {"cookie", id});
        if (groupId != DecommitGroupId) {
            SendToBSProxy(SelfId(), groupId, event.release(), id, std::move(traceId));
        } else if (ProxyId) {
            Send(ProxyId, event.release(), 0, id, std::move(traceId));
        } else {
            std::unique_ptr<IEventBase> response;
            switch (const ui32 type = event->Type()) {
                case TEvBlobStorage::EvGet: {
                    auto& get = static_cast<TEvBlobStorage::TEvGet&>(*event);
                    response = get.MakeErrorResponse(NKikimrProto::OK, "proxy has vanished", TGroupId::FromValue(groupId));
                    auto& r = static_cast<TEvBlobStorage::TEvGetResult&>(*response);
                    for (size_t i = 0; i < r.ResponseSz; ++i) {
                        r.Responses[i].Status = NKikimrProto::NODATA;
                        if (get.PhantomCheck) {
                            r.Responses[i].LooksLikePhantom.emplace(true);
                        }
                    }
                    r.ExecutionRelay = std::move(get.ExecutionRelay);
                    break;
                }

                case TEvBlobStorage::EvCheckIntegrity: {
                    auto& check = static_cast<TEvBlobStorage::TEvCheckIntegrity&>(*event);
                    auto result = check.MakeErrorResponse(NKikimrProto::NODATA, "proxy has vanished",
                        TGroupId::FromValue(groupId));
                    result->Id = check.Id;
                    result->ExecutionRelay = std::move(check.ExecutionRelay);
                    response = std::move(result); // re-resolve the key, just as Get does for a stale physical mapping
                    break;
                }

                default:
                    Y_ABORT("unexpected request type for decommission proxy Type# 0x%08" PRIx32, type);
            }
            Send(SelfId(), response.release(), 0, id);
        }

        RegisterRequest(id, query, std::move(context), {}, false, std::move(executionRelay));
    }

} // NKikimr::NBlobDepot
