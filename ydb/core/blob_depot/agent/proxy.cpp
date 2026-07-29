#include "agent_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT_AGENT

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

            case TEvBlobStorage::EvCheckIntegrity:
                static_cast<TEvBlobStorage::TEvCheckIntegrity&>(*event).ExecutionRelay = executionRelay;
                break;
        }

        const ui64 id = NextOtherRequestId++;
        auto getQueryId = [&] {
            auto *p = dynamic_cast<TQuery*>(sender);
            return p ? std::make_optional(p->GetQueryId()) : std::nullopt;
        };
        YDB_LOG_DEBUG("SendToProxy",
            {"marker", "BDA46"},
            {"agentId", LogId},
            {"queryId", getQueryId()},
            {"groupId", groupId},
            {"decommitGroupId", DecommitGroupId},
            {"type", event->Type()},
            {"cookie", id});
        if (groupId != DecommitGroupId) {
            SendToBSProxy(SelfId(), groupId, event.release(), id);
        } else if (ProxyId) {
            Send(ProxyId, event.release(), 0, id);
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

                default:
                    Y_ABORT("unexpected request type for decommission proxy Type# 0x%08" PRIx32, type);
            }
            Send(SelfId(), response.release(), 0, id);
        }

        RegisterRequest(id, sender, std::move(context), {}, false, std::move(executionRelay));
    }

} // NKikimr::NBlobDepot
