#include "read_session_info.h"


namespace NKikimr {
namespace NMsgBusProxy {

void TPersQueueGetReadSessionsInfoWorkerWithPQv0::SendStatusRequest(const TString& sessionName, TActorId actorId, const TActorContext& ctx) {
    if (sessionName.EndsWith("_v1")) {
        SendStatusRequest<NGRpcProxy::V1::TEvPQProxy::TEvReadSessionStatus>(actorId, ctx);
    } else {
        SendStatusRequest<NGRpcProxy::TEvPQProxy::TEvReadSessionStatus>(actorId, ctx);
    }
}

} // namespace NMsgBusProxy
} // namespace NKikimr
