#include <ydb/core/base/hive.h>
#include "msgbus_tabletreq.h"
#include "msgbus_securereq.h"

namespace NKikimr {
namespace NMsgBusProxy {

class TMessageBusFillNode : public TMessageBusSecureRequest<TMessageBusTabletRequest<TMessageBusFillNode, TEvHive::TEvFillNodeResult>> {
    THolder<TBusFillNode> Request;
    ui32 NodeId;
public:
    static ui64 GetHiveTabletId(const TActorContext& ctx) {
        return AppData(ctx)->DomainsInfo->GetHive();
    }

    TMessageBusFillNode(TBusMessageContext& msg)
        : TMessageBusSecureRequest(msg, true, TDuration::Minutes(30), false)
        , Request(static_cast<TBusFillNode*>(msg.ReleaseMessage()))
        , NodeId(Request->Record.GetNodeID())
    {
        SetSecurityToken(Request->Record.GetSecurityToken());
        SetRequireAdminAccess(true);
        SetPeerName(msg.GetPeerName());
    }

    std::pair<ui64, TAutoPtr<IEventBase>> MakeReqPair(const TActorContext& ctx) {
        ui64 TabletId = GetHiveTabletId(ctx);
        return std::make_pair(TabletId, new TEvHive::TEvFillNode(NodeId));
    }

    void Handle(TEvHive::TEvFillNodeResult::TPtr& ev, const TActorContext& ctx) {
        NMsgBusProxy::EResponseStatus status;
        switch (ev->Get()->Record.GetStatus()) {
        case NKikimrProto::OK:
            status = MSTATUS_OK;
            break;
        case NKikimrProto::ERROR:
            status = MSTATUS_ERROR;
            break;
        case NKikimrProto::TIMEOUT:
            status = MSTATUS_TIMEOUT;
            break;
        default:
            status = MSTATUS_INTERNALERROR;
            break;
        }
        return SendReplyAndDie(new TBusResponseStatus(status), ctx);
    }
};

IActor* CreateMessageBusFillNode(TBusMessageContext& msg) {
    return new TMessageBusFillNode(msg);
}

}
}
