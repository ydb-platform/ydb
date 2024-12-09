#include <ydb/core/base/hive.h>
#include "msgbus_tabletreq.h"
#include "msgbus_securereq.h"

namespace NKikimr {
namespace NMsgBusProxy {

class TMessageBusDrainNode : public TMessageBusSecureRequest<TMessageBusTabletRequest<TMessageBusDrainNode, TEvHive::TEvDrainNodeResult>> {
    THolder<TBusDrainNode> Request;
    ui32 NodeId;
public:
    static ui64 GetHiveTabletId(const TActorContext& ctx) {
        return AppData(ctx)->DomainsInfo->GetHive();
    }

    TMessageBusDrainNode(TBusMessageContext& msg)
        : TMessageBusSecureRequest(msg, true, TDuration::Minutes(30), false)
        , Request(static_cast<TBusDrainNode*>(msg.ReleaseMessage()))
        , NodeId(Request->Record.GetNodeID())
    {
        SetSecurityToken(Request->Record.GetSecurityToken());
        SetRequireAdminAccess(true);
        SetPeerName(msg.GetPeerName());
    }

    std::pair<ui64, TAutoPtr<IEventBase>> MakeReqPair(const TActorContext& ctx) {
        ui64 TabletId = GetHiveTabletId(ctx);
        return std::make_pair(TabletId, new TEvHive::TEvDrainNode(NodeId));
    }

    void Handle(TEvHive::TEvDrainNodeResult::TPtr& ev, const TActorContext& ctx) {
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

IActor* CreateMessageBusDrainNode(TBusMessageContext& msg) {
    return new TMessageBusDrainNode(msg);
}

}
}
