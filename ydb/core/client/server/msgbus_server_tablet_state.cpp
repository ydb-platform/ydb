#include "msgbus_server.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/base/tablet_types.h>
#include <ydb/public/lib/base/msgbus.h>
#include <ydb/core/tablet/tablet_sys.h>

namespace NKikimr {
namespace NMsgBusProxy {

using namespace NNodeWhiteboard;

class TMessageBusTabletStateRequest : public TActorBootstrapped<TMessageBusTabletStateRequest>, public TMessageBusSessionIdentHolder {
protected:
    TAutoPtr<TEvInterconnect::TEvNodesInfo> NodesInfo;
    TMap<ui64, TAutoPtr<TEvWhiteboard::TEvTabletStateResponse>> PerNodeTabletInfo;
    size_t NodesRequested;
    size_t NodesReceived;
    const NKikimrClient::TTabletStateRequest Record;

public:
    void SendReplyAndDie(NBus::TBusMessage *reply, const TActorContext &ctx) {
        SendReplyMove(reply);
        Die(ctx);
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_FORWARDING_ACTOR;
    }

    TMessageBusTabletStateRequest(TBusMessageContext &msg)
        : TMessageBusSessionIdentHolder(msg)
        , NodesRequested(0)
        , NodesReceived(0)
        , Record(static_cast<TBusTabletStateRequest*>(msg.GetMessage())->Record)
    {}

    void Bootstrap(const TActorContext &ctx) {
        const TActorId nameserviceId = GetNameserviceActorId();
        ctx.Schedule(TDuration::Seconds(60), new TEvents::TEvWakeup());
        ctx.Send(nameserviceId, new TEvInterconnect::TEvListNodes());
        Become(&TThis::StateRequestedBrowse);
    }

    STFUNC(StateRequestedBrowse) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvInterconnect::TEvNodesInfo, Handle);
            CFunc(TEvents::TSystem::Wakeup, Timeout);
        }
    }

    STFUNC(StateRequestedTabletInfo) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvWhiteboard::TEvTabletStateResponse, Handle);
            HFunc(TEvents::TEvUndelivered, Undelivered);
            CFunc(TEvents::TSystem::Wakeup, Timeout);
        }
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr &ev, const TActorContext &ctx) {
        NodesInfo = ev->Release();
        if (!NodesInfo->Nodes.empty()) {
            for (const auto& ni : NodesInfo->Nodes) {
                TActorId tabletStateActorId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(ni.NodeId);
                ctx.Send(tabletStateActorId, new TEvWhiteboard::TEvTabletStateRequest(), IEventHandle::FlagTrackDelivery, ni.NodeId);
                ++NodesRequested;
            }
            Become(&TThis::StateRequestedTabletInfo);
        } else {
            NoData(ctx);
        }
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        ui64 nodeId = ev.Get()->Cookie;
        PerNodeTabletInfo[nodeId].Reset();
        NodeTabletInfoReceived(ctx);
    }

    void Handle(TEvWhiteboard::TEvTabletStateResponse::TPtr &ev, const TActorContext &ctx) {
        ui64 nodeId = ev.Get()->Cookie;
        PerNodeTabletInfo[nodeId] = ev->Release();
        NodeTabletInfoReceived(ctx);
    }

    void NodeTabletInfoReceived(const TActorContext &ctx) {
        ++NodesReceived;
        if (NodesReceived == NodesRequested) {
            RenderResponse(ctx);
        }
    }

    void RenderResponse(const TActorContext &ctx) {
        TAutoPtr<TBusResponse> response = new TBusResponse();
        auto& record = response->Record;
        record.SetStatus(MSTATUS_OK);
        for (const auto& ni : NodesInfo->Nodes) {
            const auto& pr_node = *PerNodeTabletInfo.find(ni.NodeId);
            if (pr_node.second.Get() != nullptr) {
                for (const NKikimrWhiteboard::TTabletStateInfo& st_info : pr_node.second.Get()->Record.GetTabletStateInfo()) {
                    if (Record.HasAlive()) {
                        if (Record.GetAlive() == false && st_info.GetState() != NKikimrWhiteboard::TTabletStateInfo::Dead)
                            continue;
                        if (Record.GetAlive() == true && st_info.GetState() == NKikimrWhiteboard::TTabletStateInfo::Dead)
                            continue;
                    }
                    if (Record.HasTabletType()) {
                        if (Record.GetTabletType() != st_info.GetType())
                            continue;
                    }
                    if (Record.TabletIDsSize() > 0) {
                        bool found = false;
                        for (size_t i = 0; i < Record.TabletIDsSize(); ++i) {
                            if (Record.GetTabletIDs(i) == st_info.GetTabletId()) {
                                found = true;
                                break;
                            }
                        }
                        if (!found)
                            continue;
                    }
                    auto state = record.AddTabletStateInfo();
                    state->CopyFrom(st_info);
                    state->SetHost(ni.Host);
                }
            }
        }
        SendReplyAndDie(response.Release(), ctx);
    }

    void NoData(const TActorContext &ctx) {
        SendReplyAndDie(new TBusResponseStatus(MSTATUS_ERROR, "No data"), ctx);
    }

    void Timeout(const TActorContext &ctx) {
        SendReplyAndDie(new TBusResponseStatus(MSTATUS_TIMEOUT, "Timeout"), ctx);
    }
};

IActor* CreateMessageBusTabletStateRequest(TBusMessageContext &msg) {
    return new TMessageBusTabletStateRequest(msg);
}


}
}
