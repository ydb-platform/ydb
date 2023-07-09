#pragma once
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/core/mon.h>
#include <library/cpp/actors/interconnect/interconnect.h>
#include <library/cpp/json/json_writer.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include "viewer.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;
using namespace NNodeWhiteboard;

class TCountersHostsList : public TActorBootstrapped<TCountersHostsList> {
    NMon::TEvHttpInfo::TPtr Event;
    THolder<TEvInterconnect::TEvNodesInfo> NodesInfo;
    TMap<TNodeId, THolder<TEvWhiteboard::TEvSystemStateResponse>> NodesResponses;
    ui32 NodesRequested = 0;
    ui32 NodesReceived = 0;
    bool StaticNodesOnly = false;
    bool DynamicNodesOnly = false;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TCountersHostsList(IViewer*, NMon::TEvHttpInfo::TPtr& ev)
        : Event(ev)
    {}

    void Bootstrap(const TActorContext& ctx) {
        const auto& params(Event->Get()->Request.GetParams());
        StaticNodesOnly = FromStringWithDefault<bool>(params.Get("static_only"), StaticNodesOnly);
        DynamicNodesOnly = FromStringWithDefault<bool>(params.Get("dynamic_only"), DynamicNodesOnly);
        const TActorId nameserviceId = GetNameserviceActorId();
        ctx.Send(nameserviceId, new TEvInterconnect::TEvListNodes());
        ctx.Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup());
        Become(&TThis::StateRequestedList);
    }

    STFUNC(StateRequestedList) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvInterconnect::TEvNodesInfo, Handle);
            CFunc(TEvents::TSystem::Wakeup, Timeout);
        }
    }

    STFUNC(StateRequestedSysInfo) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvWhiteboard::TEvSystemStateResponse, Handle);
            HFunc(TEvents::TEvUndelivered, Undelivered);
            HFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            CFunc(TEvents::TSystem::Wakeup, Timeout);
        }
    }

    void SendRequest(ui32 nodeId, const TActorContext& ctx) {
        TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(nodeId);
        THolder<TEvWhiteboard::TEvSystemStateRequest> request = MakeHolder<TEvWhiteboard::TEvSystemStateRequest>();
        ctx.Send(whiteboardServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
        ++NodesRequested;
    }

    void NodeStateInfoReceived(const TActorContext& ctx) {
        ++NodesReceived;
        if (NodesRequested == NodesReceived) {
            ReplyAndDie(ctx);
        }
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev, const TActorContext& ctx) {
        NodesInfo = ev->Release();
        ui32 minAllowedNodeId = std::numeric_limits<ui32>::min();
        ui32 maxAllowedNodeId = std::numeric_limits<ui32>::max();
        TIntrusivePtr<TDynamicNameserviceConfig> dynamicNameserviceConfig = AppData()->DynamicNameserviceConfig;
        if (dynamicNameserviceConfig) {
            if (StaticNodesOnly) {
                maxAllowedNodeId = dynamicNameserviceConfig->MaxStaticNodeId;
            }
            if (DynamicNodesOnly) {
                minAllowedNodeId = dynamicNameserviceConfig->MaxStaticNodeId + 1;
            }
        }
        for (const auto& nodeInfo : NodesInfo->Nodes) {
            if (nodeInfo.NodeId >= minAllowedNodeId && nodeInfo.NodeId <= maxAllowedNodeId) {
                SendRequest(nodeInfo.NodeId, ctx);
            }
        }
        Become(&TThis::StateRequestedSysInfo);
    }

    void Handle(TEvWhiteboard::TEvSystemStateResponse::TPtr& ev, const TActorContext& ctx) {
        ui64 nodeId = ev.Get()->Cookie;
        NodesResponses[nodeId] = ev->Release();
        NodeStateInfoReceived(ctx);
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr& ev, const TActorContext& ctx) {
        ui32 nodeId = ev.Get()->Cookie;
        if (NodesResponses.emplace(nodeId, nullptr).second) {
            NodeStateInfoReceived(ctx);
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev, const TActorContext& ctx) {
        ui32 nodeId = ev->Get()->NodeId;
        if (NodesResponses.emplace(nodeId, nullptr).second) {
            NodeStateInfoReceived(ctx);
        }
    }

    void ReplyAndDie(const TActorContext& ctx) {
        TStringStream text;
        for (const auto& [nodeId, sysInfo] : NodesResponses) {
            if (sysInfo) {
                const auto& record(sysInfo->Record);
                if (record.SystemStateInfoSize() > 0) {
                    const auto& state(record.GetSystemStateInfo(0));
                    TString host = state.GetHost();
                    if (host.empty()) {
                        const TEvInterconnect::TNodeInfo* nodeInfo = NodesInfo->GetNodeInfo(nodeId);
                        if (nodeInfo != nullptr) {
                            host = nodeInfo->Host;
                        }
                    }
                    if (!host.empty()) {
                        TString port;
                        for (const auto& endpoint : state.GetEndpoints()) {
                            if (endpoint.GetName() == "http-mon") {
                                port = endpoint.GetAddress();
                                break;
                            }
                        }
                        if (port.empty()) {
                            port = ":8765";
                        }
                        host += port;
                        text << host << Endl;
                    }
                }
            }
        }
        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(HTTPOKTEXT + text.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void Timeout(const TActorContext &ctx) {
        ReplyAndDie(ctx);
    }
};

}
}
