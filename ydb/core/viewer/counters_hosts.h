#pragma once
#include "viewer.h"
#include <library/cpp/json/json_writer.h>
#include <ydb/core/base/nameservice.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/interconnect/interconnect.h>

namespace NKikimr::NViewer {

using namespace NActors;
using namespace NNodeWhiteboard;

class TCountersHostsList : public TActorBootstrapped<TCountersHostsList> {
    using TBase = TActorBootstrapped<TCountersHostsList>;

    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    THolder<TEvInterconnect::TEvNodesInfo> NodesInfo;
    TMap<TNodeId, THolder<TEvWhiteboard::TEvSystemStateResponse>> NodesResponses;
    THashSet<TActorId> TcpProxies;
    ui32 NodesRequested = 0;
    ui32 NodesReceived = 0;
    bool StaticNodesOnly = false;
    bool DynamicNodesOnly = false;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TCountersHostsList(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap() {
        const auto& params(Event->Get()->Request.GetParams());
        StaticNodesOnly = FromStringWithDefault<bool>(params.Get("static_only"), StaticNodesOnly);
        DynamicNodesOnly = FromStringWithDefault<bool>(params.Get("dynamic_only"), DynamicNodesOnly);
        const TActorId nameserviceId = GetNameserviceActorId();
        Send(nameserviceId, new TEvInterconnect::TEvListNodes());
        Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup());
        Become(&TThis::StateRequestedList);
    }

    STFUNC(StateRequestedList) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            cFunc(TEvents::TSystem::Wakeup, Timeout);
        }
    }

    STFUNC(StateRequestedSysInfo) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWhiteboard::TEvSystemStateResponse, Handle);
            hFunc(TEvents::TEvUndelivered, Undelivered);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            hFunc(TEvInterconnect::TEvNodeConnected, Connected);
            cFunc(TEvents::TSystem::Wakeup, Timeout);
        }
    }

    void SendRequest(ui32 nodeId) {
        TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(nodeId);
        THolder<TEvWhiteboard::TEvSystemStateRequest> request = MakeHolder<TEvWhiteboard::TEvSystemStateRequest>();
        Send(whiteboardServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
        NodesRequested++;
    }

    void NodeStateInfoReceived() {
        ++NodesReceived;
        if (NodesRequested == NodesReceived) {
            ReplyAndDie();
        }
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
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
                SendRequest(nodeInfo.NodeId);
            }
        }
        Become(&TThis::StateRequestedSysInfo);
    }

    void Handle(TEvWhiteboard::TEvSystemStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        NodesResponses[nodeId] = ev->Release();
        NodeStateInfoReceived();
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr& ev) {
        ui32 nodeId = ev.Get()->Cookie;
        if (NodesResponses.emplace(nodeId, nullptr).second) {
            NodeStateInfoReceived();
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        ui32 nodeId = ev->Get()->NodeId;
        TcpProxies.erase(ev->Sender);
        if (NodesResponses.emplace(nodeId, nullptr).second) {
            NodeStateInfoReceived();
        }
    }

    void Connected(TEvInterconnect::TEvNodeConnected::TPtr& ev) {
        TcpProxies.insert(ev->Sender);
    }

    void ReplyAndDie() {
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
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKTEXT(Event->Get()) + text.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    void PassAway() {
        for (auto &tcpPorxy: TcpProxies) {
            Send(tcpPorxy, new TEvents::TEvUnsubscribe);
        }
        TBase::PassAway();
    }

    void Timeout() {
        ReplyAndDie();
    }
};

}
