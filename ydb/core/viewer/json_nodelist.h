#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/json/json_writer.h>
#include <ydb/library/services/services.pb.h>
#include "viewer.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;

class TJsonNodeList : public TActorBootstrapped<TJsonNodeList> {
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    TAutoPtr<TEvInterconnect::TEvNodesInfo> NodesInfo;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonNodeList(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap(const TActorContext& ctx) {
        const TActorId nameserviceId = GetNameserviceActorId();
        ctx.Send(nameserviceId, new TEvInterconnect::TEvListNodes());
        ctx.Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup());
        Become(&TThis::StateRequestedBrowse);
    }

    STFUNC(StateRequestedBrowse) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvInterconnect::TEvNodesInfo, Handle);
            CFunc(TEvents::TSystem::Wakeup, Timeout);
        }
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr &ev, const TActorContext &ctx) {
        NodesInfo = ev->Release();
        ReplyAndDie(ctx);
    }

    void ReplyAndDie(const TActorContext &ctx) {
        NJson::TJsonValue json;
        json.SetType(NJson::EJsonValueType::JSON_ARRAY);
        if (NodesInfo != nullptr) {
            for (auto it = NodesInfo->Nodes.begin(); it != NodesInfo->Nodes.end(); ++it) {
                const TEvInterconnect::TNodeInfo& nodeInfo = *it;
                NJson::TJsonValue& jsonNodeInfo = json.AppendValue(NJson::TJsonValue());
                jsonNodeInfo["Id"] = nodeInfo.NodeId;
                if (!nodeInfo.Host.empty()) {
                    jsonNodeInfo["Host"] = nodeInfo.Host;
                }
                if (!nodeInfo.ResolveHost.empty()) {
                    jsonNodeInfo["ResolveHost"] = nodeInfo.ResolveHost;
                }
                jsonNodeInfo["Address"] = nodeInfo.Address;
                jsonNodeInfo["Port"] = nodeInfo.Port;
                if (nodeInfo.Location != TNodeLocation()) {
                    NJson::TJsonValue& jsonPhysicalLocation = jsonNodeInfo["PhysicalLocation"];
                    const auto& x = nodeInfo.Location.GetLegacyValue();
                    jsonPhysicalLocation["DataCenter"] = x.DataCenter;
                    jsonPhysicalLocation["Room"] = x.Room;
                    jsonPhysicalLocation["Rack"] = x.Rack;
                    jsonPhysicalLocation["Body"] = x.Body;
                    jsonPhysicalLocation["DataCenterId"] = nodeInfo.Location.GetDataCenterId();
                    jsonPhysicalLocation["Location"] = nodeInfo.Location.ToString();
                }
            }
        }
        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), NJson::WriteJson(json, false)), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void Timeout(const TActorContext &ctx) {
        ReplyAndDie(ctx);
    }
};

template <>
struct TJsonRequestSchema<TJsonNodeList> {
    static YAML::Node GetSchema() {
        return YAML::Load(R"___(
            type: array
            title: TEvNodeListResponse
            items:
                type: object
                title: TNodeInfo
                properties:
                    Id:
                        type: integer
                    Host:
                        type: string
                    Address:
                        type: string
                    Port:
                        type: integer
            )___");
    }
};

template <>
struct TJsonRequestSummary<TJsonNodeList> {
    static TString GetSummary() {
        return "Nodes list";
    }
};

template <>
struct TJsonRequestDescription<TJsonNodeList> {
    static TString GetDescription() {
        return "Returns list of nodes";
    }
};

}
}
