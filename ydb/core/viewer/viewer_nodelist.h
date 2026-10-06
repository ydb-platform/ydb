#pragma once
#include "json_pipe_req.h"
#include "yaml/yaml.h"
#include <ydb/library/actors/interconnect/interconnect.h>

namespace NKikimr::NViewer {

using namespace NActors;

class TJsonNodeList : public TViewerPipeClient {
    TRequestResponse<TEvInterconnect::TEvNodesInfo> NodesInfo;
    using TBase = TViewerPipeClient;
    using TThis = TJsonNodeList;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonNodeList(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : TBase(viewer, ev, "/viewer/nodelist")
    {}

    void Bootstrap() override {
        if (TBase::NeedToRedirect()) {
            return;
        }
        NodesInfo = MakeRequest<TEvInterconnect::TEvNodesInfo>(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
        Become(&TThis::StateRequestedBrowse, TDuration::Seconds(10), new TEvents::TEvWakeup());
    }

    STATEFN(StateRequestedBrowse) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            cFunc(TEvents::TSystem::Wakeup, TBase::HandleTimeout);
        }
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        if (NodesInfo.Set(std::move(ev))) {
            RequestDone();
        }
    }

    void ReplyAndPassAway() override {
        NJson::TJsonValue json;
        json.SetType(NJson::EJsonValueType::JSON_ARRAY);
        std::optional<std::unordered_set<TNodeId>> databaseNodeIds;
        if (IsDatabaseRequest()) {
            auto nodes = GetDatabaseNodes();
            databaseNodeIds = std::unordered_set<TNodeId>(nodes.begin(), nodes.end());
        }
        if (NodesInfo.IsOk()) {
            for (auto it = NodesInfo->Nodes.begin(); it != NodesInfo->Nodes.end(); ++it) {
                if (databaseNodeIds && databaseNodeIds->count(it->NodeId) == 0) {
                    continue; // skip nodes that are not in the database
                }
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
                    NActorsInterconnect::TNodeLocation location;
                    nodeInfo.Location.Serialize(&location, false);
                    if (location.GetDataCenter().empty()) {
                        location.ClearDataCenter();
                    }
                    if (location.GetRack().empty()) {
                        location.ClearRack();
                    }
                    if (location.GetUnit().empty() || location.GetUnit() == "0") {
                        location.ClearUnit();
                    }
                    Proto2Json(location, jsonNodeInfo["Location"]);

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
        TBase::ReplyAndPassAway(GetHTTPOKJSON(json));
    }

    static YAML::Node GetSwagger() {
        YAML::Node node = YAML::Load(R"___(
            get:
                tags:
                - viewer
                summary: Nodes list
                description: Returns list of nodes
                responses:
                    200:
                        description: Successful response
                        content:
                            application/json:
                                schema:
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
        auto locationSchema = TProtoToYaml::ProtoToYamlSchema<NActorsInterconnect::TNodeLocation>();
        for (const auto* field : {"DataCenterNum", "RoomNum", "RackNum", "BodyNum", "Body"}) {
            locationSchema["properties"].remove(field);
        }
        locationSchema["description"] =
            "Node location from nameservice. Omitted if no location is supplied. "
            "Empty DataCenter and Rack, and empty or \"0\" Unit, are omitted. "
            "The object can be empty after normalization.";
        node["get"]["responses"]["200"]["content"]["application/json"]["schema"]["items"]["properties"]["Location"] =
            locationSchema;
        return node;
    }
};

}
