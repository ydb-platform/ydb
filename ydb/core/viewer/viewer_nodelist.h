#pragma once
#include "json_pipe_req.h"
#include "yaml/yaml.h"
#include <ydb/library/actors/interconnect/interconnect.h>
#include <set>

namespace NKikimr::NViewer {

using namespace NActors;

class TJsonNodeList : public TViewerPipeClient {
    TRequestResponse<TEvInterconnect::TEvNodesInfo> NodesInfo;
    TRequestResponse<NSysView::TEvSysView::TEvGetStoragePoolsResponse> StoragePools;
    TRequestResponse<NSysView::TEvSysView::TEvGetGroupsResponse> Groups;
    TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse> VSlots;
    std::unordered_set<TString> StoragePoolNames;
    bool IncludeStorage = false;
    using TBase = TViewerPipeClient;
    using TThis = TJsonNodeList;

    bool RequestStorageNodes() {
        if (IsStrictDatabaseOnlyRequest() || !Viewer->CheckAccessViewer(GetRequest())) {
            TBase::ReplyAndPassAway(GETHTTPACCESSDENIED("text/plain", "include_storage requires viewer access"));
            return false;
        }

        // Serverless databases use the storage pools of their shared database.
        const auto& navigate = ResourceNavigateResponse ? ResourceNavigateResponse : DatabaseNavigateResponse;
        if (!navigate || !navigate->IsOk() || navigate->Get()->Request->ResultSet.size() != 1) {
            TBase::ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", "Failed to resolve database storage pools"));
            return false;
        }
        const auto& domain = navigate->Get()->Request->ResultSet.front().DomainDescription;
        if (!domain) {
            TBase::ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", "Failed to resolve database storage pools"));
            return false;
        }
        for (const auto& pool : domain->Description.GetStoragePools()) {
            StoragePoolNames.insert(pool.GetName());
        }
        if (!StoragePoolNames.empty()) {
            StoragePools = MakeCachedRequestBSControllerPools();
            Groups = MakeCachedRequestBSControllerGroups();
            VSlots = MakeCachedRequestBSControllerVSlots();
        }
        return true;
    }

    bool CollectStorageNodes(std::unordered_set<TNodeId>& nodeIds) const {
        if (StoragePoolNames.empty()) {
            return true;
        }
        if (!StoragePools.IsOk() || !Groups.IsOk() || !VSlots.IsOk()) {
            return false;
        }
        auto missingPools = StoragePoolNames;
        std::set<std::pair<ui64, ui64>> poolIds;
        for (const auto& pool : StoragePools->Record.GetEntries()) {
            if (StoragePoolNames.contains(pool.GetInfo().GetName())) {
                poolIds.emplace(pool.GetKey().GetBoxId(), pool.GetKey().GetStoragePoolId());
                missingPools.erase(pool.GetInfo().GetName());
            }
        }
        if (!missingPools.empty()) {
            return false;
        }
        std::unordered_set<ui32> groupIds;
        for (const auto& group : Groups->Record.GetEntries()) {
            if (poolIds.contains({group.GetInfo().GetBoxId(), group.GetInfo().GetStoragePoolId()})) {
                groupIds.insert(group.GetKey().GetGroupId());
            }
        }
        for (const auto& slot : VSlots->Record.GetEntries()) {
            if (groupIds.contains(slot.GetInfo().GetGroupId())) {
                nodeIds.insert(slot.GetKey().GetNodeId());
            }
        }
        return true;
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonNodeList(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : TBase(viewer, ev, "/viewer/nodelist")
    {
        IncludeStorage = FromStringWithDefault<bool>(Params.Get("include_storage"), false);
        if (IncludeStorage) {
            UseCache = FromStringWithDefault<bool>(Params.Get("use_cache"), true);
        }
    }

    void Bootstrap() override {
        if (TBase::NeedToRedirect()) {
            return;
        }
        // An unscoped nameservice list already contains the cluster's storage nodes.
        if (IncludeStorage && IsDatabaseRequest() && !RequestStorageNodes()) {
            return;
        }
        NodesInfo = MakeRequest<TEvInterconnect::TEvNodesInfo>(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
        Become(&TThis::StateRequestedBrowse, TDuration::Seconds(10), new TEvents::TEvWakeup());
    }

    STATEFN(StateRequestedBrowse) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(NSysView::TEvSysView::TEvGetStoragePoolsResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetGroupsResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetVSlotsResponse, Handle);
            cFunc(TEvents::TSystem::Wakeup, TBase::HandleTimeout);
            default:
                return TBase::StateWork(ev);
        }
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        if (NodesInfo.Set(std::move(ev))) {
            RequestDone();
        }
    }

    void Handle(NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr& ev) {
        if (StoragePools.Set(std::move(ev))) {
            RequestDone();
        }
    }

    void Handle(NSysView::TEvSysView::TEvGetGroupsResponse::TPtr& ev) {
        if (Groups.Set(std::move(ev))) {
            RequestDone();
        }
    }

    void Handle(NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr& ev) {
        if (VSlots.Set(std::move(ev))) {
            RequestDone();
        }
    }

    void ReplyAndPassAway() override {
        std::unordered_set<TNodeId> storageNodeIds;
        if (IncludeStorage && IsDatabaseRequest() && (!NodesInfo.IsOk() || !CollectStorageNodes(storageNodeIds))) {
            TBase::ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", "Failed to resolve database storage nodes"));
            return;
        }
        NJson::TJsonValue json;
        json.SetType(NJson::EJsonValueType::JSON_ARRAY);
        std::optional<std::unordered_set<TNodeId>> databaseNodeIds;
        if (IsDatabaseRequest()) {
            auto nodes = GetDatabaseNodes();
            databaseNodeIds = std::unordered_set<TNodeId>(nodes.begin(), nodes.end());
        }
        if (NodesInfo.IsOk()) {
            for (auto it = NodesInfo->Nodes.begin(); it != NodesInfo->Nodes.end(); ++it) {
                const bool storageNode = it->IsStatic && storageNodeIds.contains(it->NodeId);
                if (databaseNodeIds && databaseNodeIds->count(it->NodeId) == 0 && !storageNode) {
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
                if (!nodeInfo.Location.GetItems().empty()) {
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
                }

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
        TBase::ReplyAndPassAway(GetHTTPOKJSON(json));
    }

    static YAML::Node GetSwagger() {
        YAML::Node node = YAML::Load(R"___(
            get:
                tags:
                - viewer
                summary: Nodes list
                description: Returns list of nodes
                parameters:
                - name: database
                  in: query
                  description: Database whose endpoint nodes should be returned. Omit to return all nameservice nodes.
                  schema:
                    type: string
                - name: include_storage
                  in: query
                  description: Also include static nodes hosting groups from the database's storage pools. Requires viewer access for database-scoped requests. Serverless databases use the shared database's pools. Has no effect without a database filter.
                  schema:
                    type: boolean
                    default: false
                responses:
                    403:
                        description: Insufficient permissions to include storage nodes
                    500:
                        description: Failed to resolve the requested storage node metadata
                    504:
                        description: Timed out while resolving node metadata
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
            "Node location from nameservice. Only fields known to this Viewer version are included. "
            "Unknown protobuf fields are not exposed. Omitted if no location is supplied. "
            "Normalization follows /viewer/nodes: empty DataCenter and Rack, and empty or \"0\" Unit, are omitted. "
            "Module \"0\" is preserved, including values converted from legacy RoomNum. "
            "The object can be empty after normalization.";
        node["get"]["responses"]["200"]["content"]["application/json"]["schema"]["items"]["properties"]["Location"] =
            locationSchema;
        return node;
    }
};

}
