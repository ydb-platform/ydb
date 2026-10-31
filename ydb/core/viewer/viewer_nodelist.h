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
    bool IncludeRootStaticPool = false;
    bool StoragePoolsFromCache = false;
    using TBase = TViewerPipeClient;
    using TThis = TJsonNodeList;

    enum class EStorageNodesResult {
        Ok,
        MissingPool,
        Error,
    };

    template<typename TResponse>
    void HandleStorageResponse(typename TResponse::TPtr& ev, TRequestResponse<TResponse>& response) {
        auto pipe = PipeInfo.find(GetBSControllerId());
        // Ignore late responses from a pipe whose pending requests have already failed.
        if (pipe != PipeInfo.end() && response.Set(std::move(ev))) {
            --pipe->second.Requests;
            RequestDone();
        }
    }

    void RequestStorageData() {
        if (!StoragePoolNames.empty()) {
            StoragePools = MakeCachedRequestBSControllerPools();
            // Cached responses are already complete; pipe requests finish asynchronously.
            StoragePoolsFromCache = StoragePools.IsOk();
        }
        Groups = MakeCachedRequestBSControllerGroups();
        VSlots = MakeCachedRequestBSControllerVSlots();
    }

    bool RequestStorageNodes() {
        if (IsStrictDatabaseOnlyRequest() || !Viewer->CheckAccessViewer(GetRequest())) {
            TBase::ReplyAndPassAway(GETHTTPACCESSDENIED("text/plain", "include_storage requires viewer access"));
            return false;
        }

        const auto& navigate = DatabaseNavigateResponse;
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
        const auto& domainKey = domain->Description.GetDomainKey();
        IncludeRootStaticPool = domainKey.GetSchemeShard() == AppData()->DomainsInfo->GetDomain()->SchemeRoot
            && domainKey.GetPathId() == 1;
        if (!StoragePoolNames.empty() || IncludeRootStaticPool) {
            RequestStorageData();
        }
        return true;
    }

    EStorageNodesResult CollectStorageNodes(std::unordered_set<TNodeId>& nodeIds) const {
        if (StoragePoolNames.empty() && !IncludeRootStaticPool) {
            return EStorageNodesResult::Ok;
        }
        if ((!StoragePoolNames.empty() && !StoragePools.IsOk()) || !Groups.IsOk() || !VSlots.IsOk()) {
            return EStorageNodesResult::Error;
        }
        auto missingPools = StoragePoolNames;
        std::set<std::pair<ui64, ui64>> poolIds;
        if (IncludeRootStaticPool) {
            poolIds.emplace(0, 0);
        }
        if (StoragePools.IsOk()) {
            for (const auto& pool : StoragePools->Record.GetEntries()) {
                if (StoragePoolNames.contains(pool.GetInfo().GetName())) {
                    poolIds.emplace(pool.GetKey().GetBoxId(), pool.GetKey().GetStoragePoolId());
                    missingPools.erase(pool.GetInfo().GetName());
                }
            }
        }
        if (!missingPools.empty()) {
            return EStorageNodesResult::MissingPool;
        }
        std::unordered_set<ui32> groupIds;
        for (const auto& group : Groups->Record.GetEntries()) {
            if (poolIds.contains({group.GetInfo().GetBoxId(), group.GetInfo().GetStoragePoolId()})) {
                groupIds.insert(group.GetKey().GetGroupId());
            }
        }
        for (const auto& slot : VSlots->Record.GetEntries()) {
            // Older generations can still host donor VDisks displayed by /storage/groups.
            if (groupIds.contains(slot.GetInfo().GetGroupId())) {
                nodeIds.insert(slot.GetKey().GetNodeId());
            }
        }
        return EStorageNodesResult::Ok;
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
        // Keep the ordinary endpoint list for serverless databases without exposing shared storage.
        if (ResourceNavigateResponse) {
            IncludeStorage = false;
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
        HandleStorageResponse(ev, StoragePools);
    }

    void Handle(NSysView::TEvSysView::TEvGetGroupsResponse::TPtr& ev) {
        HandleStorageResponse(ev, Groups);
    }

    void Handle(NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr& ev) {
        HandleStorageResponse(ev, VSlots);
    }

    void ReplyAndPassAway() override {
        std::unordered_set<TNodeId> storageNodeIds;
        if (IncludeStorage && IsDatabaseRequest()) {
            const auto result = NodesInfo.IsOk() ? CollectStorageNodes(storageNodeIds) : EStorageNodesResult::Error;
            if (result == EStorageNodesResult::MissingPool && StoragePoolsFromCache) {
                // Navigation may already know a pool missing from the cached controller snapshot.
                // Refresh all storage data once, keeping the original request deadline.
                UseCache = false;
                RequestStorageData();
                return;
            }
            if (result != EStorageNodesResult::Ok) {
                TBase::ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", "Failed to resolve database storage nodes"));
                return;
            }
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
                const bool storageNode = storageNodeIds.contains(it->NodeId);
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
                  description: Also include nodes hosting groups from the database's storage pools, including donor nodes present in controller data. Returns a single deduplicated array of endpoint and storage nodes without role labels. Requires viewer access when expanding a database's node list. The root domain also includes its static storage groups. Has no effect for serverless databases or without a database filter; serverless databases retain their ordinary endpoint list.
                  schema:
                    type: boolean
                    default: false
                - name: use_cache
                  in: query
                  description: Use cached storage topology, which can be up to 60 seconds old. Defaults to true when include_storage is enabled. A required pool missing from a cached pool response triggers one refresh without the cache. A cache miss or false causes full-cluster controller system-view reads before database filtering. Has no effect when storage-node expansion is not performed.
                  schema:
                    type: boolean
                    default: true
                responses:
                    403:
                        description: Insufficient permissions to include storage nodes
                    500:
                        description: Storage-node metadata could not be resolved because controller data is unavailable or a required pool is absent from its system-view snapshot. A missing pool can be transient after pool creation even without the Viewer cache; no partial node list is returned.
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
