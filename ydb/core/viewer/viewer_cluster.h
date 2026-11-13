#pragma once
#include "json_handlers.h"
#include "json_pipe_req.h"
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_config.h>
#include "viewer.h"
#include "viewer_helper.h"
#include "viewer_tabletinfo.h"
#include <ydb/core/util/proto_duration.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/public/api/protos/ydb_bridge_common.pb.h>
#include <library/cpp/protobuf/json/json2proto.h>

namespace NKikimr::NViewer {

using namespace NActors;
using namespace NNodeWhiteboard;

class TJsonCluster : public TViewerPipeClient {
    using TThis = TJsonCluster;
    using TBase = TViewerPipeClient;
    using TPDiskId = std::pair<TNodeId, ui32>;
    std::optional<TRequestResponse<TEvInterconnect::TEvNodesInfo>> NodesInfoResponse;
    std::optional<TRequestResponse<TEvNodeWardenStorageConfig>> NodeWardenStorageConfigResponse;
    std::optional<TRequestResponse<TEvWhiteboard::TEvNodeStateResponse>> NodeStateResponse;
    std::optional<TRequestResponse<NConsole::TEvConsole::TEvListTenantsResponse>> ListTenantsResponse;
    std::optional<TRequestResponse<NSysView::TEvSysView::TEvGetPDisksResponse>> PDisksResponse;
    std::optional<TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse>> VSlotsResponse;
    std::optional<TRequestResponse<NSysView::TEvSysView::TEvGetStorageStatsResponse>> StorageStatsResponse;
    std::optional<TRequestResponse<NSysView::TEvSysView::TEvGetGroupsResponse>> GroupsResponse;
    // bridge mode only, for the names of the pools in the sync state of piles
    std::optional<TRequestResponse<NSysView::TEvSysView::TEvGetStoragePoolsResponse>> StoragePoolsResponse;
    std::optional<TRequestResponse<TEvHive::TEvResponseHiveNodeStats>> HiveNodeStatsResponse;

    int WhiteboardStateRequestsInFlight = 0;
    bool WhiteboardRequestsSent = false;
    bool NodePileMapKnown = false;
    bool LocalPileNodeStats = false;
    bool PileRequestsStarted = false;
    TInstant Deadline;
    TActorId HttpProxy;
    struct TPileRequest {
        std::vector<TString> Endpoints;
        size_t NextEndpoint = 0;
        NHttp::THttpOutgoingRequestPtr ActiveRequest;
        std::optional<NKikimrViewer::TBridgePileInfo> Result;
    };
    std::unordered_map<ui32, TPileRequest> PileRequests;
    std::unordered_map<TNodeId, TRequestResponse<TEvWhiteboard::TEvSystemStateResponse>> SystemStateResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvWhiteboard::TEvTabletStateResponse>> TabletStateResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvViewer::TEvViewerResponse>> SystemViewerResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvViewer::TEvViewerResponse>> TabletViewerResponse;

    std::optional<TRequestResponse<NHealthCheck::TEvSelfCheckResult>> SelfCheckResult; // from the local health check
    std::optional<TRequestResponse<NHealthCheck::TEvSelfCheckResultProto>> SelfCheckResultProto; // from the metadata cache service
    TViewerPipeClient::TRequestResponse<TEvStateStorage::TEvBoardInfo> MetadataCacheEndpointsLookup;

    struct TNode {
        TEvInterconnect::TNodeInfo NodeInfo;
        NKikimrWhiteboard::TSystemStateInfo SystemState;
        TNodeId NodeId;
        TString DataCenter;
        TSubDomainKey SubDomainKey;
        std::optional<ui32> PileNum;
        bool Static = false;
        bool Connected = false;
        bool Disconnected = false;

        bool IsAlive() const {
            return !Disconnected && SystemState.HasSystemState();
        }

        int GetCandidateScore() const {
            int score = 0;
            if (Connected) {
                score += 100;
            }
            if (Static) {
                score += 10;
            }
            return score;
        }
    };

    struct TNodeBatch {
        std::vector<TNode*> NodesToAskFor;
        std::vector<TNode*> NodesToAskAbout;
        size_t Offset = 0;
        bool HasStaticNodes = false;

        TNodeId ChooseNodeId() {
            if (Offset >= NodesToAskFor.size()) {
                return 0;
            }
            return NodesToAskFor[Offset++]->NodeId;
        }
    };

    using TNodeData = std::vector<TNode>;
    using TNodeCache = std::unordered_map<TNodeId, TNode*>;

    TNodeData NodeData;
    TNodeCache NodeCache;
    std::unordered_map<TNodeId, TNodeBatch> NodeBatches;
    std::vector<TString> Problems;

    void AddProblem(const TString& problem) {
        for (const auto& p : Problems) {
            if (p == problem) {
                return;
            }
        }
        Problems.push_back(problem);
    }

    NKikimrViewer::TClusterInfo ClusterInfo;

    std::unordered_set<TTabletId> FilterTablets;
    bool OffloadMerge = true;
    size_t OffloadMergeAttempts = 2;
    bool UseHealthCheck = true;
    bool UseHealthCheckCache = false; // doesn't work for domain ?
    bool HealthCheckRequested = false;
    TString DomainName;
    TTabletId RootHiveId = 0;
    bool Tablets = false;

public:
    TJsonCluster(IViewer* viewer, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {
    }

    void Bootstrap() override {
        LocalPileNodeStats = FromStringWithDefault<bool>(Params.Get("local_pile_node_stats"), false);
        Deadline = TActivationContext::Now() + Timeout;
        // The reduced response consists of the serving pile node statistics only.
        Tablets = !LocalPileNodeStats && FromStringWithDefault<bool>(Params.Get("tablets"), false);
        OffloadMerge = FromStringWithDefault<bool>(Params.Get("offload_merge"), OffloadMerge);
        OffloadMergeAttempts = FromStringWithDefault<bool>(Params.Get("offload_merge_attempts"), OffloadMergeAttempts);
        UseHealthCheck = FromStringWithDefault<bool>(Params.Get("use_health_check"), UseHealthCheck);
        UseHealthCheckCache = FromStringWithDefault<bool>(Params.Get("use_health_check_cache"), UseHealthCheckCache);
        if (!UseHealthCheck) {
            UseHealthCheckCache = false;
        }
        NodesInfoResponse = MakeRequest<TEvInterconnect::TEvNodesInfo>(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
        if (AppData()->BridgeModeEnabled) {
            NodeWardenStorageConfigResponse = MakeRequest<TEvNodeWardenStorageConfig>(MakeBlobStorageNodeWardenID(SelfId().NodeId()),
                new TEvNodeWardenQueryStorageConfig(/*subscribe=*/ false));
        }
        NodeStateResponse = MakeWhiteboardRequest(TActivationContext::ActorSystem()->NodeId, new TEvWhiteboard::TEvNodeStateRequest());
        if (LocalPileNodeStats) {
            // This request is served inside the target pile. It neither calls other
            // piles over HTTPS nor waits for cluster tablets outside this pile.
            OffloadMerge = false;
            Become(&TThis::StateWork, Timeout, new TEvents::TEvWakeup());
            return;
        }
        if (AppData()->BridgeModeEnabled) {
            StoragePoolsResponse = MakeCachedRequestBSControllerPools();
        }
        PDisksResponse = MakeCachedRequestBSControllerPDisks();
        VSlotsResponse = MakeCachedRequestBSControllerVSlots();
        GroupsResponse = MakeCachedRequestBSControllerGroups();
        StorageStatsResponse = MakeCachedRequestBSControllerStorageStats();
        ListTenantsResponse = MakeRequestConsoleListTenants();
        if (AppData()->DomainsInfo && AppData()->DomainsInfo->Domain) {
            TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
            DomainName = TStringBuilder() << "/" << AppData()->DomainsInfo->Domain->Name;
            ClusterInfo.SetDomain(DomainName);
            if (const auto& domain = domains->Domain) {
                for (TTabletId id : domain->Coordinators) {
                    FilterTablets.insert(id);
                }
                for (TTabletId id : domain->Mediators) {
                    FilterTablets.insert(id);
                }
                for (TTabletId id : domain->TxAllocators) {
                    FilterTablets.insert(id);
                }
                FilterTablets.insert(domain->SchemeRoot);
                RootHiveId = domains->GetHive();
                FilterTablets.insert(RootHiveId);
                HiveNodeStatsResponse = MakeRequestHiveNodeStats(RootHiveId, new TEvHive::TEvRequestHiveNodeStats());
            }
            FilterTablets.insert(MakeBSControllerID());
            FilterTablets.insert(MakeDefaultHiveID());
            FilterTablets.insert(MakeCmsID());
            FilterTablets.insert(MakeNodeBrokerID());
            FilterTablets.insert(MakeTenantSlotBrokerID());
            FilterTablets.insert(MakeConsoleID());

            if (UseHealthCheck) {
                HealthCheckRequested = true;
                if (UseHealthCheckCache && AppData()->FeatureFlags.GetEnableDbMetadataCache()) {
                    MetadataCacheEndpointsLookup = MakeRequestStateStorageMetadataCacheEndpointsLookup(DomainName);
                } else {
                    SendHealthCheckRequest();
                }
            }
        }
        Become(&TThis::StateWork, Timeout, new TEvents::TEvWakeup());
    }

private:
    static constexpr size_t BATCH_SIZE = 200;

    void BuildCandidates(TNodeBatch& batch, std::vector<TNode*>& candidates) {
        auto itCandidate = candidates.begin();
        for (; itCandidate != candidates.end() && batch.NodesToAskFor.size() < OffloadMergeAttempts; ++itCandidate) {
            batch.NodesToAskFor.push_back(*itCandidate);
        }
        candidates.erase(candidates.begin(), itCandidate);
        for (TNode* node : batch.NodesToAskAbout) {
            if (node->Static) {
                batch.HasStaticNodes = true;
            }
        }
    }

    void SplitBatch(TNodeBatch& nodeBatch, std::vector<TNodeBatch>& batches) {
        std::vector<TNode*> candidates = nodeBatch.NodesToAskAbout;
        std::sort(candidates.begin(), candidates.end(), [](TNode* a, TNode* b) {
            return a->GetCandidateScore() > b->GetCandidateScore();
        });
        while (nodeBatch.NodesToAskAbout.size() > BATCH_SIZE) {
            TNodeBatch newBatch;
            size_t splitSize = std::min(BATCH_SIZE, nodeBatch.NodesToAskAbout.size() / 2);
            newBatch.NodesToAskAbout.reserve(splitSize);
            for (size_t i = 0; i < splitSize; ++i) {
                newBatch.NodesToAskAbout.push_back(nodeBatch.NodesToAskAbout.back());
                nodeBatch.NodesToAskAbout.pop_back();
            }
            BuildCandidates(newBatch, candidates);
            batches.emplace_back(std::move(newBatch));
        }
        if (!nodeBatch.NodesToAskAbout.empty()) {
            BuildCandidates(nodeBatch, candidates);
            batches.emplace_back(std::move(nodeBatch));
        }
    }

    const TBridgeInfo* GetBridgeInfo() const {
        return NodeWardenStorageConfigResponse && NodeWardenStorageConfigResponse->IsOk()
            ? NodeWardenStorageConfigResponse->Get()->BridgeInfo.get()
            : nullptr;
    }

    static bool CanQueryPile(const TBridgeInfo& bridgeInfo, const TBridgeInfo::TPile& pile) {
        if (!bridgeInfo.SelfNodePile) {
            return false;
        }
        if (pile.BridgePileId == bridgeInfo.SelfNodePile->BridgePileId) {
            return NBridge::PileStateTraits(pile.State).AllowsConnectionWithinPile;
        }
        return NBridge::PileStateTraits(bridgeInfo.SelfNodePile->State).AllowsConnection
            && NBridge::PileStateTraits(pile.State).AllowsConnection;
    }

    bool CanQueryNode(const TNode& node) const {
        const auto* bridgeInfo = GetBridgeInfo();
        if (LocalPileNodeStats && (!bridgeInfo || !bridgeInfo->SelfNodePile || !node.PileNum
                || *node.PileNum != bridgeInfo->SelfNodePile->BridgePileId.GetPileIndex())) {
            return false;
        }
        return !bridgeInfo || !node.PileNum
            || (*node.PileNum < bridgeInfo->Piles.size() && CanQueryPile(*bridgeInfo, bridgeInfo->Piles[*node.PileNum]));
    }

    std::vector<TNodeBatch> BatchNodes() {
        std::vector<TNodeBatch> batches;
        if (OffloadMerge) {
            std::unordered_map<TSubDomainKey, TNodeBatch> batchSubDomain;
            std::unordered_map<TString, TNodeBatch> batchDataCenters;
            for (TNode& node : NodeData) {
                if (!CanQueryNode(node)) {
                    continue;
                }
                if (node.Static) {
                    batchDataCenters[node.DataCenter].NodesToAskAbout.push_back(&node);
                } else {
                    batchSubDomain[node.SubDomainKey].NodesToAskAbout.push_back(&node);
                }
            }
            for (auto& [subDomainKey, nodeBatch] : batchSubDomain) {
                if (nodeBatch.NodesToAskAbout.size() == 1) {
                    TNode* node = nodeBatch.NodesToAskAbout.front();
                    batchDataCenters[node->DataCenter].NodesToAskAbout.push_back(node);
                } else {
                    SplitBatch(nodeBatch, batches);
                }
            }
            for (auto& [dataCenter, nodeBatch] : batchDataCenters) {
                SplitBatch(nodeBatch, batches);
            }
        } else {
            TNodeBatch nodeBatch;
            for (TNode& node : NodeData) {
                if (!CanQueryNode(node)) {
                    continue;
                }
                nodeBatch.NodesToAskAbout.push_back(&node);
            }
            SplitBatch(nodeBatch, batches);
        }
        return batches;
    }

    bool TimeToAskWhiteboard() {
        if (NodesInfoResponse && !NodesInfoResponse->IsDone()) {
            return false;
        }

        if (NodeStateResponse && !NodeStateResponse->IsDone()) {
            return false;
        }

        if (NodeWardenStorageConfigResponse && !NodeWardenStorageConfigResponse->IsDone()) {
            return false;
        }

        if (const auto* bridgeInfo = GetBridgeInfo(); bridgeInfo && bridgeInfo->SelfNodePile
                && !NBridge::PileStateTraits(bridgeInfo->SelfNodePile->State).AllowsConnection) {
            // Local whiteboard collection must not wait for tablets in other piles.
            return true;
        }

        if (ListTenantsResponse && !ListTenantsResponse->IsDone()) {
            return false;
        }

        if (PDisksResponse && !PDisksResponse->IsDone()) {
            return false;
        }

        if (VSlotsResponse && !VSlotsResponse->IsDone()) {
            return false;
        }

        if (GroupsResponse && !GroupsResponse->IsDone()) {
            return false;
        }

        if (StoragePoolsResponse && !StoragePoolsResponse->IsDone()) {
            return false;
        }

        if (StorageStatsResponse && !StorageStatsResponse->IsDone()) {
            return false;
        }

        if (HiveNodeStatsResponse && !HiveNodeStatsResponse->IsDone()) {
            return false;
        }

        return true;
    }

    TString GetStoragePoolName(const std::tuple<ui64, ui64>& key) const {
        const auto& [boxId, storagePoolId] = key;
        if (StoragePoolsResponse && StoragePoolsResponse->IsOk()) {
            for (const auto& entry : StoragePoolsResponse->Get()->Record.GetEntries()) {
                if (entry.GetKey().GetBoxId() == boxId && entry.GetKey().GetStoragePoolId() == storagePoolId) {
                    return entry.GetInfo().GetName();
                }
            }
        }
        if (!boxId && !storagePoolId) {
            return "static"; // static groups are not in the pool system view
        }
        return TStringBuilder() << boxId << ':' << storagePoolId;
    }

    static Ydb::Bridge::PileState::State GetPileStateFromPile(const TBridgeInfo::TPile& pile) {
        if (pile.IsPrimary) {
            return Ydb::Bridge::PileState::PRIMARY;
        } else if (pile.IsBeingPromoted) {
            return Ydb::Bridge::PileState::PROMOTED;
        } else {
            switch (pile.State) {
                case NKikimrBridge::TClusterState::DISCONNECTED:
                    return Ydb::Bridge::PileState::DISCONNECTED;
                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1:
                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2:
                    return Ydb::Bridge::PileState::NOT_SYNCHRONIZED;
                case NKikimrBridge::TClusterState::SYNCHRONIZED:
                    return Ydb::Bridge::PileState::SYNCHRONIZED;
                case NKikimrBridge::TClusterState::INACTIVE:
                    return Ydb::Bridge::PileState::INACTIVE;
                default:
                    return Ydb::Bridge::PileState::UNSPECIFIED;
            }
        }
    }

    void SendPileRequest(TPileRequest& request) {
        const auto now = TActivationContext::Now();
        if (request.NextEndpoint == request.Endpoints.size() || now >= Deadline) {
            AddProblem("no-pile-node-info");
            return;
        }
        const auto timeout = (Deadline - now) / (request.Endpoints.size() - request.NextEndpoint);
        if (timeout.MilliSeconds() < 2) {
            AddProblem("no-pile-node-info");
            return;
        }
        request.ActiveRequest = NHttp::THttpOutgoingRequest::CreateRequestGet(TStringBuilder()
            << request.Endpoints[request.NextEndpoint++] << "/viewer/json/cluster?local_pile_node_stats=true"
            << "&timeout=" << timeout.MilliSeconds() * 3 / 4);
        auto authorization = GetRequest().GetHeader("Authorization");
        if (authorization.empty()) {
            const auto cookie = GetRequest().GetHeader("Cookie");
            const NHttp::TCookies cookies(cookie);
            if (const auto session = cookies["ydb_session_id"]; !session.empty()) {
                authorization = TStringBuilder() << "Login " << session;
            }
        }
        if (!authorization.empty()) {
            request.ActiveRequest->Set("Authorization", authorization);
        }
        request.ActiveRequest->Finish();
        if (!HttpProxy) {
            HttpProxy = Register(NHttp::CreateHttpProxy());
        }
        SendRequest(HttpProxy, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(request.ActiveRequest, timeout));
    }

    // Endpoints of other piles are built from the monitoring port of this process, which is
    // common for static nodes only. A dynamic node forwards the request to a static node of its own pile.
    bool ForwardToStaticNode(const TBridgeInfo& bridgeInfo) {
        if (!CanQueryPile(bridgeInfo, *bridgeInfo.SelfNodePile)) {
            return false;
        }
        std::vector<ui32> nodes;
        for (const auto& node : NodeData) {
            if (node.Static && node.Connected && node.PileNum == bridgeInfo.SelfNodePile->BridgePileId.GetPileIndex()) {
                nodes.push_back(node.NodeId);
            }
        }
        if (nodes.empty()) {
            return false;
        }
        std::sort(nodes.begin(), nodes.end());
        const ui32 nodeId = nodes[std::hash<TString>()(GetRequest().GetRemoteAddr()) % nodes.size()];
        const TString location = TStringBuilder() << "/node/" << nodeId << GetRequest().GetUri();
        // Allow this redirect after a /node/ proxy hop. StartPileRequests only redirects
        // non-static nodes, so the static target will not redirect again.
        TBase::ReplyAndPassAway(Viewer->GetHTTPFORWARD(GetRequest(), location, {}));
        return true;
    }

    // Returns false when the request has been forwarded to another node.
    bool StartPileRequests() {
        if (PileRequestsStarted || LocalPileNodeStats || !NodePileMapKnown || NodeStateResponse) {
            return true;
        }
        const auto* bridgeInfo = GetBridgeInfo();
        if (!bridgeInfo || !bridgeInfo->SelfNodePile) {
            return true;
        }
        PileRequestsStarted = true;
        std::vector<const TBridgeInfo::TPile*> piles;
        for (const auto& pile : bridgeInfo->Piles) {
            if (pile.IsFreeSlot || &pile == bridgeInfo->SelfNodePile || CanQueryPile(*bridgeInfo, pile)
                    || !NBridge::PileStateTraits(pile.State).AllowsConnectionWithinPile) {
                continue;
            }
            piles.push_back(&pile);
        }
        if (piles.empty()) {
            return true;
        }
        const auto itSelf = NodeCache.find(SelfId().NodeId());
        if (itSelf == NodeCache.end() || !itSelf->second) {
            AddProblem("no-pile-node-info");
            return true;
        }
        if (!itSelf->second->Static) {
            if (ForwardToStaticNode(*bridgeInfo)) {
                return false;
            }
            AddProblem("no-pile-node-info");
            return true;
        }
        const auto& config = Viewer->GetKikimrRunConfig().AppConfig.GetMonitoringConfig();
        std::vector<const TNode*> staticNodes;
        for (const auto& node : NodeData) {
            if (node.Static) {
                staticNodes.push_back(&node);
            }
        }
        // Make the choice of the probed nodes reproducible.
        std::sort(staticNodes.begin(), staticNodes.end(), [](const TNode* a, const TNode* b) {
            return a->NodeId < b->NodeId;
        });
        for (const auto* pile : piles) {
            auto& request = PileRequests[pile->BridgePileId.GetPileIndex()];
            if (config.GetMonitoringPort()) {
                for (const auto* node : staticNodes) {
                    if (node->PileNum != pile->BridgePileId.GetPileIndex()) {
                        continue;
                    }
                    TString host = node->NodeInfo.Host ? node->NodeInfo.Host : node->NodeInfo.ResolveHost;
                    if (host.empty()) {
                        continue;
                    }
                    if (host.Contains(':') && !host.StartsWith('[')) {
                        host = TStringBuilder() << '[' << host << ']';
                    }
                    TString endpoint = TStringBuilder() << "https://" << host << ':' << config.GetMonitoringPort();
                    if (std::find(request.Endpoints.begin(), request.Endpoints.end(), endpoint) == request.Endpoints.end()) {
                        request.Endpoints.push_back(std::move(endpoint));
                        // Bound the number of attempts within the common HTTP deadline.
                        if (request.Endpoints.size() == 3) {
                            break;
                        }
                    }
                }
            }
            SendPileRequest(request);
        }
        return true;
    }

    bool ReadPileResponse(ui32 pileIndex, const NHttp::TEvHttpProxy::TEvHttpIncomingResponse& response,
            NKikimrViewer::TBridgePileInfo& result) {
        if (response.Error || !response.Response || response.Response->Status != "200") {
            return false;
        }
        NJson::TJsonValue json;
        if (!NJson::ReadJsonTree(response.Response->Body, &json)) {
            return false;
        }
        const auto& piles = json["BridgeInfo"]["Piles"];
        // A server ignoring local_pile_node_stats (e.g. an older version) must not
        // contribute counters for a different or unreachable pile.
        if (!piles.IsArray() || piles.GetArray().size() != 1) {
            return false;
        }
        try {
            NProtobufJson::Json2Proto(piles[0], result, NProtobufJson::TJson2ProtoConfig()
                .SetAllowUnknownFields(true)
                .SetMapAsObject(true));
        } catch (const yexception&) {
            return false;
        }
        const auto& pile = GetBridgeInfo()->Piles[pileIndex];
        return result.HasPileId() && TBridgePileId::FromProto(&result, &NKikimrViewer::TBridgePileInfo::GetPileId) == pile.BridgePileId
            && result.GetName() == pile.Name
            && result.HasNodes() && result.HasStorageNodes() && result.HasComputeNodes()
            && result.HasAliveStorageNodes() && result.HasAliveComputeNodes()
            && ui64(result.GetStorageNodes()) + result.GetComputeNodes() == result.GetNodes()
            && result.GetAliveStorageNodes() <= result.GetStorageNodes()
            && result.GetAliveComputeNodes() <= result.GetComputeNodes();
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& ev) {
        for (auto& [pileIndex, request] : PileRequests) {
            if (request.ActiveRequest && request.ActiveRequest == ev->Get()->Request) {
                request.ActiveRequest.Reset();
                NKikimrViewer::TBridgePileInfo result;
                if (ReadPileResponse(pileIndex, *ev->Get(), result)) {
                    request.Result = std::move(result);
                } else {
                    SendPileRequest(request);
                }
                RequestDone();
                return;
            }
        }
    }

    void PassAway() override {
        if (HttpProxy) {
            Send(HttpProxy, new TEvents::TEvPoisonPill());
            HttpProxy = {};
        }
        TBase::PassAway();
    }

    void ProcessResponses() {
        if (NodesInfoResponse && NodesInfoResponse->IsDone()) {
            if (NodesInfoResponse->IsOk()) {
                std::unordered_set<TString> hosts;
                for (const auto& ni : NodesInfoResponse->Get()->Nodes) {
                    TNode& node = NodeData.emplace_back();
                    node.NodeInfo = ni;
                    node.NodeId = ni.NodeId;
                    node.Static = ni.IsStatic;
                    node.DataCenter = ni.Location.GetDataCenterId();
                    hosts.insert(ni.Host);
                }
                for (TNode& node : NodeData) {
                    NodeCache.emplace(node.NodeInfo.NodeId, &node);
                }
                if (NodesInfoResponse->Get()->PileMap) {
                    for (ui32 pileNum = 0; pileNum < NodesInfoResponse->Get()->PileMap->size(); ++pileNum) {
                        for (ui32 nodeId : (*NodesInfoResponse->Get()->PileMap)[pileNum]) {
                            auto itNode = NodeCache.find(nodeId);
                            if (itNode == NodeCache.end()) {
                                continue;
                            }
                            itNode->second->PileNum = pileNum;
                        }
                    }
                    // Nodes missing from the pile map are not attributed to any pile.
                    NodePileMapKnown = std::any_of(NodeData.begin(), NodeData.end(), [](const TNode& node) {
                        return node.PileNum.has_value();
                    });
                }
                ClusterInfo.SetNodesTotal(NodesInfoResponse->Get()->Nodes.size());
                ClusterInfo.SetHosts(hosts.size());
            } else {
                AddProblem("no-nodes-info");
            }
            NodesInfoResponse.reset();
        }

        if (NodeData.empty()) {
            return;
        }

        if (NodeStateResponse && NodeStateResponse->IsDone()) {
            if (NodeStateResponse->IsOk()) {
                for (const auto& nodeStateInfo : NodeStateResponse->Get()->Record.GetNodeStateInfo()) {
                    if (nodeStateInfo.GetConnected()) {
                        TNodeId nodeId = nodeStateInfo.GetPeerNodeId()
                            ? nodeStateInfo.GetPeerNodeId()
                            : FromStringWithDefault(TStringBuf(nodeStateInfo.GetPeerName()).Before(':'), 0);
                        if (nodeId) {
                            TNode* node = NodeCache[nodeId];
                            if (node) {
                                node->Connected = true;
                            }
                        }
                    }
                }
            } else {
                AddProblem("no-node-state-info");
            }
            NodeStateResponse.reset();
        }

        if (!StartPileRequests()) {
            return;
        }

        if (HiveNodeStatsResponse && HiveNodeStatsResponse->IsDone()) {
            if (HiveNodeStatsResponse->IsOk()) {
                for (const auto& nodeStats : HiveNodeStatsResponse->Get()->Record.GetNodeStats()) {
                    TNodeId nodeId = nodeStats.GetNodeId();
                    TNode* node = NodeCache[nodeId];
                    if (node) {
                        node->SubDomainKey = TSubDomainKey(nodeStats.GetNodeDomain());
                    }
                }
            } else {
                AddProblem("no-hive-node-stats");
            }
            HiveNodeStatsResponse.reset();
        }

        if (ListTenantsResponse && ListTenantsResponse->IsDone()) {
            if (ListTenantsResponse->IsOk()) {
                Ydb::Cms::ListDatabasesResult listTenantsResult;
                ListTenantsResponse->Get()->Record.GetResponse().operation().result().UnpackTo(&listTenantsResult);
                ClusterInfo.SetTenants(listTenantsResult.paths().size());
            } else {
                AddProblem("no-tenants-info");
            }
            ListTenantsResponse.reset();
        }

        if (!WhiteboardRequestsSent && TimeToAskWhiteboard()) {
            WhiteboardRequestsSent = true;
            std::vector<TNodeBatch> batches = BatchNodes();
            SendWhiteboardRequests(batches);
            if (WhiteboardStateRequestsInFlight == 0) {
                ProcessWhiteboard();
            }
        }
    }

    void InitSystemWhiteboardRequest(NKikimrWhiteboard::TEvSystemStateRequest* request) {
        request->MutableFieldsRequired()->CopyFrom(GetDefaultWhiteboardFields<NKikimrWhiteboard::TSystemStateInfo>());
        request->AddFieldsRequired(NKikimrWhiteboard::TSystemStateInfo::kMemoryStatsFieldNumber);
        request->AddFieldsRequired(NKikimrWhiteboard::TSystemStateInfo::kCoresUsedFieldNumber);
        request->AddFieldsRequired(NKikimrWhiteboard::TSystemStateInfo::kCoresTotalFieldNumber);
        request->AddFieldsRequired(NKikimrWhiteboard::TSystemStateInfo::kNetworkUtilizationFieldNumber);
        request->AddFieldsRequired(NKikimrWhiteboard::TSystemStateInfo::kNetworkWriteThroughputFieldNumber);
        request->AddFieldsRequired(NKikimrWhiteboard::TSystemStateInfo::kRealNumberOfCpusFieldNumber);
    }

    void InitTabletWhiteboardRequest(NKikimrWhiteboard::TEvTabletStateRequest* request) {
        //request->AddFieldsRequired(-1);
        Y_UNUSED(request);
    }

    void SendWhiteboardRequest(TNodeBatch& batch) {
        TNodeId nodeId = OffloadMerge ? batch.ChooseNodeId() : 0;
        if (nodeId) {
            if (SystemViewerResponse.count(nodeId) == 0) {
                auto viewerRequest = std::make_unique<TEvViewer::TEvViewerRequest>();
                InitSystemWhiteboardRequest(viewerRequest->Record.MutableSystemRequest());
                viewerRequest->Record.SetTimeout(Timeout.MilliSeconds() / 2);
                for (const TNode* node : batch.NodesToAskAbout) {
                    viewerRequest->Record.MutableLocation()->AddNodeId(node->NodeId);
                }
                SystemViewerResponse.emplace(nodeId, MakeViewerRequest(nodeId, viewerRequest.release()));
                NodeBatches.emplace(nodeId, batch);
                ++WhiteboardStateRequestsInFlight;
            }
            if (Tablets && batch.HasStaticNodes && TabletViewerResponse.count(nodeId) == 0) {
                auto viewerRequest = std::make_unique<TEvViewer::TEvViewerRequest>();
                InitTabletWhiteboardRequest(viewerRequest->Record.MutableTabletRequest());
                viewerRequest->Record.SetTimeout(Timeout.MilliSeconds() / 2);
                for (const TNode* node : batch.NodesToAskAbout) {
                    if (node->Static) {
                        viewerRequest->Record.MutableLocation()->AddNodeId(node->NodeId);
                    }
                }
                if (viewerRequest->Record.GetLocation().NodeIdSize() > 0) {
                    TabletViewerResponse.emplace(nodeId, MakeViewerRequest(nodeId, viewerRequest.release()));
                    NodeBatches.emplace(nodeId, batch);
                    ++WhiteboardStateRequestsInFlight;
                }
            }
        } else {
            for (const TNode* node : batch.NodesToAskAbout) {
                if (node->Disconnected) {
                    continue;
                }
                TNodeId nodeId = node->NodeId;
                if (SystemStateResponse.count(nodeId) == 0) {
                    auto request = new TEvWhiteboard::TEvSystemStateRequest();
                    InitSystemWhiteboardRequest(&request->Record);
                    SystemStateResponse.emplace(nodeId, MakeWhiteboardRequest(nodeId, request));
                    ++WhiteboardStateRequestsInFlight;
                }
                if (Tablets && node->Static) {
                    if (TabletStateResponse.count(nodeId) == 0) {
                        auto request = std::make_unique<TEvWhiteboard::TEvTabletStateRequest>();
                        TabletStateResponse.emplace(nodeId, MakeWhiteboardRequest(nodeId, request.release()));
                        ++WhiteboardStateRequestsInFlight;
                    }
                }
            }
        }
    }

    void SendWhiteboardRequests(std::vector<TNodeBatch>& batches) {
        for (TNodeBatch& batch : batches) {
            SendWhiteboardRequest(batch);
        }
    }

    void ProcessWhiteboard() {
        for (const auto& [responseNodeId, response] : SystemViewerResponse) {
            if (response.IsOk()) {
                const auto& systemResponse(response.Get()->Record.GetSystemResponse());
                for (auto& systemInfo : systemResponse.GetSystemStateInfo()) {
                    TNodeId nodeId = systemInfo.GetNodeId();
                    TNode* node = NodeCache[nodeId];
                    if (node) {
                        node->SystemState = std::move(systemInfo);
                        if (!node->DataCenter) {
                            node->DataCenter = node->SystemState.GetLocation().GetDataCenter();
                        }
                    }
                }
            }
        }
        for (auto& [nodeId, response] : SystemStateResponse) {
            if (response.IsOk()) {
                auto& systemState(response.Get()->Record);
                if (systemState.SystemStateInfoSize() > 0) {
                    TNode* node = NodeCache[nodeId];
                    if (node) {
                        node->SystemState = std::move(*systemState.MutableSystemStateInfo(0));
                        if (!node->DataCenter) {
                            node->DataCenter = node->SystemState.GetLocation().GetDataCenter();
                        }
                    }
                }
            }
        }
        std::unordered_map<TTabletId, NKikimrWhiteboard::TTabletStateInfo> mergedTabletState;
        for (auto& [nodeId, response] : TabletViewerResponse) {
            if (response.IsOk()) {
                auto& tabletResponse(*(response.Get()->Record.MutableTabletResponse()));
                for (auto& tabletState : *tabletResponse.MutableTabletStateInfo()) {
                    NKikimrWhiteboard::TTabletStateInfo& mergedState(mergedTabletState[tabletState.GetTabletId()]);
                    if (tabletState.GetGeneration() > mergedState.GetGeneration()) {
                        mergedState = std::move(tabletState);
                    }
                }
            }
        }
        for (auto& [nodeId, response] : TabletStateResponse) {
            if (response.IsOk()) {
                for (auto& tabletState : *response.Get()->Record.MutableTabletStateInfo()) {
                    NKikimrWhiteboard::TTabletStateInfo& mergedState(mergedTabletState[tabletState.GetTabletId()]);
                    if (tabletState.GetGeneration() > mergedState.GetGeneration()) {
                        mergedState = std::move(tabletState);
                    }
                }
            }
        }

        struct TMemoryStats {
            ui64 Total = 0;
            ui64 Limit = 0;
        };

        std::unordered_set<TString> hostPassed;
        std::unordered_map<TString, TMemoryStats> memoryStats;
        int nodesWithNetworkUtilization = 0;

        for (TNode& node : NodeData) {
            const NKikimrWhiteboard::TSystemStateInfo& systemState = node.SystemState;
            (*ClusterInfo.MutableMapDataCenters())[node.DataCenter]++;
            if (hostPassed.insert(systemState.GetHost()).second) {
                ClusterInfo.SetNumberOfCpus(ClusterInfo.GetNumberOfCpus() + systemState.GetNumberOfCpus());
                if (systemState.LoadAverageSize() > 0) {
                    ClusterInfo.SetLoadAverage(ClusterInfo.GetLoadAverage() + systemState.GetLoadAverage(0));
                }
            }
            if (systemState.HasVersion()) {
                (*ClusterInfo.MutableMapVersions())[systemState.GetVersion()]++;
            }
            if (systemState.HasClusterName() && !ClusterInfo.GetName()) {
                ClusterInfo.SetName(systemState.GetClusterName());
            }
            ClusterInfo.SetMemoryUsed(ClusterInfo.GetMemoryUsed() + systemState.GetMemoryUsed());
            TMemoryStats& stats = memoryStats[systemState.GetHost()];
            if (systemState.GetMemoryLimit() > 0) {
                stats.Limit += systemState.GetMemoryLimit();
            }
            if (systemState.GetMemoryStats().GetMemTotal() > 0) {
                stats.Total = systemState.GetMemoryStats().GetMemTotal();
            }
            if (stats.Limit > 0 && stats.Total > 0) {
                stats.Limit = std::min(stats.Limit, stats.Total);
            }
            if (node.IsAlive()) {
                ClusterInfo.SetNodesAlive(ClusterInfo.GetNodesAlive() + 1);
            }
            (*ClusterInfo.MutableMapNodeStates())[NKikimrWhiteboard::EFlag_Name(node.SystemState.GetSystemState())]++;
            for (const TString& role : node.SystemState.GetRoles()) {
                (*ClusterInfo.MutableMapNodeRoles())[role]++;
            }
            for (const auto& poolStat : systemState.GetPoolStats()) {
                TString poolName = poolStat.GetName();
                NKikimrWhiteboard::TSystemStateInfo_TPoolStats* targetPoolStat = nullptr;
                for (NKikimrWhiteboard::TSystemStateInfo_TPoolStats& ps : *ClusterInfo.MutablePoolStats()) {
                    if (ps.GetName() == poolName) {
                        targetPoolStat = &ps;
                        break;
                    }
                }
                if (targetPoolStat == nullptr) {
                    targetPoolStat = ClusterInfo.AddPoolStats();
                    targetPoolStat->SetName(poolName);
                }
                double poolUsage = targetPoolStat->GetUsage() * targetPoolStat->GetThreads();
                ui32 usageThreads = poolStat.GetLimit() ? poolStat.GetLimit() : poolStat.GetThreads();
                poolUsage += poolStat.GetUsage() * usageThreads;
                ui32 poolThreads = targetPoolStat->GetThreads() + poolStat.GetThreads();
                if (poolThreads != 0) {
                    double threadUsage = poolUsage / poolThreads;
                    targetPoolStat->SetUsage(threadUsage);
                    targetPoolStat->SetThreads(poolThreads);
                }
                if (systemState.GetCoresTotal() == 0) {
                    ClusterInfo.SetCoresUsed(ClusterInfo.GetCoresUsed() + poolStat.GetUsage() * usageThreads);
                    if (poolStat.GetName() != "IO") {
                        ClusterInfo.SetCoresTotal(ClusterInfo.GetCoresTotal() + poolStat.GetThreads());
                    }
                }
            }
            if (systemState.GetCoresTotal() != 0) {
                ClusterInfo.SetCoresUsed(ClusterInfo.GetCoresUsed() + systemState.GetCoresUsed());
                ClusterInfo.SetCoresTotal(ClusterInfo.GetCoresTotal() + systemState.GetCoresTotal());
            }
            if (systemState.HasNetworkUtilization()) {
                ClusterInfo.SetNetworkUtilization(ClusterInfo.GetNetworkUtilization() + systemState.GetNetworkUtilization());
                ++nodesWithNetworkUtilization;
            }
            if (systemState.HasNetworkWriteThroughput()) {
                ClusterInfo.SetNetworkWriteThroughput(ClusterInfo.GetNetworkWriteThroughput() + systemState.GetNetworkWriteThroughput());
            }
        }

        if (nodesWithNetworkUtilization != 0) {
            ClusterInfo.SetNetworkUtilization(ClusterInfo.GetNetworkUtilization() / nodesWithNetworkUtilization);
        }

        for (const auto& memStats : memoryStats) {
            if (memStats.second.Total > 0) {
                ClusterInfo.SetMemoryTotal(ClusterInfo.GetMemoryTotal() + memStats.second.Total);
            } else {
                ClusterInfo.SetMemoryTotal(ClusterInfo.GetMemoryTotal() + memStats.second.Limit);
            }
        }

        for (auto& [tabletId, tabletState] : mergedTabletState) {
            if (FilterTablets.empty() || FilterTablets.count(tabletId)) {
                auto tabletFlag = GetWhiteboardFlag(GetFlagFromTabletState(tabletState.GetState()));
                tabletState.SetOverall(tabletFlag);
                (*ClusterInfo.AddSystemTablets()) = std::move(tabletState);
            }
        }
    }

    void WhiteboardRequestDone() {
        --WhiteboardStateRequestsInFlight;
        if (WhiteboardStateRequestsInFlight == 0) {
            ProcessWhiteboard();
        }
        RequestDone();
    }

    std::unique_ptr<NHealthCheck::TEvSelfCheckRequest> MakeSelfCheckRequest() {
        auto request = std::make_unique<NHealthCheck::TEvSelfCheckRequest>();
        request->Database = DomainName;
        i64 timeoutMs = Timeout.MilliSeconds() * 80 / 100;
        if (timeoutMs <= 0) {
            timeoutMs = 1;
        }
        SetDuration(TDuration::MilliSeconds(timeoutMs), *request->Request.mutable_operation_params()->mutable_operation_timeout());
        return request;
    }

    void SendHealthCheckRequest() {
        SelfCheckResult = MakeRequest<NHealthCheck::TEvSelfCheckResult>(NHealthCheck::MakeHealthCheckID(), MakeSelfCheckRequest().release());
    }

    void Handle(NHealthCheck::TEvSelfCheckResult::TPtr& ev) {
        if (SelfCheckResult->Set(std::move(ev))) {
            RequestDone();
        }
    }

    void Handle(NHealthCheck::TEvSelfCheckResultProto::TPtr& ev) {
        if (SelfCheckResultProto->Set(std::move(ev))) {
            RequestDone();
        }
    }

    void Handle(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
        if (MetadataCacheEndpointsLookup.Set(std::move(ev))) {
            if (MetadataCacheEndpointsLookup.IsOk()) {
                auto activeNode = TDatabaseMetadataCache::PickActiveNode(MetadataCacheEndpointsLookup->InfoEntries);
                if (activeNode != 0) {
                    TActorId cache = MakeDatabaseMetadataCacheId(activeNode);
                    auto request = std::make_unique<NHealthCheck::TEvSelfCheckRequestProto>();
                    // Note: the database metadata cache service does not forward operation_timeout
                    // to the underlying health check, so we rely on our own Timeout wakeup.
                    SelfCheckResultProto = MakeRequest<NHealthCheck::TEvSelfCheckResultProto>(cache, request.release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, activeNode);
                }
            }
            if (!SelfCheckResultProto) {
                SendHealthCheckRequest();
            }
            RequestDone();
        }
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        if (NodesInfoResponse->Set(std::move(ev))) {
            ProcessResponses();
            RequestDone();
        }
    }

    void Handle(TEvWhiteboard::TEvNodeStateResponse::TPtr& ev) {
        if (NodeStateResponse->Set(std::move(ev))) {
            ProcessResponses();
            RequestDone();
        }
    }

    void Handle(NConsole::TEvConsole::TEvListTenantsResponse::TPtr& ev) {
        if (ListTenantsResponse->Set(std::move(ev))) {
            ProcessResponses();
            RequestDone();
        }
    }

    void Handle(NSysView::TEvSysView::TEvGetPDisksResponse::TPtr& ev) {
        if (PDisksResponse->Set(std::move(ev))) {
            ProcessResponses();
            RequestDone();
        }
    }

    void Handle(NSysView::TEvSysView::TEvGetStorageStatsResponse::TPtr& ev) {
        if (StorageStatsResponse->Set(std::move(ev))) {
            ProcessResponses();
            RequestDone();
        }
    }

    void Handle(NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr& ev) {
        if (VSlotsResponse->Set(std::move(ev))) {
            ProcessResponses();
            RequestDone();
        }
    }

    void Handle(NSysView::TEvSysView::TEvGetGroupsResponse::TPtr& ev) {
        if (GroupsResponse->Set(std::move(ev))) {
            ProcessResponses();
            RequestDone();
        }
    }

    void Handle(NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr& ev) {
        if (StoragePoolsResponse->Set(std::move(ev))) {
            ProcessResponses();
            RequestDone();
        }
    }

    void Handle(TEvHive::TEvResponseHiveNodeStats::TPtr& ev) {
        if (HiveNodeStatsResponse->Set(std::move(ev))) {
            ProcessResponses();
            RequestDone();
        }
    }

    void Handle(TEvWhiteboard::TEvSystemStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        if (SystemStateResponse[nodeId].Set(std::move(ev))) {
            WhiteboardRequestDone();
        }
    }

    void Handle(TEvWhiteboard::TEvTabletStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        if (TabletStateResponse[nodeId].Set(std::move(ev))) {
            WhiteboardRequestDone();
        }
    }

    void Handle(TEvNodeWardenStorageConfig::TPtr& ev) {
        if (NodeWardenStorageConfigResponse->Set(std::move(ev))) {
            ProcessResponses();
            RequestDone();
        }
    }

    void Handle(TEvViewer::TEvViewerResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        switch (ev->Get()->Record.Response_case()) {
            case NKikimrViewer::TEvViewerResponse::ResponseCase::kSystemResponse:
                if (SystemViewerResponse[nodeId].Set(std::move(ev))) {
                    NodeBatches.erase(nodeId);
                    WhiteboardRequestDone();
                }
                return;
            case NKikimrViewer::TEvViewerResponse::ResponseCase::kTabletResponse:
                if (TabletViewerResponse[nodeId].Set(std::move(ev))) {
                    NodeBatches.erase(nodeId);
                    WhiteboardRequestDone();
                }
                return;
            default:
                break;
        }
        TString error("WrongResponse");
        {
            auto itSystemViewerResponse = SystemViewerResponse.find(nodeId);
            if (itSystemViewerResponse != SystemViewerResponse.end()) {
                if (itSystemViewerResponse->second.Error(error)) {
                    if (NodeBatches.count(nodeId)) {
                        SendWhiteboardRequest(NodeBatches[nodeId]);
                        NodeBatches.erase(nodeId);
                    }
                    WhiteboardRequestDone();
                }
            }
        }
        {
            auto itTabletViewerResponse = TabletViewerResponse.find(nodeId);
            if (itTabletViewerResponse != TabletViewerResponse.end()) {
                if (itTabletViewerResponse->second.Error(error)) {
                    if (NodeBatches.count(nodeId)) {
                        SendWhiteboardRequest(NodeBatches[nodeId]);
                        NodeBatches.erase(nodeId);
                    }
                    WhiteboardRequestDone();
                }
            }
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        TNodeId nodeId = ev->Get()->NodeId;
        TNode* node = NodeCache[nodeId];
        if (node) {
            node->Disconnected = true;
        }
        TString error("NodeDisconnected");
        {
            auto itSystemStateResponse = SystemStateResponse.find(nodeId);
            if (itSystemStateResponse != SystemStateResponse.end()) {
                if (itSystemStateResponse->second.Error(error)) {
                    WhiteboardRequestDone();
                }
            }
        }
        {
            auto itTabletStateResponse = TabletStateResponse.find(nodeId);
            if (itTabletStateResponse != TabletStateResponse.end()) {
                if (itTabletStateResponse->second.Error(error)) {
                    WhiteboardRequestDone();
                }
            }
        }
        {
            auto itSystemViewerResponse = SystemViewerResponse.find(nodeId);
            if (itSystemViewerResponse != SystemViewerResponse.end()) {
                if (itSystemViewerResponse->second.Error(error)) {
                    if (NodeBatches.count(nodeId)) {
                        SendWhiteboardRequest(NodeBatches[nodeId]);
                        NodeBatches.erase(nodeId);
                    }
                    WhiteboardRequestDone();
                }
            }
        }
        {
            auto itTabletViewerResponse = TabletViewerResponse.find(nodeId);
            if (itTabletViewerResponse != TabletViewerResponse.end()) {
                if (itTabletViewerResponse->second.Error(error)) {
                    if (NodeBatches.count(nodeId)) {
                        SendWhiteboardRequest(NodeBatches[nodeId]);
                        NodeBatches.erase(nodeId);
                    }
                    WhiteboardRequestDone();
                }
            }
        }
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr& ev) {
        TNodeId nodeId = ev->Sender.NodeId();
        TString error("Undelivered");
        {
            auto itSystemViewerResponse = SystemViewerResponse.find(nodeId);
            if (itSystemViewerResponse != SystemViewerResponse.end()) {
                if (itSystemViewerResponse->second.Error(error)) {
                    if (NodeBatches.count(nodeId)) {
                        SendWhiteboardRequest(NodeBatches[nodeId]);
                        NodeBatches.erase(nodeId);
                    }
                    WhiteboardRequestDone();
                }
            }
        }
        {
            auto itTabletViewerResponse = TabletViewerResponse.find(nodeId);
            if (itTabletViewerResponse != TabletViewerResponse.end()) {
                if (itTabletViewerResponse->second.Error(error)) {
                    if (NodeBatches.count(nodeId)) {
                        SendWhiteboardRequest(NodeBatches[nodeId]);
                        NodeBatches.erase(nodeId);
                    }
                    WhiteboardRequestDone();
                }
            }
        }
    }

    bool OnBscError(const TString& error) {
        bool result = false;
        if (StorageStatsResponse && StorageStatsResponse->Error(error)) {
            ProcessResponses();
            result = true;
        }
        if (PDisksResponse && PDisksResponse->Error(error)) {
            ProcessResponses();
            result = true;
        }
        if (VSlotsResponse && VSlotsResponse->Error(error)) {
            ProcessResponses();
            result = true;
        }
        if (GroupsResponse && GroupsResponse->Error(error)) {
            ProcessResponses();
            result = true;
        }
        if (StoragePoolsResponse && StoragePoolsResponse->Error(error)) {
            ProcessResponses();
            result = true;
        }
        return result;
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            TString error = TStringBuilder() << "Failed to establish pipe to " << ev->Get()->TabletId << ": "
                << NKikimrProto::EReplyStatus_Name(ev->Get()->Status);
            if (ev->Get()->TabletId == GetBSControllerId()) {
                if (OnBscError(error)) {
                    AddProblem("bsc-error");
                }
            }
            if (ev->Get()->TabletId == RootHiveId) {
                if (HiveNodeStatsResponse && HiveNodeStatsResponse->Error(error)) {
                    AddProblem("hive-error");
                    ProcessResponses();
                }
            }
            if (ev->Get()->TabletId == MakeConsoleID()) {
                if (ListTenantsResponse && ListTenantsResponse->Error(error)) {
                    AddProblem("console-error");
                    ProcessResponses();
                }
            }
        }
        TBase::Handle(ev); // all RequestDone() are handled by base handler
    }

    void HandleTimeout() {
        if (WhiteboardRequestsSent && WhiteboardStateRequestsInFlight > 0) {
            // Use the responses received so far: nodes that have not answered are counted as not alive.
            WhiteboardStateRequestsInFlight = 0;
            ProcessWhiteboard();
        }
        ReplyAndPassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(TEvNodeWardenStorageConfig, Handle);
            hFunc(TEvWhiteboard::TEvNodeStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvSystemStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvTabletStateResponse, Handle);
            hFunc(TEvViewer::TEvViewerResponse, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            hFunc(NConsole::TEvConsole::TEvListTenantsResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetPDisksResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetStorageStatsResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetVSlotsResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetGroupsResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetStoragePoolsResponse, Handle);
            hFunc(TEvHive::TEvResponseHiveNodeStats, Handle);
            hFunc(NHealthCheck::TEvSelfCheckResult, Handle);
            hFunc(NHealthCheck::TEvSelfCheckResultProto, Handle);
            hFunc(TEvents::TEvUndelivered, Undelivered);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
            default:
                return TBase::StateWork(ev);
        }
    }

    static TString GetKey(const NKikimrSysView::TStorageStatsEntry& entry) {
        return entry.GetPDiskFilter() + ",ErasureSpecies:" + entry.GetErasureSpecies();
    }

    static ui64 GetSlotSize(const NKikimrSysView::TPDiskInfo& pdiskInfo, ui32 groupSizeInUnits) {
        ui64 slotSize = pdiskInfo.GetEnforcedDynamicSlotSize();
        if (!slotSize) {
            slotSize = pdiskInfo.GetExpectedSlotSize();
        }
        if (!slotSize) {
            const ui32 slotCount = pdiskInfo.GetExpectedSlotCount() ? pdiskInfo.GetExpectedSlotCount() : 16;
            slotSize = pdiskInfo.GetTotalSize() / slotCount;
        }
        return TPDiskConfig::GetOwnerQuota(
            slotSize, groupSizeInUnits, pdiskInfo.GetSlotSizeInUnits(), pdiskInfo.GetExpectedSlotSize(),
            pdiskInfo.HasUserChunkPoolSize() ? std::make_optional(pdiskInfo.GetUserChunkPoolSize()) : std::nullopt);
    }

    static NKikimrWhiteboard::EFlag GetClusterStateFromSelfCheck(const Ydb::Monitoring::SelfCheckResult& result) {
        switch (result.self_check_result()) {
            case Ydb::Monitoring::SelfCheck::GOOD:
                return NKikimrWhiteboard::EFlag::Green;
            case Ydb::Monitoring::SelfCheck::DEGRADED:
                return NKikimrWhiteboard::EFlag::Yellow;
            case Ydb::Monitoring::SelfCheck::MAINTENANCE_REQUIRED:
                return NKikimrWhiteboard::EFlag::Red;
            case Ydb::Monitoring::SelfCheck::EMERGENCY:
                return NKikimrWhiteboard::EFlag::Red;
            default:
                return NKikimrWhiteboard::EFlag::Grey;
        }
    }

    void FillBridgeInfo(const std::unordered_map<ui32, std::unordered_map<TString, ui32>>& pileToGroupStatus) {
        if (!NodeWardenStorageConfigResponse || !NodeWardenStorageConfigResponse->IsDone()) {
            return;
        }
        if (!NodeWardenStorageConfigResponse->IsOk()) {
            AddProblem("no-node-warden-storage-config");
            return;
        }
        if (!NodeWardenStorageConfigResponse->Get()->BridgeInfo) {
            AddProblem("empty-node-warden-bridge-info");
            return;
        }
        const auto& srcBridgeInfo = *NodeWardenStorageConfigResponse->Get()->BridgeInfo.get();
        auto& pbBridgeInfo = *ClusterInfo.MutableBridgeInfo();
        struct TPileNodes {
            ui32 Storage = 0;
            ui32 Compute = 0;
            ui32 AliveStorage = 0;
            ui32 AliveCompute = 0;
        };
        std::unordered_map<ui32, TPileNodes> pileNodes;
        for (const auto& node : NodeData) {
            if (node.PileNum) {
                auto& nodes = pileNodes[*node.PileNum];
                if (node.Static) {
                    ++nodes.Storage;
                    nodes.AliveStorage += node.IsAlive();
                } else {
                    ++nodes.Compute;
                    nodes.AliveCompute += node.IsAlive();
                }
            }
        }
        for (const auto& pile : srcBridgeInfo.Piles) {
            if (pile.IsFreeSlot || (LocalPileNodeStats && &pile != srcBridgeInfo.SelfNodePile)) {
                continue;
            }
            auto& pbBridgePileInfo = *pbBridgeInfo.AddPiles();
            pile.BridgePileId.CopyToProto(&pbBridgePileInfo, &std::decay_t<decltype(pbBridgePileInfo)>::SetPileId);
            pbBridgePileInfo.SetName(pile.Name);
            pbBridgePileInfo.SetState(GetPileStateFromPile(pile));
            if (const auto it = pileToGroupStatus.find(pbBridgePileInfo.GetPileId()); it != pileToGroupStatus.end()) {
                for (const auto& [status, count] : it->second) {
                    (*pbBridgePileInfo.MutableGroupStatuses())[status] = count;
                }
            }
            if (!NodePileMapKnown) {
                continue;
            }
            auto nodes = pileNodes[pile.BridgePileId.GetPileIndex()];
            // A node that has not answered by the deadline is counted as not alive.
            // A DISCONNECTED pile can still be queried from within that same pile.
            bool aliveKnown = WhiteboardRequestsSent && CanQueryPile(srcBridgeInfo, pile);
            if (auto it = PileRequests.find(pile.BridgePileId.GetPileIndex()); it != PileRequests.end() && it->second.Result) {
                const auto& remote = *it->second.Result;
                ClusterInfo.SetNodesTotal(ClusterInfo.GetNodesTotal() - (nodes.Storage + nodes.Compute) + remote.GetNodes());
                ClusterInfo.SetNodesAlive(ClusterInfo.GetNodesAlive() + remote.GetAliveStorageNodes() + remote.GetAliveComputeNodes());
                nodes.Storage = remote.GetStorageNodes();
                nodes.Compute = remote.GetComputeNodes();
                nodes.AliveStorage = remote.GetAliveStorageNodes();
                nodes.AliveCompute = remote.GetAliveComputeNodes();
                aliveKnown = true;
            }
            pbBridgePileInfo.SetNodes(nodes.Storage + nodes.Compute);
            pbBridgePileInfo.SetStorageNodes(nodes.Storage);
            pbBridgePileInfo.SetComputeNodes(nodes.Compute);
            if (aliveKnown) {
                pbBridgePileInfo.SetAliveStorageNodes(nodes.AliveStorage);
                pbBridgePileInfo.SetAliveComputeNodes(nodes.AliveCompute);
            }
        }
    }

    void ReplyWithLocalPileNodeStats() {
        FillBridgeInfo({});
        NKikimrViewer::TClusterInfo result;
        if (ClusterInfo.HasBridgeInfo()) {
            *result.MutableBridgeInfo() = std::move(*ClusterInfo.MutableBridgeInfo());
        }
        result.SetVersion(Viewer->GetCapabilityVersion("/viewer/cluster"));
        for (const auto& problem : Problems) {
            result.AddProblems(problem);
        }
        TBase::ReplyAndPassAway(GetHTTPOKJSON(result));
    }

    void ReplyAndPassAway() override {
        if (LocalPileNodeStats) {
            return ReplyWithLocalPileNodeStats();
        }
        if (StorageStatsResponse && StorageStatsResponse->IsOk()) {
            for (NKikimrSysView::TStorageStatsEntry& entry : *StorageStatsResponse->Get()->Record.MutableEntries()) {
                if (entry.GetPDiskFilter().empty() || !TStringBuf(entry.GetPDiskFilter()).StartsWith("Type:")) {
                    continue;
                }
                NKikimrSysView::TStorageStatsEntry& newEntry = (*ClusterInfo.AddStorageStats()) = std::move(entry);
                newEntry.ClearPDiskFilterData(); // remove binary data
                //newEntry.ClearAvailableSizeToCreate();
            }
        } else {
            AddProblem("no-storage-stats");
        }

        std::unordered_map<TPDiskId, const NKikimrSysView::TPDiskInfo&> pDisksIndex;

        if (PDisksResponse && PDisksResponse->IsOk()) {
            for (const NKikimrSysView::TPDiskEntry& entry : PDisksResponse->Get()->Record.GetEntries()) {
                const NKikimrSysView::TPDiskKey& key = entry.GetKey();
                const NKikimrSysView::TPDiskInfo& info = entry.GetInfo();
                (*ClusterInfo.MutableMapStorageTotal())[info.GetType()] += info.GetTotalSize();
                (*ClusterInfo.MutableMapStorageUsed())[info.GetType()] += info.GetTotalSize() - info.GetAvailableSize();
                pDisksIndex.emplace(std::make_pair(key.GetNodeId(), key.GetPDiskId()), info);
            }
        } else {
            AddProblem("no-pdisk-info");
        }

        std::unordered_map<ui32, const NKikimrSysView::TGroupInfo&> groupsIndex;
        std::unordered_set<ui32> proxyGroups;
        std::unordered_map<ui32, std::unordered_map<TString, ui32>> pileToGroupStatus;

        struct TSyncAccumulator {
            NKikimrViewer::TBridgePileSyncInfo Info;
            bool Unsynced = false;

            void Add(const NKikimrSysView::TGroupInfo& group) {
                const TString& stage = group.GetBridgeSyncStage();
                ++(*Info.MutableGroupsInStage())[stage];
                if (stage != NKikimrBridge::TGroupState::EStage_Name(NKikimrBridge::TGroupState::SYNCED)) {
                    Unsynced = true;
                    Info.SetBytesRemaining(Info.GetBytesRemaining() + group.GetBridgeSyncRemainingBytes());
                    Info.SetBytesPerSecond(Info.GetBytesPerSecond() + group.GetBridgeSyncCopyBytesPerSecond());
                    if (!group.GetBridgeSyncRemainingBytesComplete()) {
                        Info.SetGroupsNotEstimated(Info.GetGroupsNotEstimated() + 1);
                    }
                }
            }

            void Finish() {
                Info.SetBytesRemaining(Info.GetBytesRemaining());
                Info.SetBytesPerSecond(Info.GetBytesPerSecond());
                Info.SetGroupsNotEstimated(Info.GetGroupsNotEstimated());
                if (Info.GetBytesPerSecond()) {
                    Info.SetSecondsLeft(Info.GetBytesRemaining() / Info.GetBytesPerSecond());
                }
            }
        };
        using TPoolKey = std::tuple<ui64, ui64>; // BoxId, StoragePoolId; static groups have none of them
        std::unordered_map<ui32, TSyncAccumulator> pileSync;
        std::unordered_map<ui32, std::map<TPoolKey, TSyncAccumulator>> pilePoolSync;

        if (GroupsResponse && GroupsResponse->IsOk()) {
            for (const NKikimrSysView::TGroupEntry& entry : GroupsResponse->Get()->Record.GetEntries()) {
                const NKikimrSysView::TGroupInfo& info = entry.GetInfo();
                if (info.HasProxyGroupId()) {
                    proxyGroups.insert(info.GetProxyGroupId());
                }
            }
            for (const NKikimrSysView::TGroupEntry& entry : GroupsResponse->Get()->Record.GetEntries()) {
                const NKikimrSysView::TGroupKey& key = entry.GetKey();
                const NKikimrSysView::TGroupInfo& info = entry.GetInfo();
                if (proxyGroups.count(key.GetGroupId())) {
                    continue; // ignore proxy groups
                }
                if (info.HasBridgePileId()) {
                    pileToGroupStatus[info.GetBridgePileId()][info.GetOperatingStatus()]++;
                    if (info.HasBridgeSyncStage()) {
                        pileSync[info.GetBridgePileId()].Add(info);
                        pilePoolSync[info.GetBridgePileId()][{info.GetBoxId(), info.GetStoragePoolId()}].Add(info);
                    }
                }
                groupsIndex.emplace(key.GetGroupId(), info);
            }
        } else {
            AddProblem("no-group-info");
        }

        FillBridgeInfo(pileToGroupStatus);
        if (ClusterInfo.HasBridgeInfo()) {
            for (auto& pbBridgePileInfo : *ClusterInfo.MutableBridgeInfo()->MutablePiles()) {
                if (auto it = pileSync.find(pbBridgePileInfo.GetPileId());
                        it != pileSync.end() && it->second.Unsynced) {
                    it->second.Finish();
                    *pbBridgePileInfo.MutableSync() = std::move(it->second.Info);
                    for (auto& [key, pool] : pilePoolSync[pbBridgePileInfo.GetPileId()]) {
                        if (pool.Unsynced) {
                            pool.Finish();
                            auto *pbPool = pbBridgePileInfo.AddStoragePools();
                            pbPool->SetName(GetStoragePoolName(key));
                            *pbPool->MutableSync() = std::move(pool.Info);
                        }
                    }
                }
            }
        }

        if (VSlotsResponse && VSlotsResponse->IsOk()) {
            std::unordered_map<TString, NKikimrSysView::TStorageStatsEntry&> storageStatsByType;
            for (auto& entry : *ClusterInfo.MutableStorageStats()) {
                auto it = storageStatsByType.emplace(GetKey(entry), entry).first;
                it->second.ClearCurrentAllocatedSize();
                it->second.ClearCurrentAvailableSize();
            }
            for (const NKikimrSysView::TVSlotEntry& entry : VSlotsResponse->Get()->Record.GetEntries()) {
                const NKikimrSysView::TVSlotKey& key = entry.GetKey();
                const NKikimrSysView::TVSlotInfo& info = entry.GetInfo();
                auto itPDisk = pDisksIndex.find(std::make_pair(key.GetNodeId(), key.GetPDiskId()));
                auto itGroup = groupsIndex.find(info.GetGroupId());
                if (itPDisk != pDisksIndex.end() && itGroup != groupsIndex.end()) {
                    ui64 allocated = info.GetAllocatedSize();
                    ui64 slotSize = GetSlotSize(itPDisk->second, itGroup->second.GetGroupSizeInUnits());
                    ui64 slotAvailable = slotSize > allocated ? slotSize - allocated : 0;
                    ui64 available = info.GetAvailableSize();
                    if (slotSize < available || available == 0) {
                        available = slotAvailable;
                    }
                    TString type = TString("Type:") + itPDisk->second.GetType() + ",ErasureSpecies:" + itGroup->second.GetErasureSpeciesV2();
                    auto itStats = storageStatsByType.find(type);
                    if (itStats != storageStatsByType.end()) {
                        itStats->second.SetCurrentAllocatedSize(itStats->second.GetCurrentAllocatedSize() + allocated);
                        const ui64 currentAvailable = itStats->second.GetCurrentAvailableSize();
                        itStats->second.SetCurrentAvailableSize(currentAvailable + Min(available, Max<ui64>() - currentAvailable));
                    }
                }
            }
        } else {
            AddProblem("no-vslot-info");
        }

        ClusterInfo.SetVersion(Viewer->GetCapabilityVersion("/viewer/cluster"));
        for (const auto& problem : Problems) {
            ClusterInfo.AddProblems(problem);
        }
        for (const auto& [dataCenter, nodes] : ClusterInfo.GetMapDataCenters()) {
            ClusterInfo.AddDataCenters(dataCenter);
        }
        for (const auto& [version, count] : ClusterInfo.GetMapVersions()) {
            ClusterInfo.AddVersions(version);
        }
        for (const auto& [type, size] : ClusterInfo.GetMapStorageTotal()) {
            ClusterInfo.SetStorageTotal(ClusterInfo.GetStorageTotal() + size);
        }
        for (const auto& [type, size] : ClusterInfo.GetMapStorageUsed()) {
            ClusterInfo.SetStorageUsed(ClusterInfo.GetStorageUsed() + size);
        }
        if (CachedDataMaxAge) {
            ClusterInfo.SetCachedDataMaxAge(CachedDataMaxAge.MilliSeconds());
        }
        NKikimrWhiteboard::EFlag clusterState = NKikimrWhiteboard::EFlag::Grey;
        if (SelfCheckResult && SelfCheckResult->IsOk()) {
            clusterState = GetClusterStateFromSelfCheck(SelfCheckResult->Get()->Result);
        } else if (SelfCheckResultProto && SelfCheckResultProto->IsOk()) {
            clusterState = GetClusterStateFromSelfCheck(SelfCheckResultProto->Get()->Record);
        } else if (HealthCheckRequested) {
            AddProblem("no-health-check");
        } else {
            ui64 worstNodes = 0;
            for (NKikimrWhiteboard::EFlag flag = NKikimrWhiteboard::EFlag::Grey; flag <= NKikimrWhiteboard::EFlag::Red; flag = NKikimrWhiteboard::EFlag(flag + 1)) {
                auto itNodes = ClusterInfo.GetMapNodeStates().find(NKikimrWhiteboard::EFlag_Name(flag));
                if (itNodes == ClusterInfo.GetMapNodeStates().end()) {
                    continue;
                }
                auto& nodes = itNodes->second;
                if (nodes > worstNodes / 100) { // only if it's more than 1% of all nodes
                    clusterState = flag;
                }
                worstNodes += nodes;
            }
        }
        ClusterInfo.SetOverall(GetViewerFlag(clusterState));
        TBase::ReplyAndPassAway(GetHTTPOKJSON(ClusterInfo));
    }

public:
    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "Cluster information",
            .Description = "Returns information about cluster"
        });
        yaml.AddParameter({
            .Name = "local_pile_node_stats",
            .Description = "internal: return only node statistics of the serving bridge pile (BridgeInfo with a single pile); "
                "other cluster information is not collected and tablets parameter is ignored",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "enums",
            .Description = "convert enums to strings",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "tablets",
            .Description = "return system tablets state",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "ui64",
            .Description = "return ui64 as number",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "timeout",
            .Description = "timeout in ms",
            .Type = "integer",
        });
        yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<NKikimrViewer::TClusterInfo>());
        return yaml;
    }
};

}
