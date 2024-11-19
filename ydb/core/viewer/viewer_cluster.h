#pragma once
#include "json_handlers.h"
#include "json_pipe_req.h"
#include "viewer.h"
#include "viewer_helper.h"
#include "viewer_tabletinfo.h"
#include <library/cpp/protobuf/json/proto2json.h>

namespace NKikimr::NViewer {

using namespace NProtobufJson;
using namespace NActors;
using namespace NNodeWhiteboard;

class TJsonCluster : public TViewerPipeClient {
    using TThis = TJsonCluster;
    using TBase = TViewerPipeClient;
    std::optional<TRequestResponse<TEvInterconnect::TEvNodesInfo>> NodesInfoResponse;
    std::optional<TRequestResponse<TEvWhiteboard::TEvNodeStateResponse>> NodeStateResponse;
    std::optional<TRequestResponse<NConsole::TEvConsole::TEvListTenantsResponse>> ListTenantsResponse;
    std::optional<TRequestResponse<NSysView::TEvSysView::TEvGetPDisksResponse>> PDisksResponse;
    std::optional<TRequestResponse<NSysView::TEvSysView::TEvGetStorageStatsResponse>> StorageStatsResponse;
    std::optional<TRequestResponse<TEvHive::TEvResponseHiveNodeStats>> HiveNodeStatsResponse;

    int WhiteboardStateRequestsInFlight = 0;
    std::unordered_map<TNodeId, TRequestResponse<TEvWhiteboard::TEvSystemStateResponse>> SystemStateResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvWhiteboard::TEvTabletStateResponse>> TabletStateResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvViewer::TEvViewerResponse>> SystemViewerResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvViewer::TEvViewerResponse>> TabletViewerResponse;

    struct TNode {
        TEvInterconnect::TNodeInfo NodeInfo;
        NKikimrWhiteboard::TSystemStateInfo SystemState;
        TNodeId NodeId;
        TString DataCenter;
        TSubDomainKey SubDomainKey;
        bool Static = false;
        bool Connected = false;
        bool Disconnected = false;

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
    TTabletId RootHiveId = 0;
    TJsonSettings JsonSettings;
    ui32 Timeout;
    bool Tablets = false;

public:
    TJsonCluster(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        InitConfig(params);
        Tablets = FromStringWithDefault<bool>(params.Get("tablets"), false);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        OffloadMerge = FromStringWithDefault<bool>(params.Get("offload_merge"), OffloadMerge);
        OffloadMergeAttempts = FromStringWithDefault<bool>(params.Get("offload_merge_attempts"), OffloadMergeAttempts);
    }

    void Bootstrap() override {
        NodesInfoResponse = MakeRequest<TEvInterconnect::TEvNodesInfo>(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
        NodeStateResponse = MakeWhiteboardRequest(TActivationContext::ActorSystem()->NodeId, new TEvWhiteboard::TEvNodeStateRequest());
        PDisksResponse = RequestBSControllerPDisks();
        StorageStatsResponse = RequestBSControllerStorageStats();
        ListTenantsResponse = MakeRequestConsoleListTenants();
        if (AppData()->DomainsInfo && AppData()->DomainsInfo->Domain) {
            TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
            ClusterInfo.SetDomain(TStringBuilder() << "/" << AppData()->DomainsInfo->Domain->Name);
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
        }
        Become(&TThis::StateWork, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
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

    std::vector<TNodeBatch> BatchNodes() {
        std::vector<TNodeBatch> batches;
        if (OffloadMerge) {
            std::unordered_map<TSubDomainKey, TNodeBatch> batchSubDomain;
            std::unordered_map<TString, TNodeBatch> batchDataCenters;
            for (TNode& node : NodeData) {
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
                nodeBatch.NodesToAskAbout.push_back(&node);
            }
            SplitBatch(nodeBatch, batches);
        }
        return batches;
    }

    bool TimeToAskWhiteboard() {
        if (NodesInfoResponse) {
            return false;
        }

        if (NodeStateResponse) {
            return false;
        }

        if (ListTenantsResponse) {
            return false;
        }

        if (PDisksResponse) {
            return false;
        }

        if (StorageStatsResponse) {
            return false;
        }

        if (HiveNodeStatsResponse) {
            return false;
        }

        return true;
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
                        TNodeId nodeId = FromStringWithDefault(TStringBuf(nodeStateInfo.GetPeerName()).Before(':'), 0);
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

        if (PDisksResponse && PDisksResponse->IsDone()) {
            if (PDisksResponse->IsOk()) {
                for (const NKikimrSysView::TPDiskEntry& entry : PDisksResponse->Get()->Record.GetEntries()) {
                    const NKikimrSysView::TPDiskInfo& info = entry.GetInfo();
                    (*ClusterInfo.MutableMapStorageTotal())[info.GetType()] += info.GetTotalSize();
                    (*ClusterInfo.MutableMapStorageUsed())[info.GetType()] += info.GetTotalSize() - info.GetAvailableSize();
                }
            } else {
                AddProblem("no-pdisk-info");
            }
            PDisksResponse.reset();
        }

        if (StorageStatsResponse && StorageStatsResponse->IsDone()) {
            if (StorageStatsResponse->IsOk()) {
                for (NKikimrSysView::TStorageStatsEntry& entry : *StorageStatsResponse->Get()->Record.MutableEntries()) {
                    NKikimrSysView::TStorageStatsEntry& newEntry = (*ClusterInfo.AddStorageStats()) = std::move(entry);
                    newEntry.ClearPDiskFilterData(); // remove binary data
                }
            } else {
                AddProblem("no-storage-stats");
            }
            StorageStatsResponse.reset();
        }

        if (TimeToAskWhiteboard()) {
            std::vector<TNodeBatch> batches = BatchNodes();
            SendWhiteboardRequests(batches);
        }
    }

    void InitSystemWhiteboardRequest(NKikimrWhiteboard::TEvSystemStateRequest* request) {
        request->MutableFieldsRequired()->CopyFrom(GetDefaultWhiteboardFields<NKikimrWhiteboard::TSystemStateInfo>());
        request->AddFieldsRequired(NKikimrWhiteboard::TSystemStateInfo::kCoresUsedFieldNumber);
        request->AddFieldsRequired(NKikimrWhiteboard::TSystemStateInfo::kCoresTotalFieldNumber);
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
                viewerRequest->Record.SetTimeout(Timeout / 2);
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
                viewerRequest->Record.SetTimeout(Timeout / 2);
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

        std::unordered_set<TString> hostPassed;

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
            ClusterInfo.SetMemoryTotal(ClusterInfo.GetMemoryTotal() + systemState.GetMemoryLimit());
            if (!node.Disconnected && node.SystemState.HasSystemState()) {
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
        ReplyAndPassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(TEvWhiteboard::TEvNodeStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvSystemStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvTabletStateResponse, Handle);
            hFunc(TEvViewer::TEvViewerResponse, Handle);
            hFunc(NConsole::TEvConsole::TEvListTenantsResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetPDisksResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetStorageStatsResponse, Handle);
            hFunc(TEvHive::TEvResponseHiveNodeStats, Handle);
            hFunc(TEvents::TEvUndelivered, Undelivered);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void ReplyAndPassAway() override {
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
        NKikimrWhiteboard::EFlag worstState = NKikimrWhiteboard::EFlag::Grey;
        ui64 worstNodes = 0;
        for (NKikimrWhiteboard::EFlag flag = NKikimrWhiteboard::EFlag::Grey; flag <= NKikimrWhiteboard::EFlag::Red; flag = NKikimrWhiteboard::EFlag(flag + 1)) {
            auto itNodes = ClusterInfo.GetMapNodeStates().find(NKikimrWhiteboard::EFlag_Name(flag));
            if (itNodes == ClusterInfo.GetMapNodeStates().end()) {
                continue;
            }
            auto& nodes = itNodes->second;
            if (nodes > worstNodes / 100) { // only if it's more than 1% of all nodes
                worstState = flag;
            }
            worstNodes += nodes;
        }
        ClusterInfo.SetOverall(GetViewerFlag(worstState));
        TStringStream out;
        Proto2Json(ClusterInfo, out, {
            .EnumMode = TProto2JsonConfig::EnumValueMode::EnumName,
            .MapAsObject = true,
            .StringifyNumbers = TProto2JsonConfig::EStringifyNumbersMode::StringifyInt64Always,
            .WriteNanAsString = true,
        });
        TBase::ReplyAndPassAway(GetHTTPOKJSON(out.Str()));
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
