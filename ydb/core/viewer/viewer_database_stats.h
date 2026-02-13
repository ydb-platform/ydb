#pragma once
#include "json_handlers.h"
#include "json_pipe_req.h"
#include "viewer.h"
#include <ydb/library/actors/interconnect/interconnect.h>
#include <algorithm>
#include <deque>

namespace NKikimr::NViewer {

using namespace NActors;
using namespace NNodeWhiteboard;
using TNavigate = NSchemeCache::TSchemeCacheNavigate;

// Simple database/storage stats endpoint modeled after viewer_nodes
class TJsonDatabaseStats : public TViewerPipeClient {
    using TBase = TViewerPipeClient;
    using TBase::ReplyAndPassAway;
    using TGroupId = ui32;
    using TStoragePoolId = std::pair<ui64, ui64>; // box : storage pool id
    using TPDiskId = std::pair<TNodeId, ui32>; // node id : pdisk id

    // Requests
    TRequestResponse<TEvInterconnect::TEvNodesInfo> NodesInfoResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvWhiteboard::TEvSystemStateResponse>> SystemStateResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvWhiteboard::TEvNodeStateResponse>> NodeStateResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvWhiteboard::TEvPDiskStateResponse>> PDiskStateResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvWhiteboard::TEvVDiskStateResponse>> VDiskStateResponse;

    // SysView storage filters and responses
    TRequestResponse<NSysView::TEvSysView::TEvGetStoragePoolsResponse> PoolsResponse;
    TRequestResponse<NSysView::TEvSysView::TEvGetGroupsResponse> GroupsResponse;
    TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse> VSlotsResponse;

    std::unordered_set<TString> StoragePoolNames;
    std::vector<TNodeId> DatabaseNodes;
    std::vector<TNodeId> StorageNodes;
    NKikimrSysView::TStoragePoolEntry StaticStoragePool;
    std::unordered_map<TStoragePoolId, const NKikimrSysView::TStoragePoolEntry&> StoragePools;
    std::unordered_map<TGroupId, const NKikimrSysView::TGroupEntry&> StorageGroups;

    bool Streaming = false;
    NKikimrViewer::TDatabaseStats Result;
    NHttp::THttpOutgoingResponsePtr HttpStream;
    TDuration RefreshPeriod = TDuration::MilliSeconds(1000);
    ui32 RefreshMajorEvery = 3;
    ui32 RefreshIteration = 0;
    ui32 MovingAverageWindow = 5;

    struct TMetricsSample {
        ui64 GrpcRequestBytes = 0;
        ui64 GrpcResponseBytes = 0;
        float DatabaseCoresUsed = 0;
        ui64 DatabaseMemoryUsed = 0;
        ui64 DatabaseNetworkBytes = 0;
        ui64 DatabaseToStorageBytes = 0;
        ui64 StorageToDatabaseBytes = 0;
        float StorageCoresUsed = 0;
        ui64 StorageMemoryUsed = 0;
        ui64 StorageNetworkBytes = 0;
        ui64 DiskReadBytes = 0;
        ui64 DiskWriteBytes = 0;
        ui64 StorageConsumed = 0;
    };

    std::deque<TMetricsSample> MetricsHistory;
    
    enum class EWakeupTag : ui64 {
        Timeout,
        Refresh,
    };

    struct TEvWakeup : TEvents::TEvWakeup {
        TEvWakeup(EWakeupTag tag) : TEvents::TEvWakeup(static_cast<ui64>(tag)) {}
    };

public:
    TJsonDatabaseStats(IViewer* viewer, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev)
        : TBase(viewer, ev, "/viewer/database_stats")
    {
        StaticStoragePool.MutableInfo()->SetName("");
    }

    void Bootstrap() override {
        if (TBase::NeedToRedirect()) {
            return;
        }
        if (!IsDatabaseRequest()) {
            return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Database is required"), "NoDatabase");
        }
        if (ResourceNavigateResponse && ResourceNavigateResponse->IsOk()) {
            CollectStoragePoolsAllowed(ResourceNavigateResponse->GetRef());
        }
        if (DatabaseNavigateResponse && DatabaseNavigateResponse->IsOk()) {
            CollectStoragePoolsAllowed(DatabaseNavigateResponse->GetRef());
        }

        ConfigureRefreshSettings();

        NodesInfoResponse = MakeRequest<TEvInterconnect::TEvNodesInfo>(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
        PoolsResponse = MakeCachedRequestBSControllerPools();
        GroupsResponse = MakeCachedRequestBSControllerGroups();
        VSlotsResponse = MakeCachedRequestBSControllerVSlots();
        DatabaseNodes = GetDatabaseNodes();
        Result.SetDatabaseNodes(DatabaseNodes.size());
        RequestDatabaseNodes();
        ProcessResponses();

        Send(HttpEvent->Sender, new NHttp::TEvHttpProxy::TEvSubscribeForCancel(), IEventHandle::FlagTrackDelivery);
        if (Streaming = GetRequest().Accepts("text/event-stream")) {
            HttpStream = HttpEvent->Get()->Request->CreateResponseString(Viewer->GetChunkedHTTPOK(GetRequest(), "text/event-stream"));
            Send(HttpEvent->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(HttpStream));
        } else {
            TBase::Schedule(Timeout, new TEvWakeup(EWakeupTag::Timeout));
        }
        Become(&TJsonDatabaseStats::StateWork);
    }

    void CollectStoragePoolsAllowed(const TEvTxProxySchemeCache::TEvNavigateKeySetResult& ev) {
        for (const auto& entry : ev.Request->ResultSet) {
            if (entry.Status == TNavigate::EStatus::Ok && entry.DomainDescription) {
                for (const auto& info : entry.DomainDescription->Description.GetStoragePools()) {
                    StoragePoolNames.insert(info.GetName());
                }
                if (entry.DomainDescription->Description.GetDomainKey().GetSchemeShard() == AppData()->DomainsInfo->GetDomain()->SchemeRoot
                    && entry.DomainDescription->Description.GetDomainKey().GetPathId() == 1) {
                    StoragePoolNames.insert(StaticStoragePool.GetInfo().GetName());
                }
            }
        }
    }

    void ConfigureRefreshSettings() {
        constexpr ui64 kMinRefreshPeriodMs = 100;
        constexpr ui64 kMaxRefreshPeriodMs = 600000;
        if (Params.Has("refresh_period")) {
            const ui64 requested = FromStringWithDefault<ui64>(Params.Get("refresh_period"), RefreshPeriod.MilliSeconds());
            RefreshPeriod = TDuration::MilliSeconds(std::clamp(requested, kMinRefreshPeriodMs, kMaxRefreshPeriodMs));
        }
        if (Params.Has("refresh_major_every")) {
            RefreshMajorEvery = FromStringWithDefault<ui32>(Params.Get("refresh_major_every"), RefreshMajorEvery);
        }
        if (Params.Has("moving_avg_window")) {
            const ui32 requested = FromStringWithDefault<ui32>(Params.Get("moving_avg_window"), MovingAverageWindow);
            MovingAverageWindow = std::clamp<ui32>(requested, 1u, 100u);
        }
    }

    void InitDatabaseNodeWhiteboardRequest(NKikimrWhiteboard::TEvSystemStateRequest& request) {
        request.AddFieldsRequired(NKikimrWhiteboard::TSystemStateInfo::kCoresUsedFieldNumber);
        request.AddFieldsRequired(NKikimrWhiteboard::TSystemStateInfo::kCoresTotalFieldNumber);
        request.AddFieldsRequired(NKikimrWhiteboard::TSystemStateInfo::kMemoryUsedFieldNumber);
        request.AddFieldsRequired(NKikimrWhiteboard::TSystemStateInfo::kMemoryLimitFieldNumber);
        request.AddFieldsRequired(NKikimrWhiteboard::TSystemStateInfo::kGrpcRequestBytesFieldNumber);
        request.AddFieldsRequired(NKikimrWhiteboard::TSystemStateInfo::kGrpcResponseBytesFieldNumber);
    }

    void InitStorageNodeWhiteboardRequest(NKikimrWhiteboard::TEvSystemStateRequest& request) {
        request.AddFieldsRequired(NKikimrWhiteboard::TSystemStateInfo::kCoresUsedFieldNumber);
        request.AddFieldsRequired(NKikimrWhiteboard::TSystemStateInfo::kCoresTotalFieldNumber);
        request.AddFieldsRequired(NKikimrWhiteboard::TSystemStateInfo::kMemoryUsedFieldNumber);
        request.AddFieldsRequired(NKikimrWhiteboard::TSystemStateInfo::kMemoryLimitFieldNumber);
    }

    void InitNodeStateWhiteboardRequest(NKikimrWhiteboard::TEvNodeStateRequest& request) {
        request.AddFieldsRequired(NKikimrWhiteboard::TNodeStateInfo::kConnectedFieldNumber);
        request.AddFieldsRequired(NKikimrWhiteboard::TNodeStateInfo::kPeerNodeIdFieldNumber);
        request.AddFieldsRequired(NKikimrWhiteboard::TNodeStateInfo::kPeerNameFieldNumber);
        request.AddFieldsRequired(NKikimrWhiteboard::TNodeStateInfo::kBytesWrittenFieldNumber);
        request.AddFieldsRequired(NKikimrWhiteboard::TNodeStateInfo::kWriteThroughputFieldNumber);
    }

    void RequestDatabaseNodes() {
        for (TNodeId nodeId : DatabaseNodes) {
            if (SystemStateResponse.count(nodeId) == 0) {
                auto request = std::make_unique<NNodeWhiteboard::TEvWhiteboard::TEvSystemStateRequest>();
                InitDatabaseNodeWhiteboardRequest(request->Record);
                SystemStateResponse[nodeId] = MakeWhiteboardRequest(nodeId, request.release());
            }
            if (NodeStateResponse.count(nodeId) == 0) {
                auto request = std::make_unique<NNodeWhiteboard::TEvWhiteboard::TEvNodeStateRequest>();
                InitNodeStateWhiteboardRequest(request->Record);
                NodeStateResponse[nodeId] = MakeWhiteboardRequest(nodeId, request.release());
            }
        }
    }

    void RequestStorageNodes() {
        for (TNodeId nodeId : StorageNodes) {
            if (SystemStateResponse.count(nodeId) == 0) {
                auto request = std::make_unique<NNodeWhiteboard::TEvWhiteboard::TEvSystemStateRequest>();
                InitStorageNodeWhiteboardRequest(request->Record);
                SystemStateResponse[nodeId] = MakeWhiteboardRequest(nodeId, request.release());
            }
            if (PDiskStateResponse.count(nodeId) == 0) {
                auto request = std::make_unique<NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateRequest>();
                PDiskStateResponse[nodeId] = MakeWhiteboardRequest(nodeId, request.release());
            }
            if (VDiskStateResponse.count(nodeId) == 0) {
                auto request = std::make_unique<NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateRequest>();
                VDiskStateResponse[nodeId] = MakeWhiteboardRequest(nodeId, request.release());
            }
            if (NodeStateResponse.count(nodeId) == 0) {
                auto request = std::make_unique<NNodeWhiteboard::TEvWhiteboard::TEvNodeStateRequest>();
                InitNodeStateWhiteboardRequest(request->Record);
                NodeStateResponse[nodeId] = MakeWhiteboardRequest(nodeId, request.release());
            }
        }
    }

    bool IsPrimaryDataReady() {
        return NodesInfoResponse.IsDone()
            && PoolsResponse.IsDone()
            && GroupsResponse.IsDone()
            && VSlotsResponse.IsDone();
    }

    void ProcessResponses() {
        if (!IsPrimaryDataReady()) {
            return;
        }
        if (NodesInfoResponse.IsError()) {
            NodesInfoResponse = MakeRequest<TEvInterconnect::TEvNodesInfo>(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
            return;
        }
        if (PoolsResponse.IsError()) {
            PoolsResponse = MakeCachedRequestBSControllerPools();
            return;
        }
        if (GroupsResponse.IsError()) {
            GroupsResponse = MakeCachedRequestBSControllerGroups();
            return;
        }
        if (VSlotsResponse.IsError()) {
            VSlotsResponse = MakeCachedRequestBSControllerVSlots();
            return;
        }
        // all primary data is ready and valid
        if (PoolsResponse.IsOk() && !StoragePoolNames.empty() && StoragePools.empty()) {
            for (const auto& entry : PoolsResponse->Record.GetEntries()) {
                const auto& key = entry.GetKey();
                const auto& info = entry.GetInfo();
                if (StoragePoolNames.count(info.GetName()) != 0) {
                    StoragePools.emplace(std::make_pair(key.GetBoxId(), key.GetStoragePoolId()), entry);
                }
            }
        }
        if (GroupsResponse.IsOk() && !StoragePools.empty() && StorageGroups.empty()) {
            for (const auto& entry : GroupsResponse->Record.GetEntries()) {
                const auto& key = entry.GetKey();
                const auto& info = entry.GetInfo();
                if (StoragePools.count({info.GetBoxId(), info.GetStoragePoolId()}) != 0) {
                    StorageGroups.emplace(key.GetGroupId(), entry);
                }
            }
            Result.SetStorageGroups(StorageGroups.size());
        }
        if (VSlotsResponse.IsOk() && !StorageGroups.empty() && StorageNodes.empty()) {
            std::unordered_set<TNodeId> storageNodes;
            for (const auto& vslot : VSlotsResponse->Record.GetEntries()) {
                storageNodes.insert(vslot.GetKey().GetNodeId());
            }
            StorageNodes.assign(storageNodes.begin(), storageNodes.end());
            Result.SetStorageNodes(StorageNodes.size());
            RequestStorageNodes();
        }
    }

    void RefreshMajor() {
        if (ResourceBoardInfoResponse) {
            ResourceBoardInfoResponse = MakeRequestStateStorageEndpointsLookup(SharedDatabase);
        } else {
            DatabaseBoardInfoResponse = MakeRequestStateStorageEndpointsLookup(Database);
        }
    }

    void RefreshMinor() {
        SystemStateResponse.clear();
        NodeStateResponse.clear();
        PDiskStateResponse.clear();
        VDiskStateResponse.clear();
        RequestDatabaseNodes();
        RequestStorageNodes();
    }

    static std::unordered_set<TNodeId> BuildIndex(const std::vector<TNodeId>& array) {
        std::unordered_set<TNodeId> result;
        result.reserve(array.size());
        for (auto id : array) {
            result.insert(id);
        }
        return result;
    }

    void ProcessWhiteboardResponses() {
        auto isDatabaseNode = [databaseNodesIndex = BuildIndex(DatabaseNodes)](TNodeId nodeId) {
            return databaseNodesIndex.count(nodeId) != 0;
        };
        auto isStorageNode = [storageNodesIndex = BuildIndex(StorageNodes)](TNodeId nodeId) {
            return storageNodesIndex.count(nodeId) != 0;
        };
        bool absentDatabaseNodeInfo = false;
        bool incompleteDatabaseStats = false;
        bool incompleteStorageStats = false;
        bool unknownSlotSize = false;
        bool unknownPDisk = false;
        ui64 grpcRequestBytes = 0;
        ui64 grpcResponseBytes = 0;
        ui64 databaseCoresTotal = 0;
        float databaseCoresUsed = 0;
        ui64 databaseMemoryTotal = 0;
        ui64 databaseMemoryUsed = 0;
        ui64 databaseNetworkBytes = 0;
        ui64 databaseToStorageBytes = 0;
        ui64 storageToDatabaseBytes = 0;
        ui64 storageCoresTotal = 0;
        float storageCoresUsed = 0;
        ui64 storageMemoryTotal = 0;
        ui64 storageMemoryUsed = 0;
        ui64 storageNetworkBytes = 0;
        ui64 diskReadBytes = 0;
        ui64 diskWriteBytes = 0;
        ui64 storageTotal = 0;
        ui64 storageConsumed = 0;
        std::unordered_map<TPDiskId, const NKikimrWhiteboard::TPDiskStateInfo&> pDisksIdx;

        Result.ClearProblems();
        for (TNodeId nodeId : DatabaseNodes) {
            if (SystemStateResponse.count(nodeId)) {
                const auto& response(SystemStateResponse[nodeId]);
                if (!response.IsDone() || response.IsError()) {
                    incompleteDatabaseStats = true;
                } else {
                    const auto& record(response->Record.GetSystemStateInfo(0));
                    grpcRequestBytes += record.GetGrpcRequestBytes();
                    grpcResponseBytes += record.GetGrpcResponseBytes();
                    databaseCoresTotal += record.GetCoresTotal();
                    databaseCoresUsed += record.GetCoresUsed();
                    databaseMemoryTotal += record.GetMemoryLimit();
                    databaseMemoryUsed += record.GetMemoryUsed();
                }
            } else {
                absentDatabaseNodeInfo = true;
            }
            {
                const auto& response(NodeStateResponse[nodeId]);
                if (!response.IsDone() || response.IsError()) {
                    incompleteDatabaseStats = true;
                } else {
                    for (const auto& record : response->Record.GetNodeStateInfo()) {
                        if (!record.GetConnected()) {
                            continue;
                        }
                        if (isDatabaseNode(record.GetPeerNodeId())) {
                            databaseNetworkBytes += record.GetWriteThroughput();
                        } else if (isStorageNode(record.GetPeerNodeId())) {
                            databaseToStorageBytes += record.GetWriteThroughput();
                        }
                    }
                }
            }
        }
        for (TNodeId nodeId : StorageNodes) {
            {
                const auto& response(SystemStateResponse[nodeId]);
                if (!response.IsDone() || response.IsError()) {
                    incompleteStorageStats = true;
                } else {
                    const auto& record(response->Record.GetSystemStateInfo(0));
                    storageCoresTotal += record.GetCoresTotal();
                    storageCoresUsed += record.GetCoresUsed();
                    storageMemoryTotal += record.GetMemoryLimit();
                    storageMemoryUsed += record.GetMemoryUsed();
                }
            }
            {
                const auto& response(NodeStateResponse[nodeId]);
                if (!response.IsDone() || response.IsError()) {
                    incompleteStorageStats = true;
                } else {
                    for (const auto& record : response->Record.GetNodeStateInfo()) {
                        if (!record.GetConnected()) {
                            continue;
                        }
                        if (isDatabaseNode(record.GetPeerNodeId())) {
                            storageToDatabaseBytes += record.GetWriteThroughput();
                        } else if (isStorageNode(record.GetPeerNodeId())) {
                            storageNetworkBytes += record.GetWriteThroughput();
                        }
                    }
                }
            }
            {
                const auto& response(PDiskStateResponse[nodeId]);
                if (!response.IsDone() || response.IsError()) {
                    incompleteStorageStats = true;
                } else {
                    for (const auto& record : response->Record.GetPDiskStateInfo()) {
                        pDisksIdx.emplace(std::make_pair(nodeId, record.GetPDiskId()), record);
                    }
                }
            }
            {
                const auto& response(VDiskStateResponse[nodeId]);
                if (!response.IsDone() || response.IsError()) {
                    incompleteStorageStats = true;
                } else {
                    for (const auto& record : response->Record.GetVDiskStateInfo()) {
                        if (StorageGroups.count(record.GetVDiskId().GetGroupID()) == 0) {
                            continue;
                        }
                        diskReadBytes += record.GetReadThroughput();
                        diskWriteBytes += record.GetWriteThroughput();
                        storageConsumed += record.GetAllocatedSize();
                        auto itPDisk = pDisksIdx.find(std::make_pair(nodeId, record.GetPDiskId()));
                        if (itPDisk != pDisksIdx.end()) {
                            auto slotSize = itPDisk->second.GetEnforcedDynamicSlotSize();
                            if (!slotSize) {
                                auto slotCount = itPDisk->second.GetExpectedSlotCount();
                                if (!slotCount) {
                                    unknownSlotSize = true;
                                    storageTotal += record.GetAvailableSize();
                                    continue;
                                }
                                slotSize = itPDisk->second.GetTotalSize() / slotCount;
                            }
                            storageTotal += slotSize;
                        } else {
                            unknownPDisk = true;
                        }
                    }
                }
            }
        }

        if (Streaming) {
            TMetricsSample smoothed{
                .GrpcRequestBytes = grpcRequestBytes,
                .GrpcResponseBytes = grpcResponseBytes,
                .DatabaseCoresUsed = databaseCoresUsed,
                .DatabaseMemoryUsed = databaseMemoryUsed,
                .DatabaseNetworkBytes = databaseNetworkBytes,
                .DatabaseToStorageBytes = databaseToStorageBytes,
                .StorageToDatabaseBytes = storageToDatabaseBytes,
                .StorageCoresUsed = storageCoresUsed,
                .StorageMemoryUsed = storageMemoryUsed,
                .StorageNetworkBytes = storageNetworkBytes,
                .DiskReadBytes = diskReadBytes,
                .DiskWriteBytes = diskWriteBytes,
                .StorageConsumed = storageConsumed,
            };
            ApplyMovingAverage(smoothed);
            grpcRequestBytes = smoothed.GrpcRequestBytes;
            grpcResponseBytes = smoothed.GrpcResponseBytes;
            databaseCoresUsed = smoothed.DatabaseCoresUsed;
            databaseMemoryUsed = smoothed.DatabaseMemoryUsed;
            databaseNetworkBytes = smoothed.DatabaseNetworkBytes;
            databaseToStorageBytes = smoothed.DatabaseToStorageBytes;
            storageToDatabaseBytes = smoothed.StorageToDatabaseBytes;
            storageCoresUsed = smoothed.StorageCoresUsed;
            storageMemoryUsed = smoothed.StorageMemoryUsed;
            storageNetworkBytes = smoothed.StorageNetworkBytes;
            diskReadBytes = smoothed.DiskReadBytes;
            diskWriteBytes = smoothed.DiskWriteBytes;
            storageConsumed = smoothed.StorageConsumed;
        } else if (!MetricsHistory.empty()) {
            MetricsHistory.clear();
        }

        /// set results

        if (absentDatabaseNodeInfo) {
            Result.AddProblems("absent-database-node-info");
        }
        if (incompleteDatabaseStats) {
            Result.AddProblems("incomplete-database-stats");
        }
        if (incompleteStorageStats) {
            Result.AddProblems("incomplete-storage-stats");
        }
        if (unknownSlotSize) {
            Result.AddProblems("unknown-slot-size");
        }
        if (unknownPDisk) {
            Result.AddProblems("unknown-pdisk");
        }
        Result.SetGrpcRequestBytes(grpcRequestBytes);
        Result.SetGrpcResponseBytes(grpcResponseBytes);
        if (databaseCoresTotal > 0) {
            Result.SetDatabaseCpuUsage(databaseCoresUsed / databaseCoresTotal);
        }
        if (databaseMemoryTotal > 0) {
            Result.SetDatabaseMemoryUsage(static_cast<float>(databaseMemoryUsed) / databaseMemoryTotal);
        }
        if (storageCoresTotal > 0) {
            Result.SetStorageCpuUsage(storageCoresUsed / storageCoresTotal);
        }
        if (storageMemoryTotal > 0) {
            Result.SetStorageMemoryUsage(static_cast<float>(storageMemoryUsed) / storageMemoryTotal);
        }
        Result.SetDatabaseNetwork(databaseNetworkBytes);
        Result.SetStorageNetwork(storageNetworkBytes);
        Result.SetStorageNetworkRead(storageToDatabaseBytes);
        Result.SetStorageNetworkWrite(databaseToStorageBytes);
        Result.SetDiskRead(diskReadBytes);
        Result.SetDiskWrite(diskWriteBytes);
        Result.SetStorageTotal(storageTotal);
        Result.SetStorageConsumed(storageConsumed);
    }

    void ApplyMovingAverage(TMetricsSample& metrics) {
        if (!MovingAverageWindow) {
            return;
        }
        MetricsHistory.push_back(metrics);
        while (MetricsHistory.size() > MovingAverageWindow) {
            MetricsHistory.pop_front();
        }
        const size_t count = MetricsHistory.size();
        ui64 sumGrpcRequestBytes = 0;
        ui64 sumGrpcResponseBytes = 0;
        long double sumDatabaseCoresUsed = 0;
        ui64 sumDatabaseMemoryUsed = 0;
        ui64 sumDatabaseNetworkBytes = 0;
        ui64 sumDatabaseToStorageBytes = 0;
        ui64 sumStorageToDatabaseBytes = 0;
        long double sumStorageCoresUsed = 0;
        ui64 sumStorageMemoryUsed = 0;
        ui64 sumStorageNetworkBytes = 0;
        ui64 sumDiskReadBytes = 0;
        ui64 sumDiskWriteBytes = 0;
        ui64 sumStorageConsumed = 0;
        for (const auto& item : MetricsHistory) {
            sumGrpcRequestBytes += item.GrpcRequestBytes;
            sumGrpcResponseBytes += item.GrpcResponseBytes;
            sumDatabaseCoresUsed += item.DatabaseCoresUsed;
            sumDatabaseMemoryUsed += item.DatabaseMemoryUsed;
            sumDatabaseNetworkBytes += item.DatabaseNetworkBytes;
            sumDatabaseToStorageBytes += item.DatabaseToStorageBytes;
            sumStorageToDatabaseBytes += item.StorageToDatabaseBytes;
            sumStorageCoresUsed += item.StorageCoresUsed;
            sumStorageMemoryUsed += item.StorageMemoryUsed;
            sumStorageNetworkBytes += item.StorageNetworkBytes;
            sumDiskReadBytes += item.DiskReadBytes;
            sumDiskWriteBytes += item.DiskWriteBytes;
            sumStorageConsumed += item.StorageConsumed;
        }
        metrics.GrpcRequestBytes = static_cast<ui64>(sumGrpcRequestBytes / count);
        metrics.GrpcResponseBytes = static_cast<ui64>(sumGrpcResponseBytes / count);
        metrics.DatabaseCoresUsed = static_cast<float>(sumDatabaseCoresUsed / count);
        metrics.DatabaseMemoryUsed = static_cast<ui64>(sumDatabaseMemoryUsed / count);
        metrics.DatabaseNetworkBytes = static_cast<ui64>(sumDatabaseNetworkBytes / count);
        metrics.DatabaseToStorageBytes = static_cast<ui64>(sumDatabaseToStorageBytes / count);
        metrics.StorageToDatabaseBytes = static_cast<ui64>(sumStorageToDatabaseBytes / count);
        metrics.StorageCoresUsed = static_cast<float>(sumStorageCoresUsed / count);
        metrics.StorageMemoryUsed = static_cast<ui64>(sumStorageMemoryUsed / count);
        metrics.StorageNetworkBytes = static_cast<ui64>(sumStorageNetworkBytes / count);
        metrics.DiskReadBytes = static_cast<ui64>(sumDiskReadBytes / count);
        metrics.DiskWriteBytes = static_cast<ui64>(sumDiskWriteBytes / count);
        metrics.StorageConsumed = static_cast<ui64>(sumStorageConsumed / count);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(NSysView::TEvSysView::TEvGetStoragePoolsResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetGroupsResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetVSlotsResponse, Handle);
            hFunc(TEvWhiteboard::TEvSystemStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvNodeStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvPDiskStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvVDiskStateResponse, Handle);
            hFunc(TEvStateStorage::TEvBoardInfo, Handle);
            hFunc(TEvWakeup, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
            cFunc(NHttp::TEvHttpProxy::EvRequestCancelled, Cancelled);
        }
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        if (NodesInfoResponse.Set(std::move(ev))) {
            ProcessResponses();
            RequestDone();
        }
    }

    void Handle(NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr& ev) {
        if (PoolsResponse.Set(std::move(ev))) {
            ProcessResponses();
            RequestDone();
        }
    }

    void Handle(NSysView::TEvSysView::TEvGetGroupsResponse::TPtr& ev) {
        if (GroupsResponse.Set(std::move(ev))) {
            ProcessResponses();
            RequestDone();
        }
    }

    void Handle(NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr& ev) {
        if (VSlotsResponse.Set(std::move(ev))) {
            ProcessResponses();
            RequestDone();
        }
    }

    void Handle(TEvWhiteboard::TEvSystemStateResponse::TPtr& ev) {
        ui32 nodeId = ev->Cookie;
        if (SystemStateResponse[nodeId].Set(std::move(ev))) {
            ProcessResponses();
            RequestDone();
        }
    }

    void Handle(TEvWhiteboard::TEvNodeStateResponse::TPtr& ev) {
        ui32 nodeId = ev->Cookie;
        if (NodeStateResponse[nodeId].Set(std::move(ev))) {
            ProcessResponses();
            RequestDone();
        }
    }

    void Handle(TEvWhiteboard::TEvPDiskStateResponse::TPtr& ev) {
        ui32 nodeId = ev->Cookie;
        if (PDiskStateResponse[nodeId].Set(std::move(ev))) {
            ProcessResponses();
            RequestDone();
        }
    }

    void Handle(TEvWhiteboard::TEvVDiskStateResponse::TPtr& ev) {
        ui32 nodeId = ev->Cookie;
        if (VDiskStateResponse[nodeId].Set(std::move(ev))) {
            ProcessResponses();
            RequestDone();
        }
    }

    void Handle(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
        if (ResourceBoardInfoResponse) {
            ResourceBoardInfoResponse->Set(std::move(ev));
        } else {
            DatabaseBoardInfoResponse->Set(std::move(ev));
        }
        StoragePools.clear();
        StorageGroups.clear();
        StorageNodes.clear();
        SystemStateResponse.clear();
        NodeStateResponse.clear();
        PDiskStateResponse.clear();
        VDiskStateResponse.clear();
        DatabaseNodes = GetDatabaseNodes();
        Result.SetDatabaseNodes(DatabaseNodes.size());
        RequestDatabaseNodes();
        NodesInfoResponse = MakeRequest<TEvInterconnect::TEvNodesInfo>(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
        PoolsResponse = MakeCachedRequestBSControllerPools();
        GroupsResponse = MakeCachedRequestBSControllerGroups();
        VSlotsResponse = MakeCachedRequestBSControllerVSlots();
        RequestDone();
    }

    void Handle(TEvWakeup::TPtr& ev) {
        switch (static_cast<EWakeupTag>(ev->Get()->Tag)) {
            case EWakeupTag::Timeout:
                return ReplyAndPassAway(GetHTTPGATEWAYTIMEOUT(), "Timeout");
            case EWakeupTag::Refresh:
                if (RefreshMajorEvery && ++RefreshIteration >= RefreshMajorEvery) {
                    RefreshIteration = 0;
                    return RefreshMajor();
                } else {
                    return RefreshMinor();
                }
        }
    }

    void HandleNodeError(TNodeId nodeId, const TString& error) {
        if (SystemStateResponse.count(nodeId) && SystemStateResponse[nodeId].Error(error)) {
            ProcessResponses();
            RequestDone();
        }
        if (NodeStateResponse.count(nodeId) && NodeStateResponse[nodeId].Error(error)) {
            ProcessResponses();
            RequestDone();
        }
        if (PDiskStateResponse.count(nodeId) && PDiskStateResponse[nodeId].Error(error)) {
            ProcessResponses();
            RequestDone();
        }
        if (VDiskStateResponse.count(nodeId) && VDiskStateResponse[nodeId].Error(error)) {
            ProcessResponses();
            RequestDone();
        }
    }

    void Cancelled() {
        PassAway();
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Get()->SourceType == NHttp::TEvHttpProxy::EvSubscribeForCancel) {
            return Cancelled();
        }
        static const TString error = "Undelivered";
        TNodeId nodeId = ev->Cookie;
        HandleNodeError(nodeId, error);
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        static const TString error = "NodeDisconnected";
        TNodeId nodeId = ev->Get()->NodeId;
        HandleNodeError(nodeId, error);
    }

    void ReplyAndPassAway() override {
        AddEvent("Reply");
        ProcessWhiteboardResponses();
        if (Streaming) {
            TStringStream content;
            content << "event: DatabaseStats\n";
            content << "data: ";
            Proto2Json(Result, content);
            content << "\n\n";
            auto dataChunk = HttpStream->CreateDataChunk(content.Str());
            Send(HttpEvent->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(dataChunk));
            Schedule(RefreshPeriod, new TEvWakeup(EWakeupTag::Refresh));
            // continue to live for streaming
        } else {
            TBase::ReplyAndPassAway(TBase::GetHTTPOKJSON(Result));
        }
    }

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "Database stats",
            .Description = "Returns database stats"
        });
        return yaml;
    }
};

}
