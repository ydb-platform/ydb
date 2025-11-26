#pragma once
#include "json_handlers.h"
#include "json_pipe_req.h"
#include "log.h"
#include "viewer.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/protos/table_stats.pb.h>

namespace NKikimr::NViewer {

using namespace NActors;
using namespace NKikimrTabletBase;
namespace TEvSchemeShard = NSchemeShard::TEvSchemeShard;
using TNavigate = NSchemeCache::TSchemeCacheNavigate;

class TJsonStorageStats : public TViewerPipeClient {
    using TThis = TJsonStorageStats;
    using TBase = TViewerPipeClient;
    using TBase::ReplyAndPassAway;
    using TStoragePoolId = std::pair<ui64, ui64>; // box : storage pool id
    using TGroupId = ui32;

    struct TPathStorageInfo {
        ui64 DataSize = 0;
        ui64 IndexSize = 0;
        std::vector<TTabletId> Tablets;
    };

    struct TGroupStorageInfo {
        ui64 StorageSize = 0;
        ui64 StorageCount = 0;
    };

    struct TTabletStorageInfo {
        TTabletTypes::EType Type = TTabletTypes::Unknown;
        ui64 DataSize = 0;
        ui64 IndexSize = 0;
        std::unordered_map<TGroupId, TGroupStorageInfo> Groups;
    };

    struct TVDiskRequestInfo {
        TRequestResponse<TEvGetLogoBlobIndexStatResponse> VDiskRequest;
        TGroupId GroupId;

        TVDiskRequestInfo(TRequestResponse<TEvGetLogoBlobIndexStatResponse>&& vdiskRequest, TGroupId groupId)
            : VDiskRequest(std::move(vdiskRequest)), GroupId(groupId)
        {}
    };

    std::vector<TString> Paths;
    std::unordered_set<TString> StoragePoolNames;
    NKikimrSysView::TStoragePoolEntry StaticStoragePool;
    std::unordered_map<TStoragePoolId, const NKikimrSysView::TStoragePoolEntry&> StoragePools;
    std::unordered_map<TGroupId, const NKikimrSysView::TGroupEntry&> StorageGroups;
    TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse> VSlotsResponse;
    TRequestResponse<NSysView::TEvSysView::TEvGetGroupsResponse> GroupsResponse;
    TRequestResponse<NSysView::TEvSysView::TEvGetStoragePoolsResponse> PoolsResponse;
    std::unordered_map<TString, TRequestResponse<TEvSchemeShard::TEvDescribeSchemeResult>> SchemeShardResult;
    std::unordered_map<TGroupId, std::vector<TActorId>> GroupToVDiskActorId;
    std::vector<TVDiskRequestInfo> VDiskRequests;
    std::unordered_map<TActorId, size_t> VDiskRequestIndex;
    std::unordered_map<TTabletId, TTabletStorageInfo> TabletStorageInfo;
    std::unordered_map<TString, TPathStorageInfo> PathStorageInfo;

public:
    TJsonStorageStats(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {
        StaticStoragePool.MutableInfo()->SetName("");
    }

    TString MakeFullPath(const TString& path) {
        if (Database.empty() || path.StartsWith("/")) {
            return path;
        }
        if (path.empty() || path == ".") {
            return Database;
        }
        return Database + "/" + path;
    }

    void RequestSchemeShard(const TString& path) {
        THolder<TEvTxUserProxy::TEvNavigate> request = MakeHolder<TEvTxUserProxy::TEvNavigate>();
        auto tokenObj = GetRequest().GetUserTokenObject();
        if (tokenObj) {
            request->Record.SetUserToken(tokenObj);
        }
        request->Record.MutableDescribePath()->SetPath(path);
        request->Record.MutableDescribePath()->MutableOptions()->SetReturnPartitioningInfo(true);
        request->Record.MutableDescribePath()->MutableOptions()->SetReturnPartitionStats(true);
        auto& ssReq = SchemeShardResult[path] = MakeRequest<TEvSchemeShard::TEvDescribeSchemeResult>(MakeTxProxyID(), request.Release());
        ssReq.Span.Attribute("path", path);
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

    void Bootstrap() override {
        if (NeedToRedirect()) {
            return;
        }
        if (!IsDatabaseRequest()) {
            return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Database is required"));
        }
        std::unordered_set<TString> uniquePaths;
        for (const auto& path : Params.Range("path")) {
            for (const auto& subPath : StringSplitter(path).Split(',').ToList<TString>()) {
                if (uniquePaths.emplace(subPath).second) {
                    Paths.push_back(subPath);
                }
            }
        }
        if (PostData.Has("path")) {
            const NJson::TJsonValue& pathsJson = PostData["path"];
            if (pathsJson.IsString()) {
                const TString& path = pathsJson.GetStringRobust();
                if (uniquePaths.emplace(path).second) {
                    Paths.push_back(path);
                }
            } else if (pathsJson.IsArray()) {
                for (const auto& pathJson : pathsJson.GetArray()) {
                    if (pathJson.IsString()) {
                        const TString& path = pathJson.GetStringRobust();
                        if (uniquePaths.emplace(path).second) {
                            Paths.push_back(path);
                        }
                    }
                }
            }
        }
        if (Paths.empty()) {
            return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "No path specified"));
        }
        for (const auto& path : Paths) {
            if (path.StartsWith("/") && !path.StartsWith(Database)) {
                return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Invalid path specified"));
           }
        }
        if (ResourceNavigateResponse && ResourceNavigateResponse->IsOk()) {
            CollectStoragePoolsAllowed(ResourceNavigateResponse->GetRef());
        }
        if (DatabaseNavigateResponse && DatabaseNavigateResponse->IsOk()) {
            CollectStoragePoolsAllowed(DatabaseNavigateResponse->GetRef());
        }
        if (StoragePoolNames.empty()) {
            return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "No storage pools found"));
        }
        VSlotsResponse = MakeCachedRequestBSControllerVSlots();
        GroupsResponse = MakeCachedRequestBSControllerGroups();
        if (StoragePoolNames.size() != 1 || *StoragePoolNames.begin() != StaticStoragePool.GetInfo().GetName()) {
            PoolsResponse = MakeCachedRequestBSControllerPools();
        } else {
            StoragePools.emplace(std::make_pair(StaticStoragePool.GetKey().GetBoxId(), StaticStoragePool.GetKey().GetStoragePoolId()), StaticStoragePool);
        }
        for (const auto& path : Paths) {
            RequestSchemeShard(MakeFullPath(path));
        }
        ProcessResponses(); // for cached responses
        Become(&TThis::StateRequestedDescribe, Timeout, new TEvents::TEvWakeup());
    }

    void ProcessDescribe(const TEvSchemeShard::TEvDescribeSchemeResult& describeResult) {
        const auto& record(describeResult.GetRecord());
        TString path = record.GetPath();
        auto& pathStorageInfo(PathStorageInfo[path]);
        const auto& pathDescription(record.GetPathDescription());
        if (pathDescription.HasTableStats()) {
            pathStorageInfo.DataSize = pathDescription.GetTableStats().GetDataSize();
            pathStorageInfo.IndexSize = pathDescription.GetTableStats().GetIndexSize();
        }
        for (size_t idx = 0; idx < pathDescription.TablePartitionsSize(); ++idx) {
            const auto& partition = pathDescription.GetTablePartitions(idx);
            TTabletId tabletId = partition.GetDatashardId();
            pathStorageInfo.Tablets.push_back(tabletId);
            auto& tabletStorageInfo = TabletStorageInfo[tabletId];
            tabletStorageInfo.Type = TTabletTypes::DataShard;
            if (idx < pathDescription.TablePartitionStatsSize()) {
                const auto& stats = pathDescription.GetTablePartitionStats(idx);
                tabletStorageInfo.DataSize += stats.GetDataSize();
                tabletStorageInfo.IndexSize += stats.GetIndexSize();
            }
        }
        for (auto tabletId : pathDescription.GetColumnTableDescription().GetSharding().GetColumnShards()) {
            pathStorageInfo.Tablets.push_back(tabletId);
            TabletStorageInfo[tabletId].Type = TTabletTypes::ColumnShard;
        }
        for (auto tabletId : pathDescription.GetColumnStoreDescription().GetColumnShards()) {
            pathStorageInfo.Tablets.push_back(tabletId);
            TabletStorageInfo[tabletId].Type = TTabletTypes::ColumnShard;
        }
        if (pathDescription.HasPersQueueGroup()) {
            for (const auto& partition : pathDescription.GetPersQueueGroup().GetPartitions()) {
                TTabletId tabletId = partition.GetTabletId();
                pathStorageInfo.Tablets.push_back(tabletId);
                TabletStorageInfo[tabletId].Type = TTabletTypes::PersQueue;
            }
            TTabletId tabletId = pathDescription.GetPersQueueGroup().GetBalancerTabletID();
            pathStorageInfo.Tablets.push_back(tabletId);
            TabletStorageInfo[tabletId].Type = TTabletTypes::PersQueueReadBalancer;
        }
        if (pathDescription.HasRtmrVolumeDescription()) {
            for (const auto& partition : pathDescription.GetRtmrVolumeDescription().GetPartitions()) {
                TTabletId tabletId = partition.GetTabletId();
                pathStorageInfo.Tablets.push_back(tabletId);
                TabletStorageInfo[tabletId].Type = TTabletTypes::RTMRPartition;
            }
        }
        if (pathDescription.HasBlockStoreVolumeDescription()) {
            for (const auto& partition : pathDescription.GetBlockStoreVolumeDescription().GetPartitions()) {
                TTabletId tabletId = partition.GetTabletId();
                pathStorageInfo.Tablets.push_back(tabletId);
                TabletStorageInfo[tabletId].Type = TTabletTypes::BlockStorePartition;
            }
            if (pathDescription.GetBlockStoreVolumeDescription().HasVolumeTabletId()) {
                TTabletId tabletId = pathDescription.GetBlockStoreVolumeDescription().GetVolumeTabletId();
                pathStorageInfo.Tablets.push_back(tabletId);
                TabletStorageInfo[tabletId].Type = TTabletTypes::BlockStoreVolume;
            }
        }
        if (pathDescription.GetKesus().HasKesusTabletId()) {
            TTabletId tabletId = pathDescription.GetKesus().GetKesusTabletId();
            pathStorageInfo.Tablets.push_back(tabletId);
            TabletStorageInfo[tabletId].Type = TTabletTypes::Kesus;
        }
        if (pathDescription.HasSolomonDescription()) {
            for (const auto& partition : pathDescription.GetSolomonDescription().GetPartitions()) {
                TTabletId tabletId = partition.GetTabletId();
                pathStorageInfo.Tablets.push_back(tabletId);
                TabletStorageInfo[tabletId].Type = TTabletTypes::KeyValue;
            }
        }
        if (pathDescription.GetFileStoreDescription().HasIndexTabletId()) {
            TTabletId tabletId = pathDescription.GetFileStoreDescription().GetIndexTabletId();
            pathStorageInfo.Tablets.push_back(tabletId);
            TabletStorageInfo[tabletId].Type = TTabletTypes::FileStore;
        }
        if (pathDescription.GetSequenceDescription().HasSequenceShard()) {
            TTabletId tabletId = pathDescription.GetSequenceDescription().GetSequenceShard();
            pathStorageInfo.Tablets.push_back(tabletId);
            TabletStorageInfo[tabletId].Type = TTabletTypes::SequenceShard;
        }
        if (pathDescription.GetReplicationDescription().HasControllerId()) {
            TTabletId tabletId = pathDescription.GetReplicationDescription().GetControllerId();
            pathStorageInfo.Tablets.push_back(tabletId);
            TabletStorageInfo[tabletId].Type = TTabletTypes::ReplicationController;
        }
        if (pathDescription.GetBlobDepotDescription().HasTabletId()) {
            TTabletId tabletId = pathDescription.GetBlobDepotDescription().GetTabletId();
            pathStorageInfo.Tablets.push_back(tabletId);
            TabletStorageInfo[tabletId].Type = TTabletTypes::BlobDepot;
        }

        if (pathDescription.GetSelf().GetPathType() == NKikimrSchemeOp::EPathType::EPathTypeDir
            || pathDescription.GetSelf().GetPathType() == NKikimrSchemeOp::EPathType::EPathTypeSubDomain
            || pathDescription.GetSelf().GetPathType() == NKikimrSchemeOp::EPathType::EPathTypeExtSubDomain) {
            if (record.GetPathId() == 1 && pathDescription.HasDomainDescription()) {
                const auto& domainDescription(pathDescription.GetDomainDescription());
                for (TTabletId tabletId : domainDescription.GetProcessingParams().GetCoordinators()) {
                    pathStorageInfo.Tablets.push_back(tabletId);
                    TabletStorageInfo[tabletId].Type = NKikimrTabletBase::TTabletTypes::Coordinator;
                }
                for (TTabletId tabletId : domainDescription.GetProcessingParams().GetMediators()) {
                    pathStorageInfo.Tablets.push_back(tabletId);
                    TabletStorageInfo[tabletId].Type = NKikimrTabletBase::TTabletTypes::Mediator;
                }
                if (domainDescription.GetProcessingParams().HasSchemeShard()) {
                    pathStorageInfo.Tablets.push_back(domainDescription.GetProcessingParams().GetSchemeShard());
                    TabletStorageInfo[domainDescription.GetProcessingParams().GetSchemeShard()].Type = NKikimrTabletBase::TTabletTypes::SchemeShard;
                }
                if (domainDescription.GetProcessingParams().HasHive()) {
                    pathStorageInfo.Tablets.push_back(domainDescription.GetProcessingParams().GetHive());
                    TabletStorageInfo[domainDescription.GetProcessingParams().GetHive()].Type = NKikimrTabletBase::TTabletTypes::Hive;
                }
                if (domainDescription.GetProcessingParams().HasGraphShard()) {
                    pathStorageInfo.Tablets.push_back(domainDescription.GetProcessingParams().GetGraphShard());
                    TabletStorageInfo[domainDescription.GetProcessingParams().GetGraphShard()].Type = NKikimrTabletBase::TTabletTypes::GraphShard;
                }
                if (domainDescription.GetProcessingParams().HasSysViewProcessor()) {
                    pathStorageInfo.Tablets.push_back(domainDescription.GetProcessingParams().GetSysViewProcessor());
                    TabletStorageInfo[domainDescription.GetProcessingParams().GetSysViewProcessor()].Type = NKikimrTabletBase::TTabletTypes::SysViewProcessor;
                }
                if (domainDescription.GetProcessingParams().HasStatisticsAggregator()) {
                    pathStorageInfo.Tablets.push_back(domainDescription.GetProcessingParams().GetStatisticsAggregator());
                    TabletStorageInfo[domainDescription.GetProcessingParams().GetStatisticsAggregator()].Type = NKikimrTabletBase::TTabletTypes::StatisticsAggregator;
                }
                if (domainDescription.GetProcessingParams().HasBackupController()) {
                    pathStorageInfo.Tablets.push_back(domainDescription.GetProcessingParams().GetBackupController());
                    TabletStorageInfo[domainDescription.GetProcessingParams().GetBackupController()].Type = NKikimrTabletBase::TTabletTypes::BackupController;
                }
                TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
                auto* domain = domains->GetDomain();
                if (record.GetPathOwnerId() == domain->SchemeRoot) {
                    pathStorageInfo.Tablets.push_back(domain->SchemeRoot);
                    TabletStorageInfo[domain->SchemeRoot].Type = NKikimrTabletBase::TTabletTypes::SchemeShard;
                    pathStorageInfo.Tablets.push_back(domains->GetHive());
                    TabletStorageInfo[domains->GetHive()].Type = NKikimrTabletBase::TTabletTypes::Hive;
                    pathStorageInfo.Tablets.push_back(MakeBSControllerID());
                    TabletStorageInfo[MakeBSControllerID()].Type = NKikimrTabletBase::TTabletTypes::BSController;
                    pathStorageInfo.Tablets.push_back(MakeConsoleID());
                    TabletStorageInfo[MakeConsoleID()].Type = NKikimrTabletBase::TTabletTypes::Console;
                    pathStorageInfo.Tablets.push_back(MakeNodeBrokerID());
                    TabletStorageInfo[MakeNodeBrokerID()].Type = NKikimrTabletBase::TTabletTypes::NodeBroker;
                    pathStorageInfo.Tablets.push_back(MakeTenantSlotBrokerID());
                    TabletStorageInfo[MakeTenantSlotBrokerID()].Type = NKikimrTabletBase::TTabletTypes::TenantSlotBroker;
                    pathStorageInfo.Tablets.push_back(MakeCmsID());
                    TabletStorageInfo[MakeCmsID()].Type = NKikimrTabletBase::TTabletTypes::Cms;
                    for (TTabletId tabletId : domain->Coordinators) {
                        pathStorageInfo.Tablets.push_back(tabletId);
                        TabletStorageInfo[tabletId].Type = NKikimrTabletBase::TTabletTypes::Coordinator;
                    }
                    for (TTabletId tabletId : domain->Mediators) {
                        pathStorageInfo.Tablets.push_back(tabletId);
                        TabletStorageInfo[tabletId].Type = NKikimrTabletBase::TTabletTypes::Mediator;
                    }
                    for (TTabletId tabletId : domain->TxAllocators) {
                        pathStorageInfo.Tablets.push_back(tabletId);
                        TabletStorageInfo[tabletId].Type = NKikimrTabletBase::TTabletTypes::TxAllocator;
                    }
                }
                std::ranges::sort(pathStorageInfo.Tablets);
                auto duplicates = std::ranges::unique(pathStorageInfo.Tablets);
                pathStorageInfo.Tablets.erase(duplicates.begin(), duplicates.end());
            }
        }
    }

    void ProcessResponses() {
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
        }
        if (VSlotsResponse.IsOk() && !StorageGroups.empty() && GroupToVDiskActorId.empty()) {
            size_t vdisks = 0;
            for (const auto& vslot : VSlotsResponse->Record.GetEntries()) {
                if (StorageGroups.count(vslot.GetInfo().GetGroupId()) == 0) {
                    continue;
                }
                GroupToVDiskActorId[vslot.GetInfo().GetGroupId()].emplace_back(MakeBlobStorageVDiskID(vslot.GetKey().GetNodeId(), vslot.GetKey().GetPDiskId(), vslot.GetKey().GetVSlotId()));
                ++vdisks;
            }
            VDiskRequests.reserve(vdisks);
        }
        if (!GroupToVDiskActorId.empty() && VDiskRequests.empty()) {
            for (const auto& [groupId, vdiskActorIds] : GroupToVDiskActorId) {
                for (const auto& vdiskActorId : vdiskActorIds) {
                    size_t requestIndex = VDiskRequests.size();
                    if (VDiskRequestIndex.emplace(vdiskActorId, requestIndex).second) {
                        VDiskRequests.emplace_back(MakeRequest<TEvGetLogoBlobIndexStatResponse>(vdiskActorId, new TEvGetLogoBlobIndexStatRequest(), 0, requestIndex), groupId);
                    }
                }
            }
        }
    }

    void ProcessVDiskResponse(size_t requestIndex) {
        const auto& vDiskInfo(VDiskRequests[requestIndex]);
        for (const auto& record : vDiskInfo.VDiskRequest->Record.stat().tablets()) {
            TTabletId tabletId = record.tablet_id();
            auto& tabletStorageInfo(TabletStorageInfo[tabletId]);
            for (const auto& channel : record.channels()) {
                auto& groupStorageInfo(tabletStorageInfo.Groups[vDiskInfo.GroupId]);
                groupStorageInfo.StorageSize += channel.data_size();
                groupStorageInfo.StorageCount += channel.count();
            }
        }
    }

    STATEFN(StateRequestedDescribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NSysView::TEvSysView::TEvGetVSlotsResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetGroupsResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetStoragePoolsResponse, Handle);
            hFunc(TEvGetLogoBlobIndexStatResponse, Handle);
            hFunc(TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, TBase::Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
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

    void Handle(TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        auto& result(SchemeShardResult[ev->Get()->GetRecord().GetPath()]);
        if (result.Set(std::move(ev))) {
            if (result.IsOk()) {
                ProcessDescribe(result.GetRef());
            }
            RequestDone();
        }
    }

    void Handle(TEvGetLogoBlobIndexStatResponse::TPtr& ev) {
        if (ev->Cookie < VDiskRequests.size()) {
            if (VDiskRequests[ev->Cookie].VDiskRequest.Set(std::move(ev))) {
                ProcessVDiskResponse(ev->Cookie);
                RequestDone();
            }
        } else {
            AddEvent("Unknown TEvGetLogoBlobIndexStatResponse");
        }
    }

    void ReplyAndPassAway() override {
        bool returnEverything = FromStringWithDefault<bool>(Params.Get("everything"), false);
        bool returnGroups = FromStringWithDefault<bool>(Params.Get("groups"), returnEverything);
        bool returnTablets = FromStringWithDefault<bool>(Params.Get("tablets"), returnEverything);
        bool returnMedia = FromStringWithDefault<bool>(Params.Get("media"), returnEverything);
        NJson::TJsonValue json;
        NJson::TJsonValue& jsonPaths(json["Paths"]);
        for (const auto& path : Paths) {
            NJson::TJsonValue& jsonPath(jsonPaths.AppendValue({}));
            auto fullPath(MakeFullPath(path));
            jsonPath["Path"] = path ? path : fullPath;
            jsonPath["FullPath"] = fullPath;
            const auto& pathStorageInfo(PathStorageInfo[fullPath]);
            ui64 dataSize = 0;
            ui64 indexSize = 0;
            ui64 storageSize = 0;
            ui64 storageCount = 0;
            NJson::TJsonValue& jsonTablets(jsonPath["Tablets"]);
            if (returnTablets) {
                jsonTablets.SetType(NJson::JSON_ARRAY);
            }
            std::map<TGroupId, TGroupStorageInfo> groupsAccumulated;
            std::map<TString, TGroupStorageInfo> mediaAccumulated;
            for (TTabletId tabletId : pathStorageInfo.Tablets) {
                const auto& tabletStorageInfo(TabletStorageInfo[tabletId]);
                dataSize += tabletStorageInfo.DataSize;
                indexSize += tabletStorageInfo.IndexSize;
                ui64 shardStorageSize = 0;
                ui64 shardStorageCount = 0;
                for (const auto& [groupId, groupStorageInfo] : tabletStorageInfo.Groups) {
                    auto& groupAccumulated = groupsAccumulated[groupId];
                    groupAccumulated.StorageSize += groupStorageInfo.StorageSize;
                    groupAccumulated.StorageCount += groupStorageInfo.StorageCount;
                    const auto& groupInfo(StorageGroups.find(groupId)->second.GetInfo());
                    TStoragePoolId poolId(groupInfo.GetBoxId(), groupInfo.GetStoragePoolId());
                    const auto& poolInfo(StoragePools.find(poolId)->second.GetInfo());
                    auto& mediaAccumulatedEntry = mediaAccumulated[poolInfo.GetKind()];
                    mediaAccumulatedEntry.StorageSize += groupStorageInfo.StorageSize;
                    mediaAccumulatedEntry.StorageCount += groupStorageInfo.StorageCount;
                    shardStorageSize += groupStorageInfo.StorageSize;
                    shardStorageCount += groupStorageInfo.StorageCount;
                    storageSize += groupStorageInfo.StorageSize;
                    storageCount += groupStorageInfo.StorageCount;
                }
                if (!returnTablets) {
                    continue;
                }
                NJson::TJsonValue& jsonTablet(jsonTablets.AppendValue({}));
                jsonTablet["Type"] = TTabletTypes::TypeToStr(tabletStorageInfo.Type);
                jsonTablet["TabletId"] = TStringBuilder() << tabletId;
                if (tabletStorageInfo.DataSize) {
                    jsonTablet["DataSize"] = tabletStorageInfo.DataSize;
                }
                if (tabletStorageInfo.IndexSize) {
                    jsonTablet["IndexSize"] = tabletStorageInfo.IndexSize;
                }
                jsonTablet["StorageSize"] = shardStorageSize;
                jsonTablet["StorageCount"] = shardStorageCount;
            }
            if (dataSize == 0) {
                dataSize = pathStorageInfo.DataSize;
                indexSize = pathStorageInfo.IndexSize;
            }
            if (dataSize) {
                jsonPath["DataSize"] = dataSize;
            }
            if (indexSize) {
                jsonPath["IndexSize"] = indexSize;
            }
            jsonPath["StorageSize"] = storageSize;
            jsonPath["StorageCount"] = storageCount;
            if (!returnTablets) {
                jsonTablets = pathStorageInfo.Tablets.size();
            }
            NJson::TJsonValue& jsonGroups(jsonPath["Groups"]);
            if (returnGroups) {
                jsonGroups.SetType(NJson::JSON_ARRAY);
                for (const auto& [groupId, groupStorageInfo] : groupsAccumulated) {
                    NJson::TJsonValue& jsonGroup(jsonGroups.AppendValue({}));
                    jsonGroup["GroupId"] = TStringBuilder() << groupId;
                    jsonGroup["StorageSize"] = groupStorageInfo.StorageSize;
                    jsonGroup["StorageCount"] = groupStorageInfo.StorageCount;
                }
            } else {
                jsonGroups = groupsAccumulated.size();
            }
            NJson::TJsonValue& jsonMedia(jsonPath["Media"]);
            if (returnMedia) {
                jsonMedia.SetType(NJson::JSON_ARRAY);
                for (const auto& [mediaKind, groupStorageInfo] : mediaAccumulated) {
                    NJson::TJsonValue& jsonMediaEntry(jsonMedia.AppendValue({}));
                    jsonMediaEntry["Kind"] = mediaKind;
                    jsonMediaEntry["StorageSize"] = groupStorageInfo.StorageSize;
                    jsonMediaEntry["StorageCount"] = groupStorageInfo.StorageCount;
                }
            } else {
                jsonMedia = mediaAccumulated.size();
            }
        }
        ReplyAndPassAway(GetHTTPOKJSON(json));
    }

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "Schema storage stats",
            .Description = "Returns detailed information about schema storage"
        });
        yaml.AddParameter({
            .Name = "path",
            .Description = "schema path, could be many paths separated by comma or multiple path parameters",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "everything",
            .Description = "return everything (groups, tablets, media)",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "groups",
            .Description = "return storage groups info",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "tablets",
            .Description = "return tablets info",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "media",
            .Description = "return media kind info",
            .Type = "boolean",
        });
        yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<NKikimrViewer::TEvDescribeSchemeInfo>());
        return yaml;
    }
};

}
