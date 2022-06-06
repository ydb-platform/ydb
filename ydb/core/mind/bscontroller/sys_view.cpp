#include "sys_view.h"
#include "group_geometry_info.h"

namespace NKikimr::NBsController {

using namespace NSysView;

TPDiskId TransformKey(const NKikimrSysView::TPDiskKey& key) {
    return TPDiskId(key.GetNodeId(), key.GetPDiskId());
}

void FillKey(NKikimrSysView::TPDiskKey* key, const TPDiskId& id) {
    key->SetNodeId(id.NodeId);
    key->SetPDiskId(id.PDiskId);
}

TVSlotId TransformKey(const NKikimrSysView::TVSlotKey& key) {
    return TVSlotId(key.GetNodeId(), key.GetPDiskId(), key.GetVSlotId());
}

void FillKey(NKikimrSysView::TVSlotKey* key, const TVSlotId& id) {
    key->SetNodeId(id.NodeId);
    key->SetPDiskId(id.PDiskId);
    key->SetVSlotId(id.VSlotId);
}

TGroupId TransformKey(const NKikimrSysView::TGroupKey& key) {
    return key.GetGroupId();
}

void FillKey(NKikimrSysView::TGroupKey* key, const TGroupId& id) {
    key->SetGroupId(id);
}

TBlobStorageController::TBoxStoragePoolId TransformKey(const NKikimrSysView::TStoragePoolKey& key) {
    return std::make_tuple(key.GetBoxId(), key.GetStoragePoolId());
}

void FillKey(NKikimrSysView::TStoragePoolKey* key, const TBlobStorageController::TBoxStoragePoolId& id) {
    key->SetBoxId(std::get<0>(id));
    key->SetStoragePoolId(std::get<1>(id));
}

struct TGroupDiskInfo {
    const NKikimrBlobStorage::TPDiskMetrics *PDiskMetrics;
    const NKikimrBlobStorage::TVDiskMetrics *VDiskMetrics;
    ui32 ExpectedSlotCount;
};

void CalculateGroupUsageStats(NKikimrSysView::TGroupInfo *info, const std::vector<TGroupDiskInfo>& disks,
        TBlobStorageGroupType type) {
    ui64 allocatedSize = 0;
    ui64 totalSize = 0;
    for (const TGroupDiskInfo& disk : disks) {
        const auto& metrics = *disk.VDiskMetrics;
        if (metrics.HasAllocatedSize()) {
            allocatedSize = Max(allocatedSize, metrics.GetAllocatedSize());
        }

        const auto& pdiskMetrics = *disk.PDiskMetrics;
        ui64 slotSize = 0;
        if (pdiskMetrics.HasEnforcedDynamicSlotSize()) {
            slotSize = pdiskMetrics.GetEnforcedDynamicSlotSize();
        } else if (pdiskMetrics.GetTotalSize()) {
            slotSize = pdiskMetrics.GetTotalSize() / disk.ExpectedSlotCount;
        }

        if (slotSize) {
            totalSize = Min(totalSize ? totalSize : Max<ui64>(), slotSize);
        }
    }
    const ui64 a = totalSize * disks.size() * type.DataParts() / type.TotalPartCount();
    const ui64 b = allocatedSize * disks.size() * type.DataParts() / type.TotalPartCount();
    info->SetAllocatedSize(b);
    info->SetAvailableSize(b < a ? a - b : 0);
}

class TSystemViewsCollector : public TActor<TSystemViewsCollector> {
    TControllerSystemViewsState State;
    std::optional<std::vector<NKikimrSysView::TStorageStatsEntry>> StorageStats;
    std::vector<std::pair<TPDiskId, const NKikimrSysView::TPDiskInfo*>> PDiskIndex;
    std::vector<std::pair<TVSlotId, const NKikimrSysView::TVSlotInfo*>> VSlotIndex;
    std::vector<std::pair<TGroupId, const NKikimrSysView::TGroupInfo*>> GroupIndex;
    std::vector<std::pair<TBlobStorageController::TBoxStoragePoolId, const NKikimrSysView::TStoragePoolInfo*>> StoragePoolIndex;
    TBlobStorageController::THostRecordMap HostRecords;
    ui32 GroupReserveMin = 0;
    ui32 GroupReservePart = 0;
    NMonitoring::TDynamicCounterPtr Counters;
    std::unordered_set<std::tuple<TString>> PDiskFilterCounters;
    std::unordered_set<std::tuple<TString, TString>> ErasureCounters;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BSC_SYSTEM_VIEWS_COLLECTOR;
    }

    TSystemViewsCollector(NMonitoring::TDynamicCounterPtr counters)
        : TActor(&TSystemViewsCollector::StateWork)
        , Counters(std::move(counters))
    {}

    ~TSystemViewsCollector() {
        Counters->RemoveSubgroup("subsystem", "storage_stats");
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvControllerUpdateSystemViews, Handle);
        hFunc(TEvSysView::TEvGetPDisksRequest, Handle);
        hFunc(TEvSysView::TEvGetVSlotsRequest, Handle);
        hFunc(TEvSysView::TEvGetGroupsRequest, Handle);
        hFunc(TEvSysView::TEvGetStoragePoolsRequest, Handle);
        hFunc(TEvSysView::TEvGetStorageStatsRequest, Handle);
        cFunc(TEvents::TSystem::Poison, PassAway);
    )

    void Handle(TEvControllerUpdateSystemViews::TPtr& ev) {
        auto *msg = ev->Get();
        auto& newState = msg->State;
        Merge(State.PDisks, newState.PDisks, msg->DeletedPDisks, PDiskIndex);
        Merge(State.VSlots, newState.VSlots, msg->DeletedVSlots, VSlotIndex);
        Merge(State.Groups, newState.Groups, msg->DeletedGroups, GroupIndex);
        Merge(State.StoragePools, newState.StoragePools, msg->DeletedStoragePools, StoragePoolIndex);
        HostRecords = std::move(msg->HostRecords);
        GroupReserveMin = msg->GroupReserveMin;
        GroupReservePart = msg->GroupReservePart;
        GenerateStorageStats();
    }

    template<typename TDest, typename TSrc, typename TDeleted, typename TIndex>
    void Merge(TDest& dest, TSrc& src, const TDeleted& deleted, TIndex& index) {
        if (!src.empty() || !deleted.empty()) {
            index.clear();
        }
        for (const auto& key : deleted) {
            dest.erase(key);
        }
        for (auto& [key, _] : src) {
            dest.erase(key);
        }
        dest.merge(std::move(src));
    }

    template <typename TResponse, typename TRequest, typename TMap, typename TIndex>
    void Reply(TRequest& request, const TMap& entries, TIndex& index) {
        const auto& record = request->Get()->Record;
        auto response = MakeHolder<TResponse>();

        if (index.empty() && !entries.empty()) {
            index.reserve(entries.size());
            for (const auto& [key, value] : entries) {
                index.emplace_back(key, &value);
            }
            std::sort(index.begin(), index.end());
        }

        auto begin = index.begin();
        auto end = index.end();
        auto comp = [](const auto& kv, const auto& key) { return kv.first < key; };

        if (record.HasFrom()) {
            auto from = TransformKey(record.GetFrom());
            begin = std::lower_bound(index.begin(), index.end(), from, comp);
            if (begin != index.end() && begin->first == from && record.HasInclusiveFrom() && !record.GetInclusiveFrom()) {
                ++begin;
            }
        }

        if (record.HasTo()) {
            auto to = TransformKey(record.GetTo());
            end = std::lower_bound(index.begin(), index.end(), to, comp);
            if (end != index.end() && end->first == to && record.GetInclusiveTo()) {
                ++end;
            }
        }

        for (; begin < end; ++begin) {
            auto* entry = response->Record.AddEntries();
            FillKey(entry->MutableKey(), begin->first);
            entry->MutableInfo()->CopyFrom(*begin->second);
        }

        Send(request->Sender, response.Release());
    }

    void Handle(TEvSysView::TEvGetPDisksRequest::TPtr& ev) {
        Reply<TEvSysView::TEvGetPDisksResponse>(ev, State.PDisks, PDiskIndex);
    }

    void Handle(TEvSysView::TEvGetVSlotsRequest::TPtr& ev) {
        Reply<TEvSysView::TEvGetVSlotsResponse>(ev, State.VSlots, VSlotIndex);
    }

    void Handle(TEvSysView::TEvGetGroupsRequest::TPtr& ev) {
        Reply<TEvSysView::TEvGetGroupsResponse>(ev, State.Groups, GroupIndex);
    }

    void Handle(TEvSysView::TEvGetStoragePoolsRequest::TPtr& ev) {
        Reply<TEvSysView::TEvGetStoragePoolsResponse>(ev, State.StoragePools, StoragePoolIndex);
    }

    void Handle(TEvSysView::TEvGetStorageStatsRequest::TPtr& ev) {
        auto response = std::make_unique<TEvSysView::TEvGetStorageStatsResponse>();
        auto& r = response->Record;
        if (StorageStats) {
            for (const auto& item : *StorageStats) {
                auto *e = r.AddEntries();
                e->CopyFrom(item);
            }
        }
        Send(ev->Sender, response.release());
    }

    void GenerateStorageStats() {
        StorageStats.emplace();
        auto& v = *StorageStats;

        using TEntityKey = std::tuple<TString, TString>; // PDiskFilter, ErasureSpecies
        std::unordered_map<TEntityKey, size_t> entityMap;
        std::unordered_map<TBlobStorageController::TBoxStoragePoolId, size_t> spToEntity;

        for (const auto erasure : {TBlobStorageGroupType::ErasureMirror3dc, TBlobStorageGroupType::Erasure4Plus2Block}) {
            for (const NKikimrBlobStorage::EPDiskType type : {NKikimrBlobStorage::ROT, NKikimrBlobStorage::SSD}) {
                TBlobStorageController::TStoragePoolInfo::TPDiskFilter filter{.Type = type};
                TSet<TBlobStorageController::TStoragePoolInfo::TPDiskFilter> filters{filter};
                TStringStream filterData;
                Save(&filterData, filters);

                NKikimrSysView::TStorageStatsEntry e;
                e.SetPDiskFilter(TBlobStorageController::TStoragePoolInfo::TPDiskFilter::ToString(filters));
                e.SetErasureSpecies(TBlobStorageGroupType::ErasureSpeciesName(erasure));
                e.SetPDiskFilterData(filterData.Str());
                entityMap[{e.GetPDiskFilter(), e.GetErasureSpecies()}] = v.size();
                v.push_back(std::move(e));
            }
        }

        for (const auto& [key, value] : State.StoragePools) {
            TEntityKey entityKey(value.GetPDiskFilter(), value.GetErasureSpeciesV2());
            const size_t index = entityMap.try_emplace(entityKey, v.size()).first->second;
            if (index == v.size()) {
                NKikimrSysView::TStorageStatsEntry entry;
                entry.SetPDiskFilter(value.GetPDiskFilter());
                entry.SetErasureSpecies(value.GetErasureSpeciesV2());
                entry.SetPDiskFilterData(value.GetPDiskFilterData());
                v.push_back(std::move(entry));
            } else {
                const auto& entry = v[index];
                Y_VERIFY(entry.GetPDiskFilter() == value.GetPDiskFilter());
                Y_VERIFY(entry.GetErasureSpecies() == value.GetErasureSpeciesV2());
                Y_VERIFY(entry.GetPDiskFilterData() == value.GetPDiskFilterData());
            }
            spToEntity[key] = index;
        }

        for (const auto& [groupId, group] : State.Groups) {
            const TBlobStorageController::TBoxStoragePoolId key(group.GetBoxId(), group.GetStoragePoolId());
            if (const auto it = spToEntity.find(key); it != spToEntity.end()) {
                auto& e = v[it->second];
                e.SetCurrentGroupsCreated(e.GetCurrentGroupsCreated() + 1);
                e.SetCurrentAllocatedSize(e.GetCurrentAllocatedSize() + group.GetAllocatedSize());
                e.SetCurrentAvailableSize(e.GetCurrentAvailableSize() + group.GetAvailableSize());
            }
        }

        auto pdiskFilterCountersToDelete = std::exchange(PDiskFilterCounters, {});
        auto erasureCountersToDelete = std::exchange(ErasureCounters, {});

        using T = std::decay_t<decltype(State.PDisks)>::value_type;
        std::unordered_map<TBlobStorageController::TBoxId, std::vector<const T*>> boxes;
        for (const auto& kv : State.PDisks) {
            if (kv.second.HasBoxId()) {
                boxes[kv.second.GetBoxId()].push_back(&kv);
            }
        }

        for (auto& entry : v) {
            TSet<TBlobStorageController::TStoragePoolInfo::TPDiskFilter> filters;
            TStringInput s(entry.GetPDiskFilterData());
            Load(&s, filters);

            for (const auto& [boxId, pdisks] : boxes) {
                TBlobStorageGroupType type(TBlobStorageGroupType::ErasureSpeciesByName(entry.GetErasureSpecies()));
                TGroupMapper mapper(TGroupGeometryInfo(type, NKikimrBlobStorage::TGroupGeometry())); // default geometry

                for (const auto& kv : pdisks) {
                    const auto& [pdiskId, pdisk] = *kv;
                    for (const auto& filter : filters) {
                        const auto sharedWithOs = pdisk.HasSharedWithOs() ? MakeMaybe(pdisk.GetSharedWithOs()) : Nothing();
                        const auto readCentric = pdisk.HasReadCentric() ? MakeMaybe(pdisk.GetReadCentric()) : Nothing();
                        if (filter.MatchPDisk(pdisk.GetCategory(), sharedWithOs, readCentric)) {
                            const TNodeLocation& location = HostRecords->GetLocation(pdiskId.NodeId);
                            const bool ok = mapper.RegisterPDisk({
                                .PDiskId = pdiskId,
                                .Location = location,
                                .Usable = true,
                                .NumSlots = pdisk.GetNumActiveSlots(),
                                .MaxSlots = pdisk.GetExpectedSlotCount(),
                                .Groups = {},
                                .SpaceAvailable = 0,
                                .Operational = true,
                                .Decommitted = false,
                            });
                            Y_VERIFY(ok);
                            break;
                        }
                    }
                }

                // calculate number of groups we can create without accounting reserve
                TGroupMapper::TGroupDefinition group;
                TString error;
                std::deque<ui64> groupSizes;
                while (mapper.AllocateGroup(groupSizes.size(), group, {}, {}, 0, false, error)) {
                    std::vector<TGroupDiskInfo> disks;
                    std::deque<NKikimrBlobStorage::TPDiskMetrics> pdiskMetrics;
                    std::deque<NKikimrBlobStorage::TVDiskMetrics> vdiskMetrics;

                    for (const auto& realm : group) {
                        for (const auto& domain : realm) {
                            for (const auto& pdiskId : domain) {
                                if (const auto it = State.PDisks.find(pdiskId); it != State.PDisks.end()) {
                                    const NKikimrSysView::TPDiskInfo& pdisk = it->second;
                                    auto& pm = *pdiskMetrics.emplace(pdiskMetrics.end());
                                    auto& vm = *vdiskMetrics.emplace(vdiskMetrics.end());
                                    if (pdisk.HasTotalSize()) {
                                        pm.SetTotalSize(pdisk.GetTotalSize());
                                    }
                                    if (pdisk.HasEnforcedDynamicSlotSize()) {
                                        pm.SetEnforcedDynamicSlotSize(pdisk.GetEnforcedDynamicSlotSize());
                                    }
                                    vm.SetAllocatedSize(0);
                                    disks.push_back({&pm, &vm, pdisk.GetExpectedSlotCount()});
                                }
                            }
                        }
                    }

                    NKikimrSysView::TGroupInfo groupInfo;
                    CalculateGroupUsageStats(&groupInfo, disks, type);
                    groupSizes.push_back(groupInfo.GetAvailableSize());

                    group.clear();
                }

                std::sort(groupSizes.begin(), groupSizes.end());

                // adjust it according to reserve
                const ui32 total = groupSizes.size() + entry.GetCurrentGroupsCreated();
                ui32 reserve = GroupReserveMin;
                while (reserve < groupSizes.size() && (reserve - GroupReserveMin) * 1000000 / total < GroupReservePart) {
                    ++reserve;
                }
                reserve = Min<ui32>(reserve, groupSizes.size());

                // cut sizes
                while (reserve >= 2) {
                    groupSizes.pop_front();
                    groupSizes.pop_back();
                }
                if (reserve) {
                    groupSizes.pop_front();
                }

                entry.SetAvailableGroupsToCreate(entry.GetAvailableGroupsToCreate() + groupSizes.size());
                entry.SetAvailableSizeToCreate(entry.GetAvailableSizeToCreate() + std::accumulate(groupSizes.begin(),
                    groupSizes.end(), ui64(0)));
            }

            auto g = Counters->GetSubgroup("subsystem", "storage_stats");

            PDiskFilterCounters.emplace(entry.GetPDiskFilter());
            pdiskFilterCountersToDelete.erase({entry.GetPDiskFilter()});
            auto pdiskFilterGroup = g->GetSubgroup("pdiskFilter", entry.GetPDiskFilter());

            ErasureCounters.emplace(entry.GetPDiskFilter(), entry.GetErasureSpecies());
            erasureCountersToDelete.erase({entry.GetPDiskFilter(), entry.GetErasureSpecies()});
            auto erasureGroup = pdiskFilterGroup->GetSubgroup("erasureSpecies", entry.GetErasureSpecies());

            erasureGroup->GetCounter("CurrentGroupsCreated")->Set(entry.GetCurrentGroupsCreated());
            erasureGroup->GetCounter("CurrentAllocatedSize")->Set(entry.GetCurrentAllocatedSize());
            erasureGroup->GetCounter("CurrentAvailableSize")->Set(entry.GetCurrentAvailableSize());
            erasureGroup->GetCounter("AvailableGroupsToCreate")->Set(entry.GetAvailableGroupsToCreate());
            erasureGroup->GetCounter("AvailableSizeToCreate")->Set(entry.GetAvailableSizeToCreate());
        }

        for (const auto& item : erasureCountersToDelete) {
            Counters
                ->GetSubgroup("subsystem", "storage_stats")
                ->GetSubgroup("pdiskFilter", std::get<0>(item))
                ->RemoveSubgroup("erasureSpecies", std::get<1>(item));
        }
        for (const auto& item : pdiskFilterCountersToDelete) {
            Counters
                ->GetSubgroup("subsystem", "storage_stats")
                ->RemoveSubgroup("pdiskFilter", std::get<0>(item));
        }
    }
};

IActor* TBlobStorageController::CreateSystemViewsCollector() {
    return new TSystemViewsCollector(GetServiceCounters(AppData()->Counters, "storage_pool_stat"));
}

void TBlobStorageController::ForwardToSystemViewsCollector(STATEFN_SIG) {
    TActivationContext::Send(ev->Forward(SystemViewsCollectorId));
}

void TBlobStorageController::Handle(TEvPrivate::TEvUpdateSystemViews::TPtr&) {
    UpdateSystemViews();
}

void CopyInfo(NKikimrSysView::TPDiskInfo* info, const THolder<TBlobStorageController::TPDiskInfo>& pDiskInfo) {
    TPDiskCategory category(pDiskInfo->Kind);
    info->SetType(category.TypeStrShort());
    info->SetKind(category.Kind());
    info->SetCategory(category);
    info->SetPath(pDiskInfo->Path);
    info->SetGuid(pDiskInfo->Guid);
    info->SetBoxId(pDiskInfo->BoxId);
    if (pDiskInfo->SharedWithOs) {
        info->SetSharedWithOs(*pDiskInfo->SharedWithOs);
    }
    if (pDiskInfo->ReadCentric) {
        info->SetReadCentric(*pDiskInfo->ReadCentric);
    }
    info->SetAvailableSize(pDiskInfo->Metrics.GetAvailableSize());
    info->SetTotalSize(pDiskInfo->Metrics.GetTotalSize());
    info->SetStatusV2(NKikimrBlobStorage::EDriveStatus_Name(pDiskInfo->Status));
    if (pDiskInfo->StatusTimestamp != TInstant::Zero()) {
        info->SetStatusChangeTimestamp(pDiskInfo->StatusTimestamp.GetValue());
    }
    if (pDiskInfo->Metrics.HasEnforcedDynamicSlotSize()) {
        info->SetEnforcedDynamicSlotSize(pDiskInfo->Metrics.GetEnforcedDynamicSlotSize());
    }
    info->SetExpectedSlotCount(pDiskInfo->ExpectedSlotCount);
    info->SetNumActiveSlots(pDiskInfo->NumActiveSlots + pDiskInfo->StaticSlotUsage);
    info->SetDecommitStatus(NKikimrBlobStorage::EDecommitStatus_Name(pDiskInfo->DecommitStatus));
}

void SerializeVSlotInfo(NKikimrSysView::TVSlotInfo *pb, const TVDiskID& vdiskId, const NKikimrBlobStorage::TVDiskMetrics& m,
        NKikimrBlobStorage::EVDiskStatus status, NKikimrBlobStorage::TVDiskKind::EVDiskKind kind, bool isBeingDeleted) {
    pb->SetGroupId(vdiskId.GroupID);
    pb->SetGroupGeneration(vdiskId.GroupGeneration);
    pb->SetFailRealm(vdiskId.FailRealm);
    pb->SetFailDomain(vdiskId.FailDomain);
    pb->SetVDisk(vdiskId.VDisk);
    if (m.HasAllocatedSize()) {
        pb->SetAllocatedSize(m.GetAllocatedSize());
    }
    if (m.HasAvailableSize()) {
        pb->SetAvailableSize(m.GetAvailableSize());
    }
    pb->SetStatusV2(NKikimrBlobStorage::EVDiskStatus_Name(status));
    pb->SetKind(NKikimrBlobStorage::TVDiskKind::EVDiskKind_Name(kind));
    if (isBeingDeleted) {
        pb->SetIsBeingDeleted(true);
    }
}

void CopyInfo(NKikimrSysView::TVSlotInfo* info, const THolder<TBlobStorageController::TVSlotInfo>& vSlotInfo) {
    SerializeVSlotInfo(info, vSlotInfo->GetVDiskId(), vSlotInfo->Metrics, vSlotInfo->GetStatus(), vSlotInfo->Kind,
        vSlotInfo->IsBeingDeleted());
}

void CopyInfo(NKikimrSysView::TGroupInfo* info, const THolder<TBlobStorageController::TGroupInfo>& groupInfo) {
    info->SetGeneration(groupInfo->Generation);
    info->SetErasureSpeciesV2(TErasureType::ErasureSpeciesName(groupInfo->ErasureSpecies));
    info->SetBoxId(std::get<0>(groupInfo->StoragePoolId));
    info->SetStoragePoolId(std::get<1>(groupInfo->StoragePoolId));
    if (groupInfo->EncryptionMode) {
        info->SetEncryptionMode(*groupInfo->EncryptionMode);
    }
    if (groupInfo->LifeCyclePhase) {
        info->SetLifeCyclePhase(*groupInfo->LifeCyclePhase);
    }

    std::vector<TGroupDiskInfo> disks;
    for (const auto& vslot : groupInfo->VDisksInGroup) {
        disks.push_back({&vslot->PDisk->Metrics, &vslot->Metrics, vslot->PDisk->ExpectedSlotCount});
    }
    CalculateGroupUsageStats(info, disks, TBlobStorageGroupType(groupInfo->ErasureSpecies));

    info->SetSeenOperational(groupInfo->SeenOperational);
    const auto& latencyStats = groupInfo->LatencyStats;
    if (latencyStats.PutTabletLog) {
        info->SetPutTabletLogLatency(latencyStats.PutTabletLog->MicroSeconds());
    }
    if (latencyStats.PutUserData) {
        info->SetPutUserDataLatency(latencyStats.PutUserData->MicroSeconds());
    }
    if (latencyStats.GetFast) {
        info->SetGetFastLatency(latencyStats.GetFast->MicroSeconds());
    }
}

void CopyInfo(NKikimrSysView::TStoragePoolInfo* info, const TBlobStorageController::TStoragePoolInfo& poolInfo) {
    info->SetName(poolInfo.Name);
    if (poolInfo.Generation) {
        info->SetGeneration(*poolInfo.Generation);
    }
    info->SetErasureSpeciesV2(TErasureType::ErasureSpeciesName(poolInfo.ErasureSpecies));
    info->SetVDiskKindV2(NKikimrBlobStorage::TVDiskKind::EVDiskKind_Name(poolInfo.VDiskKind));
    info->SetKind(poolInfo.Kind);
    info->SetNumGroups(poolInfo.NumGroups);
    if (poolInfo.EncryptionMode) {
        info->SetEncryptionMode(*poolInfo.EncryptionMode);
    }
    if (poolInfo.SchemeshardId) {
        info->SetSchemeshardId(*poolInfo.SchemeshardId);
    }
    if (poolInfo.PathItemId) {
        info->SetPathId(*poolInfo.PathItemId);
    }

    info->SetPDiskFilter(TBlobStorageController::TStoragePoolInfo::TPDiskFilter::ToString(poolInfo.PDiskFilters));

    TStringStream pdiskFilterData;
    Save(&pdiskFilterData, poolInfo.PDiskFilters);
    info->SetPDiskFilterData(pdiskFilterData.Str());
}

template<typename TDstMap, typename TDeletedSet, typename TSrcMap, typename TChangedSet>
void CopyInfo(TDstMap& dst, TDeletedSet& deleted, const TSrcMap& src, TChangedSet& changed) {
    for (const auto& key : changed) {
        if (const auto it = src.find(key); it != src.end()) {
            CopyInfo(&dst[key], it->second);
        } else {
            deleted.insert(key);
        }
    }
}

void TBlobStorageController::UpdateSystemViews() {
    if (!AppData()->FeatureFlags.GetEnableSystemViews()) {
        return;
    }

    if (!SysViewChangedPDisks.empty() || !SysViewChangedVSlots.empty() || !SysViewChangedGroups.empty() ||
            !SysViewChangedStoragePools.empty() || SysViewChangedSettings) {
        auto update = MakeHolder<TEvControllerUpdateSystemViews>();
        update->HostRecords = HostRecords;
        update->GroupReserveMin = GroupReserveMin;
        update->GroupReservePart = GroupReservePart;

        auto& state = update->State;
        CopyInfo(state.PDisks, update->DeletedPDisks, PDisks, SysViewChangedPDisks);
        CopyInfo(state.VSlots, update->DeletedVSlots, VSlots, SysViewChangedVSlots);
        CopyInfo(state.Groups, update->DeletedGroups, GroupMap, SysViewChangedGroups);
        CopyInfo(state.StoragePools, update->DeletedStoragePools, StoragePools, SysViewChangedStoragePools);

        // process static slots and static groups
        for (const auto& [pdiskId, pdisk] : StaticPDisks) {
            if (SysViewChangedPDisks.count(pdiskId) && !FindPDisk(pdiskId)) {
                auto *pb = &state.PDisks[pdiskId];
                TPDiskCategory category(pdisk.Category);
                pb->SetType(category.TypeStrShort());
                pb->SetKind(category.Kind());
                pb->SetCategory(category);
                pb->SetPath(pdisk.Path);
                pb->SetGuid(pdisk.Guid);
                if (pdisk.PDiskMetrics) {
                    pb->SetAvailableSize(pdisk.PDiskMetrics->GetAvailableSize());
                    pb->SetTotalSize(pdisk.PDiskMetrics->GetTotalSize());
                    if (pdisk.PDiskMetrics->HasEnforcedDynamicSlotSize()) {
                        pb->SetEnforcedDynamicSlotSize(pdisk.PDiskMetrics->GetEnforcedDynamicSlotSize());
                    }
                }
                pb->SetStatusV2(NKikimrBlobStorage::EDriveStatus_Name(NKikimrBlobStorage::EDriveStatus::ACTIVE));
                pb->SetDecommitStatus(NKikimrBlobStorage::EDecommitStatus_Name(NKikimrBlobStorage::EDecommitStatus::DECOMMIT_NONE));
                pb->SetExpectedSlotCount(pdisk.ExpectedSlotCount ? pdisk.ExpectedSlotCount : pdisk.StaticSlotUsage);
                pb->SetNumActiveSlots(pdisk.StaticSlotUsage);
            }
        }
        for (const auto& [vslotId, vslot] : StaticVSlots) {
            if (SysViewChangedVSlots.count(vslotId)) {
                static const NKikimrBlobStorage::TVDiskMetrics zero;
                SerializeVSlotInfo(&state.VSlots[vslotId], vslot.VDiskId, vslot.VDiskMetrics ? *vslot.VDiskMetrics : zero,
                    vslot.VDiskStatus, vslot.VDiskKind, false);
            }
        }
        for (const auto& group : AppData()->StaticBlobStorageConfig->GetGroups()) {
            if (!SysViewChangedGroups.count(group.GetGroupID())) {
                continue;
            }
            auto *pb = &state.Groups[group.GetGroupID()];
            pb->SetGeneration(group.GetGroupGeneration());
            pb->SetEncryptionMode(group.GetEncryptionMode());
            pb->SetLifeCyclePhase(group.GetLifeCyclePhase());
            pb->SetSeenOperational(true);
            pb->SetErasureSpeciesV2(TBlobStorageGroupType::ErasureSpeciesName(group.GetErasureSpecies()));

            const NKikimrBlobStorage::TVDiskMetrics zero;
            std::vector<TGroupDiskInfo> disks;
            for (const auto& realm : group.GetRings()) {
                for (const auto& domain : realm.GetFailDomains()) {
                    for (const auto& location : domain.GetVDiskLocations()) {
                        const TVSlotId vslotId(location.GetNodeID(), location.GetPDiskID(), location.GetVDiskSlotID());
                        TGroupDiskInfo disk{nullptr, nullptr, 0};
                        if (const auto it = StaticVSlots.find(vslotId); it != StaticVSlots.end()) {
                            disk.VDiskMetrics = it->second.VDiskMetrics ? &*it->second.VDiskMetrics : &zero;
                        }
                        if (const auto it = PDisks.find(vslotId.ComprisingPDiskId()); it != PDisks.end()) {
                            disk.PDiskMetrics = &it->second->Metrics;
                            disk.ExpectedSlotCount = it->second->ExpectedSlotCount;
                        }
                        if (disk.VDiskMetrics && disk.PDiskMetrics) {
                            disks.push_back(std::move(disk));
                        }
                    }
                }
            }
            CalculateGroupUsageStats(pb, disks, (TBlobStorageGroupType::EErasureSpecies)group.GetErasureSpecies());
        }

        SysViewChangedPDisks.clear();
        SysViewChangedVSlots.clear();
        SysViewChangedGroups.clear();
        SysViewChangedStoragePools.clear();
        SysViewChangedSettings = false;

        Send(SystemViewsCollectorId, update.Release());
    }

    Schedule(UpdateSystemViewsPeriod, new TEvPrivate::TEvUpdateSystemViews);
}

} // NKikimr::NBsController
