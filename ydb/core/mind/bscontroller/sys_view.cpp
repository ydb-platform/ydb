#include "sys_view.h"
#include "group_geometry_info.h"
#include "storage_stats_calculator.h"
#include "group_layout_checker.h"

#include <ydb/core/base/feature_flags.h>
#include <ydb/core/blobstorage/base/utility.h>

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
    return TGroupId::FromProto(&key, &NKikimrSysView::TGroupKey::GetGroupId);
}

void FillKey(NKikimrSysView::TGroupKey* key, const TGroupId& id) {
    key->SetGroupId(id.GetRawId());
}

TBoxStoragePoolId TransformKey(const NKikimrSysView::TStoragePoolKey& key) {
    return std::make_tuple(key.GetBoxId(), key.GetStoragePoolId());
}

void FillKey(NKikimrSysView::TStoragePoolKey* key, const TBoxStoragePoolId& id) {
    key->SetBoxId(std::get<0>(id));
    key->SetStoragePoolId(std::get<1>(id));
}

void CalculateGroupUsageStats(NKikimrSysView::TGroupInfo *info, const std::vector<TGroupDiskInfo>& disks,
        TBlobStorageGroupType type) {
    if (disks.empty()) {
        return;
    }
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

class TSystemViewsCollector : public TActorBootstrapped<TSystemViewsCollector> {
    TControllerSystemViewsState State;
    std::map<TPDiskId, const NKikimrSysView::TPDiskInfo*> PDiskIndex;
    std::map<TVSlotId, const NKikimrSysView::TVSlotInfo*> VSlotIndex;
    std::map<TGroupId, const NKikimrSysView::TGroupInfo*> GroupIndex;
    std::map<TBoxStoragePoolId, const NKikimrSysView::TStoragePoolInfo*> StoragePoolIndex;
    THostRecordMap HostRecords;
    ui32 GroupReserveMin = 0;
    ui32 GroupReservePart = 0;
    ::NMonitoring::TDynamicCounterPtr Counters;
    std::unordered_set<std::tuple<TString>> PDiskFilterCounters;
    std::unordered_set<std::tuple<TString, TString>> ErasureCounters;

    std::vector<NKikimrSysView::TStorageStatsEntry> StorageStats;
    TActorId StorageStatsCalculatorId;
    static constexpr TDuration StorageStatsUpdatePeriod = TDuration::Minutes(10);

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BSC_SYSTEM_VIEWS_COLLECTOR;
    }

    TSystemViewsCollector(::NMonitoring::TDynamicCounterPtr counters)
        : Counters(std::move(counters))
    {}

    ~TSystemViewsCollector() {
        Counters->RemoveSubgroup("subsystem", "storage_stats");
    }

    void Bootstrap(const TActorContext&) {
        Become(&TThis::StateWork);
        RunStorageStatsCalculator();
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvControllerUpdateSystemViews, Handle);
        hFunc(TEvSysView::TEvGetPDisksRequest, Handle);
        hFunc(TEvSysView::TEvGetVSlotsRequest, Handle);
        hFunc(TEvSysView::TEvGetGroupsRequest, Handle);
        hFunc(TEvSysView::TEvGetStoragePoolsRequest, Handle);
        hFunc(TEvSysView::TEvGetStorageStatsRequest, Handle);
        cFunc(NSysView::TEvSysView::EvCalculateStorageStatsRequest, RunStorageStatsCalculator);
        hFunc(TEvCalculateStorageStatsResponse, Handle);
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
    }

    void PassAway() override {
        if (StorageStatsCalculatorId) {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, StorageStatsCalculatorId, {}, nullptr, 0));
        }

        TActorBootstrapped::PassAway();
    }

    template<typename TDest, typename TSrc, typename TDeleted, typename TIndex>
    void Merge(TDest& dest, TSrc& src, const TDeleted& deleted, TIndex& index) {
        for (const auto& key : deleted) {
            dest.erase(key);
            index.erase(key);
        }
        for (auto& [key, value] : src) {
            dest.erase(key);
            index[key] = &value;
        }
        dest.merge(std::move(src));

#ifndef NDEBUG
        for (const auto& [key, value] : dest) {
            Y_DEBUG_ABORT_UNLESS(index.contains(key) && index[key] == &value);
        }
        Y_DEBUG_ABORT_UNLESS(index.size() == dest.size());
#endif
    }

    template <typename TResponse, typename TRequest, typename TIndex>
    void Reply(TRequest& request, TIndex& index) {
        const auto& record = request->Get()->Record;
        auto response = MakeHolder<TResponse>();

        auto begin = index.begin();
        auto end = index.end();

        if (record.HasFrom()) {
            auto from = TransformKey(record.GetFrom());
            begin = index.lower_bound(from);
            if (begin != index.end() && begin->first == from && record.HasInclusiveFrom() && !record.GetInclusiveFrom()) {
                ++begin;
            }
        }

        if (record.HasTo()) {
            auto to = TransformKey(record.GetTo());
            end = index.lower_bound(to);
            if (end != index.end() && end->first == to && record.GetInclusiveTo()) {
                ++end;
            }
        }

        for (; begin != end; ++begin) {
            auto* entry = response->Record.AddEntries();
            FillKey(entry->MutableKey(), begin->first);
            entry->MutableInfo()->CopyFrom(*begin->second);
        }

        Send(request->Sender, response.Release());
    }

    void Handle(TEvSysView::TEvGetPDisksRequest::TPtr& ev) {
        Reply<TEvSysView::TEvGetPDisksResponse>(ev, PDiskIndex);
    }

    void Handle(TEvSysView::TEvGetVSlotsRequest::TPtr& ev) {
        Reply<TEvSysView::TEvGetVSlotsResponse>(ev, VSlotIndex);
    }

    void Handle(TEvSysView::TEvGetGroupsRequest::TPtr& ev) {
        Reply<TEvSysView::TEvGetGroupsResponse>(ev, GroupIndex);
    }

    void Handle(TEvSysView::TEvGetStoragePoolsRequest::TPtr& ev) {
        Reply<TEvSysView::TEvGetStoragePoolsResponse>(ev, StoragePoolIndex);
    }

    void Handle(TEvSysView::TEvGetStorageStatsRequest::TPtr& ev) {
        auto response = std::make_unique<TEvSysView::TEvGetStorageStatsResponse>();
        auto& r = response->Record;
        for (const auto& item : StorageStats) {
            auto *e = r.AddEntries();
            e->CopyFrom(item);
        }
        Send(ev->Sender, response.release());
    }

    void RunStorageStatsCalculator() {
        if (StorageStatsCalculatorId) {
            return;
        }

        auto& ctx = TActivationContext::AsActorContext();
        auto actor = CreateStorageStatsCoroCalculator(
            State,
            HostRecords,
            GroupReserveMin,
            GroupReservePart);

        StorageStatsCalculatorId = RunInBatchPool(ctx, actor.release());

        Schedule(StorageStatsUpdatePeriod, new TEvCalculateStorageStatsRequest());
    }

    void Handle(TEvCalculateStorageStatsResponse::TPtr& ev) {
        auto& response = *(ev->Get());
        StorageStats = std::move(response.StorageStats);
        UpdateStorageStatsCounters(StorageStats);
        StorageStatsCalculatorId = TActorId();
    }

    void UpdateStorageStatsCounters(const std::vector<NKikimrSysView::TStorageStatsEntry>& storageStats) {
        auto pdiskFilterCountersToDelete = std::exchange(PDiskFilterCounters, {});
        auto erasureCountersToDelete = std::exchange(ErasureCounters, {});

        for (const auto& entry : storageStats) {
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

        // remove no longer present entries
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
    TActivationContext::Forward(ev, SystemViewsCollectorId);
}

void TBlobStorageController::Handle(TEvPrivate::TEvUpdateSystemViews::TPtr&) {
    UpdateSystemViews();
}

void CopyInfo(NKikimrSysView::TPDiskInfo* info, const THolder<TBlobStorageController::TPDiskInfo>& pDiskInfo,
        const TBlobStorageController::TGroupInfo::TGroupFinder& /*finder*/) {
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
    if (auto s = pDiskInfo->Metrics.GetState(); pDiskInfo->Operational || s != NKikimrBlobStorage::TPDiskState::Normal) {
        info->SetState(NKikimrBlobStorage::TPDiskState::E_Name(s));
    }
    info->SetStatusV2(NKikimrBlobStorage::EDriveStatus_Name(pDiskInfo->Status));
    if (pDiskInfo->StatusTimestamp != TInstant::Zero()) {
        info->SetStatusChangeTimestamp(pDiskInfo->StatusTimestamp.GetValue());
    }
    if (pDiskInfo->Metrics.HasEnforcedDynamicSlotSize()) {
        info->SetEnforcedDynamicSlotSize(pDiskInfo->Metrics.GetEnforcedDynamicSlotSize());
    }
    ui32 slotCount = 0;
    ui32 slotSizeInUnits = 0;
    pDiskInfo->ExtractInferredPDiskSettings(slotCount, slotSizeInUnits);
    info->SetExpectedSlotCount(slotCount);
    info->SetNumActiveSlots(pDiskInfo->NumActiveSlots + pDiskInfo->StaticSlotUsage);
    info->SetDecommitStatus(NKikimrBlobStorage::EDecommitStatus_Name(pDiskInfo->DecommitStatus));
    info->SetSlotSizeInUnits(slotSizeInUnits);
    info->SetInferPDiskSlotCountFromUnitSize(pDiskInfo->InferPDiskSlotCountFromUnitSize);
}

void SerializeVSlotInfo(NKikimrSysView::TVSlotInfo *pb, const TVDiskID& vdiskId, const NKikimrBlobStorage::TVDiskMetrics& m,
        std::optional<NKikimrBlobStorage::EVDiskStatus> status, NKikimrBlobStorage::TVDiskKind::EVDiskKind kind,
        bool isBeingDeleted)
{
    pb->SetGroupId(vdiskId.GroupID.GetRawId());
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
    if (status) {
        pb->SetStatusV2(NKikimrBlobStorage::EVDiskStatus_Name(*status));
    }
    if (m.HasState()) {
        pb->SetState(NKikimrWhiteboard::EVDiskState_Name(m.GetState()));
    }
    if (m.HasReplicated()) {
        pb->SetReplicated(m.GetReplicated());
    }
    if (m.HasDiskSpace()) {
        pb->SetDiskSpace(NKikimrWhiteboard::EFlag_Name(m.GetDiskSpace()));
    }
    if (m.HasIsThrottling()) {
        pb->SetIsThrottling(m.GetIsThrottling());
    }
    if (m.GetThrottlingRate()) {
        pb->SetThrottlingRate(m.GetThrottlingRate());
    }
    pb->SetKind(NKikimrBlobStorage::TVDiskKind::EVDiskKind_Name(kind));
    if (isBeingDeleted) {
        pb->SetIsBeingDeleted(true);
    }
}

void CopyInfo(NKikimrSysView::TVSlotInfo* info, const THolder<TBlobStorageController::TVSlotInfo>& vSlotInfo,
        const TBlobStorageController::TGroupInfo::TGroupFinder& /*finder*/) {
    SerializeVSlotInfo(info, vSlotInfo->GetVDiskId(), vSlotInfo->Metrics, vSlotInfo->VDiskStatus,
        vSlotInfo->Kind, vSlotInfo->IsBeingDeleted());
}

void CopyInfo(NKikimrSysView::TGroupInfo* info, const THolder<TBlobStorageController::TGroupInfo>& groupInfo,
        const TBlobStorageController::TGroupInfo::TGroupFinder& finder) {
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

    groupInfo->BridgePileId.CopyToProto(info, &std::decay_t<decltype(*info)>::SetBridgePileId);

    if (groupInfo->BridgeProxyGroupId) {
        groupInfo->BridgeProxyGroupId->CopyToProto(info, &std::decay_t<decltype(*info)>::SetProxyGroupId);
    }

    info->SetLayoutCorrect(groupInfo->IsLayoutCorrect(finder));
    const auto& status = groupInfo->GetStatus(finder);
    info->SetOperatingStatus(NKikimrBlobStorage::TGroupStatus::E_Name(status.OperatingStatus));
    info->SetExpectedStatus(NKikimrBlobStorage::TGroupStatus::E_Name(status.ExpectedStatus));
    info->SetGroupSizeInUnits(groupInfo->GroupSizeInUnits);
}

void CopyInfo(NKikimrSysView::TStoragePoolInfo* info, const TBlobStorageController::TStoragePoolInfo& poolInfo,
        const TBlobStorageController::TGroupInfo::TGroupFinder& /*finder*/) {
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
    info->SetDefaultGroupSizeInUnits(poolInfo.DefaultGroupSizeInUnits);
}

template<typename TDstMap, typename TDeletedSet, typename TSrcMap, typename TChangedSet>
void CopyInfo(TDstMap& dst, TDeletedSet& deleted, const TSrcMap& src, TChangedSet& changed,
        const TBlobStorageController::TGroupInfo::TGroupFinder& finder) {
    for (const auto& key : changed) {
        if (const auto it = src.find(key); it != src.end()) {
            CopyInfo(&dst[key], it->second, finder);
        } else {
            deleted.insert(key);
        }
    }
}

void TBlobStorageController::UpdateSystemViews() {
    if (!AppData()->FeatureFlags.GetEnableSystemViews()) {
        return;
    }

    const TMonotonic now = TActivationContext::Monotonic();
    const TDuration expiration = TDuration::Seconds(15);
    for (auto& [key, value] : VSlots) {
        if (!value->VDiskStatus && value->VDiskStatusTimestamp + expiration <= now) {
            value->VDiskStatus = NKikimrBlobStorage::ERROR;
            SysViewChangedVSlots.insert(key);
        }
    }
    for (auto& [key, value] : StaticVSlots) {
        if (!value.VDiskStatus && value.VDiskStatusTimestamp + expiration <= now) {
            value.VDiskStatus = NKikimrBlobStorage::ERROR;
            SysViewChangedVSlots.insert(key);
        }
    }

    if (!SysViewChangedPDisks.empty() || !SysViewChangedVSlots.empty() || !SysViewChangedGroups.empty() ||
            !SysViewChangedStoragePools.empty() || SysViewChangedSettings) {
        auto update = MakeHolder<TEvControllerUpdateSystemViews>();
        update->HostRecords = HostRecords;
        update->GroupReserveMin = GroupReserveMin;
        update->GroupReservePart = GroupReservePart;

        TBlobStorageController::TGroupInfo::TGroupFinder finder = [&](TGroupId groupId) { return FindGroup(groupId); };

        for (const auto& [groupId, group] : GroupMap) {
            if (SysViewChangedGroups.contains(groupId)) {
                if (group->BridgeProxyGroupId) {
                    SysViewChangedGroups.insert(*group->BridgeProxyGroupId);
                }
                if (group->BridgeGroupInfo) {
                    for (auto id : group->BridgeGroupInfo->GetBridgeGroupIds()) {
                        SysViewChangedGroups.insert(TGroupId::FromValue(id));
                    }
                }
            }
        }

        auto& state = update->State;
        CopyInfo(state.PDisks, update->DeletedPDisks, PDisks, SysViewChangedPDisks, finder);
        CopyInfo(state.VSlots, update->DeletedVSlots, VSlots, SysViewChangedVSlots, finder);
        CopyInfo(state.Groups, update->DeletedGroups, GroupMap, SysViewChangedGroups, finder);
        CopyInfo(state.StoragePools, update->DeletedStoragePools, StoragePools, SysViewChangedStoragePools, finder);

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
                    pb->SetState(NKikimrBlobStorage::TPDiskState::E_Name(pdisk.PDiskMetrics->GetState()));
                    if (pdisk.PDiskMetrics->HasEnforcedDynamicSlotSize()) {
                        pb->SetEnforcedDynamicSlotSize(pdisk.PDiskMetrics->GetEnforcedDynamicSlotSize());
                    }
                }
                pb->SetStatusV2(NKikimrBlobStorage::EDriveStatus_Name(NKikimrBlobStorage::EDriveStatus::ACTIVE));
                pb->SetDecommitStatus(NKikimrBlobStorage::EDecommitStatus_Name(NKikimrBlobStorage::EDecommitStatus::DECOMMIT_NONE));

                ui32 slotCount = 0;
                ui32 slotSizeInUnits = 0;
                pdisk.ExtractInferredPDiskSettings(slotCount, slotSizeInUnits);

                pb->SetExpectedSlotCount(slotCount);
                pb->SetSlotSizeInUnits(slotSizeInUnits);
                pb->SetInferPDiskSlotCountFromUnitSize(pdisk.InferPDiskSlotCountFromUnitSize);
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
        TStaticGroupInfo::TStaticGroupFinder staticFinder = [this](TGroupId groupId) {
            const auto it = StaticGroups.find(groupId);
            return it != StaticGroups.end() ? &it->second : nullptr;
        };
        for (auto& [groupId, group] : StaticGroups) {
            group.UpdateStatus(now, this);
            group.UpdateLayoutCorrect(this);
        }
        for (const auto& [groupId, group] : StaticGroups) {
            if (const auto& info = group.Info; info && SysViewChangedGroups.count(groupId)) {
                auto *pb = &state.Groups[groupId];
                pb->SetGeneration(info->GroupGeneration);
                pb->SetEncryptionMode(info->GetEncryptionMode());
                pb->SetLifeCyclePhase(info->GetLifeCyclePhase());
                pb->SetSeenOperational(true);
                pb->SetErasureSpeciesV2(TBlobStorageGroupType::ErasureSpeciesName(info->Type.GetErasure()));

                const NKikimrBlobStorage::TVDiskMetrics zero;
                std::vector<TGroupDiskInfo> disks;
                std::vector<TPDiskId> pdiskIds;
                for (TActorId actorId : info->GetDynamicInfo().ServiceIdForOrderNumber) {
                    const auto& [nodeId, pdiskId, vdiskSlotId] = DecomposeVDiskServiceId(actorId);
                    const TVSlotId vslotId(nodeId, pdiskId, vdiskSlotId);
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
                    pdiskIds.emplace_back(nodeId, pdiskId);
                }
                CalculateGroupUsageStats(pb, disks, info->Type.GetErasure());

                pb->SetLayoutCorrect(group.IsLayoutCorrect(staticFinder));

                const auto& status = group.GetStatus(staticFinder);
                pb->SetOperatingStatus(NKikimrBlobStorage::TGroupStatus::E_Name(status.OperatingStatus));
                pb->SetExpectedStatus(NKikimrBlobStorage::TGroupStatus::E_Name(status.ExpectedStatus));

                info->GetBridgePileId().CopyToProto(pb, &NKikimrSysView::TGroupInfo::SetBridgePileId);

                if (const auto& proxyGroupId = info->GetBridgeProxyGroupId()) {
                    proxyGroupId->CopyToProto(pb, &NKikimrSysView::TGroupInfo::SetProxyGroupId);
                }
            }
        }

        // aggregate allocated/available size for bridged groups
        auto aggr = [&](auto& pb, auto&& ids) {
            for (auto i : ids) {
                TGroupId bridgeGroupId;
                if constexpr (std::is_same_v<decltype(i), TGroupId>) {
                    bridgeGroupId = i;
                } else {
                    bridgeGroupId = TGroupId::FromValue(i);
                }
                if (const auto it = state.Groups.find(bridgeGroupId); it != state.Groups.end()) {
                    if (it->second.HasAllocatedSize()) {
                        pb.SetAllocatedSize(Max<ui64>(pb.HasAllocatedSize() ? pb.GetAllocatedSize() : Min<ui64>(),
                            it->second.GetAllocatedSize()));
                    }
                    if (it->second.HasAvailableSize()) {
                        pb.SetAvailableSize(Min<ui64>(pb.HasAvailableSize() ? pb.GetAvailableSize() : Max<ui64>(),
                            it->second.GetAvailableSize()));
                    }
                }
            }
        };

        for (auto& [groupId, g] : state.Groups) {
            if (const TGroupInfo *group = FindGroup(groupId); group && group->BridgeGroupInfo) {
                aggr(g, group->BridgeGroupInfo->GetBridgeGroupIds());
            } else if (const auto it = StaticGroups.find(groupId); it != StaticGroups.end()) {
                aggr(g, it->second.Info->GetBridgeGroupIds());
            }
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
