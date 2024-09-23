#include "storage_stats_calculator.h"

#include "group_geometry_info.h"
#include "group_mapper.h"
#include "impl.h"
#include "sys_view.h"

#include <ydb/core/blobstorage/base/utility.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_coroutine.h>
#include <ydb/library/actors/core/events.h>

#include <util/generic/ptr.h>
#include <util/system/yassert.h>

#include <memory>
#include <vector>

namespace NKikimr::NBsController {

/* TStorageStatsCoroCalculatorImpl */

class TStorageStatsCoroCalculatorImpl : public TActorCoroImpl {
private:
    enum {
        EvResume = EventSpaceBegin(TEvents::ES_PRIVATE)
    };

    struct TExPoison {};

public:
    TStorageStatsCoroCalculatorImpl(
        const TControllerSystemViewsState& systemViewsState,
        const TBlobStorageController::THostRecordMap& hostRecordMap,
        ui32 groupReserveMin,
        ui32 groupReservePart)
        : TActorCoroImpl(/* stackSize */ 640_KB, /* allowUnhandledDtor */ true) // 640 KiB should be enough for anything!
        , SystemViewsState(systemViewsState)
        , HostRecordMap(hostRecordMap)
        , GroupReserveMin(groupReserveMin)
        , GroupReservePart(groupReservePart)
    {
    }

    void ProcessUnexpectedEvent(TAutoPtr<IEventHandle> ev) {
        switch (ev->GetTypeRewrite()) {
            case TEvents::TSystem::Poison:
                throw TExPoison();
        }

        Y_ABORT("unexpected event Type# 0x%08" PRIx32, ev->GetTypeRewrite());
    }

    void Run() override {
        try {
            RunImpl();
        } catch (const TExPoison&) {
            return;
        }
    }

    void RunImpl() {
        std::vector<NKikimrSysView::TStorageStatsEntry> storageStats;

        using TEntityKey = std::tuple<TString, TString>; // PDiskFilter, ErasureSpecies
        std::unordered_map<TEntityKey, size_t> entityMap;
        std::unordered_map<TBoxStoragePoolId, size_t> spToEntity;

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
                entityMap[{e.GetPDiskFilter(), e.GetErasureSpecies()}] = storageStats.size();
                storageStats.push_back(std::move(e));
            }
        }

        for (const auto& [key, value] : SystemViewsState.StoragePools) {
            TEntityKey entityKey(value.GetPDiskFilter(), value.GetErasureSpeciesV2());
            const size_t index = entityMap.try_emplace(entityKey, storageStats.size()).first->second;
            if (index == storageStats.size()) {
                NKikimrSysView::TStorageStatsEntry entry;
                entry.SetPDiskFilter(value.GetPDiskFilter());
                entry.SetErasureSpecies(value.GetErasureSpeciesV2());
                entry.SetPDiskFilterData(value.GetPDiskFilterData());
                storageStats.push_back(std::move(entry));
            } else {
                const auto& entry = storageStats[index];
                Y_ABORT_UNLESS(entry.GetPDiskFilter() == value.GetPDiskFilter());
                Y_ABORT_UNLESS(entry.GetErasureSpecies() == value.GetErasureSpeciesV2());
                Y_ABORT_UNLESS(entry.GetPDiskFilterData() == value.GetPDiskFilterData());
            }
            spToEntity[key] = index;
        }

        for (const auto& [groupId, group] : SystemViewsState.Groups) {
            const TBoxStoragePoolId key(group.GetBoxId(), group.GetStoragePoolId());
            if (const auto it = spToEntity.find(key); it != spToEntity.end()) {
                auto& e = storageStats[it->second];
                e.SetCurrentGroupsCreated(e.GetCurrentGroupsCreated() + 1);
                e.SetCurrentAllocatedSize(e.GetCurrentAllocatedSize() + group.GetAllocatedSize());
                e.SetCurrentAvailableSize(e.GetCurrentAvailableSize() + group.GetAvailableSize());
            }
        }

        using T = std::decay_t<decltype(SystemViewsState.PDisks)>::value_type;
        std::unordered_map<TBoxId, std::vector<const T*>> boxes;
        for (const auto& kv : SystemViewsState.PDisks) {
            if (kv.second.HasBoxId()) {
                boxes[kv.second.GetBoxId()].push_back(&kv);
            }
        }

        for (auto& entry : storageStats) {
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
                            const TNodeLocation& location = HostRecordMap->GetLocation(pdiskId.NodeId);
                            const bool usable = pdisk.GetDecommitStatus() == "DECOMMIT_NONE";
                            const bool ok = mapper.RegisterPDisk({
                                .PDiskId = pdiskId,
                                .Location = location,
                                .Usable = usable,
                                .NumSlots = pdisk.GetNumActiveSlots(),
                                .MaxSlots = pdisk.GetExpectedSlotCount(),
                                .Groups = {},
                                .SpaceAvailable = 0,
                                .Operational = true,
                                .Decommitted = false, // this flag applies only to group reconfiguration
                            });
                            Y_ABORT_UNLESS(ok);
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
                                if (const auto it = SystemViewsState.PDisks.find(pdiskId); it != SystemViewsState.PDisks.end()) {
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

                    Yield();
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
                    reserve -= 2;
                }

                if (reserve) {
                    groupSizes.pop_front();
                }

                entry.SetAvailableGroupsToCreate(entry.GetAvailableGroupsToCreate() + groupSizes.size());
                entry.SetAvailableSizeToCreate(entry.GetAvailableSizeToCreate() + std::accumulate(groupSizes.begin(),
                    groupSizes.end(), ui64(0)));
            }
        }

        Send(ParentActorId, new TEvCalculateStorageStatsResponse(std::move(storageStats)));
    }

private:
    void Yield() {
        Send(new IEventHandle(EvResume, 0, SelfActorId, {}, nullptr, 0));
        WaitForSpecificEvent([](IEventHandle& ev) { return ev.Type == EvResume; }, &TStorageStatsCoroCalculatorImpl::ProcessUnexpectedEvent);
    }

private:
    TControllerSystemViewsState SystemViewsState;
    TBlobStorageController::THostRecordMap HostRecordMap;
    ui32 GroupReserveMin = 0;
    ui32 GroupReservePart = 0;
};

std::unique_ptr<IActor> CreateStorageStatsCoroCalculator(
    const TControllerSystemViewsState& systemViewsState,
    const TBlobStorageController::THostRecordMap& hostRecordMap,
    ui32 groupReserveMin,
    ui32 groupReservePart)
{
    auto coroCalculatorImpl = MakeHolder<TStorageStatsCoroCalculatorImpl>(
        systemViewsState,
        hostRecordMap,
        groupReserveMin,
        groupReservePart);

    return std::make_unique<TActorCoro>(std::move(coroCalculatorImpl), NKikimrServices::TActivity::BS_STORAGE_STATS_ACTOR);
}

} // NKikimr::NBsController
