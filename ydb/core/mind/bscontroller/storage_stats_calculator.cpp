#include "storage_stats_calculator.h"

#include "group_geometry_info.h"
#include "group_mapper.h"
#include "impl.h"
#include "sys_view.h"

#include <library/cpp/actors/core/actor.h>

#include <memory>
#include <vector>

namespace NKikimr::NBsController {

class TStorageStatsCalculator : public TActor<TStorageStatsCalculator> {
public:
    TStorageStatsCalculator()
        : TActor(&TStorageStatsCalculator::StateWork)
    {}

    STRICT_STFUNC(StateWork,
        hFunc(TEvCalculateStorageStatsRequest, Handle);
        cFunc(TEvents::TSystem::Poison, PassAway);
    )

    void Handle(TEvCalculateStorageStatsRequest::TPtr& ev) {
        auto response = std::make_unique<TEvCalculateStorageStatsResponse>();
        const auto& request = *(ev->Get());
        response->StorageStats = GenerateStorageStats(request.SystemViewsState, request.HostRecordMap, request.GroupReserveMin, request.GroupReservePart);
        Send(ev->Sender, response.release());
    }

private:
    std::vector<NKikimrSysView::TStorageStatsEntry> GenerateStorageStats(
            const TControllerSystemViewsState& systemViewsState,
            const TBlobStorageController::THostRecordMap& hostRecordMap,
            ui32 groupReserveMin,
            ui32 groupReservePart)
    {
        std::vector<NKikimrSysView::TStorageStatsEntry> storageStats;

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
                entityMap[{e.GetPDiskFilter(), e.GetErasureSpecies()}] = storageStats.size();
                storageStats.push_back(std::move(e));
            }
        }

        for (const auto& [key, value] : systemViewsState.StoragePools) {
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
                Y_VERIFY(entry.GetPDiskFilter() == value.GetPDiskFilter());
                Y_VERIFY(entry.GetErasureSpecies() == value.GetErasureSpeciesV2());
                Y_VERIFY(entry.GetPDiskFilterData() == value.GetPDiskFilterData());
            }
            spToEntity[key] = index;
        }

        for (const auto& [groupId, group] : systemViewsState.Groups) {
            const TBlobStorageController::TBoxStoragePoolId key(group.GetBoxId(), group.GetStoragePoolId());
            if (const auto it = spToEntity.find(key); it != spToEntity.end()) {
                auto& e = storageStats[it->second];
                e.SetCurrentGroupsCreated(e.GetCurrentGroupsCreated() + 1);
                e.SetCurrentAllocatedSize(e.GetCurrentAllocatedSize() + group.GetAllocatedSize());
                e.SetCurrentAvailableSize(e.GetCurrentAvailableSize() + group.GetAvailableSize());
            }
        }

        using T = std::decay_t<decltype(systemViewsState.PDisks)>::value_type;
        std::unordered_map<TBlobStorageController::TBoxId, std::vector<const T*>> boxes;
        for (const auto& kv : systemViewsState.PDisks) {
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
                            const TNodeLocation& location = hostRecordMap->GetLocation(pdiskId.NodeId);
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
                                if (const auto it = systemViewsState.PDisks.find(pdiskId); it != systemViewsState.PDisks.end()) {
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
                ui32 reserve = groupReserveMin;
                while (reserve < groupSizes.size() && (reserve - groupReserveMin) * 1000000 / total < groupReservePart) {
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

        return storageStats;
    }
};

IActor *CreateStorageStatsCalculator() {
    return new TStorageStatsCalculator();
}

} // NKikimr::NBsController
