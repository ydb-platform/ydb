#pragma once

#include "defs.h"

#include "impl.h"

namespace NKikimr {
    namespace NBsController {

        class TBlobStorageController::TGroupSelector {
        public:
            static bool PopulateGroups(TVector<const TGroupInfo*>& groups,
                    const NKikimrBlobStorage::TEvControllerSelectGroups::TGroupParameters& params,
                    TBlobStorageController& controller) {
                groups.clear();
                if (params.HasErasureSpecies() && params.HasDesiredPDiskCategory() && params.HasDesiredVDiskCategory() &&
                        !params.HasStoragePoolSpecifier()) {
                    TGroupSpecies groupSpecies((TBlobStorageGroupType::EErasureSpecies)params.GetErasureSpecies(),
                        params.GetDesiredPDiskCategory(),
                        (NKikimrBlobStorage::TVDiskKind::EVDiskKind)params.GetDesiredVDiskCategory());
                    const auto iter = controller.IndexGroupSpeciesToGroup.find(groupSpecies);
                    if (iter != controller.IndexGroupSpeciesToGroup.end()) {
                        for (TGroupId groupId : iter->second) {
                            const TGroupInfo *group = controller.FindGroup(groupId);
                            Y_DEBUG_ABORT_UNLESS(group);
                            if (group && group->Listable() && !group->BridgePileId) {
                                groups.push_back(group);
                            }
                        }
                    }
                } else if (!params.HasErasureSpecies() && !params.HasDesiredPDiskCategory() &&
                        !params.HasDesiredVDiskCategory() && params.HasStoragePoolSpecifier()) {
                    PopulateGroupsFromStoragePools(groups, controller.StoragePools, controller.StoragePoolGroups,
                        std::bind(&TBlobStorageController::FindGroup, &controller, std::placeholders::_1),
                        params.GetStoragePoolSpecifier());
                } else {
                    return false;
                }
                return true;
            }

        private:
            template<typename T1, typename T2, typename T3, typename T4>
            static void PopulateGroupsFromStoragePools(TVector<const TGroupInfo*>& groups, const T1& storagePools,
                    const T2& storagePoolGroups, T3&& findGroupCallback, const T4& params) {
                for (const auto& kv : storagePools) {
                    const TStoragePoolInfo& info = kv.second;
                    if ((!params.HasName() || params.GetName() == info.Name) && (!params.HasKind() || params.GetKind() == info.Kind)) {
                        const TBoxStoragePoolId& id = kv.first;
                        for (auto it = storagePoolGroups.lower_bound(id); it != storagePoolGroups.end() && it->first == id; ++it) {
                            const TGroupInfo *groupInfo = findGroupCallback(it->second);
                            Y_DEBUG_ABORT_UNLESS(groupInfo);
                            if (groupInfo && groupInfo->Listable() && !groupInfo->BridgePileId) {
                                groups.push_back(groupInfo);
                            }
                        }
                    }
                }
            }
        };

    } // NBsController
} // NKikimr
