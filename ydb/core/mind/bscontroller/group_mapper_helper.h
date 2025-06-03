#pragma once
#include "impl.h"
#include "group_mapper.h"

namespace NKikimr {
    namespace NBsController {

        using THostRecordMap = TBlobStorageController::THostRecordMap;
        using TStoragePoolInfo = TBlobStorageController::TStoragePoolInfo;
        using TPDiskInfo = TBlobStorageController::TPDiskInfo;
        using TVSlotInfo = TBlobStorageController::TVSlotInfo;

        bool RegisterPDisk(TGroupMapper& mapper, TBlobStorageController &bsc, THostRecordMap& hostRecords, TPDiskId id, const TPDiskInfo& info, bool usable, 
            bool ignoreVSlotQuotaCheck, ui32 pdiskSpaceMarginPromille, bool settleOnlyOnOperationalDisks, bool isSelfHealReasonDecommit, TString whyUnusable = {});

        template<typename PDiskMapType>
        void PopulateGroupMapper(TGroupMapper& mapper, TBlobStorageController& bsc, PDiskMapType& pdisks, THostRecordMap& hostRecords, const THashSet<TPDiskId>& pdisksToRemove, const TBoxStoragePoolId& storagePoolId, const TBlobStorageController::TStoragePoolInfo& storagePool,
            bool ignoreVSlotQuotaCheck, ui32 pdiskSpaceMarginPromille, bool settleOnlyOnOperationalDisks, bool isSelfHealReasonDecommit) {
            const TBoxId boxId = std::get<0>(storagePoolId);

            auto callback = [&](const TPDiskId& id, const TPDiskInfo& info) {
                if (info.BoxId != boxId) {
                    return; // ignore disks not from desired box
                }

                if (pdisksToRemove.count(id)) {
                    return; // this PDisk is scheduled for removal
                }

                for (const auto& filter : storagePool.PDiskFilters) {
                    if (filter.MatchPDisk(info)) {
                        const bool inserted = RegisterPDisk(mapper, bsc, hostRecords, id, info, true, ignoreVSlotQuotaCheck,
                            pdiskSpaceMarginPromille, settleOnlyOnOperationalDisks, isSelfHealReasonDecommit);
                        Y_ABORT_UNLESS(inserted);
                        break;
                    }
                }
            };

            if constexpr (std::is_same_v<PDiskMapType, TOverlayMap<TPDiskId, TPDiskInfo>>) {
                pdisks.ForEach(callback);
            } else {
                for (const auto& [id, info] : pdisks) {
                    callback(id, *info);
                }
            }
        }
        
     } // NBsController
} // NKikimr
