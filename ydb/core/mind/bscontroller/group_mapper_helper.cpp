#include "group_mapper_helper.h"

namespace NKikimr {
    namespace NBsController {

        bool RegisterPDisk(TGroupMapper& mapper, TBlobStorageController &bsc, THostRecordMap& hostRecords, TPDiskId id, const TPDiskInfo& info, bool usable, 
            bool ignoreVSlotQuotaCheck, ui32 pdiskSpaceMarginPromille, bool settleOnlyOnOperationalDisks, bool isSelfHealReasonDecommit, TString whyUnusable) {
            // calculate number of used slots on this PDisk, also counting the static ones
            ui32 numSlots = info.NumActiveSlots + info.StaticSlotUsage;

            // create a set of groups residing on this PDisk
            TStackVec<ui32, 16> groups;
            for (const auto& [vslotId, vslot] : info.VSlotsOnPDisk) {
                if (!vslot->IsBeingDeleted()) {
                    groups.push_back(vslot->GroupId.GetRawId());
                }
            }

            // calculate vdisk space quota (or amount of available space when no enforcement is enabled)
            i64 availableSpace = Max<i64>();
            if (usable && !ignoreVSlotQuotaCheck) {
                if (info.SlotSpaceEnforced(bsc)) {
                    availableSpace = info.Metrics.GetEnforcedDynamicSlotSize() * (1000 - pdiskSpaceMarginPromille) / 1000;
                } else {
                    // here we assume that no space enforcement takes place and we have to calculate available space
                    // for this disk; we take it as available space and keep in mind that PDisk must have at least
                    // PDiskSpaceMarginPromille space remaining
                    availableSpace = info.Metrics.GetAvailableSize() - info.Metrics.GetTotalSize() * pdiskSpaceMarginPromille / 1000;

                    // also we have to find replicating VSlots on this PDisk and assume they consume up to
                    // max(vslotSize for every slot in group), not their actual AllocatedSize
                    for (const auto& [id, slot] : info.VSlotsOnPDisk) {
                        if (slot->Group && slot->GetStatus() != NKikimrBlobStorage::EVDiskStatus::READY) {
                            ui64 maxGroupSlotSize = 0;
                            for (const TVSlotInfo *peer : slot->Group->VDisksInGroup) {
                                maxGroupSlotSize = Max(maxGroupSlotSize, peer->Metrics.GetAllocatedSize());
                            }
                            // return actually used space to available pool
                            availableSpace += slot->Metrics.GetAllocatedSize();
                            // and consume expected slot size after replication finishes
                            availableSpace -= maxGroupSlotSize;
                        }
                    }
                }
            }

            if (!info.AcceptsNewSlots()) {
                usable = false;
                whyUnusable.append('S');
            }

            if (settleOnlyOnOperationalDisks && !info.Operational) {
                usable = false;
                whyUnusable.append('O');
            }

            if (!info.UsableInTermsOfDecommission(isSelfHealReasonDecommit)) {
                usable = false;
                whyUnusable.append('D');
            }

            // register PDisk in the mapper
            return mapper.RegisterPDisk({
                .PDiskId = id,
                .Location = hostRecords->GetLocation(id.NodeId),
                .Usable = usable,
                .NumSlots = numSlots,
                .MaxSlots = info.ExpectedSlotCount,
                .Groups = std::move(groups),
                .SpaceAvailable = availableSpace,
                .Operational = info.Operational,
                .Decommitted = info.Decommitted(),
                .WhyUnusable = std::move(whyUnusable),
            });
        }

     } // NBsController
} // NKikimr
