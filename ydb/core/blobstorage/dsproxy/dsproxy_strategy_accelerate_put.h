#pragma once

#include "dsproxy_strategy_base.h"
#include "dsproxy_blackboard.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>

namespace NKikimr {

class TAcceleratePutStrategy : public TStrategyBase {
public:
    EStrategyOutcome Process(TLogContext &logCtx, TBlobState &state, const TBlobStorageGroupInfo &info,
            TBlackboard& /*blackboard*/, TGroupDiskRequests &groupDiskRequests,
            const TAccelerationParams& accelerationParams) override {
        Y_UNUSED(accelerationParams);
        // Find the unput part and disk
        ui32 badDisksMask = 0;
        for (size_t diskIdx = 0; diskIdx < state.Disks.size(); ++diskIdx) {
            TBlobState::TDisk &disk = state.Disks[diskIdx];
            for (size_t partIdx = 0; partIdx < disk.DiskParts.size(); ++partIdx) {
                TBlobState::TDiskPart &diskPart = disk.DiskParts[partIdx];
                if (diskPart.Situation == TBlobState::ESituation::Sent) {
                    badDisksMask |= (1 << diskIdx);
                }
            }
        }

        if (badDisksMask > 0) {
            // Mark the corresponding disks 'bad'
            // Prepare part layout if possible
            TBlobStorageGroupType::TPartLayout layout;
            PreparePartLayout(state, info, &layout, badDisksMask);

            TBlobStorageGroupType::TPartPlacement partPlacement;
            bool isCorrectable = info.Type.CorrectLayout(layout, partPlacement);
            bool isPutNeeded = IsPutNeeded(state, partPlacement);
            if (isCorrectable && isPutNeeded) {
                PreparePutsForPartPlacement(logCtx, state, info, groupDiskRequests, partPlacement);
            }
        }

        return EStrategyOutcome::IN_PROGRESS;
    }
};

}//NKikimr
