#pragma once

#include "dsproxy_strategy_base.h"
#include "dsproxy_blackboard.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>

namespace NKikimr {

class TAcceleratePutStrategy : public TStrategyBase {
public:
    EStrategyOutcome Process(TLogContext &logCtx, TBlobState &state, const TBlobStorageGroupInfo &info,
            TBlackboard& /*blackboard*/, TGroupDiskRequests &groupDiskRequests, float slowDiskThreshold) override {
        Y_UNUSED(slowDiskThreshold);
        // Find the unput part and disk
        TStackVec<ui32, 2> badDiskIdxs;
        for (size_t diskIdx = 0; diskIdx < state.Disks.size(); ++diskIdx) {
            TBlobState::TDisk &disk = state.Disks[diskIdx];
            for (size_t partIdx = 0; partIdx < disk.DiskParts.size(); ++partIdx) {
                TBlobState::TDiskPart &diskPart = disk.DiskParts[partIdx];
                if (diskPart.Situation == TBlobState::ESituation::Sent) {
                    badDiskIdxs.push_back(diskIdx);
                }
            }
        }

        if (!badDiskIdxs.empty()) {
            // Mark the corresponding disks 'bad'
            // Prepare part layout if possible
            TBlobStorageGroupType::TPartLayout layout;
            PreparePartLayout(state, info, &layout, badDiskIdxs);

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
