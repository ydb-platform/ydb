#pragma once

#include "dsproxy_strategy_base.h"
#include "dsproxy_blackboard.h"

namespace NKikimr {

class TBoldStrategy : public TStrategyBase {
    const bool IsCheck;
public:
    TBoldStrategy(bool isCheck) :
        IsCheck(isCheck) {
    };

    EStrategyOutcome Process(TLogContext &logCtx, TBlobState &state, const TBlobStorageGroupInfo &info,
            TBlackboard& /*blackboard*/, TGroupDiskRequests &groupDiskRequests,
            const TAccelerationParams& accelerationParams) override {
        Y_UNUSED(accelerationParams);
        // Look at the current layout and set the status if possible
        const ui32 totalPartCount = info.Type.TotalPartCount();
        bool doLook = true;
        if (IsCheck) {
            for (ui32 diskIdx = 0; diskIdx < state.Disks.size(); ++diskIdx) {
                bool isHandoff = (diskIdx >= totalPartCount);
                ui32 beginPartIdx = (isHandoff ? 0 : diskIdx);
                ui32 endPartIdx = (isHandoff ? totalPartCount : (diskIdx + 1));
                for (ui32 partIdx = beginPartIdx; partIdx < endPartIdx; ++partIdx) {
                    TBlobState::TDisk &disk = state.Disks[diskIdx];
                    TBlobState::ESituation partSituation = disk.DiskParts[partIdx].Situation;
                    if (partSituation == TBlobState::ESituation::Unknown) {
                        doLook = false;
                    }
                }
            }
        }

        if (doLook) {
            TBlobStorageGroupInfo::EBlobState pessimisticState = TBlobStorageGroupInfo::EBS_DISINTEGRATED;
            TBlobStorageGroupInfo::EBlobState optimisticState = TBlobStorageGroupInfo::EBS_DISINTEGRATED;
            TBlobStorageGroupInfo::EBlobState altruisticState = TBlobStorageGroupInfo::EBS_DISINTEGRATED;
            EvaluateCurrentLayout(logCtx, state, info, &pessimisticState, &optimisticState, &altruisticState, false);
            if (auto res = SetAbsentForUnrecoverableAltruistic(altruisticState, state)) {
                return *res;
            } else if (auto res = ProcessOptimistic(altruisticState, optimisticState, false, state)) {
                return *res;
            } else if (auto res = ProcessPessimistic(info, pessimisticState, true, state)) {
                return *res;
            }
        }

        // Prepare new request set
        const ui32 partSize = info.Type.PartSize(state.Id);
        for (ui32 diskIdx = 0; diskIdx < state.Disks.size(); ++diskIdx) {
            bool isHandoff = (diskIdx >= totalPartCount);
            ui32 beginPartIdx = (isHandoff ? 0 : diskIdx);
            ui32 endPartIdx = (isHandoff ? totalPartCount : (diskIdx + 1));
            for (ui32 partIdx = beginPartIdx; partIdx < endPartIdx; ++partIdx) {
                TBlobState::TDisk &disk = state.Disks[diskIdx];
                TBlobState::ESituation partSituation = disk.DiskParts[partIdx].Situation;
                if (partSituation == TBlobState::ESituation::Unknown ||
                        partSituation == TBlobState::ESituation::Present) {
                    TIntervalSet<i32> fullPartInterval(0, partSize);
                    fullPartInterval.Subtract(state.Parts[partIdx].Here());
                    fullPartInterval.Subtract(disk.DiskParts[partIdx].Requested);
                    if (!fullPartInterval.IsEmpty()) {
                        // TODO(cthulhu): Consider the case when we just need to know that there is a copy
                        //                and make an index request to avoid the data transfer.
                        //
                        //                Actually consider that we dont need to prove anything if we can
                        //                read the data.

                        // TODO(cthulhu): Group logCtx, state, info and groupDiskRequests into a context.

                        AddGetRequest(logCtx, groupDiskRequests, state.Id, partIdx, disk,
                                fullPartInterval, "BPG64");
                    }
                }
            }
        }

        return EStrategyOutcome::IN_PROGRESS;
    }
};

}//NKikimr
