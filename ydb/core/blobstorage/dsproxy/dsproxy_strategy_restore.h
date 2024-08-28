#pragma once

#include "dsproxy_strategy_base.h"
#include "dsproxy_blackboard.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>

namespace NKikimr {

class TRestoreStrategy : public TStrategyBase {
public:
    void EvaluateRestoreLayout(TLogContext &logCtx, TBlobState &state, const TBlobStorageGroupInfo &info,
            TBlobStorageGroupInfo::EBlobState *optimisticState) {
        const ui32 totalPartCount = info.Type.TotalPartCount();
        ui32 errorDisks = 0;
        TSubgroupPartLayout optimisticLayout;
        for (ui32 diskIdx = 0; diskIdx < state.Disks.size(); ++diskIdx) {
            TBlobState::TDisk &disk = state.Disks[diskIdx];
            bool isHandoff = diskIdx >= totalPartCount;
            ui32 beginPartIdx = isHandoff ? 0 : diskIdx;
            ui32 endPartIdx = isHandoff ? totalPartCount : (diskIdx + 1);
            bool isErrorDisk = false;
            for (ui32 partIdx = beginPartIdx; partIdx < endPartIdx; ++partIdx) {
                if (disk.DiskParts[partIdx].Situation == TBlobState::ESituation::Error) {
                    R_LOG_DEBUG_SX(logCtx, "BPG50", "Id# " << state.Id.ToString()
                        << " restore disk# " << diskIdx
                        << " part# " << partIdx
                        << " error");
                    isErrorDisk = true;
                    errorDisks++;
                    break;
                }
            }
            if (!isErrorDisk) {
                for (ui32 partIdx = beginPartIdx; partIdx < endPartIdx; ++partIdx) {
                    const TBlobState::ESituation partSituation = disk.DiskParts[partIdx].Situation;
                    R_LOG_DEBUG_SX(logCtx, "BPG51", "Id# " << state.Id.ToString()
                        << " restore disk# " << diskIdx
                        << " part# " << partIdx
                        << " situation# " << TBlobState::SituationToString(partSituation));

                    switch (partSituation) {
                        case TBlobState::ESituation::Absent:
                        case TBlobState::ESituation::Lost:
                        case TBlobState::ESituation::Present:
                        case TBlobState::ESituation::Unknown:
                        case TBlobState::ESituation::Sent:
                            optimisticLayout.AddItem(diskIdx, partIdx, info.Type);
                            break;

                        case TBlobState::ESituation::Error:
                            break;
                    }
                }
            }
        }

        const ui32 optimisticReplicas = optimisticLayout.CountEffectiveReplicas(info.Type);
        *optimisticState = info.BlobState(optimisticReplicas, errorDisks);

        R_LOG_DEBUG_SX(logCtx, "BPG55", "restore Id# " << state.Id.ToString()
            << " optimisticReplicas# " << optimisticReplicas
            << " optimisticState# " << TBlobStorageGroupInfo::BlobStateToString(*optimisticState));
    }

    std::optional<EStrategyOutcome> SetErrorForUnrecoverableOptimistic(TBlobStorageGroupInfo::EBlobState optimisticState) {
        switch (optimisticState) {
            case TBlobStorageGroupInfo::EBS_DISINTEGRATED:
            case TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY:
            case TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY:
            case TBlobStorageGroupInfo::EBS_RECOVERABLE_DOUBTED:
                return EStrategyOutcome::Error(TStringBuilder() << "TRestoreStrategy saw optimisticState# "
                        << TBlobStorageGroupInfo::BlobStateToString(optimisticState));
            case TBlobStorageGroupInfo::EBS_FULL:
                break;
        }
        return std::nullopt;
    }

    EStrategyOutcome Process(TLogContext &logCtx, TBlobState &state, const TBlobStorageGroupInfo &info,
            TBlackboard &blackboard, TGroupDiskRequests &groupDiskRequests,
            const TAccelerationParams& accelerationParams) override {
        // Check if the work is already done.
        if (state.WholeSituation == TBlobState::ESituation::Absent) {
            return EStrategyOutcome::DONE; // nothing to restore
        }

        // Check if the blob is present in required number of replicas.
        TSubgroupPartLayout present, presentOrSent;
        const ui32 totalPartCount = info.Type.TotalPartCount();
        for (ui32 diskIdx = 0, numDisks = state.Disks.size(); diskIdx < numDisks; ++diskIdx) {
            const TBlobState::TDisk& disk = state.Disks[diskIdx];
            const bool isHandoff = diskIdx >= totalPartCount;
            const ui32 beginPartIdx = isHandoff ? 0 : diskIdx;
            const ui32 endPartIdx = isHandoff ? totalPartCount : diskIdx + 1;
            for (ui32 partIdx = beginPartIdx; partIdx < endPartIdx; ++partIdx) {
                switch (disk.DiskParts[partIdx].Situation) {
                    case TBlobState::ESituation::Present:
                        present.AddItem(diskIdx, partIdx, info.Type);
                        [[fallthrough]];
                    case TBlobState::ESituation::Sent:
                        presentOrSent.AddItem(diskIdx, partIdx, info.Type);
                        break;

                    default:
                        break;
                }
            }
        }

        const auto& quorumChecker = info.GetQuorumChecker();
        const TBlobStorageGroupInfo::TSubgroupVDisks failed(&info.GetTopology());
        if (quorumChecker.GetBlobState(present, failed) == TBlobStorageGroupInfo::EBS_FULL) {
            state.WholeSituation = TBlobState::ESituation::Present;
            return EStrategyOutcome::DONE;
        } else if (quorumChecker.GetBlobState(presentOrSent, failed) == TBlobStorageGroupInfo::EBS_FULL) {
            return EStrategyOutcome::IN_PROGRESS;
        }

        // Look at the current layout and set the status if possible
        TBlobStorageGroupInfo::EBlobState optimisticState = TBlobStorageGroupInfo::EBS_DISINTEGRATED;
        EvaluateRestoreLayout(logCtx, state, info, &optimisticState);
        if (auto res = SetErrorForUnrecoverableOptimistic(optimisticState)) {
            return *res;
        }

        TStackVec<ui32, 2> slowDiskSubgroupIdxs;
        MakeSlowSubgroupDiskMaskForTwoSlowest(state, info, blackboard, true, accelerationParams, &slowDiskSubgroupIdxs);

        bool isDone = false;
        if (!slowDiskSubgroupIdxs.empty()) {
            // If there is an exceptionally slow disk, try not touching it, mark isDone
            TBlobStorageGroupType::TPartLayout layout;
            PreparePartLayout(state, info, &layout, slowDiskSubgroupIdxs);

            TBlobStorageGroupType::TPartPlacement partPlacement;
            bool isCorrectable = info.Type.CorrectLayout(layout, partPlacement);
            if (isCorrectable) {
                isDone = true;
                if (IsPutNeeded(state, partPlacement)) {
                    PreparePutsForPartPlacement(logCtx, state, info, groupDiskRequests, partPlacement);
                }
            }
        }
        if (!isDone) {
            // Fill in the part layout
            TBlobStorageGroupType::TPartLayout layout;
            PreparePartLayout(state, info, &layout, {});
            TBlobStorageGroupType::TPartPlacement partPlacement;
            bool isCorrectable = info.Type.CorrectLayout(layout, partPlacement);
            Y_ABORT_UNLESS(isCorrectable);
            if (IsPutNeeded(state, partPlacement)) {
                PreparePutsForPartPlacement(logCtx, state, info, groupDiskRequests, partPlacement);
            }
        }

        return EStrategyOutcome::IN_PROGRESS;
    }
};


}//NKikimr
