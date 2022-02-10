#pragma once

#include "dsproxy_strategy_base.h"
#include "dsproxy_blackboard.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>

namespace NKikimr {

class TRestoreStrategy : public TStrategyBase {
public:
    void EvaluateRestoreLayout(TLogContext &logCtx, TBlobState &state,
            const TBlobStorageGroupInfo &info, TBlobStorageGroupInfo::EBlobState *pessimisticState,
            TBlobStorageGroupInfo::EBlobState *optimisticState) {
        Y_VERIFY(pessimisticState);
        Y_VERIFY(optimisticState);
        const ui32 totalPartCount = info.Type.TotalPartCount();
        ui32 errorDisks = 0;
        ui32 unknownDisks = 0;
        TSubgroupPartLayout presentLayout;
        TSubgroupPartLayout optimisticLayout;
        for (ui32 diskIdx = 0; diskIdx < state.Disks.size(); ++diskIdx) {
            TBlobState::TDisk &disk = state.Disks[diskIdx];
            bool isHandoff = (diskIdx >= totalPartCount);
            ui32 beginPartIdx = (isHandoff ? 0 : diskIdx);
            ui32 endPartIdx = (isHandoff ? totalPartCount : (diskIdx + 1));
            bool isErrorDisk = false;
            for (ui32 partIdx = beginPartIdx; partIdx < endPartIdx; ++partIdx) {
                TBlobState::ESituation partSituation = disk.DiskParts[partIdx].Situation;
                if (partSituation == TBlobState::ESituation::Error) {
                    R_LOG_DEBUG_SX(logCtx, "BPG50", "Id# " << state.Id.ToString()
                            << " Restore disk# " << diskIdx << " part# " << partIdx << " error");
                    if (!isErrorDisk) {
                        isErrorDisk = true;
                        errorDisks++;
                    }
                }
            }
            bool isUnknownDisk = true;
            if (!isErrorDisk) {
                for (ui32 partIdx = beginPartIdx; partIdx < endPartIdx; ++partIdx) {
                    TBlobState::ESituation partSituation = disk.DiskParts[partIdx].Situation;
                    if (partSituation == TBlobState::ESituation::Absent) {
                        R_LOG_DEBUG_SX(logCtx, "BPG51", "Id# " << state.Id.ToString()
                            << " restore disk# " << diskIdx << " part# " << partIdx << " absent");
                        optimisticLayout.AddItem(diskIdx, partIdx, info.Type);
                        isUnknownDisk = false;
                    } else if (partSituation == TBlobState::ESituation::Lost) {
                        R_LOG_DEBUG_SX(logCtx, "BPG63", "Id# " << state.Id.ToString()
                            << " restore disk# " << diskIdx << " part# " << partIdx << " lost");
                        optimisticLayout.AddItem(diskIdx, partIdx, info.Type);
                        isUnknownDisk = false;
                    } else if (partSituation == TBlobState::ESituation::Present) {
                        R_LOG_DEBUG_SX(logCtx, "BPG52", "Id# " << state.Id.ToString()
                            << " restore disk# " << diskIdx << " part# " << partIdx << " present");
                        presentLayout.AddItem(diskIdx, partIdx, info.Type);
                        optimisticLayout.AddItem(diskIdx, partIdx, info.Type);
                        isUnknownDisk = false;
                    } else if (partSituation == TBlobState::ESituation::Unknown) {
                        R_LOG_DEBUG_SX(logCtx, "BPG53", "Id# " << state.Id.ToString()
                            << " restore disk# " << diskIdx << " part# " << partIdx << " unknown");
                        optimisticLayout.AddItem(diskIdx, partIdx, info.Type);
                    } else if (partSituation == TBlobState::ESituation::Sent) {
                        R_LOG_DEBUG_SX(logCtx, "BPG54", "Id# " << state.Id.ToString()
                            << " restore disk# " << diskIdx << " part# " << partIdx << " sent");
                        optimisticLayout.AddItem(diskIdx, partIdx, info.Type);
                    }
                }
            }
            if (!isErrorDisk && isUnknownDisk) {
                unknownDisks++;
            }
        }

        ui32 pessimisticReplicas = presentLayout.CountEffectiveReplicas(info.Type);
        ui32 optimisticReplicas = optimisticLayout.CountEffectiveReplicas(info.Type);
        *pessimisticState = info.BlobState(pessimisticReplicas, errorDisks + unknownDisks);
        *optimisticState = info.BlobState(optimisticReplicas, errorDisks);

        R_LOG_DEBUG_SX(logCtx, "BPG55", "restore Id# " << state.Id.ToString() << " pessimisticReplicas# " << pessimisticReplicas
            << " pessimisticState# " << TBlobStorageGroupInfo::BlobStateToString(*pessimisticState)
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

    std::optional<EStrategyOutcome> IgnoreFullPessimistic(TBlobStorageGroupInfo::EBlobState pessimisticState) {
        switch (pessimisticState) {
        case TBlobStorageGroupInfo::EBS_DISINTEGRATED:
        case TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY:
        case TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY:
        case TBlobStorageGroupInfo::EBS_RECOVERABLE_DOUBTED:
            break;
        case TBlobStorageGroupInfo::EBS_FULL:
            return EStrategyOutcome::DONE; // TODO(alexvru): validate behaviour
        }
        return std::nullopt;
    }


    EStrategyOutcome Process(TLogContext &logCtx, TBlobState &state, const TBlobStorageGroupInfo &info,
            TBlackboard &blackboard, TGroupDiskRequests &groupDiskRequests) override {
        if (VerifyTheWholeSituation(state)) {
            // TODO(alexvru): ensure this branch does not hit when there are not enough parts present!
            return EStrategyOutcome::DONE; // blob is already marked as present
        }

        // Look at the current layout and set the status if possible
        TBlobStorageGroupInfo::EBlobState pessimisticState = TBlobStorageGroupInfo::EBS_DISINTEGRATED;
        TBlobStorageGroupInfo::EBlobState optimisticState = TBlobStorageGroupInfo::EBS_DISINTEGRATED;
        EvaluateRestoreLayout(logCtx, state, info, &pessimisticState, &optimisticState);

        if (auto res = SetErrorForUnrecoverableOptimistic(optimisticState)) {
            return *res;
        } else if (auto res = IgnoreFullPessimistic(pessimisticState)) {
            return *res;
        }

        // Find the slowest disk
        i32 worstSubgroupIdx = -1;
        ui64 worstPredictedNs = 0;
        ui64 nextToWorstPredictedNs = 0;
        state.GetWorstPredictedDelaysNs(info, *blackboard.GroupQueues,
                HandleClassToQueueId(blackboard.PutHandleClass),
                &worstPredictedNs, &nextToWorstPredictedNs, &worstSubgroupIdx);

        // Check if the slowest disk exceptionally slow, or just not very fast
        i32 slowDiskSubgroupIdx = -1;
        if (nextToWorstPredictedNs > 0 && worstPredictedNs > nextToWorstPredictedNs * 2) {
            slowDiskSubgroupIdx = worstSubgroupIdx;
        }

        bool isDone = false;
        if (slowDiskSubgroupIdx >= 0) {
            // If there is an exceptionally slow disk, try not touching it, mark isDone
            TBlobStorageGroupType::TPartLayout layout;
            PreparePartLayout(state, info, &layout, slowDiskSubgroupIdx);

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
            PreparePartLayout(state, info, &layout, InvalidVDiskIdx);
            TBlobStorageGroupType::TPartPlacement partPlacement;
            bool isCorrectable = info.Type.CorrectLayout(layout, partPlacement);
            Y_VERIFY(isCorrectable);
            if (IsPutNeeded(state, partPlacement)) {
                PreparePutsForPartPlacement(logCtx, state, info, groupDiskRequests, partPlacement);
            }
        }

        return EStrategyOutcome::IN_PROGRESS;
    }
};


}//NKikimr
