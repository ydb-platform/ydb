#include "dsproxy_strategy_base.h"
#include "dsproxy_blackboard.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>

namespace NKikimr {

// TODO(cthulhu): Add extensive logs

// Altruistic - suppose all ERROR and non-responding disks have all the data
// Optimistic - suppose all non-responding disks have the data, but ERROR disks are wiped
// Pessimistic - suppose all ERROR and non-responding disks are actually wiped
void TStrategyBase::EvaluateCurrentLayout(TLogContext &logCtx, TBlobState &state,
        const TBlobStorageGroupInfo &info, TBlobStorageGroupInfo::EBlobState *pessimisticState,
        TBlobStorageGroupInfo::EBlobState *optimisticState, TBlobStorageGroupInfo::EBlobState *altruisticState,
        bool considerSlowAsError) {
    Y_ABORT_UNLESS(pessimisticState);
    Y_ABORT_UNLESS(optimisticState);
    Y_ABORT_UNLESS(altruisticState);
    TSubgroupPartLayout presentLayout;
    TSubgroupPartLayout optimisticLayout;
    TSubgroupPartLayout altruisticLayout;
    ui32 errorDisks = 0;
    ui32 lostDisks = 0;
    ui32 unknownDisks = 0;

    const ui32 totalPartCount = info.Type.TotalPartCount();
    for (ui32 diskIdx = 0; diskIdx < state.Disks.size(); ++diskIdx) {
        TBlobState::TDisk &disk = state.Disks[diskIdx];
        bool isHandoff = (diskIdx >= totalPartCount);
        ui32 beginPartIdx = (isHandoff ? 0 : diskIdx);
        ui32 endPartIdx = (isHandoff ? totalPartCount : (diskIdx + 1));
        EDiskEvaluation diskEvaluation = ((considerSlowAsError && disk.IsSlow) ? EDE_ERROR : EDE_UNKNOWN);
        for (ui32 partIdx = beginPartIdx; partIdx < endPartIdx; ++partIdx) {
            TBlobState::ESituation partSituation = disk.DiskParts[partIdx].Situation;
            if (partSituation == TBlobState::ESituation::Error) {
                R_LOG_DEBUG_SX(logCtx, "BPG41", "Id# " << state.Id.ToString()
                        << " Restore Disk# " << diskIdx << " Part# " << partIdx << " Error");
                diskEvaluation = EDE_ERROR;
            }
            if (partSituation == TBlobState::ESituation::Lost) {
                R_LOG_DEBUG_SX(logCtx, "BPG65", "Id# " << state.Id.ToString()
                        << " Restore Disk# " << diskIdx << " Part# " << partIdx << " Lost");
                if (diskEvaluation != EDE_ERROR) {
                    diskEvaluation = EDE_LOST;
                }
            }
        }
        if (diskEvaluation == EDE_ERROR) {
            for (ui32 partIdx = beginPartIdx; partIdx < endPartIdx; ++partIdx) {
                altruisticLayout.AddItem(diskIdx, partIdx, info.Type);
            }
        } else if (diskEvaluation == EDE_LOST) {
            // The disk is not replicated yet, consider it totally broken as there cant be more than
            // 'handoff' unreplicated disks at any given moment and it's safe to ignore these disks.
            // If there are some error disks at the same moment, the group should be actually disintegrated.
        } else {
            for (ui32 partIdx = beginPartIdx; partIdx < endPartIdx; ++partIdx) {
                TBlobState::ESituation partSituation = disk.DiskParts[partIdx].Situation;
                if (partSituation == TBlobState::ESituation::Present) {
                    R_LOG_DEBUG_SX(logCtx, "BPG42", "Request# "
                        << " Id# " << state.Id.ToString()
                        << " Disk# " << diskIdx << " Part# " << partIdx << " Present");
                    presentLayout.AddItem(diskIdx, partIdx, info.Type);
                    optimisticLayout.AddItem(diskIdx, partIdx, info.Type);
                    altruisticLayout.AddItem(diskIdx, partIdx, info.Type);
                    diskEvaluation = EDE_NORMAL;
                } else if (partSituation == TBlobState::ESituation::Unknown
                        || partSituation == TBlobState::ESituation::Sent) {
                    R_LOG_DEBUG_SX(logCtx, "BPG43", "Id# " << state.Id.ToString()
                        << " Disk# " << diskIdx << " Part# " << partIdx << " Unknown");
                    optimisticLayout.AddItem(diskIdx, partIdx, info.Type);
                    altruisticLayout.AddItem(diskIdx, partIdx, info.Type);
                } else if (partSituation == TBlobState::ESituation::Absent) {
                    diskEvaluation = EDE_NORMAL;
                }
            }
        }
        switch (diskEvaluation) {
            case EDE_UNKNOWN:
                unknownDisks++;
                break;
            case EDE_ERROR:
                errorDisks++;
                break;
            case EDE_LOST:
                lostDisks++;
                break;
            case EDE_NORMAL:
                break;
        }
    }

    ui32 pessimisticReplicas = presentLayout.CountEffectiveReplicas(info.Type);
    ui32 optimisticReplicas = optimisticLayout.CountEffectiveReplicas(info.Type);
    ui32 altruisticReplicas = altruisticLayout.CountEffectiveReplicas(info.Type);
    *pessimisticState = info.BlobState(pessimisticReplicas, errorDisks + unknownDisks + lostDisks);
    *optimisticState = info.BlobState(optimisticReplicas, errorDisks + lostDisks);
    *altruisticState = info.BlobState(altruisticReplicas, lostDisks);

    R_LOG_DEBUG_SX(logCtx, "BPG44", "Id# " << state.Id.ToString()
        << " pessimisticReplicas# " << pessimisticReplicas
        << " altruisticState# " << TBlobStorageGroupInfo::BlobStateToString(*altruisticState)
        << " optimisticReplicas# " << optimisticReplicas
        << " optimisticState# " << TBlobStorageGroupInfo::BlobStateToString(*optimisticState)
        << " altruisticReplicas# " << altruisticReplicas
        << " pessimisticState# " << TBlobStorageGroupInfo::BlobStateToString(*pessimisticState));
}


bool TStrategyBase::IsUnrecoverableAltruistic(TBlobStorageGroupInfo::EBlobState recoveryState) {
    switch (recoveryState) {
        case TBlobStorageGroupInfo::EBS_DISINTEGRATED:
        case TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY:
            return true;
        case TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY:
        case TBlobStorageGroupInfo::EBS_RECOVERABLE_DOUBTED:
        case TBlobStorageGroupInfo::EBS_FULL:
        break;
    }
    return false;
}

std::optional<EStrategyOutcome> TStrategyBase::SetAbsentForUnrecoverableAltruistic(
        TBlobStorageGroupInfo::EBlobState recoveryState, TBlobState &state) {
    if (IsUnrecoverableAltruistic(recoveryState)) {
        state.WholeSituation = TBlobState::ESituation::Absent;
        return EStrategyOutcome::DONE;
    }
    return std::nullopt;
}

std::optional<EStrategyOutcome> TStrategyBase::ProcessOptimistic(TBlobStorageGroupInfo::EBlobState altruisticState,
        TBlobStorageGroupInfo::EBlobState optimisticState, bool isDryRun, TBlobState &state) {
    switch (optimisticState) {
        case TBlobStorageGroupInfo::EBS_DISINTEGRATED:
            if (!isDryRun) {
                return EStrategyOutcome::Error(TStringBuilder() << "TStrategyBase saw optimisticState# "
                    << TBlobStorageGroupInfo::BlobStateToString(optimisticState));
            }
            return EStrategyOutcome::DONE;
        case TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY:
            if (altruisticState & TBlobStorageGroupInfo::EBSF_FRAGMENTARY) {
                if (!isDryRun) {
                    state.WholeSituation = TBlobState::ESituation::Absent;
                }
                return EStrategyOutcome::DONE;
            } else {
                if (!isDryRun) {
                    return EStrategyOutcome::Error(TStringBuilder() << "TStrategyBase saw optimisticState# "
                        << TBlobStorageGroupInfo::BlobStateToString(optimisticState) << " with altruisticState# "
                        << TBlobStorageGroupInfo::BlobStateToString(altruisticState));
                }
                return EStrategyOutcome::DONE;
            }
        case TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY:
        case TBlobStorageGroupInfo::EBS_RECOVERABLE_DOUBTED:
        case TBlobStorageGroupInfo::EBS_FULL:
            break;
    }
    return std::nullopt;
}

std::optional<EStrategyOutcome> TStrategyBase::ProcessPessimistic(const TBlobStorageGroupInfo &info,
        TBlobStorageGroupInfo::EBlobState pessimisticState, bool doVerify, TBlobState &state) {
    switch (pessimisticState) {
        case TBlobStorageGroupInfo::EBS_DISINTEGRATED:
            break;
        case TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY:
            break;
        case TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY:
        case TBlobStorageGroupInfo::EBS_RECOVERABLE_DOUBTED:
        case TBlobStorageGroupInfo::EBS_FULL:
            if (state.Restore(info)) {
                state.WholeSituation = TBlobState::ESituation::Present;
                return EStrategyOutcome::DONE; // blob has been restored
            } else {
                Y_ABORT_UNLESS(!doVerify);
            }
            break;
    }
    return std::nullopt;
}

void TStrategyBase::AddGetRequest(TLogContext &logCtx, TGroupDiskRequests &groupDiskRequests, TLogoBlobID &fullId,
        ui32 partIdx, TBlobState::TDisk &disk, TIntervalSet<i32> &intervalSet, const char *logMarker) {
    TLogoBlobID id(fullId, partIdx + 1);
    R_LOG_DEBUG_SX(logCtx, logMarker, "AddGet disk# " << disk.OrderNumber
            << " Id# " << id.ToString()
            << " Intervals# " << intervalSet.ToString());
    groupDiskRequests.AddGet(disk.OrderNumber, id, intervalSet);
    disk.DiskParts[partIdx].Requested.Add(intervalSet);
}

void TStrategyBase::PreparePartLayout(const TBlobState &state, const TBlobStorageGroupInfo &info,
        TBlobStorageGroupType::TPartLayout *layout, const TStackVec<ui32, 2>& slowDiskIdxs) {
    Y_ABORT_UNLESS(layout);
    const ui32 totalPartCount = info.Type.TotalPartCount();
    const ui32 blobSubringSize = info.Type.BlobSubgroupSize();
    layout->VDiskPartMask.resize(blobSubringSize);
    memset(&layout->VDiskPartMask[0], 0, sizeof(ui32) * blobSubringSize);
    for (ui32 diskIdx = 0; diskIdx < state.Disks.size(); ++diskIdx) {
        const TBlobState::TDisk &disk = state.Disks[diskIdx];
        bool isHandoff = (diskIdx >= totalPartCount);
        ui32 beginPartIdx = (isHandoff ? 0 : diskIdx);
        ui32 endPartIdx = (isHandoff ? totalPartCount : (diskIdx + 1));
        bool isErrorDisk = false;
        for (ui32 partIdx = beginPartIdx; partIdx < endPartIdx; ++partIdx) {
            TBlobState::ESituation partSituation = disk.DiskParts[partIdx].Situation;
            if (partSituation == TBlobState::ESituation::Error) {
                isErrorDisk = true;
                break;
            }
        }
        if (!isErrorDisk) {
            for (ui32 partIdx = beginPartIdx; partIdx < endPartIdx; ++partIdx) {
                TBlobState::ESituation partSituation = disk.DiskParts[partIdx].Situation;
                bool isOnSlowDisk = (std::find(slowDiskIdxs.begin(), slowDiskIdxs.end(), diskIdx) != slowDiskIdxs.end());
                if (partSituation == TBlobState::ESituation::Present ||
                        (!isOnSlowDisk && partSituation == TBlobState::ESituation::Sent)) {
                    layout->VDiskPartMask[diskIdx] |= (1ul << partIdx);
                }
                layout->VDiskMask |= (1ul << diskIdx);
            }
        }
    }
    if (slowDiskIdxs.empty()) {
        layout->SlowVDiskMask = 0;
    } else {
        layout->SlowVDiskMask = 0;
        for (ui32 slowDiskIdx : slowDiskIdxs) {
            Y_DEBUG_ABORT_UNLESS(slowDiskIdx < sizeof(layout->SlowVDiskMask) * 8);
            layout->SlowVDiskMask |= (1ull << slowDiskIdx);
        }
    }
}

bool TStrategyBase::IsPutNeeded(const TBlobState &state, const TBlobStorageGroupType::TPartPlacement &partPlacement) {
    bool isNeeded = false;
    for (const TBlobStorageGroupType::TPartPlacement::TVDiskPart& record : partPlacement.Records) {
        const TBlobState::TDisk &disk = state.Disks[record.VDiskIdx];
        TBlobState::ESituation partSituation = disk.DiskParts[record.PartIdx].Situation;
        switch (partSituation) {
            case TBlobState::ESituation::Unknown:
            case TBlobState::ESituation::Absent:
            case TBlobState::ESituation::Lost:
                isNeeded = true;
                break;
            case TBlobState::ESituation::Error:
                Y_ABORT("unexpected Situation");
            case TBlobState::ESituation::Present:
            case TBlobState::ESituation::Sent:
                break;
        }
    }
    return isNeeded;
}

void TStrategyBase::PreparePutsForPartPlacement(TLogContext &logCtx, TBlobState &state,
        const TBlobStorageGroupInfo &info, TGroupDiskRequests &groupDiskRequests,
        TBlobStorageGroupType::TPartPlacement &partPlacement) {
    bool isPartsAvailable = true;
    Y_DEBUG_ABORT_UNLESS(state.Parts.size() == info.Type.TotalPartCount());
    for (auto& record : partPlacement.Records) {
        const ui32 partIdx = record.PartIdx;
        Y_DEBUG_ABORT_UNLESS(partIdx < state.Parts.size());
        auto& part = state.Parts[partIdx];
        if (!part.Data.IsMonolith() || part.Data.GetMonolith().size() != info.Type.PartSize(TLogoBlobID(state.Id, partIdx + 1))) {
            isPartsAvailable = false;
            break;
        }
    }

    if (!isPartsAvailable) {
        // Prepare new put request set
        TIntervalSet<i32> fullInterval(0, state.Id.BlobSize());
        Y_ABORT_UNLESS(fullInterval == state.Whole.Here(), "Can't put unrestored blob! Unexpected blob state# %s", state.ToString().c_str());

        TStackVec<TRope, TypicalPartsInBlob> partData(info.Type.TotalPartCount());
        ErasureSplit((TErasureType::ECrcMode)state.Id.CrcMode(), info.Type,
            state.Whole.Data.Read(0, state.Id.BlobSize()), partData);

        for (ui32 partIdx = 0; partIdx < info.Type.TotalPartCount(); ++partIdx) {
            auto& part = state.Parts[partIdx];
            const ui32 partSize = info.Type.PartSize(TLogoBlobID(state.Id, partIdx + 1));
            if (partSize) {
                state.AddPartToPut(partIdx, std::move(partData[partIdx]));
                Y_ABORT_UNLESS(part.Data.IsMonolith());
                Y_ABORT_UNLESS(part.Data.GetMonolith().size() == partSize);
            }
        }
    }

    for (auto& record : partPlacement.Records) {
        // send record.PartIdx to record.VDiskIdx if needed
        TBlobState::TDisk &disk = state.Disks[record.VDiskIdx];
        TBlobState::ESituation partSituation = disk.DiskParts[record.PartIdx].Situation;
        A_LOG_DEBUG_SX(logCtx, "BPG33 ", "partPlacement record partSituation# " << TBlobState::SituationToString(partSituation)
                << " to# " << (ui32)record.VDiskIdx
                << " blob Id# " << TLogoBlobID(state.Id, record.PartIdx + 1).ToString());
        bool isNeeded = false;
        switch (partSituation) {
            case TBlobState::ESituation::Unknown:
            case TBlobState::ESituation::Absent:
            case TBlobState::ESituation::Lost:
                isNeeded = true;
                break;
            case TBlobState::ESituation::Error:
                Y_ABORT_UNLESS(false);
                break;
            case TBlobState::ESituation::Present:
                break;
            case TBlobState::ESituation::Sent:
                break;
        }

        if (isNeeded) {
            TLogoBlobID partId(state.Id, record.PartIdx + 1);
            A_LOG_DEBUG_SX(logCtx, "BPG32", "Sending missing VPut part# " << (ui32)record.PartIdx
                    << " to# " << (ui32)record.VDiskIdx
                    << " blob Id# " << partId.ToString());
            Y_ABORT_UNLESS(state.Parts[record.PartIdx].Data.IsMonolith());
            groupDiskRequests.AddPut(disk.OrderNumber, partId, state.Parts[record.PartIdx].Data.GetMonolith(),
                TDiskPutRequest::ReasonInitial, info.Type.IsHandoffInSubgroup(record.VDiskIdx), state.BlobIdx);
            disk.DiskParts[record.PartIdx].Situation = TBlobState::ESituation::Sent;
        }
    }
}

size_t TStrategyBase::RealmDomain2SubgroupIdx3dc(size_t realm, size_t domain, size_t numFailRealms) {
    return realm + domain * numFailRealms;
}

void TStrategyBase::Evaluate3dcSituation(const TBlobState &state,
        size_t numFailRealms, size_t numFailDomainsPerFailRealm,
        const TBlobStorageGroupInfo &info,
        bool considerSlowAsError,
        TBlobStorageGroupInfo::TSubgroupVDisks &inOutSuccess,
        TBlobStorageGroupInfo::TSubgroupVDisks &inOutError,
        bool &outIsDegraded) {
    outIsDegraded = false;
    for (size_t realm = 0; realm < numFailRealms; ++realm) {
        ui8 numErrorsInRealm = 0;
        for (size_t domain = 0; domain < numFailDomainsPerFailRealm; ++domain) {
            size_t subgroupIdx = RealmDomain2SubgroupIdx3dc(realm, domain, numFailRealms);
            const TBlobState::TDisk &disk = state.Disks[subgroupIdx];
            const TBlobState::ESituation situation = disk.DiskParts[realm].Situation;
            TBlobStorageGroupInfo::TSubgroupVDisks *subgroup = nullptr;
            if (situation == TBlobState::ESituation::Present) {
                subgroup = &inOutSuccess;
            } else if (situation == TBlobState::ESituation::Error || (considerSlowAsError && disk.IsSlow)) {
                subgroup = &inOutError;
                numErrorsInRealm++;
            }
            if (subgroup) {
                *subgroup += TBlobStorageGroupInfo::TSubgroupVDisks(&info.GetTopology(), subgroupIdx);
            }
        }
        if (numErrorsInRealm == numFailDomainsPerFailRealm) {
            outIsDegraded = true;
        }
    }
}

void TStrategyBase::Prepare3dcPartPlacement(const TBlobState &state,
        size_t numFailRealms, size_t numFailDomainsPerFailRealm,
        ui8 preferredReplicasPerRealm, bool considerSlowAsError,
        TBlobStorageGroupType::TPartPlacement &outPartPlacement) {
    for (size_t realm = 0; realm < numFailRealms; ++realm) {
        ui8 placed = 0;
        for (size_t domain = 0; placed < preferredReplicasPerRealm
                && domain < numFailDomainsPerFailRealm; ++domain) {
            size_t subgroupIdx = RealmDomain2SubgroupIdx3dc(realm, domain, numFailRealms);
            const TBlobState::TDisk &disk = state.Disks[subgroupIdx];
            const TBlobState::ESituation situation = disk.DiskParts[realm].Situation;
            if (situation != TBlobState::ESituation::Error) {
                if (situation == TBlobState::ESituation::Present) {
                    placed++;
                } else if (!considerSlowAsError || !disk.IsSlow) {
                    if (situation != TBlobState::ESituation::Sent) {
                        outPartPlacement.Records.emplace_back(subgroupIdx, realm);
                    }
                    placed++;
                }
            }
        }
    }
}

ui32 TStrategyBase::MakeSlowSubgroupDiskMask(TBlobState &state, const TBlobStorageGroupInfo &info, TBlackboard &blackboard,
        bool isPut) {
    if (info.GetTotalVDisksNum() == 1) {
        // when there is only one disk, we consider it not slow
        return 0;
    }
    // Find the slowest disk
    switch (blackboard.AccelerationMode) {
        case TBlackboard::AccelerationModeSkipOneSlowest: {
            TDiskDelayPredictions worstDisks;
            state.GetWorstPredictedDelaysNs(info, *blackboard.GroupQueues,
                    (isPut ? HandleClassToQueueId(blackboard.PutHandleClass) :
                            HandleClassToQueueId(blackboard.GetHandleClass)), 1,
                    &worstDisks);

            // Check if the slowest disk exceptionally slow, or just not very fast
            ui32 slowDiskSubgroupMask = 0;
            if (worstDisks[1].PredictedNs > 0 && worstDisks[0].PredictedNs > worstDisks[1].PredictedNs * 2) {
                slowDiskSubgroupMask = 1 << worstDisks[0].DiskIdx;
            }

            // Mark single slow disk
            for (size_t diskIdx = 0; diskIdx < state.Disks.size(); ++diskIdx) {
                state.Disks[diskIdx].IsSlow = false;
            }
            if (slowDiskSubgroupMask > 0) {
                state.Disks[worstDisks[0].DiskIdx].IsSlow = true;
            }

            return slowDiskSubgroupMask;
        }
        case TBlackboard::AccelerationModeSkipMarked: {
            ui32 slowDiskSubgroupMask = 0;
            for (size_t diskIdx = 0; diskIdx < state.Disks.size(); ++diskIdx) {
                if (state.Disks[diskIdx].IsSlow) {
                    slowDiskSubgroupMask |= 1 << diskIdx;
                }
            }
            return slowDiskSubgroupMask;
        }
    }
    return 0;
}

}//NKikimr
