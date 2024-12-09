#pragma once

#include "defs.h"
#include "dsproxy_blackboard.h"
#include "dsproxy_strategy_put_m3dc.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>

namespace NKikimr {

    class TMirror3dcGetWithRestoreStrategy : public IStrategy {
        static constexpr ui32 NumRings = 3;
        static constexpr ui32 NumFailDomainsPerRing = 3;

    public:
        EStrategyOutcome Process(TLogContext& logCtx, TBlobState& state, const TBlobStorageGroupInfo& info,
                TBlackboard& blackboard, TGroupDiskRequests& groupDiskRequests,
                const TAccelerationParams& accelerationParams) override {
            if (state.WholeSituation == TBlobState::ESituation::Present) {
                return EStrategyOutcome::DONE;
            }

            // The way to check disk status:
            // Send reads to all disks
            PrepareGets(logCtx, state, groupDiskRequests);
            // If blob is absent, done
            if (auto res = CheckForFailureErrorNoData(logCtx, state, info)) {
                return *res;
            }
            // If blob is present at main, done
            // If blob is present in 2 domains x 2 rings, done
            if (BlobIsPresentAtMainOr2x2(state)) {
                // everything is in place -- all data is present; generate output blob content and signal success for
                // this one
                const bool success = RestoreWholeFromMirror(state);
                Y_ABORT_UNLESS(success);
                state.WholeSituation = TBlobState::ESituation::Present;
                return EStrategyOutcome::DONE;
            }
            // Wait until fail-model is satisfied
            if (!IsGetFailModelSatisfied(state, info)) {
                // the request hasn't completed yet, so we shall wait for it; we will re-enter this function when
                // something gets changed
                return EStrategyOutcome::IN_PROGRESS;
            }
            // Remaining disks may never answer, start the restoration
            if (!RestoreWholeFromMirror(state)) {
                return EStrategyOutcome::IN_PROGRESS;
            }

            // Try to send puts considering the slow disk
            //   on failure, send puts
            // On 'accelerate' signal, try to send more puts considering the slow disk
            Y_ABORT_UNLESS(state.WholeSituation == TBlobState::ESituation::Unknown,
                    "Blob Id# %s unexpected whole situation %" PRIu32,
                    state.Id.ToString().c_str(), ui32(state.WholeSituation));
            state.WholeSituation = TBlobState::ESituation::Present;
            const EStrategyOutcome outcome = TPut3dcStrategy(TEvBlobStorage::TEvPut::TacticMaxThroughput, false).Process(logCtx,
                state, info, blackboard, groupDiskRequests, accelerationParams);
            switch (outcome) {
                case EStrategyOutcome::IN_PROGRESS:
                    state.WholeSituation = TBlobState::ESituation::Unknown;
                    break;

                case EStrategyOutcome::ERROR:
                case EStrategyOutcome::DONE:
                    break;
            }
            return outcome;
        }

    private:
        static void PrepareGets(TLogContext& logCtx, TBlobState& state, TGroupDiskRequests& groupDiskRequests) {
            const TIntervalVec<i32> needed(0, state.Id.BlobSize()); // we need to query this interval
            for (ui32 diskIdx = 0; diskIdx < state.Disks.size(); ++diskIdx) {
                TBlobState::TDisk& disk = state.Disks[diskIdx];
                const ui32 partIdx = diskIdx % NumRings;
                TBlobState::TDiskPart& diskPart = disk.DiskParts[partIdx];
                if (diskPart.Situation == TBlobState::ESituation::Unknown) {
                    if (diskPart.Requested.IsEmpty()) {
                        TLogoBlobID id(state.Id, partIdx + 1);
                        groupDiskRequests.AddGet(disk.OrderNumber, id, needed);
                        diskPart.Requested = needed;
                        A_LOG_DEBUG_SX(logCtx, "3DCGR10", "sending Get"
                                << " diskIdx# " << diskIdx
                                << " OrderNumber# " << disk.OrderNumber
                                << " Requested# " << diskPart.Requested.ToString()
                                << " Id# " << id.ToString());
                    } else {
                        Y_ABORT_UNLESS(diskPart.Requested == needed);
                    }
                }
            }
        }

        static bool IsGetFailModelSatisfied(TBlobState &state, const TBlobStorageGroupInfo &info) {
            TBlobStorageGroupInfo::TSubgroupVDisks beingWaitedFor(&info.GetTopology());
            for (ui32 diskIdx = 0; diskIdx < state.Disks.size(); ++diskIdx) {
                const TBlobState::TDisk& disk = state.Disks[diskIdx];
                const ui32 partIdx = diskIdx % NumRings;
                const TBlobState::TDiskPart& diskPart = disk.DiskParts[partIdx];
                if (diskPart.Situation == TBlobState::ESituation::Unknown) {
                    beingWaitedFor |= TBlobStorageGroupInfo::TSubgroupVDisks(&info.GetTopology(), diskIdx);
                }
            }
            // If the fail model is exceeded, we should wait for more responses
            const auto& checker = info.GetQuorumChecker();
            return checker.CheckFailModelForSubgroup(beingWaitedFor);
        }

        static bool BlobIsPresentAtMainOr2x2(const TBlobState& state) {
            ui32 numPresentRings = 0;
            ui32 numPresentX2Rings = 0;
            for (ui32 partIdx = 0; partIdx < NumRings; ++partIdx) {
                ui32 numPresent = 0;
                for (ui32 fd = 0; fd < NumFailDomainsPerRing; ++fd) {
                    const ui32 diskIdx = fd * NumRings + partIdx;
                    numPresent += state.Disks[diskIdx].DiskParts[partIdx].Situation == TBlobState::ESituation::Present;
                }
                if (numPresent) {
                    ++numPresentRings;
                    if (numPresent >= 2) {
                        ++numPresentX2Rings;
                    }
                }
            }
            return numPresentRings == NumRings || numPresentX2Rings >= 2;
        }

        std::optional<EStrategyOutcome> CheckForFailureErrorNoData(TLogContext& logCtx, TBlobState& state,
                const TBlobStorageGroupInfo& info) {
            ui32 altruisticPresentCount = 0;
            // calculate subsets of disks with present replicas and failed disks
            TBlobStorageGroupInfo::TSubgroupVDisks present(&info.GetTopology());
            TBlobStorageGroupInfo::TSubgroupVDisks failed(&info.GetTopology());
            TBlobStorageGroupInfo::TSubgroupVDisks possiblyWritten(&info.GetTopology());
            for (ui32 diskIdx = 0; diskIdx < state.Disks.size(); ++diskIdx) {
                const TBlobState::TDisk& disk = state.Disks[diskIdx];
                const ui32 partIdx = diskIdx % NumRings;
                const TBlobState::TDiskPart& diskPart = disk.DiskParts[partIdx];
                switch (diskPart.Situation) {
                    case TBlobState::ESituation::Sent:
                    case TBlobState::ESituation::Present:
                    case TBlobState::ESituation::Unknown:
                        ++altruisticPresentCount;
                        break;

                    case TBlobState::ESituation::Absent:
                        // just ignore this one
                        break;

                    case TBlobState::ESituation::Lost:
                        possiblyWritten |= TBlobStorageGroupInfo::TSubgroupVDisks(&info.GetTopology(), diskIdx);
                        break;

                    case TBlobState::ESituation::Error:
                        failed |= TBlobStorageGroupInfo::TSubgroupVDisks(&info.GetTopology(), diskIdx);
                        break;
                }
            }

            A_LOG_DEBUG_SX(logCtx, "3DCGR02", "CheckForFailureErrorNoData"
                << " state# " << state.ToString());

            // check if we do not excess the fail model
            const auto& checker = info.GetQuorumChecker();
            if (!checker.CheckFailModelForSubgroup(failed)) {
                return EStrategyOutcome::Error("TMirror3dcGetWithRestoreStrategy failed the Fail Model check");
            }

            // check if there is a part that can be used to put the blob (or at least some hope of obtaining such part)
            if (altruisticPresentCount) {
                return std::nullopt;
            }
            // if we got here, the blob is definitely absent

            possiblyWritten |= failed; // they may have been written too
            state.WholeSituation = checker.CheckQuorumForSubgroup(possiblyWritten)
                ? TBlobState::ESituation::Error // this blob was probably written, but we can't reach it
                : TBlobState::ESituation::Absent; // this blob definitely wasn't written
            return EStrategyOutcome::DONE;
        }
    };

} // NKikimr
