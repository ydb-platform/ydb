#pragma once

#include "defs.h"
#include "dsproxy_blackboard.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>

namespace NKikimr {

    class TMirror3dcCheckGetStrategy : public IStrategy {
        static constexpr ui32 NumRings = 3;
        static constexpr ui32 NumFailDomainsPerRing = 3;

    public:
        bool DoRequestDisk(TBlobState& state, TGroupDiskRequests& groupDiskRequests, ui32 diskIdx) {
            TBlobState::TDisk& disk = state.Disks[diskIdx];
            const ui32 partIdx = diskIdx % NumRings;
            TBlobState::TDiskPart& diskPart = disk.DiskParts[partIdx];
            switch (diskPart.Situation) {
                case TBlobState::ESituation::Unknown: {
                    if (const TIntervalSet<i32> request = state.Whole.NotHere() - diskPart.Requested) {
                        TLogoBlobID id(state.Id, partIdx + 1);
                        groupDiskRequests.AddGet(disk.OrderNumber, id, request);
                        diskPart.Requested.Add(request);
                    } else {
                        Y_ABORT_UNLESS(diskPart.Requested);
                    }
                    return true;
                }
                case TBlobState::ESituation::Present:
                    break;
                case TBlobState::ESituation::Error:
                    break;
                case TBlobState::ESituation::Absent:
                    break;
                case TBlobState::ESituation::Lost:
                    break;
                case TBlobState::ESituation::Sent:
                    Y_ABORT("unexpected state");
            }
            return false;
        }

        EStrategyOutcome Process(TLogContext& logCtx, TBlobState& state, const TBlobStorageGroupInfo& info,
                TBlackboard& blackboard, TGroupDiskRequests& groupDiskRequests,
                const TAccelerationParams& accelerationParams) override {
            Y_UNUSED(blackboard);
            Y_UNUSED(accelerationParams);

            bool requested = false;
            for (ui32 diskIdx = 0; diskIdx < state.Disks.size(); ++diskIdx) {
                requested = requested || DoRequestDisk(state, groupDiskRequests, diskIdx);
            }

            TBlobStorageGroupInfo::TSubgroupVDisks failed(&info.GetTopology()), possiblyWritten(&info.GetTopology());
            TStackVec<TBlobState::ESituation, NumRings * NumFailDomainsPerRing> situations;
            for (ui32 diskIdx = 0; diskIdx < state.Disks.size(); ++diskIdx) {
                TBlobState::TDisk& disk = state.Disks[diskIdx];
                const ui32 partIdx = diskIdx % NumRings;
                const TBlobState::TDiskPart& diskPart = disk.DiskParts[partIdx];
                switch (diskPart.Situation) {
                    case TBlobState::ESituation::Error:
                        failed += TBlobStorageGroupInfo::TSubgroupVDisks(&info.GetTopology(), diskIdx);
                        [[fallthrough]];
                    case TBlobState::ESituation::Lost:
                        possiblyWritten += TBlobStorageGroupInfo::TSubgroupVDisks(&info.GetTopology(), diskIdx);
                        break;
                    case TBlobState::ESituation::Unknown:
                        return EStrategyOutcome::IN_PROGRESS;
                    default:
                        break;
                }
                situations.push_back(diskPart.Situation);
            }

            if (state.WholeSituation == TBlobState::ESituation::Present) {
                return EStrategyOutcome::DONE;
            }

            if (RestoreWholeFromMirror(state)) {
                state.WholeSituation = TBlobState::ESituation::Present;
                return EStrategyOutcome::DONE;
            }

            if (!info.GetQuorumChecker().CheckFailModelForSubgroup(failed)) {
                return EStrategyOutcome::Error("TMirror3dcBasicGetStrategy failed the Fail Model check");
            } else if (requested) {
                return EStrategyOutcome::IN_PROGRESS;
            } else if (!state.Whole.Needed.IsSubsetOf(state.Whole.Here())) {
                R_LOG_WARN_SX(logCtx, "BPG48", "missing blob# " << state.Id.ToString() << " state# " << state.ToString());
                state.WholeSituation = TBlobState::ESituation::Absent;
                if (info.GetQuorumChecker().CheckQuorumForSubgroup(possiblyWritten)) {
                    for (const TBlobState::ESituation situation : situations) {
                        switch (situation) {
                            case TBlobState::ESituation::Absent:
                            case TBlobState::ESituation::Lost:
                                break;

                            case TBlobState::ESituation::Unknown:
                            case TBlobState::ESituation::Present:
                            case TBlobState::ESituation::Sent:
                                // unexpected state
                                Y_DEBUG_ABORT_UNLESS(false);
                                [[fallthrough]];
                            case TBlobState::ESituation::Error:
                                state.WholeSituation = TBlobState::ESituation::Error;
                                break;
                        }
                    }
                }
                return EStrategyOutcome::DONE;
            } else {
                Y_ABORT("must not reach this point");
            }
        }
    };

} // NKikimr
