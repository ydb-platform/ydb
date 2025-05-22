#pragma once

#include "defs.h"
#include "dsproxy_strategy_m3of4_base.h"

#include <util/random/shuffle.h>

namespace NKikimr {

    class TMirror3of4GetStrategy : public TMirror3of4StrategyBase {
    public:
        EStrategyOutcome Process(TLogContext& /*logCtx*/, TBlobState& state, const TBlobStorageGroupInfo& info,
                TBlackboard& /*blackboard*/, TGroupDiskRequests& groupDiskRequests,
                const TAccelerationParams& accelerationParams) override {
            Y_UNUSED(accelerationParams);
            if (!CheckFailModel(state, info)) {
                state.WholeSituation = TBlobState::ESituation::Error;
                return EStrategyOutcome::Error("failure model exceeded");
            }

            // check if the blob is already restored and can be returned to caller
            if (state.WholeSituation == TBlobState::ESituation::Present || RestoreWholeFromMirror(state)) {
                state.WholeSituation = TBlobState::ESituation::Present;
                Y_ABORT_UNLESS(state.Whole.Data && state.Whole.Data.GetTotalSize(), "%s", state.ToString().data());
                return EStrategyOutcome::DONE;
            }

            TGroups groups;

            ui32 numRequested = 0;
            ui32 numDone = 0;

            TBlobStorageGroupInfo::TSubgroupVDisks failedSubgroupDisks(&info.GetTopology());

            for (auto& group : groups.Groups) {
                ui32 untouched = 0;
                ui32 done = 0;
                for (const ui32 diskIdx : group.DiskIdx) {
                    auto& p = state.Disks[diskIdx].DiskParts[group.PartIdx];
                    switch (p.Situation) {
                        // query is either unsent or unanswered for this disk/part
                        case TBlobState::ESituation::Unknown:
                            untouched += p.Requested.IsEmpty();
                            break;

                        // if this disk has answered the query
                        case TBlobState::ESituation::Error:
                            failedSubgroupDisks |= TBlobStorageGroupInfo::TSubgroupVDisks(&info.GetTopology(), diskIdx);
                            [[fallthrough]];
                        case TBlobState::ESituation::Absent:
                        case TBlobState::ESituation::Lost:
                        case TBlobState::ESituation::Present:
                            ++done;
                            break;

                        default:
                            Y_ABORT();
                    }
                    group.NumSlowDisks += state.Disks[diskIdx].IsSlow;
                }
                Y_ABORT_UNLESS(untouched == 3 || untouched == 0);
                group.Requested = !untouched;
                numRequested += group.Requested;
                group.Done = done == 3;
                numDone += group.Done;
            }

            ui32 numRequestedMetadata = 0;
            for (ui32 diskIdx = 0; diskIdx < state.Disks.size(); ++diskIdx) {
                const auto& disk = state.Disks[diskIdx];

                switch (disk.DiskParts[MetadataPartIdx].Situation) {
                    case TBlobState::ESituation::Unknown:
                        break;

                    case TBlobState::ESituation::Error:
                        failedSubgroupDisks |= TBlobStorageGroupInfo::TSubgroupVDisks(&info.GetTopology(), diskIdx);
                        [[fallthrough]];
                    case TBlobState::ESituation::Absent:
                    case TBlobState::ESituation::Lost:
                    case TBlobState::ESituation::Present:
                        ++numRequestedMetadata;
                        break;

                    case TBlobState::ESituation::Sent:
                        Y_ABORT();
                }
            }

            if (numRequested != numDone) { // any pending groups?
                Y_ABORT_UNLESS(numRequested == numDone + 1);
            } else if (numRequested != std::size(groups.Groups)) {
                // we have to find the next group that is not requested yet
                TStackVec<TGroups::TGroupInfo*, 4> candidates;
                for (auto& group : groups.Groups) {
                    if (!group.Requested) {
                        candidates.push_back(&group);
                    }
                }
                // leave the slow group for the last query
                auto pred = [](const auto *x, const auto *y) { return x->NumSlowDisks < y->NumSlowDisks; };
                Shuffle(candidates.begin(), candidates.end());
                std::stable_sort(candidates.begin(), candidates.end(), pred);
                // pick the first candidate
                Y_ABORT_UNLESS(candidates);
                auto& group = *candidates.front();
                // issue queries
                for (const ui32 diskIdx : group.DiskIdx) {
                    auto& disk = state.Disks[diskIdx];
                    auto& part = disk.DiskParts[group.PartIdx];
                    Y_ABORT_UNLESS(part.Requested.IsEmpty()); // ensure we haven't requested any data yet
                    const TLogoBlobID id(state.Id, group.PartIdx + 1);
                    groupDiskRequests.AddGet(disk.OrderNumber, id, state.Whole.NotHere());
                    part.Requested.Add(state.Whole.NotHere());
                }
            } else if (!numRequestedMetadata) { // no metadata was requested, but we need it to make decision -- issue queries to all disks
                for (auto& disk : state.Disks) {
                    groupDiskRequests.AddGet(disk.OrderNumber, TLogoBlobID(state.Id, MetadataPartIdx + 1), 0, 0);
                }
            } else if (numRequestedMetadata == state.Disks.size()) {
                state.WholeSituation = CouldHaveBeenWritten(state, info)
                    ? TBlobState::ESituation::Error // blob could have been written, we can't just report NODATA
                    : TBlobState::ESituation::Absent; // blob couldn't have been written, we treat it as absent one
                return EStrategyOutcome::DONE;
            }

            return EStrategyOutcome::IN_PROGRESS; // shall wait for more answers
        }

    private:
        bool CouldHaveBeenWritten(TBlobState& state, const TBlobStorageGroupInfo& info) {
            TBlobStorageGroupInfo::TSubgroupVDisks data(&info.GetTopology());
            TBlobStorageGroupInfo::TSubgroupVDisks any(&info.GetTopology());

            for (ui32 diskIdx = 0; diskIdx < state.Disks.size(); ++diskIdx) {
                const auto& disk = state.Disks[diskIdx];

                for (ui32 partIdx = 0; partIdx < disk.DiskParts.size(); ++partIdx) {
                    const auto& part = disk.DiskParts[partIdx];

                    switch (part.Situation) {
                        case TBlobState::ESituation::Unknown: // we should have already probed all parts on all disks
                            Y_ABORT_UNLESS(!DiskPartsAllowed[partIdx][diskIdx]);
                            break;

                        case TBlobState::ESituation::Sent: // incorrect state
                            Y_ABORT();

                        case TBlobState::ESituation::Absent:
                            break;

                        case TBlobState::ESituation::Error: // on error we assume that the part is here
                        case TBlobState::ESituation::Lost: // on NOT_YET we also assume that part was written here
                        case TBlobState::ESituation::Present: // it is actually here :)
                            any |= {&info.GetTopology(), diskIdx};
                            if (info.Type.PartSize(TLogoBlobID(state.Id, partIdx + 1))) {
                                data |= {&info.GetTopology(), diskIdx};
                            }
                            break;
                    }
                }
            }

            return data.GetNumSetItems() >= 3 && any.GetNumSetItems() >= 5;
        }
    };

} // NKikimr
