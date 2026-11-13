#pragma once

#include "dsproxy_blackboard.h"
#include "dsproxy_strategy_put_mirror3dc.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_mirror3dc_write_quorum.h>

#include <algorithm>
#include <array>
#include <tuple>

namespace NKikimr {

class TMirror3dcGetStrategy : public IStrategy {
    const TNodeLayoutInfoPtr NodeLayout;
    const bool PhantomCheck;
    const bool MustRestoreFirst;
    const bool AcceleratePut;

    struct TReadState {
        TIntervalSet<i32> Needed;
        TIntervalSet<i32> Missing;
        std::array<bool, 3> Included;
        TSubgroupPartLayout Parts;
        TBlobStorageGroupInfo::TSubgroupVDisks Failed;
        TBlobStorageGroupInfo::TSubgroupVDisks Uncertain;
        TBlobStorageGroupInfo::TSubgroupVDisks Unknown;
        bool HasErrors = false;
        TBlobStorageGroupInfo::EBlobState BlobState = TBlobStorageGroupInfo::EBS_DISINTEGRATED;

        TReadState(const TBlobStorageGroupInfo::TTopology& topology, const TIntervalSet<i32>& needed)
            : Needed(needed)
            , Failed(&topology)
            , Uncertain(&topology)
            , Unknown(&topology)
        {}
    };

    ui64 DiskDistance(const TBlobState& state, ui32 diskIdx) const {
        const auto& disk = state.Disks[diskIdx];
        ui64 score = ui64((diskIdx < 3 ? 0 : 1) + (disk.IsSlow ? 2 : 0)) << 32;
        if (NodeLayout) {
            const auto& diskItems = NodeLayout->LocationPerOrderNumber[disk.OrderNumber].GetItems();
            const auto& selfItems = NodeLayout->SelfLocation.GetItems();
            ui32 commonPrefixKey = Min<ui32>();
            for (auto diskIt = diskItems.begin(), selfIt = selfItems.begin();
                    diskIt != diskItems.end() && selfIt != selfItems.end() && *diskIt == *selfIt;
                    ++diskIt, ++selfIt) {
                commonPrefixKey = diskIt->first;
            }
            score += Max<ui32>() - commonPrefixKey;
        }
        return score;
    }

    EStrategyOutcome ProcessRead(TBlobState* state, TBlackboard* blackboard,
            TGroupDiskRequests* groupDiskRequests, const TMirror3dcWriteQuorumChecker& checker,
            const TReadState& readState, const TAccelerationParams& accelerationParams) const {
        if (blackboard->AccelerationMode == TBlackboard::AccelerationModeSkipNSlowest) {
            blackboard->MarkSlowDisks(*state, false, accelerationParams);
        }
        TStackVec<std::tuple<ui64, ui32>, 9> disks;
        for (ui32 disk = 0; disk < 9; ++disk) {
            if (readState.Included[disk % 3]) {
                disks.emplace_back(DiskDistance(*state, disk), disk);
            }
        }
        std::sort(disks.begin(), disks.end());
        for (const auto& [distance, diskIdx] : disks) {
            Y_UNUSED(distance);
            auto& disk = state->Disks[diskIdx];
            const ui32 part = diskIdx % 3;
            auto& diskPart = disk.DiskParts[part];
            if (diskPart.Situation != TBlobState::ESituation::Unknown && diskPart.Situation != TBlobState::ESituation::Present) {
                continue;
            }
            if (const auto request = readState.Missing - diskPart.Requested) {
                groupDiskRequests->AddGet(disk.OrderNumber, TLogoBlobID(state->Id, part + 1), request);
                diskPart.Requested.Add(request);
            }
            return EStrategyOutcome::IN_PROGRESS;
        }

        // Without ERROR, NOT_YET replies confirm missing data for an ordinary Get.
        const auto absenceState = checker.GetBlobState(readState.Parts,
            readState.HasErrors ? readState.Uncertain : readState.Failed);
        if (PhantomCheck) {
            // A likely phantom is still an error until every disk confirms missing data.
            state->LooksLikePhantom = absenceState != TBlobStorageGroupInfo::EBS_DISINTEGRATED;
        }
        if (absenceState == TBlobStorageGroupInfo::EBS_DISINTEGRATED || (PhantomCheck && readState.HasErrors)) {
            state->WholeSituation = TBlobState::ESituation::Error;
        } else {
            state->WholeSituation = TBlobState::ESituation::Absent;
        }
        return EStrategyOutcome::DONE;
    }

    EStrategyOutcome ProcessRestore(TLogContext* logCtx, TBlobState* state, const TBlobStorageGroupInfo& info,
            TBlackboard* blackboard, TGroupDiskRequests* groupDiskRequests,
            const TMirror3dcWriteQuorumChecker& checker, const TReadState& readState,
            const TAccelerationParams& accelerationParams) const {
        // On the first call, read all parts from every disk outside F realms in parallel.
        // Requested prevents duplicate reads on subsequent calls.
        for (ui32 disk = 0; disk < 9; ++disk) {
            if (readState.Unknown[disk]) {
                auto& diskPart = state->Disks[disk].DiskParts[disk % 3];
                if (diskPart.Requested.IsEmpty()) {
                    groupDiskRequests->AddGet(state->Disks[disk].OrderNumber,
                        TLogoBlobID(state->Id, disk % 3 + 1), readState.Needed);
                    diskPart.Requested = readState.Needed;
                }
            }
        }
        if (!readState.Missing) {
            if (readState.BlobState == TBlobStorageGroupInfo::EBS_DISINTEGRATED) {
                return EStrategyOutcome::Error("Mirror-3-dc read exceeds the ensured failure model");
            }
            if (checker.CheckQuorumForLayout(readState.Parts)) {
                state->WholeSituation = TBlobState::ESituation::Present;
                return EStrategyOutcome::DONE;
            }
            // Pending reads are provisional failures only for deciding when restoration may start.
            if (checker.GetBlobState(readState.Parts, readState.Unknown) == TBlobStorageGroupInfo::EBS_DISINTEGRATED) {
                return EStrategyOutcome::IN_PROGRESS;
            }
            const auto outcome = TMirror3dcPutStrategy(TEvBlobStorage::TEvPut::TacticMaxThroughput,
                false, AcceleratePut).Process(*logCtx, *state, info, *blackboard, *groupDiskRequests,
                    accelerationParams);
            if (outcome == EStrategyOutcome::DONE) {
                state->WholeSituation = TBlobState::ESituation::Present;
            }
            return outcome;
        }
        if (readState.Unknown) {
            return EStrategyOutcome::IN_PROGRESS;
        }
        const auto absenceState = checker.GetBlobState(readState.Parts, readState.Uncertain);
        if (PhantomCheck) {
            state->LooksLikePhantom = absenceState != TBlobStorageGroupInfo::EBS_DISINTEGRATED;
        }
        if (absenceState == TBlobStorageGroupInfo::EBS_DISINTEGRATED || (PhantomCheck && readState.HasErrors)) {
            state->WholeSituation = TBlobState::ESituation::Error;
        } else {
            state->WholeSituation = TBlobState::ESituation::Absent;
        }
        return EStrategyOutcome::DONE;
    }

public:
    TMirror3dcGetStrategy(const TNodeLayoutInfoPtr& nodeLayout, bool phantomCheck,
            bool mustRestoreFirst = false, bool acceleratePut = false)
        : NodeLayout(nodeLayout)
        , PhantomCheck(phantomCheck)
        , MustRestoreFirst(mustRestoreFirst)
        , AcceleratePut(acceleratePut)
    {}

    EStrategyOutcome Process(TLogContext& logCtx, TBlobState& state, const TBlobStorageGroupInfo& info,
            TBlackboard& blackboard, TGroupDiskRequests& groupDiskRequests,
            const TAccelerationParams& accelerationParams) override {
        if (state.WholeSituation == TBlobState::ESituation::Present) {
            return EStrategyOutcome::DONE;
        }
        const auto checker = TMirror3dcWriteQuorumChecker::TryCreate(info, state.Id.Hash());
        if (!checker || state.Disks.size() != 9) {
            return EStrategyOutcome::Error("Invalid mirror-3-dc read state");
        }

        TReadState readState(info.GetTopology(), MustRestoreFirst
            ? TIntervalSet<i32>(0, state.Id.BlobSize()) : state.Whole.Needed);
        for (ui32 part = 0; part < 3; ++part) {
            const ui32 realm = info.GetVDiskId(state.Disks[part].OrderNumber).FailRealm;
            const auto realmState = info.GetFailRealmState(realm);
            readState.Included[part] = !IsExcluded(realmState);
            if (readState.Included[part]) {
                const auto& data = state.Parts[part];
                state.Whole.Data.CopyFrom(data.Data, data.Here() & (readState.Needed - state.Whole.Here()));
            }
        }

        readState.Missing = readState.Needed - state.Whole.Here();
        if (!MustRestoreFirst && !readState.Missing) {
            state.WholeSituation = TBlobState::ESituation::Present;
            return EStrategyOutcome::DONE;
        }

        for (ui32 disk = 0; disk < 9; ++disk) {
            const ui32 part = disk % 3;
            if (!readState.Included[part]) {
                continue;
            }
            auto& diskPart = state.Disks[disk].DiskParts[part];
            switch (diskPart.Situation) {
                case TBlobState::ESituation::Present:
                    readState.Parts.AddItem(disk, part, info.Type);
                    break;
                case TBlobState::ESituation::Error:
                    readState.HasErrors = true;
                    readState.Failed += TBlobStorageGroupInfo::TSubgroupVDisks(&info.GetTopology(), disk);
                    [[fallthrough]];
                case TBlobState::ESituation::Lost:
                    readState.Uncertain += TBlobStorageGroupInfo::TSubgroupVDisks(&info.GetTopology(), disk);
                    break;
                case TBlobState::ESituation::Unknown:
                    readState.Unknown += TBlobStorageGroupInfo::TSubgroupVDisks(&info.GetTopology(), disk);
                    break;
                case TBlobState::ESituation::Absent:
                    break;
                case TBlobState::ESituation::Sent:
                    if (!MustRestoreFirst) {
                        return EStrategyOutcome::Error("Unexpected pending write in mirror-3-dc read");
                    }
                    break;
            }
        }

        readState.BlobState = checker->GetBlobState(readState.Parts, readState.Failed);
        // Return ERROR when failed disks exceed the current ensured failure model.
        // E=0 provides no failure bound, so continue reading to look for a copy.
        // Phantom checks collect replies and report each blob's verdict without aborting the batch.
        if (!PhantomCheck && readState.BlobState == TBlobStorageGroupInfo::EBS_DISINTEGRATED
                && checker->GetEnsuredLayout() != EMirror3dcLayout::LAYOUT_0) {
            return EStrategyOutcome::Error("Mirror-3-dc read exceeds the ensured failure model");
        }
        return MustRestoreFirst
            ? ProcessRestore(&logCtx, &state, info, &blackboard, &groupDiskRequests, *checker, readState,
                accelerationParams)
            : ProcessRead(&state, &blackboard, &groupDiskRequests, *checker, readState, accelerationParams);
    }
};

} // namespace NKikimr
