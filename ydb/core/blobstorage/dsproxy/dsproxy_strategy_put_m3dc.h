#pragma once

#include "dsproxy_strategy_base.h"
#include "dsproxy_blackboard.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>

namespace NKikimr {

class TPut3dcStrategy : public TStrategyBase {
public:
    static constexpr size_t NumFailRealms = 3;
    static constexpr size_t NumFailDomainsPerFailRealm = 3;

    const TEvBlobStorage::TEvPut::ETactic Tactic;
    const bool EnableRequestMod3x3ForMinLatecy;

    TPut3dcStrategy(TEvBlobStorage::TEvPut::ETactic tactic, bool enableRequestMod3x3ForMinLatecy)
        : Tactic(tactic)
        , EnableRequestMod3x3ForMinLatecy(enableRequestMod3x3ForMinLatecy)
    {}

    ui8 PreferredReplicasPerRealm(bool isDegraded) const {
        // calculate the least number of replicas we have to provide per each realm
        ui8 preferredReplicasPerRealm = (isDegraded ? 2 : 1);
        if (Tactic == TEvBlobStorage::TEvPut::TacticMinLatency) {
            preferredReplicasPerRealm = (EnableRequestMod3x3ForMinLatecy ? 3 : 2);
        }
        return preferredReplicasPerRealm;
    }

    EStrategyOutcome ProcessSingleDc(TLogContext& logCtx, TBlobState& state,
            const TBlobStorageGroupInfo& info, TGroupDiskRequests& requests) {
        if (!info.SurvivingDc || *info.SurvivingDc >= NumFailRealms) {
            return EStrategyOutcome::Error("Invalid surviving realm for mirror-3-dc");
        }
        const ui32 realm = *info.SurvivingDc;
        ui32 present = 0;
        ui32 errors = 0;
        TBlobStorageGroupType::TPartPlacement placement;
        for (ui32 diskIdx = 0; diskIdx < state.Disks.size(); ++diskIdx) {
            const auto& disk = state.Disks[diskIdx];
            if (info.GetVDiskId(disk.OrderNumber).FailRealm != realm) {
                continue;
            }
            const ui32 partIdx = diskIdx % NumFailRealms;
            const auto situation = disk.DiskParts[partIdx].Situation;
            if (situation == TBlobState::ESituation::Present) {
                ++present;
            } else if (situation == TBlobState::ESituation::Error) {
                ++errors;
            } else if (situation != TBlobState::ESituation::Sent) {
                placement.Records.emplace_back(diskIdx, partIdx);
            }
        }
        if (present >= 2) {
            return EStrategyOutcome::DONE;
        }
        if (errors >= 2) {
            return EStrategyOutcome::Error("mirror-3-dc surviving realm has fewer than two writable VDisks");
        }
        if (IsPutNeeded(state, placement)) {
            PreparePutsForPartPlacement(logCtx, state, info, requests, placement);
        }
        return EStrategyOutcome::IN_PROGRESS;
    }

    EStrategyOutcome Process(TLogContext &logCtx, TBlobState &state, const TBlobStorageGroupInfo &info,
            TBlackboard& blackboard, TGroupDiskRequests &groupDiskRequests,
            const TAccelerationParams& accelerationParams) override {
        if (info.EnableSingleDcMode) {
            return ProcessSingleDc(logCtx, state, info, groupDiskRequests);
        }
        TBlobStorageGroupType::TPartPlacement partPlacement;
        bool degraded = false;
        bool isDone = false;
        ui32 slowDiskSubgroupMask = MakeSlowSubgroupDiskMask(state, blackboard, true, accelerationParams);
        do {
            if (slowDiskSubgroupMask == 0) {
                break; // ignore this case
            }
            TBlobStorageGroupInfo::TSubgroupVDisks success(&info.GetTopology());
            TBlobStorageGroupInfo::TSubgroupVDisks error(&info.GetTopology());
            Evaluate3dcSituation(state, NumFailRealms, NumFailDomainsPerFailRealm, info, false, success, error, degraded);
            TBlobStorageGroupInfo::TSubgroupVDisks slow = TBlobStorageGroupInfo::TSubgroupVDisks::CreateFromMask(
                    &info.GetTopology(), slowDiskSubgroupMask);
            if ((success | error) & slow) {
                break; // slow disk is already marked as successful or erroneous
            }
            error += slow;

            // check for failure tolerance; we issue ERROR in case when it is not possible to achieve success condition in
            // any way; also check if we have already finished writing replicas
            const auto& checker = info.GetQuorumChecker();
            if (checker.CheckFailModelForSubgroup(error)) {
                if (checker.CheckQuorumForSubgroup(success)) {
                    return EStrategyOutcome::DONE;
                }

                // now check every realm and check if we have to issue some write requests to it
                Prepare3dcPartPlacement(state, NumFailRealms, NumFailDomainsPerFailRealm,
                        PreferredReplicasPerRealm(degraded), true, false, partPlacement, isDone);
            }
        } while (false);
        if (!isDone) {
            TBlobStorageGroupInfo::TSubgroupVDisks success(&info.GetTopology());
            TBlobStorageGroupInfo::TSubgroupVDisks error(&info.GetTopology());
            Evaluate3dcSituation(state, NumFailRealms, NumFailDomainsPerFailRealm, info, false, success, error, degraded);

            // check for failure tolerance; we issue ERROR in case when it is not possible to achieve success condition in
            // any way; also check if we have already finished writing replicas
            const auto& checker = info.GetQuorumChecker();
            if (!checker.CheckFailModelForSubgroup(error)) {
                return EStrategyOutcome::Error("TPut3dcStrategy failed the Fail Model check");
            } else if (checker.CheckQuorumForSubgroup(success)) {
                return EStrategyOutcome::DONE;
            }

            // now check every realm and check if we have to issue some write requests to it
            partPlacement.Records.clear();
            bool fullPlacement;
            Prepare3dcPartPlacement(state, NumFailRealms, NumFailDomainsPerFailRealm,
                    PreferredReplicasPerRealm(degraded), false, false, partPlacement, fullPlacement);
        }
        if (IsPutNeeded(state, partPlacement)) {
            PreparePutsForPartPlacement(logCtx, state, info, groupDiskRequests, partPlacement);
        }
        return EStrategyOutcome::IN_PROGRESS;
    }
};


}//NKikimr
