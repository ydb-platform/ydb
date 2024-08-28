#pragma once

#include "dsproxy_strategy_base.h"
#include "dsproxy_blackboard.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>

namespace NKikimr {

class TAcceleratePut3dcStrategy : public TStrategyBase {
public:
    static constexpr size_t NumFailRealms = 3;
    static constexpr size_t NumFailDomainsPerFailRealm = 3;

    const TEvBlobStorage::TEvPut::ETactic Tactic;
    const bool EnableRequestMod3x3ForMinLatecy;

    TAcceleratePut3dcStrategy(TEvBlobStorage::TEvPut::ETactic tactic, bool enableRequestMod3x3ForMinLatecy)
        : Tactic(tactic)
        , EnableRequestMod3x3ForMinLatecy(enableRequestMod3x3ForMinLatecy)
    {}

    ui8 PreferredReplicasPerRealm(bool isDegraded) const {
        // calculate the least number of replicas we have to provide per each realm
        if (Tactic == TEvBlobStorage::TEvPut::TacticMinLatency) {
            return EnableRequestMod3x3ForMinLatecy ? 3 : 2;
        }
        return isDegraded ? 2 : 1;
    }

    EStrategyOutcome Process(TLogContext &logCtx, TBlobState &state, const TBlobStorageGroupInfo &info,
            TBlackboard& /*blackboard*/, TGroupDiskRequests &groupDiskRequests,
            const TAccelerationParams& accelerationParams) override {
        Y_UNUSED(accelerationParams);
        // Find the unput parts and disks
        ui32 badDiskMask = 0;
        for (size_t diskIdx = 0; diskIdx < state.Disks.size(); ++diskIdx) {
            TBlobState::TDisk &disk = state.Disks[diskIdx];
            for (size_t partIdx = 0; partIdx < disk.DiskParts.size(); ++partIdx) {
                TBlobState::TDiskPart &diskPart = disk.DiskParts[partIdx];
                if (diskPart.Situation == TBlobState::ESituation::Sent) {
                    badDiskMask |= (1 << diskIdx);
                }
            }
        }
        if (badDiskMask > 0) {
            // Mark all the slow disks
            for (size_t diskIdx = 0; diskIdx < state.Disks.size(); ++diskIdx) {
                state.Disks[diskIdx].IsSlow = badDiskMask & (1 << diskIdx);
            }

            // Prepare part placement if possible
            TBlobStorageGroupType::TPartPlacement partPlacement;
            bool degraded = false;

            // check if we are in degraded mode -- that means that we have one fully failed realm
            TBlobStorageGroupInfo::TSubgroupVDisks success(&info.GetTopology());
            TBlobStorageGroupInfo::TSubgroupVDisks error(&info.GetTopology());
            Evaluate3dcSituation(state, NumFailRealms, NumFailDomainsPerFailRealm, info, true, success, error, degraded);

            // check for failure tolerance; we issue ERROR in case when it is not possible to achieve success condition in
            // any way; also check if we have already finished writing replicas
            const auto& checker = info.GetQuorumChecker();
            if (checker.CheckFailModelForSubgroup(error)) {
                if (checker.CheckQuorumForSubgroup(success)) {
                    // OK
                    return EStrategyOutcome::DONE;
                }

                // now check every realm and check if we have to issue some write requests to it
                Prepare3dcPartPlacement(state, NumFailRealms, NumFailDomainsPerFailRealm,
                    PreferredReplicasPerRealm(degraded), true, partPlacement);

                if (IsPutNeeded(state, partPlacement)) {
                    PreparePutsForPartPlacement(logCtx, state, info, groupDiskRequests, partPlacement);
                }
            }
        }

        return EStrategyOutcome::IN_PROGRESS;
    }
};


}//NKikimr
