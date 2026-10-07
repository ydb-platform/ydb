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

    ui8 PreferredReplicasPerRealm(const T3dcSituation& situation) const {
        // calculate the least number of replicas we have to provide per each realm
        if (Tactic == TEvBlobStorage::TEvPut::TacticMinLatency) {
            return EnableRequestMod3x3ForMinLatecy || situation.MaxNotReadyInRealm >= 2 ? 3 : 2;
        }
        return situation.MaxErrorsInRealm == NumFailDomainsPerFailRealm ? 2 : 1;
    }

    EStrategyOutcome Process(TLogContext &logCtx, TBlobState &state, const TBlobStorageGroupInfo &info,
            TBlackboard& blackboard, TGroupDiskRequests &groupDiskRequests,
            const TAccelerationParams& accelerationParams) override {
        Y_UNUSED(accelerationParams);
        // Find the unput parts and disks
        bool unresponsiveDisk = false;
        for (size_t diskIdx = 0; diskIdx < state.Disks.size() && !unresponsiveDisk; ++diskIdx) {
            TBlobState::TDisk &disk = state.Disks[diskIdx];
            for (TBlobState::TDiskPart &diskPart : disk.DiskParts) {
                if (diskPart.Situation == TBlobState::ESituation::Sent) {
                    unresponsiveDisk = true;
                    break;
                }
            }
        }
        if (unresponsiveDisk) {
            blackboard.MarkSlowDisks(state, true, accelerationParams);

            for (bool considerSlowAsError : {true, false}) {
                // Prepare part placement if possible
                TBlobStorageGroupType::TPartPlacement partPlacement;

                // Count failed disks per realm to choose how many replicas to request.
                TBlobStorageGroupInfo::TSubgroupVDisks success(&info.GetTopology());
                TBlobStorageGroupInfo::TSubgroupVDisks error(&info.GetTopology());
                const auto situation = Evaluate3dcSituation(state, NumFailRealms, NumFailDomainsPerFailRealm, info,
                        considerSlowAsError, success, error);
                // check for failure tolerance; we issue ERROR in case when it is not possible to achieve success condition in
                // any way; also check if we have already finished writing replicas
                const auto& checker = info.GetQuorumChecker();
                if (checker.CheckFailModelForSubgroup(error)) {
                    if (checker.CheckQuorumForSubgroup(success)) {
                        // OK
                        return EStrategyOutcome::DONE;
                    }

                    // now check every realm and check if we have to issue some write requests to it
                    bool fullPlacement;
                    Prepare3dcPartPlacement(state, NumFailRealms, NumFailDomainsPerFailRealm,
                        PreferredReplicasPerRealm(situation), considerSlowAsError, true, partPlacement, fullPlacement);

                    if (considerSlowAsError && !fullPlacement) {
                        // unable to place all parts to fast disks, retry
                        continue;
                    }

                    if (IsPutNeeded(state, partPlacement)) {
                        PreparePutsForPartPlacement(logCtx, state, info, groupDiskRequests, partPlacement);
                    }
                    break;
                }
            }
        }

        return EStrategyOutcome::IN_PROGRESS;
    }
};


}//NKikimr
