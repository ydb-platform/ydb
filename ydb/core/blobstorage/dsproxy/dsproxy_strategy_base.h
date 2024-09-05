#pragma once

#include "dsproxy_blackboard.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>

namespace NKikimr {

class TStrategyBase : public IStrategy {
protected:
    enum EDiskEvaluation {
        EDE_UNKNOWN,
        EDE_ERROR,
        EDE_LOST,
        EDE_NORMAL
    };

    static constexpr ui32 InvalidVDiskIdx = Max<ui32>();

    void Finish(NKikimrProto::EReplyStatus status);
    // Altruistic - suppose all ERROR and non-responding disks have all the data
    // Optimistic - suppose all non-responding disks have the data, but ERROR disks are wiped
    // Pessimistic - suppose all ERROR and non-responding disks are actually wiped
    void EvaluateCurrentLayout(TLogContext &logCtx, TBlobState &state,
            const TBlobStorageGroupInfo &info, TBlobStorageGroupInfo::EBlobState *pessimisticState,
            TBlobStorageGroupInfo::EBlobState *optimisticState, TBlobStorageGroupInfo::EBlobState *altruisticState,
            bool considerSlowAsError);
    bool IsUnrecoverableAltruistic(TBlobStorageGroupInfo::EBlobState recoveryState);
    std::optional<EStrategyOutcome> SetAbsentForUnrecoverableAltruistic(TBlobStorageGroupInfo::EBlobState recoveryState, TBlobState &state);
    std::optional<EStrategyOutcome> ProcessOptimistic(TBlobStorageGroupInfo::EBlobState altruisticState,
            TBlobStorageGroupInfo::EBlobState optimisticState, bool isDryRun, TBlobState &state);
    std::optional<EStrategyOutcome> ProcessPessimistic(const TBlobStorageGroupInfo &info, TBlobStorageGroupInfo::EBlobState pessimisticState,
            bool doVerify, TBlobState &state);
    void AddGetRequest(TLogContext &logCtx, TGroupDiskRequests &groupDiskRequests, TLogoBlobID &fullId, ui32 partIdx,
            TBlobState::TDisk &disk, TIntervalSet<i32> &intervalSet, const char *logMarker);
    void PreparePartLayout(const TBlobState &state, const TBlobStorageGroupInfo &info,
            TBlobStorageGroupType::TPartLayout *layout,  const TStackVec<ui32, 2>& slowDiskIdxs);
    bool IsPutNeeded(const TBlobState &state, const TBlobStorageGroupType::TPartPlacement &partPlacement);
    void PreparePutsForPartPlacement(TLogContext &logCtx, TBlobState &state,
            const TBlobStorageGroupInfo &info, TGroupDiskRequests &groupDiskRequests,
            TBlobStorageGroupType::TPartPlacement &partPlacement);
    size_t RealmDomain2SubgroupIdx3dc(size_t realm, size_t domain, size_t numFailRealms);
    void Evaluate3dcSituation(const TBlobState &state,
        size_t numFailRealms, size_t numFailDomainsPerFailRealm,
        const TBlobStorageGroupInfo &info,
        bool considerSlowAsError,
        TBlobStorageGroupInfo::TSubgroupVDisks &inOutSuccess,
        TBlobStorageGroupInfo::TSubgroupVDisks &inOutError,
        bool &outIsDegraded);
    void Prepare3dcPartPlacement(const TBlobState &state, size_t numFailRealms, size_t numFailDomainsPerFailRealm,
            ui8 preferredReplicasPerRealm, bool considerSlowAsError,
            TBlobStorageGroupType::TPartPlacement &outPartPlacement);
    // Sets IsSlow for the slow disk, resets for other disks.
    // returns bit mask with 1 on positions of slow disks
    ui32 MakeSlowSubgroupDiskMask(TBlobState &state, const TBlobStorageGroupInfo &info, TBlackboard &blackboard, bool isPut,
            const TAccelerationParams& accelerationParams);
};


}//NKikimr
