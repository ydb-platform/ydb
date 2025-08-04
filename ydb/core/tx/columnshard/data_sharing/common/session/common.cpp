#include "common.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/data_locks/locks/snapshot.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

#include <util/string/builder.h>

namespace NKikimr::NOlap::NDataSharing {

TString TCommonSession::DebugString() const {
    return TStringBuilder() << "{id=" << SessionId << ";context=" << TransferContext.DebugString() << ";state=" << State << ";}";
}

TConclusionStatus TCommonSession::TryStart(NColumnShard::TColumnShard& shard) {
    const NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("info", Info);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("info", "Start");
    AFL_VERIFY(State == EState::Prepared);

    AFL_VERIFY(!!LockGuard);
    const auto& index = shard.GetIndexAs<TColumnEngineForLogs>();
    THashMap<TInternalPathId, std::vector<std::shared_ptr<TPortionDataAccessor>>> portionsByPath;
    THashSet<TString> StoragesIds;
    for (auto&& i : GetPathIdsForStart()) {
        const auto& g = index.GetGranuleVerified(i);
        for (auto&& p : g.GetPortionsOlderThenSnapshot(GetSnapshotBarrier())) {
            if (shard.GetDataLocksManager()->IsLocked(
                    *p.second, NDataLocks::ELockCategory::Sharing, { "sharing_session:" + GetSessionId() })) {
                return TConclusionStatus::Fail("failed to start cursor: portion is locked");
            }
//            portionsByPath[i].emplace_back(p.second);
        }
    }

    if (shard.GetStoragesManager()->GetSharedBlobsManager()->HasExternalModifications()) {
        return TConclusionStatus::Fail("failed to start cursor: has external modifications");
    }

    TConclusionStatus status = DoStart(shard, std::move(portionsByPath));
    if (status.Ok()) {
        State = EState::InProgress;
    }
    return status;
}

void TCommonSession::PrepareToStart(const NColumnShard::TColumnShard& shard) {
    const NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("info", Info);
    AFL_VERIFY(State == EState::Created);
    State = EState::Prepared;
    AFL_VERIFY(!LockGuard);
    LockGuard = shard.GetDataLocksManager()->RegisterLock<NDataLocks::TSnapshotLock>("sharing_session:" + GetSessionId(),
        TransferContext.GetSnapshotBarrierVerified(), GetPathIdsForStart(), NDataLocks::ELockCategory::Sharing, true);
    shard.GetSharingSessionsManager()->StartSharingSession();
}

void TCommonSession::Finish(const NColumnShard::TColumnShard& shard, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) {
    AFL_VERIFY(State == EState::InProgress || State == EState::Prepared);
    State = EState::Finished;
    shard.GetSharingSessionsManager()->FinishSharingSession();
    AFL_VERIFY(LockGuard);
    LockGuard->Release(*dataLocksManager);
}

}