#include "common.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/data_locks/locks/snapshot.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

#include <util/string/builder.h>

namespace NKikimr::NOlap::NDataSharing {

TString TCommonSession::DebugString() const {
    return TStringBuilder() << "{id=" << SessionId << ";context=" << TransferContext.DebugString() << ";state=" << State << ";}";
}

bool TCommonSession::TryStart(const NColumnShard::TColumnShard& shard) {
    const NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("info", Info);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("info", "Start");
    AFL_VERIFY(State == EState::Prepared);

    AFL_VERIFY(!!LockGuard);
    const auto& index = shard.GetIndexAs<TColumnEngineForLogs>();
    THashMap<ui64, std::vector<std::shared_ptr<TPortionInfo>>> portionsByPath;
    THashSet<TString> StoragesIds;
    for (auto&& i : GetPathIdsForStart()) {
        auto& portionsVector = portionsByPath[i];
        const auto& g = index.GetGranuleVerified(i);
        for (auto&& p : g.GetPortionsOlderThenSnapshot(GetSnapshotBarrier())) {
            using namespace NOlap::NDataLocks;
            if (shard.GetDataLocksManager()->IsLocked(*p.second, TLockFilter::AllBut({ TManager::GetSharingSessionLockName(GetSessionId()) }))) {
                return false;
            }
            portionsVector.emplace_back(p.second);
        }
    }

    if (shard.GetStoragesManager()->GetSharedBlobsManager()->HasExternalModifications()) {
        return false;
    }

    AFL_VERIFY(DoStart(shard, portionsByPath));
    State = EState::InProgress;
    return true;
}

void TCommonSession::PrepareToStart(const NColumnShard::TColumnShard& shard) {
    const NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("info", Info);
    AFL_VERIFY(State == EState::Created);
    State = EState::Prepared;
    AFL_VERIFY(!LockGuard);
    using namespace NDataLocks;
    LockGuard = shard.GetDataLocksManager()->RegisterLock<TSnapshotLock>(TManager::GetSharingSessionLockName(GetSessionId()),
        TransferContext.GetSnapshotBarrierVerified(), GetPathIdsForStart(), ELockCategory::ReadOnly);
    shard.GetSharingSessionsManager()->StartSharingSession();
}

void TCommonSession::Finish(const NColumnShard::TColumnShard& shard, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) {
    AFL_VERIFY(State == EState::InProgress);
    State = EState::Finished;
    shard.GetSharingSessionsManager()->FinishSharingSession();
    AFL_VERIFY(LockGuard);
    LockGuard->Release(*dataLocksManager);
}

}