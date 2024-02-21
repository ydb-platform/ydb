#include "common.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/data_locks/locks/list.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

#include <util/string/builder.h>

namespace NKikimr::NOlap::NDataSharing {

TString TCommonSession::DebugString() const {
    return TStringBuilder() << "{id=" << SessionId << ";context=" << TransferContext.DebugString() << ";}";
}

bool TCommonSession::Start(const NColumnShard::TColumnShard& shard) {
    const NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("info", Info);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("info", "Start");
    AFL_VERIFY(!IsStartingFlag);
    IsStartingFlag = true;
    AFL_VERIFY(!IsStartedFlag);
    const auto& index = shard.GetIndexAs<TColumnEngineForLogs>();
    THashMap<ui64, std::vector<std::shared_ptr<TPortionInfo>>> portionsByPath;
    std::vector<std::shared_ptr<TPortionInfo>> portionsLock;
    THashMap<TString, THashSet<TUnifiedBlobId>> local;
    for (auto&& i : GetPathIdsForStart()) {
//        const auto insertTableSnapshot = shard.GetInsertTable().GetMinCommittedSnapshot(i);
//        const auto shardSnapshot = shard.GetLastCompletedTx();
//        if (shard.GetInsertTable().GetMinCommittedSnapshot(i).value_or(shard.GetLastPlannedSnapshot()) <= GetSnapshotBarrier()) {
//            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("insert_table_snapshot", insertTableSnapshot)("last_completed_tx", shardSnapshot)("barrier", GetSnapshotBarrier());
//            IsStartingFlag = false;
//            return false;
//        }
        auto& portionsVector = portionsByPath[i];
        const auto& g = index.GetGranuleVerified(i);
        for (auto&& p : g.GetPortionsOlderThenSnapshot(GetSnapshotBarrier())) {
            if (shard.GetDataLocksManager()->IsLocked(*p.second)) {
                IsStartingFlag = false;
                return false;
            }
            portionsVector.emplace_back(p.second);
            portionsLock.emplace_back(p.second);
        }
    }

    IsStartedFlag = DoStart(shard, portionsByPath);
    if (IsFinishedFlag) {
        IsStartedFlag = false;
    }
    if (IsStartedFlag) {
        shard.GetDataLocksManager()->RegisterLock<NDataLocks::TListPortionsLock>("sharing_session:" + GetSessionId(), portionsLock, true);
    }
    IsStartingFlag = false;
    return IsStartedFlag;
}

void TCommonSession::Finish(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) {
    AFL_VERIFY(!IsFinishedFlag);
    IsFinishedFlag = true;
    if (IsStartedFlag) {
        dataLocksManager->UnregisterLock("sharing_session:" + GetSessionId());
    }
}

}