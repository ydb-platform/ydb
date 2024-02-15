#include "common.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/data_locks/locks/list.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

#include <util/string/builder.h>

namespace NKikimr::NOlap::NDataSharing {

TString TCommonSession::DebugString() const {
    return TStringBuilder() << "{id=" << SessionId << ";snapshot=" << SnapshotBarrier.DebugString() << ";}";
}

bool TCommonSession::Start(const NColumnShard::TColumnShard& shard) {
    AFL_VERIFY(!IsStartedFlag);
    const auto& index = shard.GetIndexAs<TColumnEngineForLogs>();
    THashMap<ui64, std::vector<std::shared_ptr<TPortionInfo>>> portionsByPath;
    std::vector<std::shared_ptr<TPortionInfo>> portionsLock;
    THashMap<TString, THashSet<TUnifiedBlobId>> local;
    for (auto&& i : GetPathIdsForStart()) {
        if (shard.GetInsertTable().GetMinCommittedSnapshot(i).value_or(shard.GetLastCompletedTx()) <= GetSnapshotBarrier()) {
            return false;
        }
        auto& portionsVector = portionsByPath[i];
        const auto& g = index.GetGranuleVerified(i);
        for (auto&& p : g.GetPortionsOlderThenSnapshot(SnapshotBarrier)) {
            if (shard.GetDataLocksManager()->IsLocked(*p.second)) {
                return false;
            }
            portionsVector.emplace_back(p.second);
            portionsLock.emplace_back(p.second);
        }
    }

    IsStartedFlag = DoStart(shard, portionsByPath);
    if (IsStartedFlag) {
        shard.GetDataLocksManager()->RegisterLock<NDataLocks::TListPortionsLock>(GetSessionId(), portionsLock, true);
    }
    return IsStartedFlag;
}

void TCommonSession::Finish(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) {
    AFL_VERIFY(!IsFinishedFlag);
    IsFinishedFlag = true;
    dataLocksManager->UnregisterLock(GetSessionId());
}

}