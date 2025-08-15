#include "background_controller.h"
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NColumnShard {

bool TBackgroundController::StartCompaction(const TInternalPathId pathId, const TString& taskId) {
    auto [it, _] = ActiveCompactionInfo.emplace(pathId, NOlap::TPlanCompactionInfo{ pathId, taskId });
    it->second.Start();
    return true;
}

void TBackgroundController::FinishCompaction(const TInternalPathId pathId) {
    auto it = ActiveCompactionInfo.find(pathId);
    AFL_VERIFY(it != ActiveCompactionInfo.end());
    if (it->second.Finish()) {
        ActiveCompactionInfo.erase(it);
    }
    Counters->OnCompactionFinish(pathId);
}

void TBackgroundController::CheckDeadlines() {
    for (auto&& i : ActiveCompactionInfo) {
        if (TMonotonic::Now() - i.second.GetStartTime() > NOlap::TCompactionLimits::CompactionTimeout) {
            AFL_CRIT(NKikimrServices::TX_COLUMNSHARD)("event", "deadline_compaction")("path_id", i.first)("task_id", i.second.GetTaskId());
            AFL_VERIFY_DEBUG(false);
        }
    }
}

}
