#include "background_controller.h"
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NColumnShard {

bool TBackgroundController::StartCompaction(const NOlap::TPlanCompactionInfo& info) {
    Y_ABORT_UNLESS(ActiveCompactionInfo.emplace(info.GetPathId(), info).second);
    return true;
}

void TBackgroundController::CheckDeadlines() {
    for (auto&& i : ActiveCompactionInfo) {
        if (TMonotonic::Now() - i.second.GetStartTime() > NOlap::TCompactionLimits::CompactionTimeout) {
            AFL_CRIT(NKikimrServices::TX_COLUMNSHARD)("event", "deadline_compaction");
            Y_DEBUG_ABORT_UNLESS(false);
        }
    }
}

void TBackgroundController::CheckDeadlinesIndexation() {
    for (auto&& i : ActiveIndexationTasks) {
        if (TMonotonic::Now() - i.second > NOlap::TCompactionLimits::CompactionTimeout) {
            AFL_CRIT(NKikimrServices::TX_COLUMNSHARD)("event", "deadline_indexation")("task_id", i.first);
            Y_DEBUG_ABORT_UNLESS(false);
        }
    }
}

TInstant TBackgroundController::GetLastCompactionFinishInstant(ui64 pathId) const {
    auto findInstant = LastCompactionFinishByPathId.find(pathId);
    if (findInstant.IsEnd()) {
        return TInstant::Zero();
    }
    return findInstant->second;
}

void TBackgroundController::StartIndexing(const NOlap::TColumnEngineChanges& changes) {
    LastIndexationInstant = TMonotonic::Now();
    Y_ABORT_UNLESS(ActiveIndexationTasks.emplace(changes.GetTaskIdentifier(), TMonotonic::Now()).second);
}

void TBackgroundController::FinishIndexing(const NOlap::TColumnEngineChanges& changes) {
    Y_ABORT_UNLESS(ActiveIndexationTasks.erase(changes.GetTaskIdentifier()));
}

TString TBackgroundController::DebugStringIndexation() const {
    TStringBuilder sb;
    sb << "{";
    sb << "task_ids=";
    for (auto&& i : ActiveIndexationTasks) {
        sb << i.first << ",";
    }
    sb << ";";
    sb << "}";
    return sb;
}

}
