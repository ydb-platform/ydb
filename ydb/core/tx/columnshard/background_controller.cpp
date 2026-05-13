#include "background_controller.h"

#include <ydb/core/tx/columnshard/engines/changes/compaction.h>
#include <ydb/core/tx/columnshard/engines/changes/counters/general.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NColumnShard {

bool TBackgroundController::StartCompaction(const TInternalPathId pathId, const TString& taskId) {
    auto [it, _] = ActiveCompactionInfo.emplace(std::make_pair(pathId, taskId), NOlap::TPlanCompactionInfo{ pathId, taskId });
    it->second.Start();
    return true;
}

void TBackgroundController::FinishCompaction(const TInternalPathId pathId, const TString& taskId) {
    auto it = ActiveCompactionInfo.find(std::make_pair(pathId, taskId));
    AFL_VERIFY(it != ActiveCompactionInfo.end());
    if (it->second.Finish()) {
        NOlap::NChanges::TGeneralCompactionCounters::OnCompactionFinished(it->second.GetDuration().MicroSeconds());
        ActiveCompactionInfo.erase(it);
    }
    Counters->OnCompactionFinish(pathId);
}

void TBackgroundController::CheckDeadlines() {
    for (auto&& i : ActiveCompactionInfo) {
        if (TMonotonic::Now() - i.second.GetStartTime() > NOlap::TCompactionLimits::CompactionTimeout) {
            YDB_LOG_CRIT("",
                {"event", "deadline_compaction"},
                {"path_id", i.first.first},
                {"task_id", i.second.GetTaskId()});
            // uncomment it for debug purpose
            // AFL_VERIFY_DEBUG(false);
        }
    }
}

}   // namespace NKikimr::NColumnShard
