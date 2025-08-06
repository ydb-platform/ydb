#pragma once
#include "engines/changes/abstract/compaction_info.h"
#include "engines/portions/meta.h"
#include <ydb/core/tx/columnshard/counters/counters_manager.h>
#include <ydb/core/tx/columnshard/common/path_id.h>

namespace NKikimr::NOlap {
class TColumnEngineChanges;
}

namespace NKikimr::NColumnShard {

class TBackgroundController {
private:
    using TCurrentCompaction = THashMap<TInternalPathId, NOlap::TPlanCompactionInfo>;
    TCurrentCompaction ActiveCompactionInfo;
    std::optional<ui64> WaitingCompactionPriority;

    THashMap<TString, TMonotonic> ActiveIndexationTasks;

    std::shared_ptr<TBackgroundControllerCounters> Counters;
    bool ActiveCleanupPortions = false;
    bool ActiveCleanupTables = false;
    bool ActiveCleanupInsertTable = false;
    YDB_READONLY(TMonotonic, LastIndexationInstant, TMonotonic::Zero());
public:
    TBackgroundController(std::shared_ptr<TBackgroundControllerCounters> counters)
        : Counters(std::move(counters)) {
    }
    THashSet<NOlap::TPortionAddress> GetConflictTTLPortions() const;
    THashSet<NOlap::TPortionAddress> GetConflictCompactionPortions() const;

    void UpdateWaitingPriority(const ui64 priority) {
        if (!WaitingCompactionPriority || *WaitingCompactionPriority < priority) {
            WaitingCompactionPriority = priority;
        }
    }

    void ResetWaitingPriority() {
        WaitingCompactionPriority.reset();
    }

    std::optional<ui64> GetWaitingPriorityOptional() {
        return WaitingCompactionPriority;
    }

    void CheckDeadlines();

    bool StartCompaction(const TInternalPathId pathId, const TString& taskId);
    void FinishCompaction(const TInternalPathId pathId);

    ui32 GetCompactionsCount() const {
        return ActiveCompactionInfo.size();
    }

    void StartCleanupPortions() {
        Y_ABORT_UNLESS(!ActiveCleanupPortions);
        ActiveCleanupPortions = true;
    }
    void FinishCleanupPortions() {
        Y_ABORT_UNLESS(ActiveCleanupPortions);
        ActiveCleanupPortions = false;
    }
    bool IsCleanupPortionsActive() const {
        return ActiveCleanupPortions;
    }

    void StartCleanupTables() {
        Y_ABORT_UNLESS(!ActiveCleanupTables);
        ActiveCleanupTables = true;
    }
    void FinishCleanupTables() {
        Y_ABORT_UNLESS(ActiveCleanupTables);
        ActiveCleanupTables = false;
    }
    bool IsCleanupTablesActive() const {
        return ActiveCleanupTables;
    }

    void StartCleanupInsertTable() {
        Y_ABORT_UNLESS(!ActiveCleanupInsertTable);
        ActiveCleanupInsertTable = true;
    }
    void FinishCleanupInsertTable() {
        Y_ABORT_UNLESS(ActiveCleanupInsertTable);
        ActiveCleanupInsertTable = false;
    }
    bool IsCleanupInsertTableActive() const {
        return ActiveCleanupInsertTable;
    }

    void CheckDeadlinesIndexation();

    void StartIndexing(const NOlap::TColumnEngineChanges& changes);
    void FinishIndexing(const NOlap::TColumnEngineChanges& changes);
    TString DebugStringIndexation() const;
    i64 GetIndexingActiveCount() const {
        return ActiveIndexationTasks.size();
    }
};

}
