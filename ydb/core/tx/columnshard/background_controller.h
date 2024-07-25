#pragma once
#include "engines/changes/abstract/compaction_info.h"
#include "engines/portions/meta.h"

namespace NKikimr::NOlap {
class TColumnEngineChanges;
}

namespace NKikimr::NColumnShard {

class TBackgroundController {
private:
    THashMap<TString, TMonotonic> ActiveIndexationTasks;

    using TCurrentCompaction = THashMap<ui64, NOlap::TPlanCompactionInfo>;
    TCurrentCompaction ActiveCompactionInfo;
    THashMap<ui64, TInstant> LastCompactionFinishInstants; // pathId to finish time

    bool ActiveCleanupPortions = false;
    bool ActiveCleanupTables = false;
    bool ActiveCleanupInsertTable = false;
    YDB_READONLY(TMonotonic, LastIndexationInstant, TMonotonic::Zero());
public:
    THashSet<NOlap::TPortionAddress> GetConflictTTLPortions() const;
    THashSet<NOlap::TPortionAddress> GetConflictCompactionPortions() const;

    void CheckDeadlines();
    void CheckDeadlinesIndexation();

    bool StartCompaction(const NOlap::TPlanCompactionInfo& info);
    void FinishCompaction(const NOlap::TPlanCompactionInfo& info) {
        Y_ABORT_UNLESS(ActiveCompactionInfo.erase(info.GetPathId()));
        TInstant& lastFinishInstant = LastCompactionFinishInstants[info.GetPathId()];
        lastFinishInstant = std::max(lastFinishInstant, TInstant::Now());
    }
    const TCurrentCompaction& GetActiveCompaction() const {
        return ActiveCompactionInfo;
    }
    ui32 GetCompactionsCount() const {
        return ActiveCompactionInfo.size();
    }
    TInstant GetLastCompactionFinishInstant(ui64 pathId) const;
    TInstant GetLastCompactionFinishInstant() const;

    void StartIndexing(const NOlap::TColumnEngineChanges& changes);
    void FinishIndexing(const NOlap::TColumnEngineChanges& changes);
    TString DebugStringIndexation() const;
    i64 GetIndexingActiveCount() const {
        return ActiveIndexationTasks.size();
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
};

}
