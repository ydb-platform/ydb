#pragma once
#include "engines/changes/abstract/compaction_info.h"
#include "engines/portions/meta.h"

namespace NKikimr::NOlap {
class TColumnEngineChanges;
}

namespace NKikimr::NColumnShard {

class TBackgroundActivity {
public:
    enum EBackActivity : ui32 {
        NONE = 0x00,
        INDEX = 0x01,
        COMPACT = 0x02,
        CLEAN  = 0x04,
        TTL = 0x08,
        ALL = 0xffff
    };

    static TBackgroundActivity Indexation() { return TBackgroundActivity(INDEX); }
    static TBackgroundActivity Compaction() { return TBackgroundActivity(COMPACT); }
    static TBackgroundActivity Cleanup() { return TBackgroundActivity(CLEAN); }
    static TBackgroundActivity Ttl() { return TBackgroundActivity(TTL); }
    static TBackgroundActivity All() { return TBackgroundActivity(ALL); }
    static TBackgroundActivity None() { return TBackgroundActivity(NONE); }

    TBackgroundActivity() = default;

    bool HasIndexation() const { return Activity & INDEX; }
    bool HasCompaction() const { return Activity & COMPACT; }
    bool HasCleanup() const { return Activity & CLEAN; }
    bool HasTtl() const { return Activity & TTL; }
    bool HasAll() const { return Activity == ALL; }

    TString DebugString() const;

private:
    EBackActivity Activity = NONE;

    TBackgroundActivity(EBackActivity activity)
        : Activity(activity)
    {}
};

class TBackgroundController {
private:
    THashMap<TString, TMonotonic> ActiveIndexationTasks;

    using TCurrentCompaction = THashMap<ui64, NOlap::TPlanCompactionInfo>;
    TCurrentCompaction ActiveCompactionInfo;

    bool ActiveCleanup = false;
    bool TtlStarted = false;
    YDB_READONLY(TMonotonic, LastIndexationInstant, TMonotonic::Zero());
public:
    THashSet<NOlap::TPortionAddress> GetConflictTTLPortions() const;
    THashSet<NOlap::TPortionAddress> GetConflictCompactionPortions() const;

    void CheckDeadlines();
    void CheckDeadlinesIndexation();

    bool StartCompaction(const NOlap::TPlanCompactionInfo& info);
    void FinishCompaction(const NOlap::TPlanCompactionInfo& info) {
        Y_ABORT_UNLESS(ActiveCompactionInfo.erase(info.GetPathId()));
    }
    const TCurrentCompaction& GetActiveCompaction() const {
        return ActiveCompactionInfo;
    }
    ui32 GetCompactionsCount() const {
        return ActiveCompactionInfo.size();
    }

    void StartIndexing(const NOlap::TColumnEngineChanges& changes);
    void FinishIndexing(const NOlap::TColumnEngineChanges& changes);
    TString DebugStringIndexation() const;
    i64 GetIndexingActiveCount() const {
        return ActiveIndexationTasks.size();
    }

    void StartCleanup() {
        Y_ABORT_UNLESS(!ActiveCleanup);
        ActiveCleanup = true;
    }
    void FinishCleanup() {
        Y_ABORT_UNLESS(ActiveCleanup);
        ActiveCleanup = false;
    }
    bool IsCleanupActive() const {
        return ActiveCleanup;
    }

    void StartTtl();
    void FinishTtl() {
        Y_ABORT_UNLESS(TtlStarted);
        TtlStarted = false;
    }
    bool IsTtlActive() const {
        return TtlStarted;
    }
};

}
