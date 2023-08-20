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
    bool ActiveIndexing = false;
    THashSet<ui64> IndexingGranules;

    using TCurrentCompaction = THashMap<ui64, NOlap::TPlanCompactionInfo>;
    TCurrentCompaction ActiveCompactionInfo;
    THashMap<ui64, THashSet<ui64>> CompactionInfoGranules;

    bool ActiveCleanup = false;
    THashSet<ui64> TtlGranules;

public:
    THashSet<ui64> GetConflictTTLGranules() const;
    THashSet<ui64> GetBusyGranules() const;

    void CheckDeadlines();

    bool StartCompaction(const NOlap::TPlanCompactionInfo& info, const NOlap::TColumnEngineChanges& changes);
    void FinishCompaction(const NOlap::TPlanCompactionInfo& info) {
        Y_VERIFY(ActiveCompactionInfo.erase(info.GetPathId()));
        Y_VERIFY(CompactionInfoGranules.erase(info.GetPathId()));
    }
    const TCurrentCompaction& GetActiveCompaction() const {
        return ActiveCompactionInfo;
    }
    ui32 GetCompactionsCount() const {
        return ActiveCompactionInfo.size();
    }

    void StartIndexing(const NOlap::TColumnEngineChanges& changes);
    void FinishIndexing() {
        Y_VERIFY(ActiveIndexing);
        ActiveIndexing = false;
        IndexingGranules.clear();
    }
    bool IsIndexingActive() const {
        return ActiveIndexing;
    }

    void StartCleanup() {
        Y_VERIFY(!ActiveCleanup);
        ActiveCleanup = true;
    }
    void FinishCleanup() {
        Y_VERIFY(ActiveCleanup);
        ActiveCleanup = false;
    }
    bool IsCleanupActive() const {
        return ActiveCleanup;
    }

    void StartTtl(const NOlap::TColumnEngineChanges& changes);
    void FinishTtl() {
        Y_VERIFY(!TtlGranules.empty());
        TtlGranules.clear();
    }
    bool IsTtlActive() const {
        return !TtlGranules.empty();
    }

    bool HasSplitCompaction(const ui64 pathId) const {
        auto it = ActiveCompactionInfo.find(pathId);
        if (it == ActiveCompactionInfo.end()) {
            return false;
        }
        return !it->second.IsInternal();
    }
};

}
