#pragma once

#include "flat_bio_eggs.h"
#include "flat_sausage_gut.h"

#include <ydb/core/base/blobstorage.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/generic/array_ref.h>
#include <util/generic/function_ref.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

#include <optional>
#include <utility>

namespace NKikimr::NTable::NPage {
class TBtreeIndexNode;
}

namespace NKikimr::NSharedCache {

struct TCollection;
class TSharedCachePages;

using TPageLocation = NTable::NPage::TPageLocation;

using TPendingInMemoryPages = THashMap<TLogoBlobID, TSet<NTable::NPage::TPageLocation>>;

// One B-tree being walked by the cache itself, one index level at a time in write order.
struct TCacheBTreeWalk {
    void PrepareIndexBatch();
    static TVector<TPageLocation> ReadChildren(const NTable::NPage::TBtreeIndexNode& node, bool childrenAreData);
    bool AppendNodeDataPages(bool queueDataPages, TFunctionRef<bool()> flush);

    template <class TAppendData, class TAdvanceIndex, class TFinish>
    void AdvanceLevels(TAppendData&& appendData, TAdvanceIndex&& advanceIndex, TFinish&& finish) {
        while (true) {
            if (PendingDataPages && !appendData()) {
                return;
            }

            if (Next >= CurrentLevel.size()) {
                if (!NextLevel) {
                    if (finish()) {
                        Done = true;
                    }
                    return;
                }
                CurrentLevel = std::move(NextLevel);
                NextLevel.clear();
                Next = 0;
                BatchEnd = 0;
                ++Level;
                continue;
            }

            // A zero-level tree has its data page as the root.
            if (Level >= Seed.LevelCount) {
                PendingDataPages = std::move(CurrentLevel);
                CurrentLevel.clear();
                Next = 0;
                BatchEnd = 0;
                continue;
            }

            PrepareIndexBatch();
            if (!advanceIndex(CurrentLevel[Next])) {
                return;
            }
        }
    }

    template <class THandOver>
    void AcceptChildren(const NTable::NPage::TBtreeIndexNode& node, bool childrenAreData, THandOver&& handOver) {
        auto children = ReadChildren(node, childrenAreData);
        if (childrenAreData) {
            PendingDataPages = std::move(children);
        } else {
            handOver(children, Level + 1);
            for (auto& child : children) {
                NextLevel.push_back(std::move(child));
            }
        }
    }

    TActorId Owner;
    NPageCollection::TBtreeSeed Seed;

    TVector<TPageLocation> CurrentLevel;
    TVector<TPageLocation> NextLevel;
    TVector<TPageLocation> PendingDataPages;
    TVector<TPageLocation> DataPageBatch;
    ui64 DataPageBatchBytes = 0;
    size_t Next = 0;
    size_t BatchEnd = 0;
    ui32 Level = 0;
    TLogoBlobID NotifyCollectionId;
    TVector<TPageLocation> PagesToNotify;
    bool Started = false;
    bool Done = false;
    bool Invalid = false;
};

enum class EWalkRunState {
    Walking,
    Draining,
    Cancelled,
};

enum class EWalkControllerState {
    Idle,
    Pending,
    Blocked,
};

struct TWalkRun {
    enum class EAdvanceResult {
        Pending,
        Invalid,
        Draining,
    };

    template <class TAdvanceWalk>
    EAdvanceResult Advance(TAdvanceWalk&& advanceWalk) {
        if (State != EWalkRunState::Walking || !std::exchange(NeedsAdvance, false)) {
            return EAdvanceResult::Pending;
        }

        bool pending = false;
        bool failed = false;
        for (auto& walk : Walks) {
            if (!walk.Done) {
                advanceWalk(walk);
            }
            failed |= walk.Invalid;
            pending |= !walk.Done;
        }

        if (failed) {
            return EAdvanceResult::Invalid;
        }
        if (pending) {
            return EAdvanceResult::Pending;
        }
        State = EWalkRunState::Draining;
        return EAdvanceResult::Draining;
    }

    ui32 FetchesInFlight = 0;
    EWalkRunState State = EWalkRunState::Walking;
    bool NeedsAdvance = true;
    TVector<TCacheBTreeWalk> Walks;
};

struct TWalkCollectionState {
    void UpdateWalkIndex(const TLogoBlobID& collectionId, THashMap<TLogoBlobID, TVector<TLogoBlobID>>& walksByIndex);
    bool StartWalkRun();

    TMap<TActorId, TVector<NPageCollection::TBtreeSeed>> SeedsByOwner;
    TLogoBlobID IndexCollectionId;
    std::optional<TWalkRun> Run;
    EWalkControllerState ControllerState = EWalkControllerState::Idle;
};

template <class TState>
class TWalkScheduler {
public:
    bool HasActiveWalks() const {
        // Cancelled runs still count until their dispatched I/O drains.
        return ActiveRunCount != 0;
    }

    bool IsIdle(const TLogoBlobID& collectionId) const {
        const auto* state = States.FindPtr(collectionId);
        // The index association keeps the collection alive.
        return !state || (!state->Run && !state->IndexCollectionId);
    }

    void EraseCollection(const TLogoBlobID& collectionId) {
        Y_DEBUG_ABORT_UNLESS(IsIdle(collectionId));
        States.erase(collectionId);
    }

    void IndexPagesChanged(const TLogoBlobID& collectionId) {
        if (const auto* collections = WalkCollectionsByIndex.FindPtr(collectionId)) {
            for (const auto& id : *collections) {
                auto& run = State(id).Run;
                if (run && run->State == EWalkRunState::Walking) {
                    run->NeedsAdvance = true;
                    AdvanceNeeded = true;
                }
            }
        }
    }

protected:
    TState& State(const TLogoBlobID& collectionId) {
        return States[collectionId];
    }

    bool HasWalkRun(const TLogoBlobID& collectionId) const {
        const auto* state = States.FindPtr(collectionId);
        return state && state->Run;
    }

    void UpdateWalkIndex(const TLogoBlobID& collectionId) {
        State(collectionId).UpdateWalkIndex(collectionId, WalkCollectionsByIndex);
    }

    TVector<TLogoBlobID> GetWalkCollections(const TLogoBlobID& indexCollectionId) const {
        if (const auto* collections = WalkCollectionsByIndex.FindPtr(indexCollectionId)) {
            return *collections;
        }
        return {};
    }

    void ContinueWalk(const TLogoBlobID& collectionId) {
        ContinuationNeeded = true;
        State(collectionId).Run->NeedsAdvance = true;
    }

    bool FinishDrainedRun(const TLogoBlobID& collectionId) {
        auto* state = States.FindPtr(collectionId);
        if (!state || !state->Run || state->Run->State == EWalkRunState::Walking || state->Run->FetchesInFlight != 0) {
            return false;
        }

        Y_ENSURE(ActiveRunCount > 0);
        --ActiveRunCount;
        state->Run.reset();
        WalkRunsInProgress.erase(collectionId);
        RunsToFinish.erase(collectionId);
        StartWalkRun(collectionId);
        return true;
    }

    template <class TPump>
    bool CancelRun(const TLogoBlobID& collectionId, TPump&& pump) {
        auto& run = State(collectionId).Run;
        if (!run) {
            StartWalkRun(collectionId);
            return false;
        }

        if (run->State != EWalkRunState::Cancelled) {
            run->State = EWalkRunState::Cancelled;
            // Other walks may share index requests that cancellation is about to remove.
            if (!run->Walks.empty()) {
                IndexPagesChanged(run->Walks.front().Seed.IndexCollectionId);
            }
            run->Walks.clear();
            WalkRunsInProgress.erase(collectionId);
            pump();
        }
        return true;
    }

    template <class TAdvanceRun, class TInvalidateRun>
    bool AdvanceRuns(TAdvanceRun&& advanceRun, TInvalidateRun&& invalidateRun) {
        if (!std::exchange(AdvanceNeeded, false)) {
            return false;
        }
        ContinuationNeeded = false;
        TVector<TLogoBlobID> invalid;
        for (const TLogoBlobID& id : WalkRunsInProgress) {
            const auto result = advanceRun(id);
            if (result == TWalkRun::EAdvanceResult::Invalid) {
                invalid.push_back(id);
            } else if (result == TWalkRun::EAdvanceResult::Draining) {
                RunsToFinish.insert(id);
            }
        }

        for (const TLogoBlobID& id : invalid) {
            invalidateRun(id);
        }
        if (ContinuationNeeded) {
            AdvanceNeeded = true;
        }
        return ContinuationNeeded;
    }

    template <class TFinishRun>
    void FinishReadyRuns(TFinishRun&& finishRun) {
        while (!RunsToFinish.empty()) {
            const TLogoBlobID id = *RunsToFinish.begin();
            RunsToFinish.erase(id);
            finishRun(id);
        }
    }

private:
    void StartWalkRun(const TLogoBlobID& collectionId) {
        if (State(collectionId).StartWalkRun()) {
            ++ActiveRunCount;
            WalkRunsInProgress.insert(collectionId);
            AdvanceNeeded = true;
        }
    }

    THashMap<TLogoBlobID, TState> States;
    THashSet<TLogoBlobID> WalkRunsInProgress;
    THashSet<TLogoBlobID> RunsToFinish;
    THashMap<TLogoBlobID, TVector<TLogoBlobID>> WalkCollectionsByIndex;
    ui64 ActiveRunCount = 0;
    bool AdvanceNeeded = false;
    bool ContinuationNeeded = false;
};

class ICacheBTreeWalkHost {
public:
    virtual ~ICacheBTreeWalkHost() = default;

    virtual TCollection* FindWalkCollection(const TLogoBlobID& id) = 0;
    virtual TPendingInMemoryPages& PendingWalkPages() = 0;
    virtual TSharedCachePages* WalkCachePages() = 0;
    // Regular-mode index reads; in-memory index reads go through PendingWalkPages.
    virtual void FetchWalkIndexLevel(TCollection& collection, TVector<NTable::NPage::TPageLocation>&& locations,
        const TLogoBlobID& walkCollectionId) = 0;
    virtual void SendWalkStickyPages(
        TCollection& collection, const TActorId& owner, const TVector<NTable::NPage::TPageLocation>& locations) = 0;
    virtual void CancelQueuedWalkRequestsAndPump(const TLogoBlobID& walkCollectionId) = 0;
    virtual void TryDropExpiredCollection(TCollection& collection) = 0;
    virtual void ScheduleWalkContinuation() = 0;
};

class TCacheBTreeWalkController : private TWalkScheduler<TWalkCollectionState> {
public:
    using TWalkScheduler::EraseCollection;
    using TWalkScheduler::HasActiveWalks;
    using TWalkScheduler::IndexPagesChanged;
    using TWalkScheduler::IsIdle;

    explicit TCacheBTreeWalkController(ICacheBTreeWalkHost& host)
        : Host(host)
    {
    }

    void UpdateSeeds(TCollection& collection, const TActorId& owner, TVector<NPageCollection::TBtreeSeed> seeds,
        bool replayStickyWalk = false);

    // Any path making a waiting/yielded run runnable must set its NeedsAdvance and the common AdvanceNeeded.
    void Advance();
    void FinishReady();
    void FinishFetch(const TLogoBlobID& walkCollectionId);
    void FetchStarted(const TLogoBlobID& walkCollectionId);
    void InvalidateRun(const TLogoBlobID& walkCollectionId);
    void InvalidateDataCollection(const TLogoBlobID& collectionId);
    void DropForIndexCollection(const TLogoBlobID& indexCollectionId);
    void RestartForIndexCollection(const TLogoBlobID& indexCollectionId);
    void DropIndexOnlyWalks(const TLogoBlobID& indexCollectionId);

private:
    enum class EBatchResult {
        Flushed,
        Queued,
    };

    void FinishWalkRunIfDrained(TLogoBlobID collectionId);
    void CancelWalkRun(TCollection& collection);
    void RestartWalkRun(TCollection& collection);
    void InvalidateWalkRun(TCollection& collection);
    bool QueueInMemoryPages(TCollection& collection, TArrayRef<const TPageLocation> locations);
    void AdvanceWalk(TCacheBTreeWalk& walk, const TLogoBlobID& walkCollectionId);
    bool AdvanceIndexPage(TCacheBTreeWalk& walk, TCollection& indexCollection, const TLogoBlobID& walkCollectionId,
        TPageLocation location);
    void HandOverIndexLevel(TCacheBTreeWalk& walk, TArrayRef<const NTable::NPage::TPageLocation> locations, ui32 level);
    EBatchResult FlushDataPageBatch(TCacheBTreeWalk& walk, TCollection& dataCollection);
    bool AddNodeDataPagesToBatch(TCacheBTreeWalk& walk, TCollection& dataCollection);
    void AppendPageToNotify(
        TCacheBTreeWalk& walk, const TLogoBlobID& collectionId, const NTable::NPage::TPageLocation& location);
    void FlushPagesToNotify(TCacheBTreeWalk& walk);

private:
    ICacheBTreeWalkHost& Host;
};

} // namespace NKikimr::NSharedCache
