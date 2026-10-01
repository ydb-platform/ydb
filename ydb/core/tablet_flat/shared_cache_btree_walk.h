#pragma once

#include "flat_bio_eggs.h"
#include "shared_cache_events.h"

#include <ydb/core/base/blobstorage.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/generic/array_ref.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NSharedCache {

struct TCollection;
class TSharedCachePages;

using TPendingInMemoryPages = THashMap<TLogoBlobID, TSet<NTable::NPage::TPageLocation>>;

// One B-tree being walked by the cache itself, one index level at a time in write order.
struct TCacheBTreeWalk {
    TActorId Owner;
    NSharedCache::TEvAttach::TBtreeSeed Seed;

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
    ui32 FetchesInFlight = 0;
    EWalkRunState State = EWalkRunState::Walking;
    bool NeedsAdvance = true;
    TVector<TCacheBTreeWalk> Walks;
};

struct TWalkCollectionState {
    TMap<TActorId, TVector<TEvAttach::TBtreeSeed>> SeedsByOwner;
    TLogoBlobID IndexCollectionId;
    std::optional<TWalkRun> Run;
    EWalkControllerState ControllerState = EWalkControllerState::Idle;
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

class TCacheBTreeWalkController {
public:
    explicit TCacheBTreeWalkController(ICacheBTreeWalkHost& host)
        : Host(host)
    {
    }

    void UpdateSeeds(TCollection& collection, const TActorId& owner, TVector<TEvAttach::TBtreeSeed> seeds,
        bool replayStickyWalk = false);

    bool HasActiveWalks() const {
        // Include cancelled runs waiting for dispatched I/O.
        return ActiveRunCount != 0;
    }

    // Any path making a waiting/yielded run runnable must set its NeedsAdvance and the common AdvanceNeeded.
    void Advance();
    void IndexPagesChanged(const TLogoBlobID& collectionId);
    void FinishReady();
    void FinishFetch(const TLogoBlobID& walkCollectionId);
    void FetchStarted(const TLogoBlobID& walkCollectionId);
    void InvalidateRun(const TLogoBlobID& walkCollectionId);
    void InvalidateDataCollection(const TLogoBlobID& collectionId);
    void DropForIndexCollection(const TLogoBlobID& indexCollectionId);
    void RestartForIndexCollection(const TLogoBlobID& indexCollectionId);
    void DropIndexOnlyWalks(const TLogoBlobID& indexCollectionId);
    bool IsIdle(const TLogoBlobID& collectionId) const;
    void EraseCollection(const TLogoBlobID& collectionId);

private:
    enum class EBatchResult {
        Flushed,
        Queued,
    };

    TWalkCollectionState& State(const TLogoBlobID& collectionId);
    void UpdateWalkIndex(TCollection& collection);
    TVector<TLogoBlobID> GetWalkCollections(const TLogoBlobID& indexCollectionId) const;
    void StartWalkRun(TCollection& collection);
    void FinishWalkRun(TCollection& collection);
    bool FinishWalkRunIfDrained(TCollection& collection);
    void CancelWalkRun(TCollection& collection);
    void RestartWalkRun(TCollection& collection);
    void InvalidateWalkRun(TCollection& collection);
    bool QueueInMemoryPages(TCollection& collection, TArrayRef<const TPageLocation> locations);
    void AdvanceWalk(TCacheBTreeWalk& walk, const TLogoBlobID& walkCollectionId);
    void HandOverIndexLevel(TCacheBTreeWalk& walk, TArrayRef<const NTable::NPage::TPageLocation> locations, ui32 level);
    EBatchResult FlushDataPageBatch(TCacheBTreeWalk& walk, TCollection& dataCollection);
    bool AddNodeDataPagesToBatch(TCacheBTreeWalk& walk, TCollection& dataCollection);
    void AppendPageToNotify(
        TCacheBTreeWalk& walk, const TLogoBlobID& collectionId, const NTable::NPage::TPageLocation& location);
    void FlushPagesToNotify(TCacheBTreeWalk& walk);

private:
    ICacheBTreeWalkHost& Host;
    // Erase this entry whenever the matching collection leaves the actor's Collections map.
    THashMap<TLogoBlobID, TWalkCollectionState> States;
    THashSet<TLogoBlobID> WalkRunsInProgress;
    // Only recheck completion after a run stops walking or its pending queue drains.
    THashSet<TLogoBlobID> RunsToFinish;
    // Waiting for I/O or cache capacity does not make an ordinary actor message useful to the walk.
    bool AdvanceNeeded = false;
    THashMap<TLogoBlobID, TVector<TLogoBlobID>> WalkCollectionsByIndex;
    ui64 ActiveRunCount = 0;
    // A queued data-page batch yields; another actor turn must continue discovery.
    bool ContinuationNeeded = false;
    static constexpr ui64 MaxWalkBatchBytes = NTabletFlatExecutor::NBlockIO::BlockSize;
};

} // namespace NKikimr::NSharedCache
