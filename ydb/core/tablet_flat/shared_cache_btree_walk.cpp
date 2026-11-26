#include "flat_page_btree_index.h"
#include "shared_cache_btree_walk.h"
#include "shared_cache_pages.h"
#include "shared_sausagecache_state.h"

#include <util/generic/algorithm.h>

#include <utility>

namespace NKikimr::NSharedCache {

namespace {
    constexpr ui64 MaxWalkBatchBytes = NTabletFlatExecutor::NBlockIO::BlockSize;
}

void TWalkCollectionState::UpdateWalkIndex(
    const TLogoBlobID& collectionId, THashMap<TLogoBlobID, TVector<TLogoBlobID>>& walksByIndex)
{
    TLogoBlobID indexCollectionId;
    // Check every owner; production owners have at most two seeds (current and historic).
    for (const auto& [_, seeds] : SeedsByOwner) {
        for (const auto& seed : seeds) {
            Y_ENSURE(seed.DataCollectionId == collectionId);
            if (!indexCollectionId) {
                indexCollectionId = seed.IndexCollectionId;
            } else {
                Y_ENSURE(indexCollectionId == seed.IndexCollectionId);
            }
        }
    }

    if (indexCollectionId == IndexCollectionId) {
        return;
    }

    if (IndexCollectionId) {
        auto* walkCollections = walksByIndex.FindPtr(IndexCollectionId);
        Y_ENSURE(walkCollections);
        auto it = Find(*walkCollections, collectionId);
        Y_ENSURE(it != walkCollections->end());
        walkCollections->erase(it);
        if (walkCollections->empty()) {
            walksByIndex.erase(IndexCollectionId);
        }
    }
    if (indexCollectionId) {
        walksByIndex[indexCollectionId].push_back(collectionId);
    }

    IndexCollectionId = indexCollectionId;
}

bool TWalkCollectionState::StartWalkRun() {
    if (Run || ControllerState != EWalkControllerState::Pending) {
        return false;
    }
    ControllerState = EWalkControllerState::Idle;

    if (!SeedsByOwner) {
        return false;
    }

    TWalkRun run;
    for (const auto& [owner, seeds] : SeedsByOwner) {
        for (const auto& seed : seeds) {
            run.Walks.push_back(TCacheBTreeWalk{
                .Owner = owner,
                .Seed = seed,
            });
        }
    }
    Run.emplace(std::move(run));
    return true;
}

void TCacheBTreeWalkController::FinishWalkRunIfDrained(TLogoBlobID collectionId) {
    if (FinishDrainedRun(collectionId)) {
        if (auto* current = Host.FindWalkCollection(collectionId)) {
            Host.TryDropExpiredCollection(*current);
        }
    }
}

void TCacheBTreeWalkController::CancelWalkRun(TCollection& collection) {
    const TLogoBlobID collectionId = collection.Id;
    if (CancelRun(collectionId, [&] {
            Host.CancelQueuedWalkRequestsAndPump(collectionId);
        })) {
        FinishWalkRunIfDrained(collectionId);
    }
}

void TCacheBTreeWalkController::UpdateSeeds(TCollection& collection, const TActorId& owner,
    TVector<NSharedCache::TEvAttach::TBtreeSeed> seeds, bool replayStickyWalk) {
    auto* current = State(collection.Id).SeedsByOwner.FindPtr(owner);
    // Production seeds are ordered current then historic, with at most two per owner.
    const bool same = current ? *current == seeds : seeds.empty();
    const bool unblock = State(collection.Id).ControllerState == EWalkControllerState::Blocked;
    if (same && !unblock && !replayStickyWalk) {
        return;
    }

    if (seeds) {
        State(collection.Id).SeedsByOwner[owner] = std::move(seeds);
    } else {
        State(collection.Id).SeedsByOwner.erase(owner);
    }
    State(collection.Id).ControllerState = EWalkControllerState::Pending;
    UpdateWalkIndex(collection.Id);
    CancelWalkRun(collection);
}

void TCacheBTreeWalkController::RestartWalkRun(TCollection& collection) {
    State(collection.Id).ControllerState = EWalkControllerState::Pending;
    CancelWalkRun(collection);
}

void TCacheBTreeWalkController::InvalidateWalkRun(TCollection& collection) {
    State(collection.Id).ControllerState = EWalkControllerState::Blocked;
    CancelWalkRun(collection);
}

void TCacheBTreeWalkController::FinishFetch(const TLogoBlobID& walkCollectionId) {
    if (!walkCollectionId) {
        return;
    }

    // A run outlives every fetch it starts; the last arriving fetch may finish the run.
    auto* collection = Host.FindWalkCollection(walkCollectionId);
    Y_ENSURE(collection && State(collection->Id).Run);
    auto& run = *State(collection->Id).Run;
    Y_ENSURE(run.FetchesInFlight > 0);
    --run.FetchesInFlight;

    FinishWalkRunIfDrained(collection->Id);
}

void TCacheBTreeWalkController::FetchStarted(const TLogoBlobID& walkCollectionId) {
    auto* collection = Host.FindWalkCollection(walkCollectionId);
    Y_ENSURE(collection && State(collection->Id).Run && State(collection->Id).Run->State != EWalkRunState::Cancelled);
    ++State(collection->Id).Run->FetchesInFlight;
}

void TCacheBTreeWalkController::InvalidateRun(const TLogoBlobID& walkCollectionId) {
    auto* collection = Host.FindWalkCollection(walkCollectionId);
    Y_ENSURE(collection);
    InvalidateWalkRun(*collection);
}

void TCacheBTreeWalkController::InvalidateDataCollection(const TLogoBlobID& collectionId) {
    if (auto* collection = Host.FindWalkCollection(collectionId)) {
        if (HasWalkRun(collectionId)) {
            InvalidateWalkRun(*collection);
        }
    }
}

void TCacheBTreeWalk::PrepareIndexBatch() {
    if (Next >= BatchEnd) {
        ui64 batchBytes = 0;
        BatchEnd = Next;
        while (BatchEnd < CurrentLevel.size()) {
            const ui64 pageSize = CurrentLevel[BatchEnd].Size;
            if (BatchEnd > Next && pageSize > MaxWalkBatchBytes - batchBytes) {
                break;
            }

            batchBytes += pageSize;
            ++BatchEnd;
            if (batchBytes >= MaxWalkBatchBytes) {
                break;
            }
        }
    }
}

TVector<TPageLocation> TCacheBTreeWalk::ReadChildren(const NTable::NPage::TBtreeIndexNode& node, bool childrenAreData) {
    TVector<TPageLocation> children;
    children.reserve(node.GetChildrenCount());
    for (NTable::NPage::TRecIdx pos : xrange(node.GetChildrenCount())) {
        children.push_back(std::get<NTable::NPage::TPageLocation>(node.GetChild(pos, childrenAreData)));
    }
    return children;
}

void TCacheBTreeWalkController::AdvanceWalk(TCacheBTreeWalk& walk, const TLogoBlobID& walkCollectionId) {
    auto* indexCollection = Host.FindWalkCollection(walk.Seed.IndexCollectionId);
    auto* dataCollection = Host.FindWalkCollection(walk.Seed.DataCollectionId);
    if (!indexCollection || !indexCollection->PageCollection || !dataCollection) {
        // Only an attach creates a walk, so a missing collection means the part is gone.
        walk.Invalid = true;
        walk.Done = true;
        return;
    }

    if (!walk.Started) {
        walk.Started = true;
        walk.CurrentLevel.push_back(walk.Seed.Root);
        HandOverIndexLevel(walk, walk.CurrentLevel, walk.Level);
    }

    walk.AdvanceLevels(
        [&] {
            return AddNodeDataPagesToBatch(walk, *dataCollection);
        },
        [&](const TPageLocation& location) {
            return AdvanceIndexPage(walk, *indexCollection, walkCollectionId, location);
        },
        [&] {
            if (FlushDataPageBatch(walk, *dataCollection) != EBatchResult::Flushed) {
                return false;
            }
            FlushPagesToNotify(walk);
            return true;
        });
}

bool TCacheBTreeWalkController::AdvanceIndexPage(
    TCacheBTreeWalk& walk, TCollection& indexCollection, const TLogoBlobID& walkCollectionId, TPageLocation location) {
    auto* page = indexCollection.PageSet.FindPage(location.Offset);
    if (!page || page->State == PageStateNo) {
        TVector<TPageLocation> toRequest;
        for (size_t i = walk.Next; i < walk.BatchEnd; ++i) {
            const auto& batchLocation = walk.CurrentLevel[i];
            auto* batchPage = indexCollection.PageSet.FindPage(batchLocation.Offset);
            if (!batchPage || batchPage->State == PageStateNo) {
                toRequest.push_back(batchLocation);
            }
        }

        // Index pages always live in the main group; its mode decides how they are fetched.
        if (indexCollection.GetCacheMode() == ECacheMode::TryKeepInMemory) {
            // Wait for loader progress; a self-continuation cannot unblock a capacity-limited index read.
            QueueInMemoryPages(indexCollection, toRequest);
        } else {
            Host.FetchWalkIndexLevel(indexCollection, std::move(toRequest), walkCollectionId);
        }
        return false; // wait for the pages to arrive, the next drive continues the walk
    }

    if (page->State != PageStateLoaded && page->State != PageStateEvicted) {
        return false;
    }
    ++walk.Next;

    const bool childrenAreData = (walk.Level + 1 >= walk.Seed.LevelCount);
    if (childrenAreData && !walk.Seed.QueueDataPages && !walk.Seed.Sticky) {
        // This walk only keeps the index level resident, its data pages are of no use.
        return true;
    }

    auto ref = TSharedPageRef::MakeUsed(page, Host.WalkCachePages()->GCList, page->Type);
    Y_DEBUG_ABORT_UNLESS(ref.IsUsed(), "walked B-tree page cannot be used");
    NTable::NPage::TBtreeIndexNode node(TPinnedPageRef(ref).GetData(), /*v2Format=*/true);

    walk.AcceptChildren(node, childrenAreData, [&](TArrayRef<const TPageLocation> children, ui32 level) {
        HandOverIndexLevel(walk, children, level);
    });
    return true;
}

void TCacheBTreeWalkController::HandOverIndexLevel(
    TCacheBTreeWalk& walk, TArrayRef<const TPageLocation> locations, ui32 level) {
    if ((!walk.Seed.Sticky && !walk.Seed.IndexCollectionSticky) || level >= walk.Seed.LevelCount) {
        return; // not sticky, or these are the data pages
    }

    for (const auto& location : locations) {
        AppendPageToNotify(walk, walk.Seed.IndexCollectionId, location);
    }
}

bool TCacheBTreeWalkController::QueueInMemoryPages(TCollection& collection, TArrayRef<const TPageLocation> locations) {
    auto& queue = Host.PendingWalkPages()[collection.Id];
    bool queued = false;
    for (const auto& location : locations) {
        auto* page = collection.PageSet.FindPage(location.Offset);
        if (!page || page->State == PageStateNo) {
            if (queue.emplace(location).second) {
                queued = true;
            }
        }
    }
    return queued;
}

TCacheBTreeWalkController::EBatchResult TCacheBTreeWalkController::FlushDataPageBatch(
    TCacheBTreeWalk& walk, TCollection& dataCollection) {
    if (!walk.DataPageBatch) {
        return EBatchResult::Flushed;
    }

    const bool queueDataPages =
        walk.Seed.QueueDataPages && dataCollection.GetCacheMode() == ECacheMode::TryKeepInMemory;
    // Like V1's mode-switch scan, keep discovering locations while the loader is out of cache budget.
    const bool queued = queueDataPages && QueueInMemoryPages(dataCollection, walk.DataPageBatch);

    if (walk.Seed.Sticky) {
        // Index notifications are parents-first; flush them before handing over data pages.
        FlushPagesToNotify(walk);
        Host.SendWalkStickyPages(dataCollection, walk.Owner, walk.DataPageBatch);
    }

    walk.DataPageBatch.clear();
    walk.DataPageBatchBytes = 0;
    if (queued) {
        ContinueWalk(dataCollection.Id);
    }
    return queued ? EBatchResult::Queued : EBatchResult::Flushed;
}

bool TCacheBTreeWalk::AppendNodeDataPages(bool queueDataPages, TFunctionRef<bool()> flush) {
    if (!Seed.Sticky && !queueDataPages) {
        PendingDataPages.clear();
        return true;
    }

    ui64 nodeBytes = 0;
    for (const auto& location : PendingDataPages) {
        nodeBytes += location.Size;
    }

    if (DataPageBatch &&
        (DataPageBatchBytes >= MaxWalkBatchBytes || nodeBytes > MaxWalkBatchBytes - DataPageBatchBytes))
    {
        if (!flush()) {
            return false;
        }
    }

    DataPageBatch.reserve(DataPageBatch.size() + PendingDataPages.size());
    for (auto& location : PendingDataPages) {
        DataPageBatch.push_back(std::move(location));
    }
    DataPageBatchBytes += nodeBytes;
    PendingDataPages.clear();

    if (DataPageBatchBytes >= MaxWalkBatchBytes) {
        return flush();
    }
    return true;
}

bool TCacheBTreeWalkController::AddNodeDataPagesToBatch(TCacheBTreeWalk& walk, TCollection& dataCollection) {
    const bool queueDataPages =
        walk.Seed.QueueDataPages && dataCollection.GetCacheMode() == ECacheMode::TryKeepInMemory;
    return walk.AppendNodeDataPages(queueDataPages, [&] {
        return FlushDataPageBatch(walk, dataCollection) == EBatchResult::Flushed;
    });
}

void TCacheBTreeWalkController::AppendPageToNotify(
    TCacheBTreeWalk& walk, const TLogoBlobID& collectionId, const TPageLocation& location) {
    if (walk.NotifyCollectionId != collectionId) {
        FlushPagesToNotify(walk);
        walk.NotifyCollectionId = collectionId;
    }

    walk.PagesToNotify.push_back(location);
    if (walk.PagesToNotify.size() >= TEvStickyCollectionPages::MaxBatchLocations) {
        FlushPagesToNotify(walk);
    }
}

void TCacheBTreeWalkController::FlushPagesToNotify(TCacheBTreeWalk& walk) {
    if (walk.PagesToNotify.empty()) {
        return;
    }

    if (auto* collection = Host.FindWalkCollection(walk.NotifyCollectionId)) {
        Host.SendWalkStickyPages(*collection, walk.Owner, walk.PagesToNotify);
    }
    walk.PagesToNotify.clear();
    walk.NotifyCollectionId = TLogoBlobID();
}

void TCacheBTreeWalkController::Advance() {
    const bool schedule = AdvanceRuns(
        [&](const TLogoBlobID& id) {
            auto* collection = Host.FindWalkCollection(id);
            Y_ENSURE(collection && State(collection->Id).Run);
            return State(collection->Id).Run->Advance([&](TCacheBTreeWalk& walk) {
                AdvanceWalk(walk, collection->Id);
            });
        },
        [&](const TLogoBlobID& id) {
            if (auto* collection = Host.FindWalkCollection(id)) {
                InvalidateWalkRun(*collection);
            }
        });
    if (schedule) {
        Host.ScheduleWalkContinuation();
    }
}

void TCacheBTreeWalkController::FinishReady() {
    FinishReadyRuns([&](const TLogoBlobID& id) {
        if (auto* collection = Host.FindWalkCollection(id)) {
            FinishWalkRunIfDrained(collection->Id);
        }
    });
}

void TCacheBTreeWalkController::DropIndexOnlyWalks(const TLogoBlobID& indexCollectionId) {
    for (const TLogoBlobID& id : GetWalkCollections(indexCollectionId)) {
        auto* collection = Host.FindWalkCollection(id);
        Y_ENSURE(collection);
        const bool hasIndexOnlyWalk = AnyOf(State(collection->Id).SeedsByOwner, [&](const auto& item) {
            return AnyOf(item.second, [&](const auto& seed) {
                return !seed.QueueDataPages && !seed.Sticky && !seed.IndexCollectionSticky &&
                       seed.IndexCollectionId == indexCollectionId;
            });
        });
        if (hasIndexOnlyWalk) {
            InvalidateWalkRun(*collection);
        }
    }
}

void TCacheBTreeWalkController::DropForIndexCollection(const TLogoBlobID& indexCollectionId) {
    for (const TLogoBlobID& id : GetWalkCollections(indexCollectionId)) {
        auto* collection = Host.FindWalkCollection(id);
        Y_ENSURE(collection);
        InvalidateWalkRun(*collection);
    }
}

void TCacheBTreeWalkController::RestartForIndexCollection(const TLogoBlobID& indexCollectionId) {
    for (const TLogoBlobID& id : GetWalkCollections(indexCollectionId)) {
        auto* collection = Host.FindWalkCollection(id);
        Y_ENSURE(collection);
        RestartWalkRun(*collection);
    }
}
} // namespace NKikimr::NSharedCache
