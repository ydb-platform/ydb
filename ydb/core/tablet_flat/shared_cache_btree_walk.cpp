#include "flat_page_btree_index.h"
#include "shared_cache_btree_walk.h"
#include "shared_cache_pages.h"
#include "shared_sausagecache_state.h"

#include <util/generic/algorithm.h>

#include <utility>

namespace NKikimr::NSharedCache {

TWalkCollectionState& TCacheBTreeWalkController::State(const TLogoBlobID& collectionId) {
    return States[collectionId];
}

bool TCacheBTreeWalkController::IsIdle(const TLogoBlobID& collectionId) const {
    const auto* state = States.FindPtr(collectionId);
    // Keep the collection alive while its index association exists, or WalkCollectionsByIndex would dangle.
    return !state || (!state->Run && !state->IndexCollectionId);
}

void TCacheBTreeWalkController::EraseCollection(const TLogoBlobID& collectionId) {
    Y_DEBUG_ABORT_UNLESS(IsIdle(collectionId));
    States.erase(collectionId);
}

void TCacheBTreeWalkController::UpdateWalkIndex(TCollection& collection) {
    TLogoBlobID indexCollectionId;
    // Check every owner; production owners have at most two seeds (current and historic).
    for (const auto& [_, seeds] : State(collection.Id).SeedsByOwner) {
        for (const auto& seed : seeds) {
            Y_ENSURE(seed.DataCollectionId == collection.Id);
            if (!indexCollectionId) {
                indexCollectionId = seed.IndexCollectionId;
            } else {
                Y_ENSURE(indexCollectionId == seed.IndexCollectionId);
            }
        }
    }

    if (indexCollectionId == State(collection.Id).IndexCollectionId) {
        return;
    }

    if (State(collection.Id).IndexCollectionId) {
        auto* walkCollections = WalkCollectionsByIndex.FindPtr(State(collection.Id).IndexCollectionId);
        Y_ENSURE(walkCollections);
        auto it = Find(*walkCollections, collection.Id);
        Y_ENSURE(it != walkCollections->end());
        walkCollections->erase(it);
        if (walkCollections->empty()) {
            WalkCollectionsByIndex.erase(State(collection.Id).IndexCollectionId);
        }
    }
    if (indexCollectionId) {
        WalkCollectionsByIndex[indexCollectionId].push_back(collection.Id);
    }

    State(collection.Id).IndexCollectionId = indexCollectionId;
}

TVector<TLogoBlobID> TCacheBTreeWalkController::GetWalkCollections(const TLogoBlobID& indexCollectionId) const {
    if (const auto* walkCollections = WalkCollectionsByIndex.FindPtr(indexCollectionId)) {
        return *walkCollections;
    }
    return {};
}

void TCacheBTreeWalkController::StartWalkRun(TCollection& collection) {
    if (State(collection.Id).Run || State(collection.Id).ControllerState != EWalkControllerState::Pending) {
        return;
    }
    State(collection.Id).ControllerState = EWalkControllerState::Idle;

    if (!State(collection.Id).SeedsByOwner) {
        return;
    }

    TWalkRun run;
    for (const auto& [owner, seeds] : State(collection.Id).SeedsByOwner) {
        for (const auto& seed : seeds) {
            run.Walks.push_back(TCacheBTreeWalk{
                .Owner = owner,
                .Seed = seed,
            });
        }
    }
    State(collection.Id).Run.emplace(std::move(run));
    ++ActiveRunCount;
    WalkRunsInProgress.insert(collection.Id);
    AdvanceNeeded = true;
}

void TCacheBTreeWalkController::FinishWalkRun(TCollection& collection) {
    Y_ENSURE(State(collection.Id).Run);
    Y_DEBUG_ABORT_UNLESS(State(collection.Id).Run->FetchesInFlight == 0);
    Y_ENSURE(ActiveRunCount > 0);
    --ActiveRunCount;

    State(collection.Id).Run.reset();
    WalkRunsInProgress.erase(collection.Id);

    RunsToFinish.erase(collection.Id);
    StartWalkRun(collection);
}

bool TCacheBTreeWalkController::FinishWalkRunIfDrained(TCollection& collection) {
    if (!State(collection.Id).Run || State(collection.Id).Run->State == EWalkRunState::Walking ||
        State(collection.Id).Run->FetchesInFlight != 0)
    {
        return false;
    }

    const TLogoBlobID collectionId = collection.Id;
    FinishWalkRun(collection);
    if (auto* current = Host.FindWalkCollection(collectionId)) {
        Host.TryDropExpiredCollection(*current);
    }
    return true;
}

void TCacheBTreeWalkController::CancelWalkRun(TCollection& collection) {
    if (!State(collection.Id).Run) {
        StartWalkRun(collection);
        return;
    }
    auto& run = *State(collection.Id).Run;
    if (run.State != EWalkRunState::Cancelled) {
        run.State = EWalkRunState::Cancelled;
        // Other walks may share index requests that cancellation is about to remove.
        if (!run.Walks.empty()) {
            IndexPagesChanged(run.Walks.front().Seed.IndexCollectionId);
        }
        run.Walks.clear();
        // A cancelled run leaves the scheduler immediately; a pending intent starts a fresh
        // run only after the cancelled one drains.
        WalkRunsInProgress.erase(collection.Id);
        Host.CancelQueuedWalkRequestsAndPump(collection.Id);
    }
    FinishWalkRunIfDrained(collection);
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
    UpdateWalkIndex(collection);
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

    FinishWalkRunIfDrained(*collection);
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
        if (const auto* state = States.FindPtr(collectionId); state && state->Run) {
            InvalidateWalkRun(*collection);
        }
    }
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

    while (true) {
        if (walk.PendingDataPages) {
            if (!AddNodeDataPagesToBatch(walk, *dataCollection)) {
                return;
            }
        }

        if (walk.Next >= walk.CurrentLevel.size()) {
            if (!walk.NextLevel) {
                if (FlushDataPageBatch(walk, *dataCollection) != EBatchResult::Flushed) {
                    return;
                }
                break;
            }
            walk.CurrentLevel = std::move(walk.NextLevel);
            walk.NextLevel.clear();
            walk.Next = 0;
            walk.BatchEnd = 0;
            ++walk.Level;
            continue;
        }

        // A zero-level tree has its data page as the root.
        if (walk.Level >= walk.Seed.LevelCount) {
            walk.PendingDataPages = std::move(walk.CurrentLevel);
            walk.CurrentLevel.clear();
            walk.Next = 0;
            walk.BatchEnd = 0;
            continue;
        }

        if (walk.Next >= walk.BatchEnd) {
            ui64 batchBytes = 0;
            walk.BatchEnd = walk.Next;
            while (walk.BatchEnd < walk.CurrentLevel.size()) {
                const ui64 pageSize = walk.CurrentLevel[walk.BatchEnd].Size;
                if (walk.BatchEnd > walk.Next && pageSize > MaxWalkBatchBytes - batchBytes) {
                    break;
                }

                batchBytes += pageSize;
                ++walk.BatchEnd;
                if (batchBytes >= MaxWalkBatchBytes) {
                    break;
                }
            }
        }

        const TPageLocation location = walk.CurrentLevel[walk.Next];
        auto* page = indexCollection->PageSet.FindPage(location.Offset);
        if (!page || page->State == PageStateNo) {
            TVector<TPageLocation> toRequest;
            for (size_t i = walk.Next; i < walk.BatchEnd; ++i) {
                const auto& batchLocation = walk.CurrentLevel[i];
                auto* batchPage = indexCollection->PageSet.FindPage(batchLocation.Offset);
                if (!batchPage || batchPage->State == PageStateNo) {
                    toRequest.push_back(batchLocation);
                }
            }

            // Index pages always live in the main group; its mode decides how they are fetched.
            if (indexCollection->GetCacheMode() == ECacheMode::TryKeepInMemory) {
                // Wait for loader progress; a self-continuation cannot unblock a capacity-limited index read.
                QueueInMemoryPages(*indexCollection, toRequest);
            } else {
                Host.FetchWalkIndexLevel(*indexCollection, std::move(toRequest), walkCollectionId);
            }
            return; // wait for the pages to arrive, the next drive continues the walk
        }

        if (page->State != PageStateLoaded && page->State != PageStateEvicted) {
            return;
        }
        ++walk.Next;

        const bool childrenAreData = (walk.Level + 1 >= walk.Seed.LevelCount);
        if (childrenAreData && !walk.Seed.QueueDataPages && !walk.Seed.Sticky) {
            // This walk only keeps the index level resident, its data pages are of no use.
            continue;
        }

        auto ref = TSharedPageRef::MakeUsed(page, Host.WalkCachePages()->GCList, page->Type);
        Y_DEBUG_ABORT_UNLESS(ref.IsUsed(), "walked B-tree page cannot be used");
        NTable::NPage::TBtreeIndexNode node(TPinnedPageRef(ref).GetData(), /*v2Format=*/true);

        TVector<TPageLocation> children;
        children.reserve(node.GetChildrenCount());
        for (NTable::NPage::TRecIdx pos : xrange(node.GetChildrenCount())) {
            children.push_back(std::get<NTable::NPage::TPageLocation>(node.GetChild(pos, childrenAreData)));
        }

        if (childrenAreData) {
            walk.PendingDataPages = std::move(children);
        } else {
            HandOverIndexLevel(walk, children, walk.Level + 1);
            for (auto& child : children) {
                walk.NextLevel.push_back(std::move(child));
            }
        }
    }

    FlushPagesToNotify(walk);
    walk.Done = true;
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
    ContinuationNeeded |= queued;
    if (queued) {
        State(dataCollection.Id).Run->NeedsAdvance = true;
    }
    return queued ? EBatchResult::Queued : EBatchResult::Flushed;
}

bool TCacheBTreeWalkController::AddNodeDataPagesToBatch(TCacheBTreeWalk& walk, TCollection& dataCollection) {
    const bool queueDataPages =
        walk.Seed.QueueDataPages && dataCollection.GetCacheMode() == ECacheMode::TryKeepInMemory;
    if (!walk.Seed.Sticky && !queueDataPages) {
        walk.PendingDataPages.clear();
        return true;
    }

    ui64 nodeBytes = 0;
    for (const auto& location : walk.PendingDataPages) {
        nodeBytes += location.Size;
    }

    if (walk.DataPageBatch &&
        (walk.DataPageBatchBytes >= MaxWalkBatchBytes || nodeBytes > MaxWalkBatchBytes - walk.DataPageBatchBytes))
    {
        if (FlushDataPageBatch(walk, dataCollection) != EBatchResult::Flushed) {
            return false;
        }
    }

    walk.DataPageBatch.reserve(walk.DataPageBatch.size() + walk.PendingDataPages.size());
    for (auto& location : walk.PendingDataPages) {
        walk.DataPageBatch.push_back(std::move(location));
    }
    walk.DataPageBatchBytes += nodeBytes;
    walk.PendingDataPages.clear();

    if (walk.DataPageBatchBytes >= MaxWalkBatchBytes) {
        return FlushDataPageBatch(walk, dataCollection) == EBatchResult::Flushed;
    }
    return true;
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

void TCacheBTreeWalkController::IndexPagesChanged(const TLogoBlobID& collectionId) {
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

void TCacheBTreeWalkController::Advance() {
    if (!std::exchange(AdvanceNeeded, false)) {
        return;
    }
    ContinuationNeeded = false;
    TVector<TLogoBlobID> invalid;
    for (const TLogoBlobID& id : WalkRunsInProgress) {
        auto* collection = Host.FindWalkCollection(id);
        Y_ENSURE(collection && State(collection->Id).Run);
        auto& run = *State(collection->Id).Run;
        if (run.State != EWalkRunState::Walking || !std::exchange(run.NeedsAdvance, false)) {
            continue;
        }

        bool pending = false;
        bool failed = false;
        for (auto& walk : run.Walks) {
            if (!walk.Done) {
                AdvanceWalk(walk, collection->Id);
            }
            failed |= walk.Invalid;
            pending |= !walk.Done;
        }

        if (failed) {
            invalid.push_back(id);
        } else if (!pending) {
            run.State = EWalkRunState::Draining;
            RunsToFinish.insert(id);
        }
    }

    for (const TLogoBlobID& id : invalid) {
        if (auto* collection = Host.FindWalkCollection(id)) {
            InvalidateWalkRun(*collection);
        }
    }
    if (ContinuationNeeded) {
        AdvanceNeeded = true;
        Host.ScheduleWalkContinuation();
    }
}

void TCacheBTreeWalkController::FinishReady() {
    while (!RunsToFinish.empty()) {
        const TLogoBlobID id = *RunsToFinish.begin();
        RunsToFinish.erase(id);
        if (auto* collection = Host.FindWalkCollection(id)) {
            FinishWalkRunIfDrained(*collection);
        }
    }
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
