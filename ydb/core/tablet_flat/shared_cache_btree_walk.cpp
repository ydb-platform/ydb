#include "flat_page_btree_index.h"
#include "shared_cache_btree_walk.h"
#include "shared_cache_pages.h"
#include "shared_sausagecache_state.h"

#include <util/generic/algorithm.h>

namespace NKikimr::NSharedCache {

TWalkCollectionState& TCacheBTreeWalkController::State(const TLogoBlobID& collectionId) {
    return States[collectionId];
}

const TWalkCollectionState& TCacheBTreeWalkController::State(const TLogoBlobID& collectionId) const {
    const auto* state = States.FindPtr(collectionId);
    Y_ENSURE(state);
    return *state;
}

bool TCacheBTreeWalkController::IsIdle(const TLogoBlobID& collectionId) const {
    const auto* state = States.FindPtr(collectionId);
    // Keep the collection alive while its index association exists, or WalkCollectionsByIndex would dangle.
    return !state || (!state->Run && !state->IndexCollectionId);
}

void TCacheBTreeWalkController::EraseCollection(const TLogoBlobID& collectionId) {
    Y_ENSURE(IsIdle(collectionId));
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

void TCacheBTreeWalkController::CancelPendingWalkPages(TCollection& collection) {
    Y_ENSURE(State(collection.Id).Run);
    auto& run = *State(collection.Id).Run;
    for (const TLogoBlobID& collectionId : run.PendingCollections) {
        auto pendingIt = Host.PendingWalkPages().find(collectionId);
        if (pendingIt == Host.PendingWalkPages().end()) {
            continue;
        }

        auto& pages = pendingIt->second;
        for (auto it = pages.begin(); it != pages.end();) {
            if (it->second == run.Id) {
                it = pages.erase(it);
            } else {
                ++it;
            }
        }
        if (pages.empty()) {
            Host.PendingWalkPages().erase(pendingIt);
        }
    }
    run.PendingCollections.clear();
}

void TCacheBTreeWalkController::StartWalkRun(TCollection& collection) {
    if (State(collection.Id).Run || State(collection.Id).ControllerState != EWalkControllerState::Pending) {
        return;
    }
    State(collection.Id).ControllerState = EWalkControllerState::Idle;

    if (!State(collection.Id).SeedsByOwner) {
        return;
    }

    ui64 loadRunId = NextWalkLoadId++;
    if (!loadRunId) {
        loadRunId = NextWalkLoadId++;
    }
    Y_ENSURE(WalkLoads.emplace(loadRunId, collection.Id).second);

    TWalkRun run;
    run.Id = loadRunId;
    for (const auto& [owner, seeds] : State(collection.Id).SeedsByOwner) {
        for (const auto& seed : seeds) {
            run.Walks.push_back(TCacheBTreeWalk{
                .Owner = owner,
                .Seed = seed,
            });
        }
    }
    State(collection.Id).Run.emplace(std::move(run));
    WalkRunsInProgress.insert(collection.Id);
}

void TCacheBTreeWalkController::FinishWalkRun(TCollection& collection) {
    Y_ENSURE(State(collection.Id).Run);
    const auto& run = *State(collection.Id).Run;
    Y_ENSURE(run.FetchesInFlight == 0);
    Y_ENSURE(WalkLoads.erase(run.Id));

    State(collection.Id).Run.reset();
    WalkRunsInProgress.erase(collection.Id);

    StartWalkRun(collection);
}

bool TCacheBTreeWalkController::HasPendingWalkPages(const TCollection& collection) const {
    if (!State(collection.Id).Run) {
        return false;
    }
    // A draining run may have no sent fetches while these leaves wait for in-memory budget.
    // It remains owned by the controller until the loader submits them or the run is cancelled.
    const auto& run = *State(collection.Id).Run;
    for (const TLogoBlobID& collectionId : run.PendingCollections) {
        if (const auto* pages = Host.PendingWalkPages().FindPtr(collectionId)) {
            if (AnyOf(*pages,
                    [&](const auto& item) {
                        return item.second == run.Id;
                    }))
            {
                return true;
            }
        }
    }
    return false;
}

bool TCacheBTreeWalkController::FinishWalkRunIfDrained(TCollection& collection) {
    if (!State(collection.Id).Run || State(collection.Id).Run->State == EWalkRunState::Walking ||
        State(collection.Id).Run->FetchesInFlight != 0 || HasPendingWalkPages(collection))
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
        run.Walks.clear();
        // A cancelled run leaves the scheduler immediately; a pending intent starts a fresh
        // run only after the cancelled one drains.
        WalkRunsInProgress.erase(collection.Id);
        CancelPendingWalkPages(collection);
        Host.CancelQueuedWalkRequestsAndPump(run.Id);
    }
    FinishWalkRunIfDrained(collection);
}

void TCacheBTreeWalkController::UpdateSeeds(
    TCollection& collection, const TActorId& owner, TVector<NSharedCache::TEvAttach::TBtreeSeed> seeds) {
    auto* current = State(collection.Id).SeedsByOwner.FindPtr(owner);
    // Production seeds are ordered current then historic, with at most two per owner.
    const bool same = current ? *current == seeds : seeds.empty();
    const bool unblock = State(collection.Id).ControllerState == EWalkControllerState::Blocked;
    if (same && !unblock) {
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

void TCacheBTreeWalkController::FinishFetch(ui64 loadRunId) {
    if (!loadRunId) {
        return;
    }

    // A run outlives every fetch it starts; the last arriving fetch may finish the run.
    auto loadIt = WalkLoads.find(loadRunId);
    Y_ENSURE(loadIt != WalkLoads.end());
    const TLogoBlobID collectionId = loadIt->second;
    auto* collection = Host.FindWalkCollection(collectionId);
    Y_ENSURE(collection && State(collection->Id).Run && State(collection->Id).Run->Id == loadRunId);
    auto& run = *State(collection->Id).Run;
    Y_ENSURE(run.FetchesInFlight > 0);
    --run.FetchesInFlight;

    FinishWalkRunIfDrained(*collection);
}

void TCacheBTreeWalkController::FetchStarted(ui64 runId) {
    auto loadIt = WalkLoads.find(runId);
    Y_ENSURE(loadIt != WalkLoads.end());
    auto* collection = Host.FindWalkCollection(loadIt->second);
    Y_ENSURE(collection && State(collection->Id).Run && State(collection->Id).Run->State != EWalkRunState::Cancelled &&
             State(collection->Id).Run->Id == runId);
    ++State(collection->Id).Run->FetchesInFlight;
}

bool TCacheBTreeWalkController::IsRunActive(ui64 runId) const {
    auto loadIt = WalkLoads.find(runId);
    if (loadIt == WalkLoads.end()) {
        return false;
    }
    auto* collection = Host.FindWalkCollection(loadIt->second);
    const auto* state = States.FindPtr(loadIt->second);
    return collection && state && state->Run && state->Run->State != EWalkRunState::Cancelled &&
           state->Run->Id == runId;
}

void TCacheBTreeWalkController::InvalidateRun(ui64 runId) {
    auto loadIt = WalkLoads.find(runId);
    Y_ENSURE(loadIt != WalkLoads.end());
    auto* collection = Host.FindWalkCollection(loadIt->second);
    Y_ENSURE(collection);
    InvalidateWalkRun(*collection);
}

void TCacheBTreeWalkController::AdvanceWalk(TCacheBTreeWalk& walk, ui64 loadRunId) {
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
        if (walk.PendingLeaves) {
            if (!AddLeafNodeToBatch(walk, *dataCollection, loadRunId)) {
                return;
            }
        }

        if (walk.Next >= walk.CurrentLevel.size()) {
            if (!walk.NextLevel) {
                if (FlushLeafBatch(walk, *dataCollection, loadRunId) != EBatchResult::Flushed) {
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
            walk.PendingLeaves = std::move(walk.CurrentLevel);
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
            Host.FetchWalkIndexLevel(*indexCollection, std::move(toRequest), loadRunId);
            return; // wait for the pages to arrive, the next drive continues the walk
        }

        if (page->State != PageStateLoaded && page->State != PageStateEvicted) {
            return;
        }
        ++walk.Next;

        const bool childrenAreData = (walk.Level + 1 >= walk.Seed.LevelCount);
        if (childrenAreData && !walk.Seed.QueueLeaves && !walk.Seed.Sticky) {
            // This walk only keeps the index level resident, its leaves are of no use.
            continue;
        }

        auto ref = TSharedPageRef::MakeUsed(page, Host.WalkCachePages()->GCList, page->Type);
        Y_ENSURE(ref.IsUsed(), "walked B-tree page cannot be used");
        NTable::NPage::TBtreeIndexNode node(TPinnedPageRef(ref).GetData(), /*v2Format=*/true);

        TVector<TPageLocation> children;
        children.reserve(node.GetChildrenCount());
        for (NTable::NPage::TRecIdx pos : xrange(node.GetChildrenCount())) {
            children.push_back(std::get<NTable::NPage::TPageLocation>(node.GetChild(pos, childrenAreData)));
        }

        if (childrenAreData) {
            walk.PendingLeaves = std::move(children);
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
        return; // not sticky, or these are the leaves
    }

    for (const auto& location : locations) {
        AppendPageToNotify(walk, walk.Seed.IndexCollectionId, location);
    }
}

TCacheBTreeWalkController::EBatchResult TCacheBTreeWalkController::FlushLeafBatch(
    TCacheBTreeWalk& walk, TCollection& dataCollection, ui64 loadRunId) {
    if (!walk.LeafBatch) {
        return EBatchResult::Flushed;
    }

    const bool queueLeaves = walk.Seed.QueueLeaves && dataCollection.GetCacheMode() == ECacheMode::TryKeepInMemory;
    auto* queue = queueLeaves ? &Host.PendingWalkPages()[dataCollection.Id] : nullptr;
    if (queue && !queue->empty()) {
        return EBatchResult::Blocked;
    }

    TWalkRun* run = nullptr;
    if (queue) {
        auto loadIt = WalkLoads.find(loadRunId);
        Y_ENSURE(loadIt != WalkLoads.end());
        auto* walkCollection = Host.FindWalkCollection(loadIt->second);
        Y_ENSURE(walkCollection && State(walkCollection->Id).Run && State(walkCollection->Id).Run->Id == loadRunId);
        run = &*State(walkCollection->Id).Run;
    }

    bool queued = false;
    for (const auto& location : walk.LeafBatch) {
        if (queue) {
            auto* page = dataCollection.PageSet.FindPage(location.Offset);
            if (!page || page->State == PageStateNo) {
                if (queue->emplace(location, loadRunId).second) {
                    run->PendingCollections.insert(dataCollection.Id);
                    queued = true;
                }
            }
        }
    }

    if (walk.Seed.Sticky) {
        // Index notifications are parents-first; flush them before handing over data pages.
        FlushPagesToNotify(walk);
        Host.SendWalkStickyPages(dataCollection, walk.Owner, walk.LeafBatch);
    }

    walk.LeafBatch.clear();
    walk.LeafBatchBytes = 0;
    return queued ? EBatchResult::Queued : EBatchResult::Flushed;
}

bool TCacheBTreeWalkController::AddLeafNodeToBatch(TCacheBTreeWalk& walk, TCollection& dataCollection, ui64 loadRunId) {
    const bool queueLeaves = walk.Seed.QueueLeaves && dataCollection.GetCacheMode() == ECacheMode::TryKeepInMemory;
    if (!walk.Seed.Sticky && !queueLeaves) {
        walk.PendingLeaves.clear();
        return true;
    }

    ui64 nodeBytes = 0;
    for (const auto& location : walk.PendingLeaves) {
        nodeBytes += location.Size;
    }

    if (walk.LeafBatch &&
        (walk.LeafBatchBytes >= MaxWalkBatchBytes || nodeBytes > MaxWalkBatchBytes - walk.LeafBatchBytes))
    {
        if (FlushLeafBatch(walk, dataCollection, loadRunId) != EBatchResult::Flushed) {
            return false;
        }
    }

    walk.LeafBatch.reserve(walk.LeafBatch.size() + walk.PendingLeaves.size());
    for (auto& location : walk.PendingLeaves) {
        walk.LeafBatch.push_back(std::move(location));
    }
    walk.LeafBatchBytes += nodeBytes;
    walk.PendingLeaves.clear();

    if (walk.LeafBatchBytes >= MaxWalkBatchBytes) {
        return FlushLeafBatch(walk, dataCollection, loadRunId) == EBatchResult::Flushed;
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

void TCacheBTreeWalkController::Advance() {
    TVector<TLogoBlobID> invalid;
    for (const TLogoBlobID& id : WalkRunsInProgress) {
        auto* collection = Host.FindWalkCollection(id);
        Y_ENSURE(collection && State(collection->Id).Run);
        auto& run = *State(collection->Id).Run;
        if (run.State != EWalkRunState::Walking) {
            continue;
        }

        bool pending = false;
        bool failed = false;
        for (auto& walk : run.Walks) {
            if (!walk.Done) {
                AdvanceWalk(walk, run.Id);
            }
            failed |= walk.Invalid;
            pending |= !walk.Done;
        }

        if (failed) {
            invalid.push_back(id);
        } else if (!pending) {
            run.State = EWalkRunState::Draining;
        }
    }

    for (const TLogoBlobID& id : invalid) {
        if (auto* collection = Host.FindWalkCollection(id)) {
            InvalidateWalkRun(*collection);
        }
    }
}

void TCacheBTreeWalkController::FinishReady() {
    const TVector<TLogoBlobID> runs(WalkRunsInProgress.begin(), WalkRunsInProgress.end());
    for (const TLogoBlobID& id : runs) {
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
                return !seed.QueueLeaves && !seed.Sticky && !seed.IndexCollectionSticky &&
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
