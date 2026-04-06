#include "multi_txids.h"
#include "datashard_impl.h"

namespace NKikimr::NDataShard {

static constexpr size_t MaxInlineCleanupReferences = 8;

TMultiTxIdEnumerator::TMultiTxIdEnumerator(TMultiTxIdManager& self)
    : Self(self)
{
    Filter = NTable::ELockMode::None;
}

TMultiTxIdEnumerator::TMultiTxIdEnumerator(TMultiTxIdManager& self, const TMultiTxId* entry, NTable::ELockMode filter)
    : Self(self)
{
    Reset(entry, filter);
}

void TMultiTxIdEnumerator::Reset(const TMultiTxId* entry, NTable::ELockMode filter) {
    Y_ENSURE(entry);
    Stack.clear();
    Filter = filter;
    if (!entry->HasFlag(EMultiTxIdFlag::Broken)) {
        Add(entry);
    }
}

TMultiTxIdEnumerator::TResult TMultiTxIdEnumerator::Next() {
    while (!Stack.empty()) {
        auto current = Stack.back();
        Stack.pop_back();
        Add(current.Entry, current.Edge);
        if (const TMultiTxId* nested = Self.FindMultiTxId(current.Edge->NestedTxId)) {
            if (!nested->HasFlag(EMultiTxIdFlag::Broken)) {
                Add(nested);
            }
        } else {
            return { current.Edge->NestedTxId, current.Edge->LockMode };
        }
    }
    return {};
}

void TMultiTxIdEnumerator::Add(const TMultiTxId* entry, const TMultiTxId::TEdge* last) {
    if (entry->Edges.Empty()) {
        return;
    }
    if (last) {
        if (last == entry->Edges.Front()) {
            return;
        }
        last = last->PrevEdge();
    } else {
        last = entry->Edges.Back();
    }
    Y_ENSURE(last != nullptr);
    for (;;) {
        if (Filter == NTable::ELockMode::None || !IsCompatibleRowLockMode(last->LockMode, Filter)) {
            Stack.push_back({ entry, last });
            break;
        }
        if (last == entry->Edges.Front()) {
            break;
        }
        last = last->PrevEdge();
    }
}

TMultiTxIdManager::TMultiTxIdManager(TDataShard& self)
    : Self(self)
{
    // This private field is not used yet, but will be needed later
    Y_UNUSED(Self);
}

TMultiTxIdManager::~TMultiTxIdManager() {
    // nothing yet
}

void TMultiTxIdManager::Clear() {
    MultiTxIds.clear();
    NextEdgeId = 1;
}

bool TMultiTxIdManager::Load(NIceDb::TNiceDb& db) {
    using Schema = TDataShard::Schema;

    Clear();

    // Tables may not exist in some inactive shards, which cannot have transactions
    if (db.HaveTable<Schema::MultiTxIds>() &&
        db.HaveTable<Schema::MultiTxIdGraph>())
    {
        if (!LoadMultiTxIds(db)) {
            return false;
        }
        if (!LoadMultiTxIdGraph(db)) {
            return false;
        }
    }

    return true;
}

bool TMultiTxIdManager::LoadMultiTxIds(NIceDb::TNiceDb& db) {
    using Schema = TDataShard::Schema;

    auto rowset = db.Table<Schema::MultiTxIds>().Select();
    if (!rowset.IsReady()) {
        return false;
    }

    while (!rowset.EndOfSet()) {
        ui64 multiTxId = rowset.GetValue<Schema::MultiTxIds::MultiTxId>();
        ui64 lockedRowsCount = rowset.GetValue<Schema::MultiTxIds::LockedRowsCount>();
        ui64 flags = rowset.GetValue<Schema::MultiTxIds::Flags>();

        auto res = MultiTxIds.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(multiTxId),
            std::forward_as_tuple(multiTxId));
        Y_ENSURE(res.second, "Unexpected duplicate MultiTxId " << multiTxId);
        auto& entry = res.first->second;
        entry.LockedRowsCount = lockedRowsCount;
        entry.Flags = flags;

        if (!rowset.Next()) {
            return false;
        }
    }

    return true;
}

bool TMultiTxIdManager::LoadMultiTxIdGraph(NIceDb::TNiceDb& db) {
    using Schema = TDataShard::Schema;

    auto rowset = db.Table<Schema::MultiTxIdGraph>().Select();
    if (!rowset.IsReady()) {
        return false;
    }

    while (!rowset.EndOfSet()) {
        ui64 edgeId = rowset.GetValue<Schema::MultiTxIdGraph::EdgeId>();
        ui64 multiTxId = rowset.GetValue<Schema::MultiTxIdGraph::MultiTxId>();
        ui64 nestedTxId = rowset.GetValue<Schema::MultiTxIdGraph::NestedTxId>();
        NTable::ELockMode nestedLockMode = rowset.GetValue<Schema::MultiTxIdGraph::NestedLockMode>();

        auto it = MultiTxIds.find(multiTxId);
        if (it != MultiTxIds.end()) {
            auto& parent = it->second;
            TMultiTxId::TEdge* edge = new TMultiTxId::TEdge(edgeId, multiTxId, nestedTxId, nestedLockMode);
            parent.Edges.PushBack(edge);
            InitializeReference(*edge);
        } else {
            Y_DEBUG_ABORT_S(
                "Cannot find parent for EdgeId# " << edgeId
                << " MultiTxId# " << multiTxId
                << " NestedTxId# " << nestedTxId
                << " NestedLockMode# " << nestedLockMode);
        }

        NextEdgeId = Max(NextEdgeId, edgeId + 1);

        if (!rowset.Next()) {
            return false;
        }
    }

    return true;
}

void TMultiTxIdManager::InitializeReference(TMultiTxId::TEdge& edge) {
    if (TMultiTxId* nested = FindMultiTxId(edge.NestedTxId)) {
        nested->References.PushBack(&edge);
        nested->ReferencesCount++;
    } else {
        auto& shadow = GetLockShadow(edge.NestedTxId);
        shadow.References.PushBack(&edge);
        shadow.ReferencesCount++;
    }
}

TMultiTxIdLockShadow& TMultiTxIdManager::GetLockShadow(ui64 lockId) {
    auto it = LockShadows.find(lockId);
    if (it == LockShadows.end()) {
        auto res = LockShadows.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(lockId),
            std::forward_as_tuple(lockId));
        Y_ENSURE(res.second);
        it = res.first;
    }
    return it->second;
}

void TMultiTxIdManager::Start() {
    // Check existing lock references and start cleaning up broken and missing
    bool enqueueCleanup = false;
    for (auto& [lockId, shadow] : LockShadows) {
        auto lock = Self.SysLocksTable().GetRawLock(lockId);
        if (!lock || lock->IsBroken()) {
            shadow.Broken = true;
            LockShadowCleanupQueue.PushBack(&shadow);
            enqueueCleanup = true;
        }
    }
    for (auto& [multiTxId, entry] : MultiTxIds) {
        if (NeedCleanup(&entry)) {
            MultiTxIdCleanupQueue.PushBack(&entry);
            enqueueCleanup = true;
        }
    }
    if (enqueueCleanup) {
        EnqueueCleanupTx();
    }
}

void TMultiTxIdManager::DecrementLockedRowsCount(NIceDb::TNiceDb& db, ui64 multiTxId) {
    using Schema = TDataShard::Schema;

    auto it = MultiTxIds.find(multiTxId);
    Y_ENSURE(it != MultiTxIds.end(), "Cannot decrement locked row count for a missing MultiTxId# " << multiTxId);
    TMultiTxId* entry = &it->second;
    Y_ENSURE(entry->LockedRowsCount > 0);
    entry->LockedRowsCount--;
    db.Table<Schema::MultiTxIds>().Key(entry->MultiTxId).Update(
        NIceDb::TUpdate<Schema::MultiTxIds::LockedRowsCount>(entry->LockedRowsCount));
    MaybeEnqueueCleanup(entry);
}

ui64 TMultiTxIdManager::CombineRowLocks(
        NIceDb::TNiceDb& db,
        ui64 currentTxId, NTable::ELockMode currentLockMode,
        ui64 lockTxId, NTable::ELockMode lockMode,
        ui64& globalTxId)
{
    using Schema = TDataShard::Schema;

    // TODO: use an in-memory index instead of brute force search
    for (auto& pr : MultiTxIds) {
        TMultiTxId& entry = pr.second;
        size_t matches = 0;
        for (const auto& edge : entry.Edges) {
            if (matches == 0) {
                if (edge.NestedTxId != currentTxId || edge.LockMode != currentLockMode) {
                    matches = 0;
                    break;
                }
                ++matches;
            } else if (matches == 1) {
                if (edge.NestedTxId != lockTxId || edge.LockMode != lockMode) {
                    matches = 0;
                    break;
                }
                ++matches;
            } else {
                matches = 0;
                break;
            }
        }
        // Return existing MultiTxId only when we have 2 exact matching edges
        if (matches == 2) {
            entry.LockedRowsCount++;
            db.Table<Schema::MultiTxIds>().Key(entry.MultiTxId).Update(
                NIceDb::TUpdate<Schema::MultiTxIds::LockedRowsCount>(entry.LockedRowsCount));
            return entry.MultiTxId;
        }
    }

    // We need GlobalTxId to create a new MultiTxId
    if (!globalTxId) {
        return 0;
    }

    auto res = MultiTxIds.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(globalTxId),
        std::forward_as_tuple(globalTxId));
    Y_ENSURE(res.second, "Unexpected duplicate MultiTxId");

    TMultiTxId& entry = res.first->second;
    entry.Edges.PushBack(new TMultiTxId::TEdge(NextEdgeId++, entry.MultiTxId, currentTxId, currentLockMode));
    entry.Edges.PushBack(new TMultiTxId::TEdge(NextEdgeId++, entry.MultiTxId, lockTxId, lockMode));
    entry.LockedRowsCount++;
    for (auto& edge : entry.Edges) {
        InitializeReference(edge);
    }

    db.Table<Schema::MultiTxIds>().Key(entry.MultiTxId).Update(
        NIceDb::TUpdate<Schema::MultiTxIds::LockedRowsCount>(entry.LockedRowsCount),
        NIceDb::TUpdate<Schema::MultiTxIds::Flags>(entry.Flags));
    for (const auto& edge : entry.Edges) {
        db.Table<Schema::MultiTxIdGraph>().Key(edge.EdgeId).Update(
            NIceDb::TUpdate<Schema::MultiTxIdGraph::MultiTxId>(edge.MultiTxId),
            NIceDb::TUpdate<Schema::MultiTxIdGraph::NestedTxId>(edge.NestedTxId),
            NIceDb::TUpdate<Schema::MultiTxIdGraph::NestedLockMode>(edge.LockMode));
    }

    globalTxId = 0;

    return entry.MultiTxId;
}

void TMultiTxIdManager::BreakMultiTxId(const TMultiTxId* entry) {
    // TODO: make it possible to break MultiTxId directly
    auto enumerator = EnumerateLocks(entry);
    while (auto result = enumerator.Next()) {
        Self.SysLocksTable().BreakLock(result.LockId);
    }
}

void TMultiTxIdManager::AddWriteConflict(const TMultiTxId* entry) {
    // TODO: make it possible to add MultiTxId to conflict list directly
    auto enumerator = EnumerateLocks(entry);
    while (auto result = enumerator.Next()) {
        Self.SysLocksTable().AddWriteConflict(result.LockId);
    }
}

void TMultiTxIdManager::OnLockBroken(NIceDb::TNiceDb& db, ui64 lockId) {
    if (auto* shadow = LockShadows.FindPtr(lockId)) {
        BreakShadow(db, shadow);
    }
}

void TMultiTxIdManager::OnLockRemoved(NIceDb::TNiceDb& db, ui64 lockId) {
    if (auto* shadow = LockShadows.FindPtr(lockId)) {
        BreakShadow(db, shadow);
    }
}

void TMultiTxIdManager::BreakShadow(NIceDb::TNiceDb& db, TMultiTxIdLockShadow* shadow) {
    if (shadow->Broken) {
        // Already known to be broken
        return;
    }
    shadow->Broken = true;
    RunLockShadowCleanup(db, shadow);
}

class TDataShard::TTxMultiTxIdCleanup
    : public NTabletFlatExecutor::ITransaction
{
public:
    explicit TTxMultiTxIdCleanup(TDataShard& self)
        : Self(self)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        Self.GetMultiTxIdManager().RunCleanup(db);
        return true;
    }

    void Complete(const TActorContext&) override {
        // nothing
    }

private:
    TDataShard& Self;
};

void TMultiTxIdManager::EnqueueCleanupTx() {
    if (!CleanupTxInFlight) {
        Self.Enqueue(new TDataShard::TTxMultiTxIdCleanup(Self));
        CleanupTxInFlight = true;
    }
}

void TMultiTxIdManager::RunCleanup(NIceDb::TNiceDb& db) {
    Y_ENSURE(CleanupTxInFlight);
    CleanupTxInFlight = false;

    if (!LockShadowCleanupQueue.Empty()) {
        RunLockShadowCleanup(db, LockShadowCleanupQueue.PopFront());
    } else if (!MultiTxIdCleanupQueue.Empty()) {
        RunMultiTxIdCleanup(db, MultiTxIdCleanupQueue.PopFront());
    }

    if (!LockShadowCleanupQueue.Empty() || !MultiTxIdCleanupQueue.Empty()) {
        EnqueueCleanupTx();
    }
}

bool TMultiTxIdManager::NeedCleanup(const TMultiTxId* entry) const {
    size_t edgesCount = 0;
    for (auto& edge : entry->Edges) {
        Y_UNUSED(edge);
        edgesCount++;
    }

    if (edgesCount == 0) {
        // We want to cleanup empty MultiTxIds
        return true;
    }

    if (entry->LockedRowsCount == 0 && entry->ReferencesCount <= 1) {
        // We want to cleanup unnecessary MultiTxIds
        return edgesCount == 1 || entry->ReferencesCount == 0;
    }

    return false;
}

void TMultiTxIdManager::MaybeEnqueueCleanup(TMultiTxId* entry) {
    if (!entry->ToCleanupQueueItem()->Empty()) {
        // Item might be in the queue already
        return;
    }

    if (NeedCleanup(entry)) {
        MultiTxIdCleanupQueue.PushBack(entry);
        EnqueueCleanupTx();
    }
}

void TMultiTxIdManager::RunLockShadowCleanup(NIceDb::TNiceDb& db, TMultiTxIdLockShadow* shadow) {
    size_t processed = 0;
    while (processed < MaxInlineCleanupReferences && !shadow->References.Empty()) {
        auto* ref = shadow->References.PopFront();
        shadow->ReferencesCount--;
        auto* parent = MultiTxIds.FindPtr(ref->MultiTxId);
        Y_ENSURE(parent, "Unexpected failure to find parent MultiTxId# " << ref->MultiTxId << " for EdgeId# " << ref->EdgeId);
        Y_ENSURE(!parent->Edges.Empty(), "Found unexpected parent MultiTxId# " << ref->MultiTxId << " for EdgeId# " << ref->EdgeId << " with no edges");
        DeleteEdge(db, ref);
        MaybeEnqueueCleanup(parent);
        ++processed;
    }

    if (shadow->References.Empty()) {
        ui64 lockId = shadow->LockId;
        LockShadows.erase(lockId);
        return;
    }

    LockShadowCleanupQueue.PushBack(shadow);
    EnqueueCleanupTx();
}

void TMultiTxIdManager::RunMultiTxIdCleanup(NIceDb::TNiceDb& db, TMultiTxId* entry) {
    using Schema = TDataShard::Schema;

    // MultiTxId without any edges is removed from all references
    // This happens even when there are rows locked to this MultiTxId, which is
    // redundant this it represents an empty set of transactions.
    if (entry->Edges.Empty()) {
        size_t processed = 0;
        while (processed < MaxInlineCleanupReferences && !entry->References.Empty()) {
            auto* ref = entry->References.PopFront();
            auto* parent = MultiTxIds.FindPtr(ref->MultiTxId);
            Y_ENSURE(parent, "Unexpected failure to find parent MultiTxId# " << ref->MultiTxId << " for EdgeId# " << ref->EdgeId);
            Y_ENSURE(!parent->Edges.Empty(), "Found unexpected parent MultiTxId# " << ref->MultiTxId << " for EdgeId# " << ref->EdgeId << " with no edges");
            entry->ReferencesCount--;
            DeleteEdge(db, ref);
            // Deleting an edge from parent might trigger its cleanup
            MaybeEnqueueCleanup(parent);
            ++processed;
        }

        if (entry->References.Empty()) {
            DeleteMultiTxId(db, entry);
            return;
        }

        MultiTxIdCleanupQueue.PushBack(entry);
        EnqueueCleanupTx();
        return;
    }

    if (entry->LockedRowsCount > 0) {
        // MultiTxId might be reused before the cleanup queue is processed
        return;
    }

    if (entry->ReferencesCount > 1) {
        // Avoid removing entries used by multiple upstream MultiTxIds
        return;
    }

    // Remove MultiTxId that has no references
    if (entry->ReferencesCount == 0) {
        while (!entry->Edges.Empty()) {
            auto* edge = entry->Edges.Front();
            ui64 nestedTxId = edge->NestedTxId;
            if (auto* child = MultiTxIds.FindPtr(nestedTxId)) {
                Y_ENSURE(child->ReferencesCount > 0);
                child->References.Remove(edge);
                child->ReferencesCount--;
                DeleteEdge(db, edge);
                // Deleting a reference to child might trigger its cleanup
                MaybeEnqueueCleanup(child);
            } else if (auto* shadow = LockShadows.FindPtr(nestedTxId)) {
                Y_ENSURE(shadow->ReferencesCount > 0);
                shadow->References.Remove(edge);
                shadow->ReferencesCount--;
                DeleteEdge(db, edge);
                // Deleting a reference to shadow might make it redundant
                if (shadow->References.Empty()) {
                    LockShadows.erase(nestedTxId);
                }
            } else {
                Y_ENSURE(false, "Unexpected failure to find NestedTxId# " << nestedTxId
                    << " from EdgeId# " << edge->EdgeId << " in MultiTxId# " << entry->MultiTxId);
            }
        }

        DeleteMultiTxId(db, entry);
        return;
    }

    // Otherwise we have edges which need to be moved up to a single parent
    Y_ENSURE(!entry->References.Empty());
    auto* ref = entry->References.Front();
    auto* parent = MultiTxIds.FindPtr(ref->MultiTxId);
    Y_ENSURE(parent, "Unexpected failure to find MultiTxId# " << ref->MultiTxId << " from EdgeId# " << ref->EdgeId);
    while (!entry->Edges.Empty()) {
        // For each edge in the current entry we relink it before the edge which
        // is referring to the current entry. This way downstream edges will
        // replace the reference without changing relative order.
        auto* edge = entry->Edges.PopFront();
        edge->MultiTxId = parent->MultiTxId;
        edge->ToEdgesItem()->LinkBefore(ref);
        db.Table<Schema::MultiTxIdGraph>().Key(edge->EdgeId).Update(
            NIceDb::TUpdate<Schema::MultiTxIdGraph::MultiTxId>(edge->MultiTxId));
    }

    // Delete the reference to this entry from parent
    entry->ReferencesCount--;
    DeleteEdge(db, ref);

    // Now we must have no edges and no references
    Y_ENSURE(entry->Edges.Empty());
    Y_ENSURE(entry->ReferencesCount == 0);
    DeleteMultiTxId(db, entry);
}

void TMultiTxIdManager::DeleteEdge(NIceDb::TNiceDb& db, TMultiTxId::TEdge* edge) {
    using Schema = TDataShard::Schema;

    db.Table<Schema::MultiTxIdGraph>().Key(edge->EdgeId).Delete();
    delete edge;
}

void TMultiTxIdManager::DeleteMultiTxId(NIceDb::TNiceDb& db, TMultiTxId* entry) {
    using Schema = TDataShard::Schema;

    ui64 multiTxId = entry->MultiTxId;
    auto it = MultiTxIds.find(multiTxId);
    Y_ENSURE(it != MultiTxIds.end() && entry == &it->second, "Unexpected MultiTxId# " << multiTxId << " not matching the hashmap entry");
    Y_ENSURE(entry->Edges.Empty(), "Cannot delete MultiTxId# " << multiTxId << " which has graph edges");
    Y_ENSURE(entry->References.Empty(), "Cannot delete MultiTxId# " << multiTxId << " which has references");
    db.Table<Schema::MultiTxIds>().Key(multiTxId).Delete();
    MultiTxIds.erase(it);

    // Avoid leaving references to deleted MultiTxId in LocalDB
    for (auto& pr : Self.GetUserTables()) {
        auto tid = pr.second->LocalTid;
        if (db.GetDatabase().HasOpenTx(tid, multiTxId)) {
            db.GetDatabase().RemoveTx(tid, multiTxId);
        }
    }
}

} // namespace NKikimr::NDataShard
