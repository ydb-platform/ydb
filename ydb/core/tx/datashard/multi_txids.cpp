#include "multi_txids.h"
#include "datashard_impl.h"

namespace NKikimr::NDataShard {

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
            if (auto itNested = MultiTxIds.find(nestedTxId); itNested != MultiTxIds.end()) {
                // Track reference links between MultiTxIds
                auto& nested = itNested->second;
                nested.References.PushBack(edge);
                nested.ReferencesCount++;
            }
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

void TMultiTxIdManager::Start() {
    // TODO
}

void TMultiTxIdManager::DecrementLockedRowsCount(NIceDb::TNiceDb& db, ui64 multiTxId) {
    using Schema = TDataShard::Schema;

    auto it = MultiTxIds.find(multiTxId);
    Y_ENSURE(it != MultiTxIds.end());
    TMultiTxId& entry = it->second;
    Y_ENSURE(entry.LockedRowsCount > 0);
    entry.LockedRowsCount--;
    db.Table<Schema::MultiTxIds>().Key(entry.MultiTxId).Update(
        NIceDb::TUpdate<Schema::MultiTxIds::LockedRowsCount>(entry.LockedRowsCount));
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

} // namespace NKikimr::NDataShard
