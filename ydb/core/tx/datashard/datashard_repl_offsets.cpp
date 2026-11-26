#include "datashard_repl_offsets.h"
#include "datashard_impl.h"

namespace NKikimr::NDataShard {

    void TReplicationSourceOffsetsDb::PersistSource(const TPathId& pathId, ui64 sourceId, const TString& sourceName) {
        using Schema = TDataShard::Schema;

        NIceDb::TNiceDb db(Txc.DB);

        db.Table<Schema::ReplicationSources>()
            .Key(pathId.OwnerId, pathId.LocalPathId, sourceId)
            .Update(NIceDb::TUpdate<Schema::ReplicationSources::SourceName>(sourceName));
    }

    void TReplicationSourceOffsetsDb::PersistSplitKey(const TPathId& pathId, ui64 sourceId, ui64 splitKeyId, const TString& splitKey, i64 offset) {
        using Schema = TDataShard::Schema;

        NIceDb::TNiceDb db(Txc.DB);

        db.Table<Schema::ReplicationSourceOffsets>()
            .Key(pathId.OwnerId, pathId.LocalPathId, sourceId, splitKeyId)
            .Update(
                NIceDb::TUpdate<Schema::ReplicationSourceOffsets::SplitKey>(splitKey),
                NIceDb::TUpdate<Schema::ReplicationSourceOffsets::MaxOffset>(offset));
    }

    void TReplicationSourceOffsetsDb::PersistSplitKeyOffset(const TPathId& pathId, ui64 sourceId, ui64 splitKeyId, i64 offset) {
        using Schema = TDataShard::Schema;

        NIceDb::TNiceDb db(Txc.DB);

        db.Table<Schema::ReplicationSourceOffsets>()
            .Key(pathId.OwnerId, pathId.LocalPathId, sourceId, splitKeyId)
            .Update(
                NIceDb::TUpdate<Schema::ReplicationSourceOffsets::MaxOffset>(offset));
    }

    void TReplicationSourceOffsetsDb::PersistSplitKeyDelete(const TPathId& pathId, ui64 sourceId, ui64 splitKeyId) {
        using Schema = TDataShard::Schema;

        NIceDb::TNiceDb db(Txc.DB);

        db.Table<Schema::ReplicationSourceOffsets>()
            .Key(pathId.OwnerId, pathId.LocalPathId, sourceId, splitKeyId)
            .Delete();
    }

    class TReplicationSourceOffsetsDb::TCleanupBatch {
        using Schema = TDataShard::Schema;
        using ECleanupStatus = TReplicationSourceOffsetsDb::ECleanupStatus;

        // Internal cleanup budgets count fixed-size keys, not user values
        // or serialized redo records.
        static constexpr size_t MaxRows = 1000;
        static constexpr size_t MaxKeyBytes = 16 * 1024;
        static constexpr size_t OffsetKeyBytes = 4 * sizeof(ui64);
        static constexpr size_t SourceKeyBytes = 3 * sizeof(ui64);

    public:
        ECleanupStatus ReadOffsets(NIceDb::TNiceDb& db, const TPathId& pathId, TCleanupCursor& cursor) {
            auto rows = db.Table<Schema::ReplicationSourceOffsets>()
                .GreaterOrEqual(pathId.OwnerId, pathId.LocalPathId, cursor.SourceId, cursor.SplitKeyId)
                .LessOrEqual(pathId.OwnerId, pathId.LocalPathId)
                .Select<Schema::ReplicationSourceOffsets::SourceId, Schema::ReplicationSourceOffsets::SplitKeyId>();
            if (!rows.IsReady()) {
                return ECleanupStatus::NotReady;
            }
            while (!rows.EndOfSet() && HasBudget(OffsetKeyBytes)) {
                cursor.SourceId = rows.GetValue<Schema::ReplicationSourceOffsets::SourceId>();
                cursor.SplitKeyId = rows.GetValue<Schema::ReplicationSourceOffsets::SplitKeyId>();
                Offsets.emplace_back(cursor.SourceId, cursor.SplitKeyId);
                KeyBytes += OffsetKeyBytes;
                if (!HasBudget(OffsetKeyBytes)) {
                    break;
                }
                if (!rows.Next()) {
                    return ECleanupStatus::NotReady;
                }
            }
            return rows.EndOfSet() ? ECleanupStatus::Done : ECleanupStatus::More;
        }

        ECleanupStatus ReadSources(NIceDb::TNiceDb& db, const TPathId& pathId, TCleanupCursor& cursor) {
            auto rows = db.Table<Schema::ReplicationSources>()
                .GreaterOrEqual(pathId.OwnerId, pathId.LocalPathId, cursor.SourceId)
                .LessOrEqual(pathId.OwnerId, pathId.LocalPathId)
                .Select<Schema::ReplicationSources::SourceId>();
            if (!rows.IsReady()) {
                return ECleanupStatus::NotReady;
            }
            while (!rows.EndOfSet() && HasBudget(SourceKeyBytes)) {
                cursor.SourceId = rows.GetValue<Schema::ReplicationSources::SourceId>();
                Sources.push_back(cursor.SourceId);
                KeyBytes += SourceKeyBytes;
                if (!HasBudget(SourceKeyBytes)) {
                    break;
                }
                if (!rows.Next()) {
                    return ECleanupStatus::NotReady;
                }
            }
            return rows.EndOfSet() ? ECleanupStatus::Done : ECleanupStatus::More;
        }

        void Remove(NIceDb::TNiceDb& db, const TPathId& pathId) const {
            for (const auto& [sourceId, splitKeyId] : Offsets) {
                db.Table<Schema::ReplicationSourceOffsets>()
                    .Key(pathId.OwnerId, pathId.LocalPathId, sourceId, splitKeyId).Delete();
            }
            for (ui64 sourceId : Sources) {
                db.Table<Schema::ReplicationSources>()
                    .Key(pathId.OwnerId, pathId.LocalPathId, sourceId).Delete();
            }
        }

    private:
        bool HasBudget(size_t bytes) const {
            return Offsets.size() + Sources.size() < MaxRows && KeyBytes + bytes <= MaxKeyBytes;
        }

        TVector<std::pair<ui64, ui64>> Offsets;
        TVector<ui64> Sources;
        size_t KeyBytes = 0;
    };

    TReplicationSourceOffsetsDb::ECleanupStatus TReplicationSourceOffsetsDb::RemoveTableBatch(const TPathId& pathId, TCleanupCursor& cursor) {
        NIceDb::TNiceDb db(Txc.DB);
        TCleanupBatch batch;
        auto next = cursor;
        auto status = ECleanupStatus::Done;
        if (!next.OffsetsDone) {
            status = batch.ReadOffsets(db, pathId, next);
            if (status == ECleanupStatus::Done) {
                next.OffsetsDone = true;
                next.SourceId = 0;
                next.SplitKeyId = 0;
            }
        }
        if (next.OffsetsDone) {
            status = batch.ReadSources(db, pathId, next);
        }
        if (status == ECleanupStatus::NotReady) {
            return status;
        }

        // Finish all reads before mutating the database, so a page fault can
        // restart the transaction without committing a partial cleanup batch.
        batch.Remove(db, pathId);
        cursor = status == ECleanupStatus::Done ? TCleanupCursor{} : next;
        return status;
    }

    bool TReplicationSourceState::TLessByKeyPrefix::operator()(TConstArrayRef<TCell> a, TConstArrayRef<TCell> b) const {
        return ComparePrefixBorders(
            *KeyColumnTypes,
            a, PrefixModeLeftBorderInclusive,
            b, PrefixModeLeftBorderInclusive) < 0;
    }

    TReplicationSourceState::TReplicationSourceState(
            const TPathId& pathId,
            const ui64 id,
            const TString& name,
            const TVector<NScheme::TTypeInfo>* keyColumnTypes)
        : PathId(pathId)
        , Id(id)
        , Name(name)
        , Offsets(TLessByKeyPrefix{ keyColumnTypes })
    {
        // We always have at least one empty split key
        auto& state = OffsetBySplitKeyId[0];
        Offsets.insert(&state);
        AddStatBytes(state);
    }

    ui64 TReplicationSourceState::CalcStatBytes(const TOffsetState& state) const {
        return (
            0 // PathId
            + 8 // Id
            + 4 + Name.size()
            + 8 // SplitKeyId
            + 4 + state.SplitKey.GetBuffer().size()
            + 8 // MaxOffset
        );
    }

    void TReplicationSourceState::AddStatBytes(const TOffsetState& state) {
        StatBytes += CalcStatBytes(state);
    }

    void TReplicationSourceState::RemoveStatBytes(const TOffsetState& state) {
        StatBytes -= CalcStatBytes(state);
    }

    TReplicationSourceState::TOffsetState* TReplicationSourceState::FindSplitKey(TConstArrayRef<TCell> key) {
        auto it = Offsets.find(key);
        if (it != Offsets.end()) {
            return *it;
        }
        return nullptr;
    }

    void TReplicationSourceState::LoadSplitKey(ui64 splitKeyId, TSerializedCellVec key, i64 maxOffset) {
        auto& state = OffsetBySplitKeyId[splitKeyId];
        state.SplitKeyId = splitKeyId;
        state.SplitKey = std::move(key);
        state.MaxOffset = Max(state.MaxOffset, maxOffset);
        Offsets.insert(&state);
        AddStatBytes(state);
        NextSplitKeyId = Max(NextSplitKeyId, splitKeyId + 1);
    }

    void TReplicationSourceState::PersistCreated(IReplicationSourceOffsetsDb& db) const {
        db.PersistSource(PathId, Id, Name);
        PersistSplitKeyCreated(db, OffsetBySplitKeyId.at(0));
    }

    void TReplicationSourceState::PersistSplitKeyCreated(IReplicationSourceOffsetsDb& db, const TOffsetState& state) const {
        db.PersistSplitKey(PathId, Id, state.SplitKeyId, state.SplitKey.GetBuffer(), state.MaxOffset);
    }

    void TReplicationSourceState::PersistSplitKeyOffset(IReplicationSourceOffsetsDb& db, const TOffsetState& state) const {
        db.PersistSplitKeyOffset(PathId, Id, state.SplitKeyId, state.MaxOffset);
    }

    TReplicationSourceState::TOffsetState* TReplicationSourceState::AddSplitKey(IReplicationSourceOffsetsDb& db, TSerializedCellVec key) {
        const ui64 splitKeyId = NextSplitKeyId++;
        auto& state = OffsetBySplitKeyId[splitKeyId];
        state.SplitKeyId = splitKeyId;
        state.SplitKey = std::move(key);
        auto res = Offsets.insert(&state);
        Y_ENSURE(res.second, "AddSplitKey used with a key that already exists");
        Y_ENSURE(res.first != Offsets.begin(), "AddSplitKey somehow added split key before the first key");
        auto& prev = **--res.first;
        state.MaxOffset = prev.MaxOffset;
        PersistSplitKeyCreated(db, state);
        AddStatBytes(state);
        return &state;
    }

    TReplicationSourceState::TOffsetState* TReplicationSourceState::AddSplitKey(IReplicationSourceOffsetsDb& db, TSerializedCellVec key, i64 offset) {
        const ui64 splitKeyId = NextSplitKeyId++;
        auto& state = OffsetBySplitKeyId[splitKeyId];
        state.SplitKeyId = splitKeyId;
        state.SplitKey = std::move(key);
        state.MaxOffset = offset;
        auto res = Offsets.insert(&state);
        Y_ENSURE(res.second, "AddSplitKey used with a key that already exists");
        Y_ENSURE(res.first != Offsets.begin(), "AddSplitKey somehow added split key before the first key");
        PersistSplitKeyCreated(db, state);
        AddStatBytes(state);
        return &state;
    }

    bool TReplicationSourceState::AdvanceMaxOffset(IReplicationSourceOffsetsDb& db, TConstArrayRef<TCell> key, i64 offset) {
        // Common case, there is a single offset for the whole shard range
        if (Y_LIKELY(Offsets.size() == 1)) {
            TOffsetState& state = OffsetBySplitKeyId.at(0);
            if (offset <= state.MaxOffset) {
                return false;
            }
            state.MaxOffset = offset;
            PersistSplitKeyOffset(db, state);
            return true;
        }

        // Find the range that contains the key
        auto stop = Offsets.upper_bound(key);
        Y_ENSURE(stop != Offsets.begin());
        auto start = std::prev(stop);

        if (offset <= (*start)->MaxOffset) {
            return false;
        }

        // Extend start to the left
        while (start != Offsets.begin()) {
            auto prev = std::prev(start);
            if (offset < (*prev)->MaxOffset) {
                break;
            }
            start = prev;
        }

        // Extend stop to the right
        while (stop != Offsets.end() && offset >= (*stop)->MaxOffset) {
            ++stop;
        }

        Y_DEBUG_ABORT_UNLESS(start != stop);

        // We update the first range
        if (offset > (*start)->MaxOffset) {
            (*start)->MaxOffset = offset;
            PersistSplitKeyOffset(db, **start);
        }

        // We remove all other splits that are submerged by the new offset
        for (auto it = ++start; it != stop;) {
            const ui64 splitKeyId = (*it)->SplitKeyId;
            db.PersistSplitKeyDelete(PathId, Id, splitKeyId);
            RemoveStatBytes(**it);
            it = Offsets.erase(it);
            OffsetBySplitKeyId.erase(splitKeyId);
        }

        return true;
    }

    bool TReplicationSourceState::EnsureSplitKey(IReplicationSourceOffsetsDb& db, const TSerializedCellVec& key) {
        if (!FindSplitKey(key.GetCells())) {
            AddSplitKey(db, key);
            return true;
        }

        return false;
    }

    bool TReplicationSourceState::EnsureSplitKey(IReplicationSourceOffsetsDb& db, const TSerializedCellVec& key, i64 offset) {
        if (auto* state = FindSplitKey(key.GetCells())) {
            if (state->MaxOffset < offset) {
                state->MaxOffset = offset;
                PersistSplitKeyOffset(db, *state);
                return true;
            }
            return false;
        } else {
            AddSplitKey(db, key, offset);
            return true;
        }
    }

    void TReplicationSourceState::OptimizeSplitKeys(IReplicationSourceOffsetsDb& db) {
        auto last = Offsets.begin();
        for (auto it = std::next(last); it != Offsets.end();) {
            auto& lastState = **last;
            auto& state = **it;
            if (lastState.MaxOffset == state.MaxOffset) {
                // Remove redundant split keys
                const ui64 splitKeyId = state.SplitKeyId;
                db.PersistSplitKeyDelete(PathId, Id, splitKeyId);
                RemoveStatBytes(state);
                it = Offsets.erase(it);
                OffsetBySplitKeyId.erase(splitKeyId);
            } else {
                last = it;
                ++it;
            }
        }
    }

    TReplicatedTableState::TReplicatedTableState(
            const TPathId& pathId,
            const TVector<NScheme::TTypeInfo>& keyColumnTypes)
        : PathId(pathId)
        , KeyColumnTypes(keyColumnTypes)
    { }

    TReplicationSourceState* TReplicatedTableState::FindSource(ui64 sourceId) {
        auto it = SourceById.find(sourceId);
        if (it != SourceById.end()) {
            return &it->second;
        }

        return nullptr;
    }

    TReplicationSourceState& TReplicatedTableState::LoadSource(ui64 sourceId, const TString& sourceName) {
        auto it = SourceById.find(sourceId);
        if (it != SourceById.end()) {
            return it->second;
        }

        auto res = SourceById.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(sourceId),
            std::forward_as_tuple(PathId, sourceId, sourceName, &KeyColumnTypes));
        auto& source = res.first->second;
        Sources[sourceName] = &source;
        NextSourceId = Max(NextSourceId, sourceId + 1);
        return source;
    }

    TReplicationSourceState& TReplicatedTableState::EnsureSource(IReplicationSourceOffsetsDb& db, const TString& sourceName) {
        auto it = Sources.find(sourceName);
        if (it != Sources.end()) {
            return *it->second;
        }

        const ui64 sourceId = NextSourceId++;
        auto res = SourceById.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(sourceId),
            std::forward_as_tuple(PathId, sourceId, sourceName, &KeyColumnTypes));
        auto& source = res.first->second;
        Sources[sourceName] = &source;
        source.PersistCreated(db);
        return source;
    }

    void TReplicatedTableState::OptimizeSplitKeys(IReplicationSourceOffsetsDb& db) {
        for (auto& pr : SourceById) {
            pr.second.OptimizeSplitKeys(db);
        }
    }

    TReplicatedTableState* TDataShard::FindReplicatedTable(const TPathId& pathId) {
        auto it = ReplicatedTables.find(pathId);
        if (it != ReplicatedTables.end()) {
            return &it->second;
        }

        return nullptr;
    }

    TReplicatedTableState* TDataShard::EnsureReplicatedTable(const TPathId& pathId) {
        auto it = ReplicatedTables.find(pathId);
        if (it != ReplicatedTables.end()) {
            return &it->second;
        }

        if (pathId.OwnerId != GetPathOwnerId()) {
            return nullptr;
        }

        const auto& userTables = GetUserTables();
        auto itUserTables = userTables.find(pathId.LocalPathId);
        if (itUserTables == userTables.end()) {
            return nullptr;
        }

        const auto& table = *itUserTables->second;
        if (!table.IsIncrementalRestore()
            && !table.IsReplicated()) {
            return nullptr;
        }

        auto res = ReplicatedTables.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(pathId),
            std::forward_as_tuple(pathId, itUserTables->second->KeyColumnTypes));
        return &res.first->second;
    }

} // namespace NKikimr::NDataShard
