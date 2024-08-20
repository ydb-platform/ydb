#include "datashard_impl.h"
#include "datashard_schema_snapshots.h"

#include <ydb/core/util/pb.h>

namespace NKikimr {
namespace NDataShard {

TSchemaSnapshot::TSchemaSnapshot(TUserTable::TCPtr schema, ui64 step, ui64 txId)
    : Schema(schema)
    , Step(step)
    , TxId(txId)
{
}

TSchemaSnapshotManager::TSchemaSnapshotManager(const TDataShard* self)
    : Self(self)
{
}

void TSchemaSnapshotManager::Reset() {
    Snapshots.clear();
    References.clear();
}

bool TSchemaSnapshotManager::Load(NIceDb::TNiceDb& db) {
    using Schema = TDataShard::Schema;

    auto rowset = db.Table<Schema::SchemaSnapshots>().Range().Select();
    if (!rowset.IsReady()) {
        return false;
    }

    while (!rowset.EndOfSet()) {
        const ui64 oid = rowset.GetValue<Schema::SchemaSnapshots::PathOwnerId>();
        const ui64 tid = rowset.GetValue<Schema::SchemaSnapshots::LocalPathId>();
        const ui64 version = rowset.GetValue<Schema::SchemaSnapshots::SchemaVersion>();
        const ui64 step = rowset.GetValue<Schema::SchemaSnapshots::Step>();
        const ui64 txId = rowset.GetValue<Schema::SchemaSnapshots::TxId>();
        const TString schema = rowset.GetValue<Schema::SchemaSnapshots::Schema>();

        NKikimrSchemeOp::TTableDescription desc;
        const bool ok = ParseFromStringNoSizeLimit(desc, schema);
        Y_ABORT_UNLESS(ok);

        const auto res = Snapshots.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(oid, tid, version),
            std::forward_as_tuple(new TUserTable(0, desc, 0), step, txId)
        );
        Y_VERIFY_S(res.second, "Duplicate schema snapshot: " << res.first->first);

        if (!rowset.Next()) {
            return false;
        }
    }

    return true;
}

bool TSchemaSnapshotManager::AddSnapshot(NTable::TDatabase& db, const TSchemaSnapshotKey& key, const TSchemaSnapshot& snapshot) {
    if (auto it = Snapshots.find(key); it != Snapshots.end()) {
        Y_VERIFY_DEBUG_S(false, "Duplicate schema snapshot: " << key);
        return false;
    }

    auto it = Self->GetUserTables().find(key.PathId);
    Y_VERIFY_S(it != Self->GetUserTables().end(), "Cannot find table: " << key.PathId);

    const auto res = Snapshots.emplace(key, snapshot);
    Y_VERIFY_S(res.second, "Duplicate schema snapshot: " << key);

    NIceDb::TNiceDb nicedb(db);
    PersistAddSnapshot(nicedb, key, snapshot);

    return true;
}

const TSchemaSnapshot* TSchemaSnapshotManager::FindSnapshot(const TSchemaSnapshotKey& key) const {
    return Snapshots.FindPtr(key);
}

void TSchemaSnapshotManager::RemoveShapshot(NTable::TDatabase& db, const TSchemaSnapshotKey& key) {
    auto it = Snapshots.find(key);
    if (it == Snapshots.end()) {
        return;
    }

    Snapshots.erase(it);

    NIceDb::TNiceDb nicedb(db);
    PersistRemoveSnapshot(nicedb, key);
}

void TSchemaSnapshotManager::RenameSnapshots(NTable::TDatabase& db,
        const TPathId& prevTableId, const TPathId& newTableId)
{
    Y_VERIFY_S(prevTableId < newTableId, "New table id should be greater than previous"
        << ": prev# " << prevTableId
        << ", new# " << newTableId);

    NIceDb::TNiceDb nicedb(db);
    for (auto it = Snapshots.lower_bound(TSchemaSnapshotKey(prevTableId, 1)); it != Snapshots.end();) {
        const auto& prevKey = it->first;
        const auto& snapshot = it->second;

        if (TPathId(prevKey.OwnerId, prevKey.PathId) != prevTableId) {
            break;
        }

        const TSchemaSnapshotKey newKey(newTableId, prevKey.Version);
        AddSnapshot(db, newKey, snapshot);
        PersistRemoveSnapshot(nicedb, prevKey);

        auto refIt = References.find(prevKey);
        if (refIt != References.end()) {
            References[newKey] = refIt->second;
            References.erase(refIt);
        }

        it = Snapshots.erase(it);
    }
}

const TSchemaSnapshotManager::TSnapshots& TSchemaSnapshotManager::GetSnapshots() const {
    return Snapshots;
}

bool TSchemaSnapshotManager::AcquireReference(const TSchemaSnapshotKey& key) {
    auto it = Snapshots.find(key);
    if (it == Snapshots.end()) {
        return false;
    }

    ++References[key];
    return true;
}

bool TSchemaSnapshotManager::ReleaseReference(const TSchemaSnapshotKey& key) {
    auto refIt = References.find(key);

    if (refIt == References.end() || refIt->second <= 0) {
        Y_DEBUG_ABORT_UNLESS(false, "ReleaseReference underflow, check acquire/release pairs");
        return false;
    }

    if (--refIt->second) {
        return false;
    }

    References.erase(refIt);

    auto it = Snapshots.find(key);
    if (it == Snapshots.end()) {
        Y_DEBUG_ABORT_UNLESS(false, "ReleaseReference on an already removed snapshot");
        return false;
    }

    return true;
}

bool TSchemaSnapshotManager::HasReference(const TSchemaSnapshotKey& key) const {
    auto refIt = References.find(key);
    if (refIt != References.end()) {
        return refIt->second;
    } else {
        return false;
    }
}

void TSchemaSnapshotManager::PersistAddSnapshot(NIceDb::TNiceDb& db, const TSchemaSnapshotKey& key, const TSchemaSnapshot& snapshot) {
    using Schema = TDataShard::Schema;
    db.Table<Schema::SchemaSnapshots>()
        .Key(key.OwnerId, key.PathId, key.Version)
        .Update(
            NIceDb::TUpdate<Schema::SchemaSnapshots::Step>(snapshot.Step),
            NIceDb::TUpdate<Schema::SchemaSnapshots::TxId>(snapshot.TxId),
            NIceDb::TUpdate<Schema::SchemaSnapshots::Schema>(snapshot.Schema->GetSchema())
        );
}

void TSchemaSnapshotManager::PersistRemoveSnapshot(NIceDb::TNiceDb& db, const TSchemaSnapshotKey& key) {
    using Schema = TDataShard::Schema;
    db.Table<Schema::SchemaSnapshots>()
        .Key(key.OwnerId, key.PathId, key.Version)
        .Delete();
}

} // NDataShard
} // NKikimr

Y_DECLARE_OUT_SPEC(, NKikimr::NDataShard::TSchemaSnapshotKey, stream, value) {
    stream << "{ table " << value.OwnerId << ":" << value.PathId << " version " << value.Version << " }";
}
