#pragma once

#include "datashard_user_table.h"
#include "snapshot_key.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>

namespace NKikimr {
namespace NDataShard {

class TDataShard;
class TSchemaSnapshotManager;

struct TSchemaSnapshot {
    TUserTable::TCPtr Schema;
    ui64 Step = 0;
    ui64 TxId = 0;

    explicit TSchemaSnapshot(TUserTable::TCPtr schema, ui64 step, ui64 txId);
};

class TSchemaSnapshotManager {
public:
    explicit TSchemaSnapshotManager(const TDataShard* self);

    void Reset();
    bool Load(NIceDb::TNiceDb& db);

    bool AddSnapshot(NTable::TDatabase& db, const TSchemaSnapshotKey& key, const TSchemaSnapshot& snapshot);
    const TSchemaSnapshot* FindSnapshot(const TSchemaSnapshotKey& key) const;
    void RemoveShapshot(NIceDb::TNiceDb& db, const TSchemaSnapshotKey& key);
    void RenameSnapshots(NTable::TDatabase& db, const TPathId& prevTableId, const TPathId& newTableId);

    bool AcquireReference(const TSchemaSnapshotKey& key);
    bool ReleaseReference(const TSchemaSnapshotKey& key);

private:
    void PersistAddSnapshot(NIceDb::TNiceDb& db, const TSchemaSnapshotKey& key, const TSchemaSnapshot& snapshot);
    void PersistRemoveSnapshot(NIceDb::TNiceDb& db, const TSchemaSnapshotKey& key);

private:
    const TDataShard* Self;
    TMap<TSchemaSnapshotKey, TSchemaSnapshot, TLess<void>> Snapshots;
    THashMap<TSchemaSnapshotKey, size_t> References;

}; // TSchemaSnapshotManager

} // NDataShard
} // NKikimr
