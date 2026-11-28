#include "datashard_locks_schema.h"
#include "datashard_locks_db.h"

namespace NKikimr::NDataShard {

bool UpdateLocksSchema(NTable::TDatabase& database) {
    using Schema = TDataShardLocksDb::TSchema;
    NIceDb::TNiceDb db(database);
    auto versionRow = db.Table<Schema::Sys>().Key(Schema::Sys_LocksFormatVersion).Select<Schema::Sys::Uint64>();
    if (!versionRow.IsReady()) {
        return false;
    }
    const auto version = versionRow.EndOfSet() ? ELocksFormatVersion::Legacy
        : ELocksFormatVersion(versionRow.GetValue<Schema::Sys::Uint64>());
    Y_ENSURE(version <= ELocksFormatVersion::Current, "Unsupported lock format version");

    // The replacement tables have different ids and names, so old redo and
    // compacted parts cannot be loaded into them. Old binaries may recreate
    // the legacy tables during a downgrade with the feature flags disabled.
    if (database.GetScheme().GetTableInfo(Schema::LegacyAncestorShardsLocksTableId)) {
        database.Alter().DropTable(Schema::LegacyAncestorShardsLocksTableId);
    }
    if (database.GetScheme().GetTableInfo(Schema::LegacyAncestorLockWriteSeqNumsTableId)) {
        database.Alter().DropTable(Schema::LegacyAncestorLockWriteSeqNumsTableId);
    }
    // Materialize must precede Truncate: adding the GC column after truncation
    // would try to alter a table already marked DataModified by this transaction.
    if (version == ELocksFormatVersion::Legacy && db.HaveTable<Schema::LockWriteSeqNums>()) {
        database.Truncate(Schema::LockWriteSeqNums::TableId);
    }
    db.Table<Schema::Sys>().Key(Schema::Sys_LocksFormatVersion).Update(
        NIceDb::TUpdate<Schema::Sys::Uint64>(ui64(ELocksFormatVersion::Current)));
    return true;
}

} // namespace NKikimr::NDataShard
