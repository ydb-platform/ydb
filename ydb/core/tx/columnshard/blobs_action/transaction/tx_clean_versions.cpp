#include "tx_clean_versions.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NColumnShard {

bool TTxSchemaVersionsCleanup::Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "TTxSchemaVersionsCleanup::Execute")("tablet_id", Self->TabletID());
    NIceDb::TNiceDb db(txc.DB);

    auto table = db.Table<NKikimr::NColumnShard::Schema::SchemaPresetVersionInfo>();
    for (const ui64 version: VersionsToRemove) {
        auto iter = Self->VersionCounters->GetVersionToKey().find(version);
        AFL_VERIFY(iter != Self->VersionCounters->GetVersionToKey().end());
        for (const NOlap::TVersionCounters::TSchemaKey& key: iter->second) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "Removing schema version from db")("vesion", version)("tablet_id", Self->TabletID());
            table.Key(key.GetId(), key.GetPlanStep(), key.GetTxId()).Delete();
        }
    }

    return true;
}
void TTxSchemaVersionsCleanup::Complete(const TActorContext& /*ctx*/) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "TTxSchemaVersionsCleanup::Complete")("tablet_id", Self->TabletID());

    for (const ui64 version: VersionsToRemove) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "Removing schema version from memory")("vesion", version)("tablet_id", Self->TabletID());
        Self->TablesManager.MutablePrimaryIndex().RemoveSchemaVersion(version);
    }

    Self->BackgroundController.FinishActiveCleanupUnusedSchemaVersions();
}

}
