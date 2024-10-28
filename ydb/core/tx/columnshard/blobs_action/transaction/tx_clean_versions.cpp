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
            // Now we need to clear diff in the next version, since base for that diff is just erased
            auto rowset = table.GreaterOrEqual(key.GetId(), key.GetPlanStep(), key.GetTxId()).Select();
            AFL_VERIFY(rowset.IsReady());
            if (!rowset.EndOfSet()) {
                const ui32 id = rowset.GetValue<Schema::SchemaPresetVersionInfo::Id>();
                const ui64 step = rowset.GetValue<Schema::SchemaPresetVersionInfo::SinceStep>();
                const ui64 txId = rowset.GetValue<Schema::SchemaPresetVersionInfo::SinceTxId>();
                NKikimrTxColumnShard::TSchemaPresetVersionInfo info;
                Y_ABORT_UNLESS(info.ParseFromString(rowset.GetValue<Schema::SchemaPresetVersionInfo::InfoProto>()));
                info.ClearDiff();
                TString serialized;
                Y_ABORT_UNLESS(info.SerializeToString(&serialized));
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "Removing diff in version from db")("vesion", info.GetSchema().GetVersion())("tablet_id", Self->TabletID());
                table.Key(id, step, txId).Update(NIceDb::TUpdate<Schema::SchemaPresetVersionInfo::InfoProto>(serialized));
            }
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
