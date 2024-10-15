#include "tx_clean_versions.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>
//#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>

namespace NKikimr::NColumnShard {

bool TTxSchemaVersionsCleanup::Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) {
    LOG_S_DEBUG("TTxSchemaVersionsCleanup::Execute, tablet id " <<  Self->TabletID());
    TMemoryProfileGuard mpg("TTxSchemaVersionsCleanup::Execute");
    TBlobGroupSelector dsGroupSelector(Self->Info());
    NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);
    NIceDb::TNiceDb db(txc.DB);

    auto table = db.Table<NKikimr::NColumnShard::Schema::SchemaPresetVersionInfo>();
    ui64 lastVersion = Self->TablesManager.MutablePrimaryIndex().LastSchemaVersion();
    Self->VersionCounters->EnumerateVersionsToErase([&](ui64 version, auto& key) {
        if (version != lastVersion) {
            LOG_S_DEBUG("Removing schema version from db " << version << " tablet id " << Self->TabletID());
            VersionsToRemove.insert(version);
            table.Key(key.Id, key.PlanStep, key.TxId).Delete();
        }
    });

    return true;
}
void TTxSchemaVersionsCleanup::Complete(const TActorContext& /*ctx*/) {
    LOG_S_DEBUG("TTxSchemaVersionsCleanup::Complete, tablet id " <<  Self->TabletID());
    TMemoryProfileGuard mpg("TTxSchemaVersionsCleanup::Complete");

    for (ui64 version: VersionsToRemove) {
        LOG_S_DEBUG("Removing schema version from memory " << version << " tablet id " << Self->TabletID());
        Self->TablesManager.MutablePrimaryIndex().RemoveSchemaVersion(version);
        Self->VersionCounters->DeleteErasedVersion(version);
    }
    VersionsToRemove.clear();

    Self->BackgroundController.FinishActiveCleanupUnusedSchemaVersions();
}

}
