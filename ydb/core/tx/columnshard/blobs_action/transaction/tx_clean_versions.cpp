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

    Self->ExecuteSchemaVersionsCleanup(db, VersionsToRemove);

    return true;
}
void TTxSchemaVersionsCleanup::Complete(const TActorContext& /*ctx*/) {
    LOG_S_DEBUG("TTxSchemaVersionsCleanup::Complete, tablet id " <<  Self->TabletID());
    TMemoryProfileGuard mpg("TTxSchemaVersionsCleanup::Complete");
    Self->CompleteSchemaVersionsCleanup(VersionsToRemove);

//    Self->BackgroundController.FinishCleanupInsertTable();
//    Self->SetupCleanupInsertTable();
}

}
