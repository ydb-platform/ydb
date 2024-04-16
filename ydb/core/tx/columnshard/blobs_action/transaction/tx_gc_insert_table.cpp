#include "tx_gc_insert_table.h"
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>

namespace NKikimr::NColumnShard {

bool TTxInsertTableCleanup::Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TBlobGroupSelector dsGroupSelector(Self->Info());
    NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);
    NIceDb::TNiceDb db(txc.DB);

    Self->TryAbortWrites(db, dbTable, std::move(WriteIdsToAbort));

    NOlap::TBlobManagerDb blobManagerDb(txc.DB);
    auto allAborted = Self->InsertTable->GetAborted();
    auto storage = Self->StoragesManager->GetInsertOperator();
    BlobsAction = storage->StartDeclareRemovingAction("TX_CLEANUP");
    for (auto& [abortedWriteId, abortedData] : allAborted) {
        Self->InsertTable->EraseAborted(dbTable, abortedData, BlobsAction);
    }
    BlobsAction->OnExecuteTxAfterRemoving(*Self, blobManagerDb, true);
    return true;
}
void TTxInsertTableCleanup::Complete(const TActorContext& /*ctx*/) {
    Y_ABORT_UNLESS(BlobsAction);
    BlobsAction->OnCompleteTxAfterRemoving(*Self, true);
    Self->EnqueueBackgroundActivities();
}

}
