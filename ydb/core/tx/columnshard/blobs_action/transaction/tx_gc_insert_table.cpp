#include "tx_gc_insert_table.h"
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>

namespace NKikimr::NColumnShard {

bool TTxInsertTableCleanup::Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TMemoryProfileGuard mpg("TTxInsertTableCleanup::Execute");
    TBlobGroupSelector dsGroupSelector(Self->Info());
    NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);
    NIceDb::TNiceDb db(txc.DB);

    Self->TryAbortWrites(db, dbTable, std::move(WriteIdsToAbort));

    NOlap::TBlobManagerDb blobManagerDb(txc.DB);
    auto allAborted = Self->InsertTable->GetAborted();
    auto storage = Self->StoragesManager->GetInsertOperator();
    BlobsAction = storage->StartDeclareRemovingAction(NOlap::NBlobOperations::EConsumer::CLEANUP_INSERT_TABLE);
    for (auto& [abortedWriteId, abortedData] : allAborted) {
        Self->InsertTable->EraseAbortedOnExecute(dbTable, abortedData, BlobsAction);
    }
    BlobsAction->OnExecuteTxAfterRemoving(blobManagerDb, true);
    return true;
}
void TTxInsertTableCleanup::Complete(const TActorContext& /*ctx*/) {
    TMemoryProfileGuard mpg("TTxInsertTableCleanup::Complete");
    auto allAborted = Self->InsertTable->GetAborted();
    for (auto& [abortedWriteId, abortedData] : allAborted) {
        Self->InsertTable->EraseAbortedOnComplete(abortedData);
    }

    Y_ABORT_UNLESS(BlobsAction);
    BlobsAction->OnCompleteTxAfterRemoving(true);
    Self->BackgroundController.FinishCleanupInsertTable();
    Self->SetupCleanupInsertTable();
}

}
