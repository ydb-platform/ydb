#include "tx_gc_insert_table.h"

namespace NKikimr::NColumnShard {

bool TTxInsertTableCleanup::Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TBlobGroupSelector dsGroupSelector(Self->Info());
    NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);
    NIceDb::TNiceDb db(txc.DB);

    Self->TryAbortWrites(db, dbTable, std::move(WriteIdsToAbort));

    TBlobManagerDb blobManagerDb(txc.DB);
    auto allAborted = Self->InsertTable->GetAborted();
    auto storage = Self->StoragesManager->GetInsertOperator();
    BlobsAction = storage->StartDeclareRemovingAction();
    for (auto& [abortedWriteId, abortedData] : allAborted) {
        Self->InsertTable->EraseAborted(dbTable, abortedData);
        Y_VERIFY(abortedData.GetBlobRange().IsFullBlob());
        BlobsAction->DeclareRemove(abortedData.GetBlobRange().GetBlobId());
    }
    BlobsAction->OnExecuteTxAfterRemoving(*Self, blobManagerDb, true);
    return true;
}
void TTxInsertTableCleanup::Complete(const TActorContext& /*ctx*/) {
    Y_VERIFY(BlobsAction);
    BlobsAction->OnCompleteTxAfterRemoving(*Self);
    Self->EnqueueBackgroundActivities();
}

}
