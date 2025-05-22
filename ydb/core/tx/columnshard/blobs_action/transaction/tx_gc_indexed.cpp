#include "tx_gc_indexed.h"

namespace NKikimr::NColumnShard {
bool TTxGarbageCollectionFinished::Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TMemoryProfileGuard mpg("TTxGarbageCollectionFinished::Execute");
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_BLOBS)("tx", "TxGarbageCollectionFinished")("event", "execute");
    NOlap::TBlobManagerDb blobManagerDb(txc.DB);
    Action->OnExecuteTxAfterCleaning(*Self, blobManagerDb);
    return true;
}
void TTxGarbageCollectionFinished::Complete(const TActorContext& /*ctx*/) {
    TMemoryProfileGuard mpg("TTxGarbageCollectionFinished::Complete");
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_BLOBS)("tx", "TxGarbageCollectionFinished")("event", "complete");
    Action->OnCompleteTxAfterCleaning(*Self, Action);
}

bool TTxGarbageCollectionStart::Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TMemoryProfileGuard mpg("TTxGarbageCollectionStart::Execute");
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_BLOBS)("tx", "TTxGarbageCollectionStart")("event", "execute");
    NOlap::TBlobManagerDb blobManagerDb(txc.DB);
    Action->OnExecuteTxBeforeCleaning(*Self, blobManagerDb);
    return true;
}
void TTxGarbageCollectionStart::Complete(const TActorContext& /*ctx*/) {
    TMemoryProfileGuard mpg("TTxGarbageCollectionStart::Complete");
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_BLOBS)("tx", "TTxGarbageCollectionStart")("event", "complete");
    Action->OnCompleteTxBeforeCleaning(*Self, Action);
    Operator->StartGC(Action);
}

}
