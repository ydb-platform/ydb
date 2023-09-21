#include "tx_gc_indexed.h"

namespace NKikimr::NColumnShard {
bool TTxGarbageCollectionFinished::Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("tx", "TxGarbageCollectionFinished")("event", "execute");
    TBlobManagerDb blobManagerDb(txc.DB);
    Action->OnExecuteTxAfterCleaning(*Self, blobManagerDb);
    return true;
}
void TTxGarbageCollectionFinished::Complete(const TActorContext& /*ctx*/) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("tx", "TxGarbageCollectionFinished")("event", "complete");
    Action->OnCompleteTxAfterCleaning(*Self, Action);
}

}
