#include "tx_gc_indexed.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_BLOBS

namespace NKikimr::NColumnShard {
bool TTxGarbageCollectionFinished::Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TMemoryProfileGuard mpg("TTxGarbageCollectionFinished::Execute");
    YDB_LOG_DEBUG("",
        {"tx", "TxGarbageCollectionFinished"},
        {"event", "execute"});
    NOlap::TBlobManagerDb blobManagerDb(txc.DB);
    Action->OnExecuteTxAfterCleaning(*Self, blobManagerDb);
    return true;
}

void TTxGarbageCollectionFinished::Complete(const TActorContext& /*ctx*/) {
    TMemoryProfileGuard mpg("TTxGarbageCollectionFinished::Complete");
    YDB_LOG_DEBUG("",
        {"tx", "TxGarbageCollectionFinished"},
        {"event", "complete"});
    Action->OnCompleteTxAfterCleaning(*Self, Action);
}

bool TTxGarbageCollectionStart::Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TMemoryProfileGuard mpg("TTxGarbageCollectionStart::Execute");
    YDB_LOG_DEBUG("",
        {"tx", "TTxGarbageCollectionStart"},
        {"event", "execute"});
    NOlap::TBlobManagerDb blobManagerDb(txc.DB);
    Action->OnExecuteTxBeforeCleaning(*Self, blobManagerDb);
    return true;
}

void TTxGarbageCollectionStart::Complete(const TActorContext& /*ctx*/) {
    TMemoryProfileGuard mpg("TTxGarbageCollectionStart::Complete");
    YDB_LOG_DEBUG("",
        {"tx", "TTxGarbageCollectionStart"},
        {"event", "complete"});
    Action->OnCompleteTxBeforeCleaning(*Self, Action);
    Operator->StartGC(Action);
}

}   // namespace NKikimr::NColumnShard
