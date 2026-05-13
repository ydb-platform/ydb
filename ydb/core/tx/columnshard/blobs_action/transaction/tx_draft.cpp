#include "tx_draft.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_WRITE

namespace NKikimr::NColumnShard {

bool TTxWriteDraft::Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TMemoryProfileGuard mpg("TTxWriteDraft::Execute");
    NOlap::TBlobManagerDb blobManagerDb(txc.DB);
    for (auto&& action : WriteController->GetBlobActions()) {
        action.second->OnExecuteTxBeforeWrite(*Self, blobManagerDb);
    }
    return true;
}

void TTxWriteDraft::Complete(const TActorContext& ctx) {
    TMemoryProfileGuard mpg("TTxWriteDraft::Complete");
    YDB_LOG_DEBUG("",
        {"event", "draft_completed"});
    Completed = true;
    for (auto&& action : WriteController->GetBlobActions()) {
        action.second->OnCompleteTxBeforeWrite(*Self);
    }
    ctx.Register(NColumnShard::CreateWriteActor(Self->TabletID(), WriteController, TInstant::Max()));
}

}   // namespace NKikimr::NColumnShard
