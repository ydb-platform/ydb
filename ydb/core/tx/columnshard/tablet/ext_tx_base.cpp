#include "ext_tx_base.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/library/actors/struct_log/log_stack.h>

namespace NKikimr::NColumnShard {

bool TExtendedTransactionBase::Execute(NTabletFlatExecutor::TTransactionContext& txc, const NActors::TActorContext& ctx) {
    YDB_LOG_CREATE_CONTEXT(
        {"tabletId", Self->TabletID()},
        {"localTxNo", TabletTxNo},
        {"method", "execute"},
        {"txInfo", TxInfo});
    return DoExecute(txc, ctx);
}

void TExtendedTransactionBase::Complete(const NActors::TActorContext& ctx) {
    YDB_LOG_CREATE_CONTEXT(
        {"tabletId", Self->TabletID()},
        {"localTxNo", TabletTxNo},
        {"method", "complete"},
        {"txInfo", TxInfo});
    DoComplete(ctx);
    NYDBTest::TControllers::GetColumnShardController()->OnAfterLocalTxCommitted(ctx, *Self, TxInfo);
}

TExtendedTransactionBase::TExtendedTransactionBase(TColumnShard* self, const TString& txInfo)
    : TBase(self)
    , TxInfo(txInfo)
    , TabletTxNo(++Self->TabletTxCounter)
{
    AFL_VERIFY(!TxInfo.empty());
}

}   //namespace NKikimr::NColumnShard
