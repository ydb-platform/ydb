#include "ext_tx_base.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>


namespace NKikimr::NColumnShard {

bool TExtendedTransactionBase::Execute(NTabletFlatExecutor::TTransactionContext& txc, const NActors::TActorContext& ctx) {
    NActors::TLogContextGuard logGuard = NActors::TLogContextBuilder::Build()("tablet_id", Self->TabletID())("local_tx_no", TabletTxNo)("method", "execute")("tx_info", TxInfo);
    return DoExecute(txc, ctx);
}
void TExtendedTransactionBase::Complete(const NActors::TActorContext& ctx) {
    NActors::TLogContextGuard logGuard = NActors::TLogContextBuilder::Build()("tablet_id", Self->TabletID())("local_tx_no", TabletTxNo)("method", "complete")("tx_info", TxInfo);
    DoComplete(ctx);
    NYDBTest::TControllers::GetColumnShardController()->OnAfterLocalTxCommitted(ctx, *Self, TxInfo);
}

TExtendedTransactionBase::TExtendedTransactionBase(TColumnShard* self, const TString& txInfo)
    : TBase(self)
    , TxInfo(txInfo)
    , TabletTxNo(++Self->TabletTxCounter)
{

}

} //namespace NKikimr::NColumnShard
