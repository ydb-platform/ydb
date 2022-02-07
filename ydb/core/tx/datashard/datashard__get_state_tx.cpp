#include "datashard_txs.h"

namespace NKikimr {
namespace NDataShard {

using namespace NTabletFlatExecutor;

TDataShard::TTxGetShardState::TTxGetShardState(TDataShard* ds, TEvDataShard::TEvGetShardState::TPtr ev)
    : TBase(ds)
    , Ev(ev)
{}

bool TDataShard::TTxGetShardState::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    Y_UNUSED(txc);
    Y_UNUSED(ctx);

    Result = MakeHolder<TEvDataShard::TEvGetShardStateResult>(Self->TabletID(), Self->State);
    if (Self->Pipeline.HasDrop())
        Result->Record.SetDropTxId(Self->Pipeline.CurrentSchemaTxId());
    return true;
}

void TDataShard::TTxGetShardState::Complete(const TActorContext& ctx) {
    ctx.Send(Ev->Get()->GetSource(), Result.Release());
}

}}
