#include "datashard_txs.h"

namespace NKikimr {
namespace NDataShard {

using namespace NTabletFlatExecutor;


TDataShard::TTxMigrateSchemeShard::TTxMigrateSchemeShard(
    TDataShard* ds,
    TEvDataShard::TEvMigrateSchemeShardRequest::TPtr ev)
    : TBase(ds)
    , Ev(std::move(ev))
{ }

bool TDataShard::TTxMigrateSchemeShard::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    using TResponse = NKikimrTxDataShard::TEvMigrateSchemeShardResponse;

    txc.DB.NoMoreReadsForTx();

    const auto& record = Ev->Get()->Record;
    const auto& tabletId = record.GetTabletId();
    const auto& currentId = record.GetCurrentSchemeShardId();
    const auto& newId = record.GetNewSchemeShardId();

    Reply.Reset(new TEvDataShard::TEvMigrateSchemeShardResponse);
    Reply->Record.SetTabletId(Self->TabletID());

    if (tabletId != Self->TabletID()) {
        Reply->Record.SetStatus(TResponse::WrongRequest);
        return true;
    }

    if (newId == Self->GetCurrentSchemeShardId()) {
        Reply->Record.SetStatus(TResponse::Already);
        return true;
    }

    if (currentId != Self->GetCurrentSchemeShardId()) {
        Reply->Record.SetStatus(TResponse::WrongRequest);
        return true;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "migrate SS from  " << currentId << " to " << newId << " at datashard " << tabletId);

    Self->PersistCurrentSchemeShardId(newId, txc);
    Self->ResetLastSchemeOpSeqNo(txc);

    // Invalidate current subdomain path id, it's no longer correct after migration
    Self->SubDomainPathId.reset();
    Self->StopWatchingSubDomainPathId();

    // Stop any finder actor querying the old schemeshard, then start a new
    // actor querying the new schemeshard.
    Self->StopFindSubDomainPathId();
    Self->StartFindSubDomainPathId(/* delayFirstRequest */ false);

    Reply->Record.SetStatus(TResponse::Success);
    return true;
}

void TDataShard::TTxMigrateSchemeShard::Complete(const TActorContext& ctx) {
    Y_ENSURE(Reply);

    NTabletPipe::CloseAndForgetClient(Self->SelfId(), Self->DbStatsReportPipe);

    ctx.Send(Ev->Sender, Reply.Release(), 0, Ev->Cookie);
}


}   // namespace NDataShard
}   // namespace NKikimr
