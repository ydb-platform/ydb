#include "datashard_txs.h"

namespace NKikimr {
namespace NDataShard {

using namespace NTabletFlatExecutor;

TDataShard::TTxStoreScanState::TTxStoreScanState(
    TDataShard *ds,
    TEvPrivate::TEvPersistScanState::TPtr ev)
    : TBase(ds)
    , Ev(ev)
{
}

bool TDataShard::TTxStoreScanState::Execute(TTransactionContext &txc,
    const TActorContext &ctx)
{
    const TEvPrivate::TEvPersistScanState* event = Ev->Get();
    ui64 txId = event->TxId;
    auto op = Self->Pipeline.FindOp(txId);

    if (!op) {
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
                   "Cannot find op " << txId << " to persist scan state at " <<
                   Self->TabletID());
        return false;
    }

    auto schemaOp = Self->FindSchemaTx(txId);
    if (!schemaOp) {
        LOG_WARN_S(ctx, NKikimrServices::TX_DATASHARD,
                   "Cannot find schema op " << txId << " to update scan state at " <<
                   Self->TabletID());
        return false;
    }

    LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                "Persist scan progress for " << txId <<
                " key size " << event->LastKey.size() <<
                " status " << event->StatusCode << " at " <<
                Self->TabletID());

    auto binaryIssues = SerializeIssues(event->Issues);
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::ScanProgress>().Key(txId).Update(
        NIceDb::TUpdate<Schema::ScanProgress::LastKey>(event->LastKey),
        NIceDb::TUpdate<Schema::ScanProgress::LastStatus>(event->StatusCode),
        NIceDb::TUpdate<Schema::ScanProgress::LastIssues>(binaryIssues));

    schemaOp->ScanState.StatusCode = event->StatusCode;
    schemaOp->ScanState.Issues = event->Issues;
    schemaOp->ScanState.LastKey = event->LastKey;
    schemaOp->ScanState.Bytes = event->Bytes;

    return true;
}

void TDataShard::TTxStoreScanState::Complete(const TActorContext &ctx)
{
    ctx.Send(Ev->Sender, new TDataShard::TEvPrivate::TEvPersistScanStateAck());
}

} // namespace NDataShard
} // namespace NKikimr
