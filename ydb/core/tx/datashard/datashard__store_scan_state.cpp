#include "datashard_txs.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_DATASHARD

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
        YDB_LOG_CTX_INFO(ctx, "Cannot find op to persist scan state at",
            {"txId", txId},
            {"TabletID", Self->TabletID()});
        return false;
    }

    auto schemaOp = Self->FindSchemaTx(txId);
    if (!schemaOp) {
        YDB_LOG_CTX_WARN(ctx, "Cannot find schema op to update scan state at",
            {"txId", txId},
            {"TabletID", Self->TabletID()});
        return false;
    }

    YDB_LOG_CTX_TRACE(ctx, "Persist scan progress for key size status at",
        {"txId", txId},
        {"size", event->LastKey.size()},
        {"StatusCode", event->StatusCode},
        {"TabletID", Self->TabletID()});

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
