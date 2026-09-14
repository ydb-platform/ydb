#include "datashard_txs.h"

namespace NKikimr {
namespace NDataShard {

using namespace NTabletFlatExecutor;

/// Get
TDataShard::TTxGetS3Upload::TTxGetS3Upload(
    TDataShard* ds,
    TEvDataShard::TEvGetS3Upload::TPtr ev)
    : TBase(ds)
    , Ev(std::move(ev))
{ }

bool TDataShard::TTxGetS3Upload::Execute(TTransactionContext& txc, const TActorContext&) {
    txc.DB.NoMoreReadsForTx();

    if (const auto* upload = Self->S3Uploads.Find(Ev->Get()->TxId)) {
        Reply.Reset(new TEvDataShard::TEvS3Upload(*upload));
    } else {
        Reply.Reset(new TEvDataShard::TEvS3Upload);
    }

    return true;
}

void TDataShard::TTxGetS3Upload::Complete(const TActorContext& ctx) {
    Y_ENSURE(Reply);
    ctx.Send(Ev->Get()->ReplyTo, Reply.Release(), 0, Ev->Cookie);
}

/// Store id
TDataShard::TTxStoreS3UploadId::TTxStoreS3UploadId(
    TDataShard* ds,
    TEvDataShard::TEvStoreS3UploadId::TPtr ev)
    : TBase(ds)
    , Ev(std::move(ev))
{ }

bool TDataShard::TTxStoreS3UploadId::Execute(TTransactionContext& txc, const TActorContext&) {
    txc.DB.NoMoreReadsForTx();

    const auto& msg = *Ev->Get();
    const auto& upload = Self->S3Uploads.Add(txc.DB, msg.TxId, msg.UploadId);
    Reply.Reset(new TEvDataShard::TEvS3Upload(upload));

    return true;
}

void TDataShard::TTxStoreS3UploadId::Complete(const TActorContext& ctx) {
    Y_ENSURE(Reply);
    ctx.Send(Ev->Get()->ReplyTo, Reply.Release(), 0, Ev->Cookie);
}

/// Change status
TDataShard::TTxChangeS3UploadStatus::TTxChangeS3UploadStatus(
    TDataShard* ds,
    TEvDataShard::TEvChangeS3UploadStatus::TPtr ev)
    : TBase(ds)
    , Ev(std::move(ev))
{ }

bool TDataShard::TTxChangeS3UploadStatus::Execute(TTransactionContext& txc, const TActorContext&) {
    txc.DB.NoMoreReadsForTx();

    auto& msg = *Ev->Get();
    const auto& upload = Self->S3Uploads.ChangeStatus(txc.DB, msg.TxId, msg.Status, std::move(msg.Error), std::move(msg.Parts));
    Reply.Reset(new TEvDataShard::TEvS3Upload(upload));

    return true;
}

void TDataShard::TTxChangeS3UploadStatus::Complete(const TActorContext& ctx) {
    Y_ENSURE(Reply);
    ctx.Send(Ev->Get()->ReplyTo, Reply.Release(), 0, Ev->Cookie);
}

}   // namespace NDataShard
}   // namespace NKikimr
