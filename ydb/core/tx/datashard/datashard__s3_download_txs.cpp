#include "datashard_txs.h"

namespace NKikimr {
namespace NDataShard {

using namespace NTabletFlatExecutor;

/// Get
TDataShard::TTxGetS3DownloadInfo::TTxGetS3DownloadInfo(
    TDataShard* ds,
    TEvDataShard::TEvGetS3DownloadInfo::TPtr ev)
    : TBase(ds)
    , Ev(std::move(ev))
{ }

bool TDataShard::TTxGetS3DownloadInfo::Execute(TTransactionContext& txc, const TActorContext&) {
    txc.DB.NoMoreReadsForTx();

    const auto* info = Self->S3Downloads.Find(Ev->Get()->TxId);
    if (!info) {
        Reply.Reset(new TEvDataShard::TEvS3DownloadInfo);
    } else {
        Reply.Reset(new TEvDataShard::TEvS3DownloadInfo(*info));
    }

    return true;
}

void TDataShard::TTxGetS3DownloadInfo::Complete(const TActorContext& ctx) {
    Y_ENSURE(Reply);
    ctx.Send(Ev->Sender, Reply.Release(), 0, Ev->Cookie);
}

/// Store
TDataShard::TTxStoreS3DownloadInfo::TTxStoreS3DownloadInfo(
    TDataShard* ds,
    TEvDataShard::TEvStoreS3DownloadInfo::TPtr ev)
    : TBase(ds)
    , Ev(std::move(ev))
{ }

bool TDataShard::TTxStoreS3DownloadInfo::Execute(TTransactionContext& txc, const TActorContext&) {
    txc.DB.NoMoreReadsForTx();
    NIceDb::TNiceDb db(txc.DB);

    const auto& info = Self->S3Downloads.Store(db, Ev->Get()->TxId, Ev->Get()->Info);
    Reply.Reset(new TEvDataShard::TEvS3DownloadInfo(info));

    return true;
}

void TDataShard::TTxStoreS3DownloadInfo::Complete(const TActorContext& ctx) {
    Y_ENSURE(Reply);
    ctx.Send(Ev->Sender, Reply.Release(), 0, Ev->Cookie);
}

}   // namespace NDataShard
}   // namespace NKikimr
