#include "datashard_txs.h"
#include "datashard_active_transaction.h"

#include <ydb/core/backup/common/metadata.h>

#include <util/string/builder.h>

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

TDataShard::TTxStoreBackupChecksum::TTxStoreBackupChecksum(
    TDataShard* ds, TEvDataShard::TEvStoreBackupChecksumRequest::TPtr ev)
    : TBase(ds)
    , Ev(std::move(ev))
{}

bool TDataShard::TTxStoreBackupChecksum::Execute(TTransactionContext& txc, const TActorContext&) {
    const auto& msg = *Ev->Get();

    // Find the active backup operation
    auto op = Self->Pipeline.FindOp(msg.TxId);
    auto* tx = dynamic_cast<TActiveTransaction*>(op.Get());
    if (!tx || !op->IsSchemeTx()) {
        // A checksum request may outlive its cancelled backup
        return true;
    }

    // Read the persisted transaction body
    NIceDb::TNiceDb db(txc.DB);
    auto row = db.Table<Schema::TxDetails>().Key(msg.TxId, Self->TabletID()).Select<Schema::TxDetails::Body>();
    if (!row.IsReady()) {
        return false;
    }
    if (row.EndOfSet()) {
        return true;
    }

    // Recover the backup task from the stored protobuf
    NKikimrTxDataShard::TFlatSchemeTransaction schemeTx;
    Y_ENSURE(schemeTx.ParseFromString(row.GetValue<Schema::TxDetails::Body>()));
    Y_ENSURE(schemeTx.HasBackup() && schemeTx.GetBackup().HasTableChecksum());
    const auto& backup = schemeTx.GetBackup();
    if (backup.GetShardNum() != 0) {
        return true;
    }
    auto& checksum = *schemeTx.MutableBackup()->MutableTableChecksum();

    using TOutcome = TEvDataShard::TBackupChecksumOutcome;
    if (checksum.HasFailureReason()) {
        Reply.Reset(new TEvDataShard::TEvStoreBackupChecksumResponse(TOutcome{ .Error = checksum.GetFailureReason() }));
        return true;
    }
    if (checksum.GetRetentionUnsupported()) {
        Reply.Reset(new TEvDataShard::TEvStoreBackupChecksumResponse(
            TOutcome{ .Error = "table checksum calculation was skipped" }));
        return true;
    }
    if (checksum.HasCount() || checksum.HasErrorCode()) {
        TOutcome outcome;
        if (checksum.GetVersion() != NBackup::TTableChecksum::Version) {
            outcome.Error = TStringBuilder() << "unsupported stored table checksum version " << checksum.GetVersion();
        } else if (checksum.HasErrorCode()) {
            outcome.Error = TStringBuilder() << checksum.GetErrorCode() << ": " << checksum.GetErrorMessage();
        } else if (checksum.GetAlgorithm().empty() || !checksum.HasHash()) {
            outcome.Error = "stored table checksum lacks its algorithm or hash";
        } else {
            outcome.Checksum = NBackup::TTableChecksum{ .Count = checksum.GetCount(), .Hash = checksum.GetHash(),
                .Algorithm = checksum.GetAlgorithm() };
        }
        Reply.Reset(new TEvDataShard::TEvStoreBackupChecksumResponse(std::move(outcome)));
        return true;
    }

    auto outcome = msg.Outcome;
    if (!outcome && checksum.GetStarted()) {
        outcome = TOutcome{ .Error = "CHECKSUM_INTERRUPTED: table checksum query interrupted during backup" };
    }

    // Record the launch before giving the uploader permission to dispatch the query
    checksum.SetStarted(true);
    if (outcome) {
        if (!outcome->Error.empty()) {
            checksum.SetFailureReason(outcome->Error);
        } else {
            const auto& saved = *outcome->Checksum;
            checksum.SetVersion(NBackup::TTableChecksum::Version);
            checksum.SetAlgorithm(saved.Algorithm);
            checksum.SetCount(saved.Count);
            checksum.SetHash(saved.Hash);
        }
    }

    // Save the outcome in the backup transaction body before acknowledging it
    TString body;
    Y_ENSURE(schemeTx.SerializeToString(&body));
    txc.DB.NoMoreReadsForTx();
    Self->Pipeline.UpdateSchemeTxBody(msg.TxId, body, txc);
    if (!tx->GetTxBody().empty()) {
        tx->SetTxBody(body);
        Y_ENSURE(tx->BuildSchemeTx());
    }
    Reply.Reset(new TEvDataShard::TEvStoreBackupChecksumResponse(std::move(outcome)));
    return true;
}

void TDataShard::TTxStoreBackupChecksum::Complete(const TActorContext& ctx) {
    if (Reply) {
        ctx.Send(Ev->Sender, Reply.Release());
    }
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
    const auto& upload = Self->S3Uploads.Store(txc.DB, msg.TxId, msg.UploadId);
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
