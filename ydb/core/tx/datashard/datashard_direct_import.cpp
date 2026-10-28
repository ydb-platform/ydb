#include "datashard_impl.h"
#include "datashard_txs.h"

#include <ydb/core/tablet_flat/flat_direct_part_writer.h>

namespace NKikimr {

// Out-of-line special members for the events that hold THolder<> of tablet_flat
// types forward-declared in datashard.h (the complete type is needed to
// construct or destroy the THolder members). TEvDataShard is a namespace in
// NKikimr (not in NDataShard).
namespace TEvDataShard {

    TEvS3DirectWriteBeginResult::TEvS3DirectWriteBeginResult(
            ui64 txId, THolder<NTabletFlatExecutor::TDirectPartWriter> writer,
            ui32 step, TRowVersion rowVersion)
        : TxId(txId)
        , Success(true)
        , Step(step)
        , RowVersion(rowVersion)
        , Writer(std::move(writer))
    {
    }

    TEvS3DirectWriteBeginResult::TEvS3DirectWriteBeginResult(ui64 txId, TString error)
        : TxId(txId)
        , Success(false)
        , Error(std::move(error))
        , Step(Max<ui32>())
    {
    }

    TEvS3DirectWriteBeginResult::~TEvS3DirectWriteBeginResult() = default;

    TEvS3DirectWriteFinish::TEvS3DirectWriteFinish(
            ui64 txId, ui64 tableId,
            THolder<NTabletFlatExecutor::TDirectPartResult> result,
            const NDataShard::TS3Download& info)
        : TxId(txId)
        , TableId(tableId)
        , Result(std::move(result))
        , Info(info)
    {
    }

    TEvS3DirectWriteFinish::~TEvS3DirectWriteFinish() = default;

} // namespace TEvDataShard

namespace NDataShard {

using namespace NTabletFlatExecutor;

bool TDataShard::IsActiveDirectImport(ui64 txId, ui64 tableId, const TActorId& sender) {
    auto op = Pipeline.FindOp(txId);
    if (!op || !op->IsSchemeTx() || !op->IsWaitingForAsyncJob()) {
        return false;
    }
    auto* tx = dynamic_cast<TActiveTransaction*>(op.Get());
    return tx && tx->GetAsyncJobActor() == sender
        && tx->GetSchemeTx().HasRestore()
        && tx->GetSchemeTx().GetRestore().GetTableId() == tableId;
}

TString TDataShard::CheckDirectImportSpace() {
    if (IsAnyChannelYellowStop()) {
        IncCounter(COUNTER_PREPARE_DISK_GROUP_OUT_OF_SPACE);
        return TStringBuilder() << "Cannot perform writes: out of disk space at tablet " << TabletID();
    }
    if (IsSubDomainOutOfSpace()) {
        IncCounter(COUNTER_PREPARE_DATABASE_DISK_SPACE_QUOTA_EXCEEDED);
        return "Cannot perform writes: database is out of disk space";
    }
    return {};
}

void TDataShard::CancelS3DirectWrite(ui64 txId) {
    auto it = DirectImports.find(txId);
    if (it != DirectImports.end()) {
        Executor()->ReleaseWritePart(it->second.Step);
        DirectImports.erase(it);
    }
}

void TDataShard::AbortDirectImportsForSpace() {
    if (DirectImports.empty()) {
        return;
    }
    const auto error = CheckDirectImportSpace();
    if (!error) {
        return;
    }
    for (const auto& [txId, session] : DirectImports) {
        Send(session.Actor, new TEvDataShard::TEvS3DirectWriteFinishResult(txId, false, error));
        Executor()->ReleaseWritePart(session.Step);
    }
    DirectImports.clear();
}

void TDataShard::Handle(TEvDataShard::TEvS3DirectWriteBegin::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();

    auto reply = [&](THolder<TEvDataShard::TEvS3DirectWriteBeginResult> result) {
        ctx.Send(ev->Sender, result.Release(), 0, ev->Cookie);
    };

    if (!IsActiveDirectImport(msg->TxId, msg->TableId, ev->Sender)) {
        return reply(MakeHolder<TEvDataShard::TEvS3DirectWriteBeginResult>(msg->TxId,
            "Restore is no longer active"));
    }
    if (DirectImports.contains(msg->TxId)) {
        return reply(MakeHolder<TEvDataShard::TEvS3DirectWriteBeginResult>(msg->TxId,
            "Direct write is already reserved"));
    }
    if (auto error = CheckDirectImportSpace()) {
        return reply(MakeHolder<TEvDataShard::TEvS3DirectWriteBeginResult>(msg->TxId, std::move(error)));
    }

    const TTableId fullTableId(GetPathOwnerId(), msg->TableId);
    const ui64 localTableId = GetLocalTableId(fullTableId);
    if (localTableId == 0) {
        return reply(MakeHolder<TEvDataShard::TEvS3DirectWriteBeginResult>(msg->TxId,
            TStringBuilder() << "Unknown table id " << msg->TableId));
    }

    // Reserve the part write on the executor (holds a GC barrier). Mirrors how
    // the direct-part unit test drives BeginWritePart directly from a handler.
    auto writer = Executor()->BeginWritePart(localTableId);
    if (!writer) {
        return reply(MakeHolder<TEvDataShard::TEvS3DirectWriteBeginResult>(msg->TxId,
            "BeginWritePart returned no writer"));
    }

    const ui32 step = writer->Step();
    // Readers at or above this planned version wait for the restore to complete.
    // Older snapshots must not see its rows, including after compaction/restart.
    const auto rowVersion = GetMvccVersion(Pipeline.FindOp(msg->TxId).Get());
    DirectImports.emplace(msg->TxId, TDirectImportSession{ev->Sender, msg->TableId, step});
    reply(MakeHolder<TEvDataShard::TEvS3DirectWriteBeginResult>(msg->TxId, std::move(writer), step, rowVersion));
}

void TDataShard::Handle(TEvDataShard::TEvS3DirectWriteFinish::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxS3DirectWriteFinish(this, ev), ctx);
}

void TDataShard::Handle(TEvDataShard::TEvS3DirectWriteAbort::TPtr& ev, const TActorContext&) {
    auto* msg = ev->Get();
    // Channel health outlives the reservation: cancellation may have released it
    // before the writer's final report arrives. Only reservation cleanup needs
    // a matching active owner and step.
    Executor()->OnYellowChannels(std::move(msg->YellowMoveChannels), std::move(msg->YellowStopChannels));
    auto it = DirectImports.find(msg->TxId);
    if (it != DirectImports.end() && it->second.Actor == ev->Sender && it->second.Step == msg->Step) {
        CancelS3DirectWrite(msg->TxId);
    }
}

TDataShard::TTxS3DirectWriteFinish::TTxS3DirectWriteFinish(TDataShard* ds, TEvDataShard::TEvS3DirectWriteFinish::TPtr& ev)
    : TBase(ds)
    , Ev(std::move(ev))
{
}

bool TDataShard::TTxS3DirectWriteFinish::Execute(TTransactionContext& txc, const TActorContext&) {
    auto* msg = Ev->Get();

    auto session = Self->DirectImports.find(msg->TxId);
    if (session == Self->DirectImports.end() || session->second.Actor != Ev->Sender
        || session->second.TableId != msg->TableId || !msg->Result
        || session->second.Step != msg->Result->Step)
    {
        Error = "Direct write reservation is no longer active";
        return true;
    }
    if (!Self->IsActiveDirectImport(msg->TxId, msg->TableId, Ev->Sender)) {
        Error = "Restore is no longer active";
        Self->CancelS3DirectWrite(msg->TxId);
        return true;
    }
    if (auto error = Self->CheckDirectImportSpace()) {
        Error = std::move(error);
        Self->CancelS3DirectWrite(msg->TxId);
        return true;
    }

    const TTableId fullTableId(Self->GetPathOwnerId(), msg->TableId);
    auto userTablePtr = Self->FindUserTable(fullTableId.PathId);
    if (!userTablePtr) {
        Success = false;
        Error = TStringBuilder() << "Unknown table id " << msg->TableId;
        Self->CancelS3DirectWrite(msg->TxId);
        return true;
    }

    const auto stats = txc.DB.GetCompactionStats(userTablePtr->LocalTid);
    if (stats.PartCount || stats.MemRowCount) {
        Error = "Cannot attach a direct import part to a non-empty table";
        Self->CancelS3DirectWrite(msg->TxId);
        return true;
    }

    // Attach the part as a bottom layer and persist the final restore progress
    // atomically, so a restart after this commit resumes to completion (via the
    // stored ProcessedBytes == ContentLength) instead of writing a second part.
    txc.Env.AttachPart(userTablePtr->LocalTid, std::move(msg->Result));
    // The executor now transfers the reservation into the attachment commit.
    Self->DirectImports.erase(session);
    Self->SetTableUpdateTime(fullTableId, TAppData::TimeProvider->Now());
    // Direct part import bypasses the memtable and compaction, so the shard
    // must invalidate cached stats explicitly.
    userTablePtr->StatsNeedUpdate = true;

    NIceDb::TNiceDb db(txc.DB);
    Self->S3Downloads.Store(db, msg->TxId, msg->Info);

    Success = true;
    return true;
}

void TDataShard::TTxS3DirectWriteFinish::Complete(const TActorContext& ctx) {
    auto* msg = Ev->Get();
    ctx.Send(Ev->Sender, new TEvDataShard::TEvS3DirectWriteFinishResult(msg->TxId, Success, Error), 0, Ev->Cookie);

    if (Success) {
        Self->UpdateTableStats(ctx);
    }
}

} // namespace NDataShard
} // namespace NKikimr
