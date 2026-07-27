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
            ui64 txId, THolder<NTabletFlatExecutor::TDirectPartWriter> writer, ui32 step)
        : TxId(txId)
        , Success(true)
        , Step(step)
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

void TDataShard::Handle(TEvDataShard::TEvS3DirectWriteBegin::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();

    auto reply = [&](THolder<TEvDataShard::TEvS3DirectWriteBeginResult> result) {
        ctx.Send(ev->Sender, result.Release(), 0, ev->Cookie);
    };

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
    reply(MakeHolder<TEvDataShard::TEvS3DirectWriteBeginResult>(msg->TxId, std::move(writer), step));
}

void TDataShard::Handle(TEvDataShard::TEvS3DirectWriteFinish::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxS3DirectWriteFinish(this, ev), ctx);
}

void TDataShard::Handle(TEvDataShard::TEvS3DirectWriteAbort::TPtr& ev, const TActorContext&) {
    Executor()->ReleaseWritePart(ev->Get()->Step);
}

TDataShard::TTxS3DirectWriteFinish::TTxS3DirectWriteFinish(TDataShard* ds, TEvDataShard::TEvS3DirectWriteFinish::TPtr& ev)
    : TBase(ds)
    , Ev(std::move(ev))
{
}

bool TDataShard::TTxS3DirectWriteFinish::Execute(TTransactionContext& txc, const TActorContext&) {
    auto* msg = Ev->Get();

    const TTableId fullTableId(Self->GetPathOwnerId(), msg->TableId);
    const ui64 localTableId = Self->GetLocalTableId(fullTableId);
    if (localTableId == 0) {
        Success = false;
        Error = TStringBuilder() << "Unknown table id " << msg->TableId;
        // Release the reserved barrier so its blobs get collected.
        if (msg->Result) {
            Self->Executor()->ReleaseWritePart(msg->Result->Step);
        }
        return true;
    }

    Y_ENSURE(msg->Result, "TxS3DirectWriteFinish without a built part");

    // Attach the part as a bottom layer and persist the final restore progress
    // atomically, so a restart after this commit resumes to completion (via the
    // stored ProcessedBytes == ContentLength) instead of writing a second part.
    txc.Env.AttachPart(localTableId, std::move(msg->Result));

    NIceDb::TNiceDb db(txc.DB);
    Self->S3Downloads.Store(db, msg->TxId, msg->Info);

    Success = true;
    return true;
}

void TDataShard::TTxS3DirectWriteFinish::Complete(const TActorContext& ctx) {
    auto* msg = Ev->Get();
    ctx.Send(Ev->Sender, new TEvDataShard::TEvS3DirectWriteFinishResult(msg->TxId, Success, Error), 0, Ev->Cookie);
}

} // namespace NDataShard
} // namespace NKikimr
