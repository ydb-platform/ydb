#include "datashard_txs.h"

namespace NKikimr {
namespace NDataShard {

using namespace NTabletFlatExecutor;

namespace {

    bool CanDiscardSnapshot(ui32 state) {
        return state == TShardState::Ready || state == TShardState::SplitSrcWaitForNoTxInFlight ||
               state == TShardState::SplitSrcMakeSnapshot;
    }

} // namespace

////////////////////////////////////////////////////////////////////////////////

TDataShard::TTxRefreshVolatileSnapshot::TTxRefreshVolatileSnapshot(
        TDataShard* ds,
        TEvDataShard::TEvRefreshVolatileSnapshotRequest::TPtr ev)
    : TBase(ds)
    , Ev(std::move(ev))
{ }

bool TDataShard::TTxRefreshVolatileSnapshot::Execute(TTransactionContext&, const TActorContext& ctx) {
    using TResponse = NKikimrTxDataShard::TEvRefreshVolatileSnapshotResponse;

    const auto& record = Ev->Get()->Record;

    const auto key = Self->GetSnapshotManager().ExpandSnapshotKey(
            record.GetOwnerId(),
            record.GetPathId(),
            record.GetStep(),
            record.GetTxId());

    Reply.Reset(new TEvDataShard::TEvRefreshVolatileSnapshotResponse);
    Reply->Record.SetOrigin(Self->TabletID());
    Reply->Record.SetSupportsTableChecksumSnapshots(true);
    Reply->Record.SetOwnerId(key.OwnerId);
    Reply->Record.SetPathId(key.PathId);
    Reply->Record.SetStep(key.Step);
    Reply->Record.SetTxId(key.TxId);
    if (const auto table = Self->FindUserTable(TPathId(key.OwnerId, key.PathId)); table && table->IsBackup) {
        NKikimrSchemeOp::TTableDescription schema;
        table->GetSchema(schema);
        if (schema.HasTableChecksumSnapshot()) {
            *Reply->Record.MutableTableChecksumSnapshot() = schema.GetTableChecksumSnapshot();
        }
    }

    if (Self->State == TShardState::WaitScheme ||
        Self->State == TShardState::SplitDstReceivingSnapshot)
    {
        Reply->Record.SetStatus(TResponse::SNAPSHOT_NOT_READY);
        return true;
    }

    const bool refreshAllowed = (
        Self->State == TShardState::Ready ||
        Self->State == TShardState::SplitSrcWaitForNoTxInFlight ||
        Self->State == TShardState::SplitSrcMakeSnapshot);

    if (Self->SrcSplitDescription && !refreshAllowed) {
        Reply->Record.SetStatus(TResponse::SNAPSHOT_TRANSFERRED);
        for (const auto& range : Self->SrcSplitDescription->GetDestinationRanges()) {
            Reply->Record.AddTransferredToShards(range.GetTabletID());
        }
        return true;
    }

    if (!refreshAllowed) {
        Reply->Record.SetStatus(TResponse::WRONG_SHARD_STATE);
        return true;
    }

    if (!Self->GetSnapshotManager().FindAvailable(key)) {
        Reply->Record.SetStatus(TResponse::SNAPSHOT_NOT_FOUND);
        return true;
    }

    Self->GetSnapshotManager().RefreshSnapshotExpireTime(key, ctx.Now());

    Reply->Record.SetStatus(TResponse::REFRESHED);
    return true;
}

void TDataShard::TTxRefreshVolatileSnapshot::Complete(const TActorContext& ctx) {
    Y_ENSURE(Reply);

    ctx.Send(Ev->Sender, Reply.Release(), 0, Ev->Cookie);
}

////////////////////////////////////////////////////////////////////////////////

TDataShard::TTxDiscardVolatileSnapshot::TTxDiscardVolatileSnapshot(
        TDataShard* ds,
        TEvDataShard::TEvDiscardVolatileSnapshotRequest::TPtr ev)
    : TBase(ds)
    , Ev(std::move(ev))
{ }

bool TDataShard::TTxDiscardVolatileSnapshot::Execute(TTransactionContext& txc, const TActorContext&) {
    using TResponse = NKikimrTxDataShard::TEvDiscardVolatileSnapshotResponse;

    const auto& record = Ev->Get()->Record;

    const auto key = Self->GetSnapshotManager().ExpandSnapshotKey(
            record.GetOwnerId(),
            record.GetPathId(),
            record.GetStep(),
            record.GetTxId());

    Reply.Reset(new TEvDataShard::TEvDiscardVolatileSnapshotResponse);
    Reply->Record.SetOrigin(Self->TabletID());
    Reply->Record.SetOwnerId(key.OwnerId);
    Reply->Record.SetPathId(key.PathId);
    Reply->Record.SetStep(key.Step);
    Reply->Record.SetTxId(key.TxId);

    if (Self->State == TShardState::WaitScheme ||
        Self->State == TShardState::SplitDstReceivingSnapshot)
    {
        Reply->Record.SetStatus(TResponse::SNAPSHOT_NOT_READY);
        return true;
    }

    const bool discardAllowed = CanDiscardSnapshot(Self->State);

    if (Self->SrcSplitDescription && !discardAllowed) {
        Reply->Record.SetStatus(TResponse::SNAPSHOT_TRANSFERRED);
        for (const auto& range : Self->SrcSplitDescription->GetDestinationRanges()) {
            Reply->Record.AddTransferredToShards(range.GetTabletID());
        }
        return true;
    }

    if (!discardAllowed) {
        Reply->Record.SetStatus(TResponse::WRONG_SHARD_STATE);
        return true;
    }

    // TODO: we may want to discard snapshots that are not created yet
    // For example abort Create tx that is still waiting in plan queue
    if (!Self->GetSnapshotManager().FindAvailable(key)) {
        Reply->Record.SetStatus(TResponse::SNAPSHOT_NOT_FOUND);
        return true;
    }

    Self->GetSnapshotManager().RemoveSnapshot(txc.DB, key);

    Reply->Record.SetStatus(TResponse::DISCARDED);
    return true;
}

void TDataShard::TTxDiscardVolatileSnapshot::Complete(const TActorContext& ctx) {
    Y_ENSURE(Reply);

    ctx.Send(Ev->Sender, Reply.Release(), 0, Ev->Cookie);
}

TDataShard::TTxReleaseChecksumSnapshots::TTxReleaseChecksumSnapshots(TDataShard* ds, ui64 operationId)
    : TBase(ds)
    , OperationId(operationId)
{}

bool TDataShard::TTxReleaseChecksumSnapshots::Execute(TTransactionContext& txc, const TActorContext&) {
    if (CanDiscardSnapshot(Self->State)) {
        Self->GetSnapshotManager().ReleaseChecksumSnapshots(txc.DB, OperationId);
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TDataShard::TTxCleanupRemovedSnapshots::TTxCleanupRemovedSnapshots(TDataShard* ds)
    : TBase(ds)
{ }

bool TDataShard::TTxCleanupRemovedSnapshots::Execute(TTransactionContext& txc, const TActorContext&) {
    Self->GetSnapshotManager().CleanupRemovedSnapshots(txc.DB);
    return true;
}

void TDataShard::TTxCleanupRemovedSnapshots::Complete(const TActorContext& ctx) {
    Y_UNUSED(ctx);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NDataShard
}   // namespace NKikimr
