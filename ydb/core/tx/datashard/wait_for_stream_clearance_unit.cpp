#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TWaitForStreamClearanceUnit : public TExecutionUnit {
public:
    TWaitForStreamClearanceUnit(TDataShard &dataShard,
                                TPipeline &pipeline);
    ~TWaitForStreamClearanceUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
    void ProcessEvent(TAutoPtr<NActors::IEventHandle> &ev,
                      TOperation::TPtr op,
                      const NActors::TActorContext &ctx);
    void Handle(TDataShard::TEvPrivate::TEvNodeDisconnected::TPtr &ev,
                TOperation::TPtr op,
                const TActorContext &ctx);
    void Handle(TEvTxProcessing::TEvStreamClearanceResponse::TPtr &ev,
                TOperation::TPtr op,
                const TActorContext &ctx);
    void Handle(TEvTxProcessing::TEvInterruptTransaction::TPtr &ev,
                TOperation::TPtr op,
                const TActorContext &ctx);
    void Handle(TEvents::TEvUndelivered::TPtr &ev,
                TOperation::TPtr op,
                const TActorContext &ctx);
    void Abort(const TString &err,
               TOperation::TPtr op,
               const TActorContext &ctx);
};

TWaitForStreamClearanceUnit::TWaitForStreamClearanceUnit(TDataShard &dataShard,
                                                         TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::WaitForStreamClearance, false, dataShard, pipeline)
{
}

TWaitForStreamClearanceUnit::~TWaitForStreamClearanceUnit()
{
}

bool TWaitForStreamClearanceUnit::IsReadyToExecute(TOperation::TPtr op) const
{
    // Pass aborted operations
    if (op->Result() || op->HasResultSentFlag() || op->IsImmediate() && WillRejectDataTx(op))
        return true;

    if (!op->IsWaitingForStreamClearance())
        return true;

    if (op->HasPendingInputEvents())
        return true;

    return false;
}

EExecutionStatus TWaitForStreamClearanceUnit::Execute(TOperation::TPtr op,
                                                      TTransactionContext &,
                                                      const TActorContext &ctx)
{
    // Pass aborted operations
    if (op->Result() || op->HasResultSentFlag() || op->IsImmediate() && CheckRejectDataTx(op, ctx)) {
        op->ResetWaitingForStreamClearanceFlag();
        op->ResetProcessDisconnectsFlag();
        return EExecutionStatus::Executed;
    }

    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

    if (!op->IsWaitingForStreamClearance()) {
        auto tid = tx->GetDataTx()->GetReadTableTransaction().GetTableId().GetTableId();
        auto &info = *DataShard.GetUserTables().at(tid);

        TAutoPtr<TEvTxProcessing::TEvStreamClearanceRequest> request
            = new TEvTxProcessing::TEvStreamClearanceRequest;
        request->Record.SetTxId(op->GetTxId());
        request->Record.SetShardId(DataShard.TabletID());
        info.Range.Serialize(*request->Record.MutableKeyRange());
        ctx.Send(tx->GetStreamSink(), request.Release(),
                 IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
                 op->GetTxId());

        op->SetWaitingForStreamClearanceFlag();
        op->SetProcessDisconnectsFlag();

        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Requested stream clearance from " << tx->GetStreamSink()
                    << " for " << *op << " at " << DataShard.TabletID());
    }

    while (op->HasPendingInputEvents()) {
        ProcessEvent(op->InputEvents().front(), op, ctx);
        op->InputEvents().pop();
    }

    if (op->IsWaitingForStreamClearance())
        return EExecutionStatus::Continue;

    op->ResetProcessDisconnectsFlag();

    return EExecutionStatus::Executed;
}

void TWaitForStreamClearanceUnit::ProcessEvent(TAutoPtr<NActors::IEventHandle> &ev,
                                               TOperation::TPtr op,
                                               const NActors::TActorContext &ctx)
{
    switch (ev->GetTypeRewrite()) {
        OHFunc(TDataShard::TEvPrivate::TEvNodeDisconnected, Handle);
        OHFunc(TEvTxProcessing::TEvStreamClearanceResponse, Handle);
        OHFunc(TEvTxProcessing::TEvInterruptTransaction, Handle);
        OHFunc(TEvents::TEvUndelivered, Handle);
        IgnoreFunc(TEvTxProcessing::TEvStreamClearancePending);
    default:
        LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD,
                    "TWaitForStreamClearanceUnit::ProcessEvent unhandled event type: " << ev->GetTypeRewrite()
                    << " event: " << ev->ToString());
        Y_DEBUG_ABORT("unexpected event %" PRIu64, (ui64)ev->GetTypeRewrite());
    }
}

void TWaitForStreamClearanceUnit::Handle(TDataShard::TEvPrivate::TEvNodeDisconnected::TPtr &ev,
                                         TOperation::TPtr op,
                                         const TActorContext &ctx)
{
    if (op->IsWaitingForStreamClearance()) {
        TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        if (ev->Get()->NodeId == tx->GetStreamSink().NodeId()) {
            Abort(TStringBuilder() << "Disconnected from stream sink (node " << ev->Get()->NodeId
                  << ") while waiting for stream clearance for " << *op << " at "
                  << DataShard.TabletID(), op, ctx);
        }
    }
}

void TWaitForStreamClearanceUnit::Handle(TEvTxProcessing::TEvStreamClearanceResponse::TPtr &ev,
                                         TOperation::TPtr op,
                                         const TActorContext &ctx)
{
    if (op->IsWaitingForStreamClearance()) {
        if (ev->Get()->Record.GetCleared()) {
            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                        "Got stream clearance for " << *op << " at " << DataShard.TabletID());
            op->ResetWaitingForStreamClearanceFlag();
        } else {
            Abort(TStringBuilder() << "Got stream clearance reject for " << *op
                  << " at " << DataShard.TabletID(),
                  op, ctx);
        }
    }
}

void TWaitForStreamClearanceUnit::Handle(TEvTxProcessing::TEvInterruptTransaction::TPtr &,
                                         TOperation::TPtr op,
                                         const TActorContext &ctx)
{
    if (op->IsWaitingForStreamClearance()) {
        Abort(TStringBuilder() << "Interrupted operation " << *op << " at "
              << DataShard.TabletID() << " while waiting for stream clearance",
              op, ctx);
    }
}

void TWaitForStreamClearanceUnit::Handle(TEvents::TEvUndelivered::TPtr &,
                                         TOperation::TPtr op,
                                         const TActorContext &ctx)
{
    if (op->IsWaitingForStreamClearance()) {
        Abort(TStringBuilder() << "Couldn't deliver stream clearance request for "
              << *op << " at " << DataShard.TabletID(),
              op, ctx);
    }
}

void TWaitForStreamClearanceUnit::Abort(const TString &err,
                                        TOperation::TPtr op,
                                        const TActorContext &ctx)
{
    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

    BuildResult(op)->AddError(NKikimrTxDataShard::TError::WRONG_SHARD_STATE, err);
    if (tx->GetScanSnapshotId()) {
        DataShard.DropScanSnapshot(tx->GetScanSnapshotId());
        tx->SetScanSnapshotId(0);
    }

    LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, err);

    op->ResetWaitingForStreamClearanceFlag();
}

void TWaitForStreamClearanceUnit::Complete(TOperation::TPtr,
                                           const TActorContext &)
{
}

THolder<TExecutionUnit> CreateWaitForStreamClearanceUnit(TDataShard &dataShard,
                                                         TPipeline &pipeline)
{
    return THolder(new TWaitForStreamClearanceUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
