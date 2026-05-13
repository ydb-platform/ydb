#include "schemeshard__operation.h"
#include "schemeshard_impl.h"

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TTxCancelTx: public ISubOperation {
    const TOperationId OperationId;
    const TOperationId TargetOperationId;
    const TActorId Sender;

public:
    TTxCancelTx(TEvSchemeShard::TEvCancelTx::TPtr ev)
        : OperationId(ev->Get()->Record.GetTxId(), 0)
        , TargetOperationId(ev->Get()->Record.GetTargetTxId(), 0)
        , Sender(ev->Sender)
    {
        const auto& record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.HasTxId());
        Y_ABORT_UNLESS(record.HasTargetTxId());
    }

    const TOperationId& GetOperationId() const override {
        return OperationId;
    }

    const TTxTransaction& GetTransaction() const override {
        static const TTxTransaction fake;
        return fake;
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Execute cancel tx"
            << ": opId# " << OperationId
            << ", target opId# " << TargetOperationId);

        auto proposeResult = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), context.SS->TabletID());
        auto result = MakeHolder<TEvSchemeShard::TEvCancelTxResult>(ui64(TargetOperationId.GetTxId()), ui64(OperationId.GetTxId()));

        auto found = context.SS->FindTx(TargetOperationId);
        if (!found) {
            result->Record.SetStatus(NKikimrScheme::StatusTxIdNotExists);
            result->Record.SetResult("Transaction not found");
            context.OnComplete.Send(Sender, std::move(result), ui64(OperationId.GetTxId()));
            return proposeResult;
        }

        TTxState& txState = *found;
        if (txState.TxType != TTxState::TxBackup && txState.TxType != TTxState::TxRestore) {
            result->Record.SetStatus(NKikimrScheme::StatusTxIsNotCancellable);
            result->Record.SetResult("Transaction is not cancellable");
            context.OnComplete.Send(Sender, std::move(result), ui64(OperationId.GetTxId()));
            return proposeResult;
        }

        if (txState.State == TTxState::Aborting) {
            result->Record.SetStatus(NKikimrScheme::StatusAccepted);
            result->Record.SetResult("Tx is cancelling at SchemeShard already");
            context.OnComplete.Send(Sender, std::move(result), ui64(OperationId.GetTxId()));
            return proposeResult;
        }

        txState.Cancel = true;

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->PersistCancelTx(db, TargetOperationId, txState);

        result->Record.SetStatus(NKikimrScheme::StatusAccepted);
        result->Record.SetResult("Cancelled at SchemeShard");
        context.OnComplete.Send(Sender, std::move(result), ui64(OperationId.GetTxId()));

        context.OnComplete.ActivateTx(TargetOperationId);
        return proposeResult;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TTxCancelTx");
    }

    bool ProgressState(TOperationContext&) override {
        Y_ABORT("no progress state for cancel tx");
    }

    void AbortUnsafe(TTxId, TOperationContext&) override {
        Y_ABORT("no AbortUnsafe for cancel tx");
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateTxCancelTx(TEvSchemeShard::TEvCancelTx::TPtr ev) {
    return new TTxCancelTx(ev);
}

}
