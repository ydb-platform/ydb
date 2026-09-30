#include "schemeshard__operation.h"
#include "schemeshard_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TTxCancelTx: public ISubOperation {
    const char* Name() const override final { return "TTxCancelTx"; }
    const char* CurrentStateName() const override final { return "none"; }

    const TTxId TxId;
    const TTxId TargetTxId;
    const TActorId Sender;

public:
    TTxCancelTx(TEvSchemeShard::TEvCancelTx::TPtr ev)
        : TxId(ev->Get()->Record.GetTxId())
        , TargetTxId(ev->Get()->Record.GetTargetTxId())
        , Sender(ev->Sender)
    {
        const auto& record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.HasTxId());
        Y_ABORT_UNLESS(record.HasTargetTxId());
    }

    const TOperationId GetId() const override {
        return {TxId, 0};
    }

    const NKikimrSchemeOp::TModifyScheme& GetModifyScheme() const override {
        static const NKikimrSchemeOp::TModifyScheme fake;
        return fake;
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        YDB_LOG_DEBUG_CTX(context.Ctx, "Execute cancel tx",
            {"txId", TxId},
            {"targetTxId", TargetTxId},
        );

        auto proposeResult = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(TxId), context.SS->TabletID());
        auto result = MakeHolder<TEvSchemeShard::TEvCancelTxResult>(ui64(TargetTxId), ui64(TxId));

        const TOperationId TargetSubOperationId(TargetTxId, 0);

        auto found = context.SS->FindTx(TargetSubOperationId);
        if (!found) {
            result->Record.SetStatus(NKikimrScheme::StatusTxIdNotExists);
            result->Record.SetResult("Transaction not found");
            context.OnComplete.Send(Sender, std::move(result), ui64(TxId));
            return proposeResult;
        }

        TTxState& txState = *found;
        if (txState.TxType != TTxState::TxBackup && txState.TxType != TTxState::TxRestore) {
            result->Record.SetStatus(NKikimrScheme::StatusTxIsNotCancellable);
            result->Record.SetResult("Transaction is not cancellable");
            context.OnComplete.Send(Sender, std::move(result), ui64(TxId));
            return proposeResult;
        }

        if (txState.State == TTxState::Aborting) {
            result->Record.SetStatus(NKikimrScheme::StatusAccepted);
            result->Record.SetResult("Tx is cancelling at SchemeShard already");
            context.OnComplete.Send(Sender, std::move(result), ui64(TxId));
            return proposeResult;
        }

        txState.Cancel = true;

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->PersistCancelTx(db, TargetSubOperationId, txState);

        result->Record.SetStatus(NKikimrScheme::StatusAccepted);
        result->Record.SetResult("Cancelled at SchemeShard");
        context.OnComplete.Send(Sender, std::move(result), ui64(TxId));

        context.OnComplete.ActivateTx(TargetSubOperationId);
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

#undef YDB_LOG_THIS_FILE_COMPONENT
