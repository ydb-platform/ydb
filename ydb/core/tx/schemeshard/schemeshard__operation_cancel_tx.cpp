#include "schemeshard__operation.h"
#include "schemeshard_impl.h"

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TTxCancelTx: public ISubOperationBase {
private:
    TEvSchemeShard::TEvCancelTx::TPtr Event;

public:
    TTxCancelTx(TEvSchemeShard::TEvCancelTx::TPtr ev)
        : Event(ev)
    {}

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const auto& evRecord = Event->Get()->Record;

        Y_VERIFY(evRecord.HasTxId());
        Y_VERIFY(evRecord.HasTargetTxId());

        LOG_DEBUG(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                  "Execute cancel tx txid #%" PRIu64,
                  evRecord.GetTargetTxId());

        auto proposeResult = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, evRecord.GetTxId(), context.SS->TabletID());

        THolder<TEvSchemeShard::TEvCancelTxResult> result = MakeHolder<TEvSchemeShard::TEvCancelTxResult>(evRecord.GetTargetTxId(), evRecord.GetTxId());

        TOperationId targetOpId = TOperationId(evRecord.GetTargetTxId(), 0);

        auto found = context.SS->FindTx(targetOpId);
        if (!found) {
            result->Record.SetStatus(NKikimrScheme::StatusTxIdNotExists);
            result->Record.SetResult("Transaction not found");
            context.OnComplete.Send(Event->Sender, std::move(result), evRecord.GetTxId());
            return proposeResult;
        }

        TTxState& txState = *found;
        if (txState.TxType != TTxState::TxBackup && txState.TxType != TTxState::TxRestore) {
            result->Record.SetStatus(NKikimrScheme::StatusTxIsNotCancellable);
            result->Record.SetResult("Transaction is not cancellable");
            context.OnComplete.Send(Event->Sender, std::move(result), evRecord.GetTxId());
            return proposeResult;
        }

        if (txState.State == TTxState::Aborting) {
            result->Record.SetStatus(NKikimrScheme::StatusAccepted);
            result->Record.SetResult("Tx is cancelling at SchemeShard already");
            context.OnComplete.Send(Event->Sender, std::move(result), evRecord.GetTxId());
            return proposeResult;
        }

        NIceDb::TNiceDb db(context.GetDB());

        /* Let's abort at the final stage of the transaction */
        txState.Cancel = true;
        context.SS->PersistCancelTx(db, targetOpId, txState);

        result->Record.SetStatus(NKikimrScheme::StatusAccepted);
        result->Record.SetResult("Cancelled at SchemeShard");
        context.OnComplete.Send(Event->Sender, std::move(result), evRecord.GetTxId());

        context.OnComplete.ActivateTx(targetOpId);
        return proposeResult;
    }

    void AbortPropose(TOperationContext&) override {
        Y_FAIL("no AbortPropose for TTxCancelTx");
    }

    void ProgressState(TOperationContext&) override {
        Y_FAIL("no progress state for cancel tx");
        return;
    }

    void AbortUnsafe(TTxId, TOperationContext&) override {
        Y_FAIL("no AbortUnsafe for cancel tx");
    }
};

}


namespace NKikimr {
namespace NSchemeShard {

ISubOperationBase::TPtr CreateTxCancelTx(TEvSchemeShard::TEvCancelTx::TPtr ev)
{
    return new TTxCancelTx(ev);
}

}
}
