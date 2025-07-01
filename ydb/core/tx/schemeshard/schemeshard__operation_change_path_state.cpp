#include "schemeshard__operation_change_path_state.h"

#include "schemeshard__operation_base.h"
#include "schemeshard__operation_common.h"
#include "schemeshard__operation_states.h"

#define LOG_I(stream) LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)

namespace NKikimr::NSchemeShard {

class TChangePathStateOp: public TSubOperationWithContext {
    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch(state) {
        case TTxState::Waiting:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state, TOperationContext& context) override {
        switch(state) {
        case TTxState::Waiting:
        case TTxState::Propose:
            return MakeHolder<TEmptyPropose>(OperationId);
        case TTxState::Done: {
            const auto* txState = context.SS->FindTx(OperationId);
            if (txState && txState->TargetPathTargetState.Defined()) {
                auto targetState = static_cast<TPathElement::EPathState>(*txState->TargetPathTargetState);
                return MakeHolder<TDone>(OperationId, targetState);
            }
            Y_ABORT("Unreachable code: TDone state should always have a target state defined for TChangePathStateOp");
        }
        default:
            return nullptr;
        }
    }

public:
    using TSubOperationWithContext::TSubOperationWithContext;
    using TSubOperationWithContext::SelectStateFunc;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const auto& tx = Transaction;
        const TTabletId schemeshardTabletId = context.SS->SelfTabletId();
        
        LOG_I("TChangePathStateOp Propose"
            << ", opId: " << OperationId
        );

        const auto& changePathState = tx.GetChangePathState();
        TString pathStr = JoinPath({tx.GetWorkingDir(), changePathState.GetPath()});
        
        const TPath& path = TPath::Resolve(pathStr, context.SS);
        
        {
            auto checks = path.Check();
            checks
                .NotEmpty()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotUnderDeleting();

            if (!checks) {
                return MakeHolder<TProposeResponse>(checks.GetStatus(), ui64(OperationId.GetTxId()), ui64(schemeshardTabletId), checks.GetError());
            }
        }

        Y_VERIFY_S(!context.SS->FindTx(OperationId), 
            "TChangePathStateOp Propose: operation already exists"
            << ", opId: " << OperationId);
        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxChangePathState, path.GetPathIdForDomain());
        
        txState.TargetPathId = path.Base()->PathId;
        txState.TargetPathTargetState = static_cast<NKikimrSchemeOp::EPathState>(changePathState.GetTargetState());
        
        path.Base()->PathState = *txState.TargetPathTargetState;
        context.DbChanges.PersistPath(path.Base()->PathId);
        
        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(schemeshardTabletId));

        txState.State = TTxState::Waiting;
        context.DbChanges.PersistTxState(OperationId);
        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState(TTxState::Waiting), context);
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TChangePathStateOp AbortPropose"
            << ", opId: " << OperationId);
        // Nothing to cleanup since Propose hasn't committed anything yet
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TChangePathStateOp AbortUnsafe"
            << ", opId: " << OperationId
            << ", forceDropId: " << forceDropTxId
        );

        context.OnComplete.DoneOperation(OperationId);
    }
};

ISubOperation::TPtr CreateChangePathState(TOperationId opId, const TTxTransaction& tx) {
    return MakeSubOperation<TChangePathStateOp>(opId, tx);
}

ISubOperation::TPtr CreateChangePathState(TOperationId opId, TTxState::ETxState state) {
    return MakeSubOperation<TChangePathStateOp>(opId, state);
}

bool CreateChangePathState(TOperationId opId, const TTxTransaction& tx, TOperationContext& context, TVector<ISubOperation::TPtr>& result) {
    if (!tx.HasChangePathState()) {
        result = {CreateReject(opId, NKikimrScheme::StatusInvalidParameter, "Missing ChangePathState")};
        return false;
    }

    const auto& changePathState = tx.GetChangePathState();
    
    if (!changePathState.HasPath()) {
        result = {CreateReject(opId, NKikimrScheme::StatusInvalidParameter, "Missing Path in ChangePathState")};
        return false;
    }

    if (!changePathState.HasTargetState()) {
        result = {CreateReject(opId, NKikimrScheme::StatusInvalidParameter, "Missing TargetState in ChangePathState")};
        return false;
    }

    TString pathStr = JoinPath({tx.GetWorkingDir(), changePathState.GetPath()});
    const TPath& path = TPath::Resolve(pathStr, context.SS);
    
    {
        auto checks = path.Check();
        checks
            .NotEmpty()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotUnderDeleting();

        if (!checks) {
            result = {CreateReject(opId, checks.GetStatus(), checks.GetError())};
            return false;
        }
    }

    result.push_back(CreateChangePathState(NextPartId(opId, result), tx));
    return true;
}

TVector<ISubOperation::TPtr> CreateChangePathState(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    TVector<ISubOperation::TPtr> result;
    CreateChangePathState(opId, tx, context, result);
    return result;
}

} // namespace NKikimr::NSchemeShard
