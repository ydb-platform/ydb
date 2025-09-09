#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->SelfTabletId() << "] " << stream)
#define LOG_I(stream) LOG_INFO_S  (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->SelfTabletId() << "] " << stream)
#define LOG_D(stream) LOG_DEBUG_S (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->SelfTabletId() << "] " << stream)

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TAlterSecret TPropose"
            << " operationId# " << OperationId;
    }

public:
    explicit TPropose(TOperationId id)
        : OperationId(id)
    {
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << " ProgressState");

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterSecret);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const auto step = TStepId(ev->Get()->StepId);

        LOG_I(DebugHint() << "HandleReply TEvOperationPlan" << ": step# " << step);

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterSecret);
        const auto& secretPathId = txState->TargetPathId;

        Y_ABORT_UNLESS(context.SS->PathsById.contains(secretPathId));
        auto secretPath = context.SS->PathsById.at(secretPathId);

        Y_ABORT_UNLESS(context.SS->Secrets.contains(secretPathId));
        const auto secretInfo = context.SS->Secrets.at(secretPathId);

        auto alterData = secretInfo->AlterData;
        Y_ABORT_UNLESS(alterData);
        Y_ABORT_UNLESS(secretInfo->Description.GetVersion() + 1 == alterData->Description.GetVersion());

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->Secrets[secretPathId] = alterData;
        context.SS->PersistSecretAlterRemove(db, secretPathId);
        context.SS->PersistSecret(db, secretPathId, *alterData);

        context.SS->ClearDescribePathCaches(secretPath);
        context.OnComplete.PublishToSchemeBoard(OperationId, secretPathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }
};

class TAlterSecret: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::Propose;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto& alterSecretProto = Transaction.GetAlterSecret();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& secretName = alterSecretProto.GetName();

        LOG_N("TAlterSecret Propose"
            << ", path: " << parentPathStr << "/" << secretName
            << ", opId: " << OperationId
        );

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        if (!Transaction.HasAlterSecret()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "AlterSecret is not present");
            return result;
        }

        NSchemeShard::TPath parentPath = NSchemeShard::TPath::Resolve(parentPathStr, context.SS);
        NSchemeShard::TPath secretPath = parentPath.Child(secretName);
        {
            NSchemeShard::TPath::TChecker checks = secretPath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsSecret()
                .NotUnderOperation()
                .IsCommonSensePath();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (secretPath.IsResolved()) {
                    result->SetPathCreateTxId(ui64(secretPath.Base()->CreateTxId));
                    result->SetPathId(secretPath.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }

        result->SetPathId(secretPath.Base()->PathId.LocalPathId);

        TString errStr;
        if (!context.SS->CheckLocks(parentPath.Base()->PathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }

        Y_ABORT_UNLESS(context.SS->Secrets.contains(secretPath.Base()->PathId));
        auto secretInfo = context.SS->Secrets.at(secretPath.Base()->PathId);

        if (secretInfo->AlterVersion == 0) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, "Secret is not created yet");
            return result;
        }

        if (secretInfo->AlterData) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, "There's another Alter in flight");
            return result;
        }

        context.MemChanges.GrabPath(context.SS, secretPath.Base()->PathId);
        context.MemChanges.GrabSecret(context.SS, secretPath.Base()->PathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);

        context.DbChanges.PersistPath(secretPath.Base()->PathId);
        context.DbChanges.PersistAlterSecret(secretPath.Base()->PathId);
        context.DbChanges.PersistTxState(OperationId);

        auto alterData = secretInfo->CreateNextVersion();
        alterData->Description.SetValue(alterSecretProto.GetValue());
        alterData->Description.SetVersion(secretInfo->AlterVersion);

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterSecret, secretPath.Base()->PathId);
        txState.State = TTxState::Propose;

        secretPath.Base()->PathState = NKikimrSchemeOp::EPathStateAlter;
        secretPath.Base()->LastTxId = OperationId.GetTxId();

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->PersistSecretAlter(db, secretPath.Base()->PathId, *secretInfo->AlterData);
        context.SS->PersistTxState(db, OperationId);

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TAlterSecret AbortPropose"
            << ", opId: " << OperationId
        );
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TAlterSecret AbortUnsafe"
            << ", opId: " << OperationId
            << ", forceDropId: " << forceDropTxId
        );

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateAlterSecret(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAlterSecret>(id, tx);
}

ISubOperation::TPtr CreateAlterSecret(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TAlterSecret>(id, state);
}

}
