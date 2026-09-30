#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TPropose: public TSubOperationState {
    virtual const char* Name() const override final { return "TPropose"; }

private:
    TOperationId OperationId;

public:
    explicit TPropose(TOperationId id)
        : OperationId(id)
    {
    }

    bool ProgressState(TOperationContext& context) override {
        YDB_LOG_INFO_CTX(context.Ctx, "Propose to coordinator");

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterSecret);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const auto step = TStepId(ev->Get()->StepId);

        YDB_LOG_INFO_CTX(context.Ctx, "Operation plan received",
            {"step", step},
        );

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
        {
            // a CANCEL (the staged replacement goes, the current delegation stays) reports that the setup of the
            // replacement is over; a DROP or a STAGE over it does not
            const auto& before = secretInfo->Description;
            const auto& after = alterData->Description;
            const bool cancelled = before.HasPendingIamDelegation() && !after.HasPendingIamDelegation()
                && after.GetIamDelegation().GetReferrerId() == before.GetIamDelegation().GetReferrerId();
            context.SS->PersistIamDelegationRevocations(db, secretPathId, before, &after,
                cancelled ? TSchemeShard::EPendingIamDelegationSetup::Over : TSchemeShard::EPendingIamDelegationSetup::MayBeInFlight);
        }
        context.SS->Secrets.Set(secretPathId, alterData);
        context.SS->PersistSecretAlterRemove(db, secretPathId);
        context.SS->PersistSecret(db, secretPathId, *alterData);

        context.SS->ClearDescribePathCaches(secretPath);
        context.OnComplete.PublishToSchemeBoard(OperationId, secretPathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }
};

class TAlterSecret: public TSubOperation {
    virtual const char* Name() const override final { return "TAlterSecret"; }

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

        YDB_LOG_NOTICE_CTX(context.Ctx, "Alter secret",
            {"path", parentPathStr + "/" + secretName},
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

        if (alterSecretProto.HasValueParamName()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter,
                "Secret value must be set via Value, however ValueParamName was passed");
            return result;
        }

        // the source of a secret never changes
        const auto delegationAlter = alterSecretProto.GetIamDelegationAlter();
        const bool storedDelegation = secretInfo->Description.HasIamDelegation();
        if (!storedDelegation) {
            if (alterSecretProto.HasIamDelegation() || delegationAlter != NKikimrSchemeOp::IAM_DELEGATION_ALTER_NONE) {
                result->SetError(NKikimrScheme::StatusInvalidParameter,
                    "Cannot change the source of a secret: IamDelegation is allowed only for IAM delegation secrets");
                return result;
            }
        } else {
            if (!AppData()->FeatureFlags.GetEnableIamDelegationSecrets()) {
                result->SetError(NKikimrScheme::StatusPreconditionFailed,
                    "IAM delegation secrets are disabled. Please contact your system administrator to enable it");
                return result;
            }
            if (alterSecretProto.HasValue()) {
                result->SetError(NKikimrScheme::StatusInvalidParameter,
                    "Cannot change the source of a secret: Value is not allowed for IAM delegation secrets");
                return result;
            }
            const auto& current = secretInfo->Description;
            const auto& requested = alterSecretProto.GetIamDelegation();
            switch (delegationAlter) {
                case NKikimrSchemeOp::IAM_DELEGATION_ALTER_NONE:
                    if (!alterSecretProto.HasIamDelegation() || !SameIamDelegation(requested, current.GetIamDelegation())) {
                        result->SetError(NKikimrScheme::StatusInvalidParameter,
                            "IamDelegation must equal the current delegation of the secret unless IamDelegationAlter says what to do with it");
                        return result;
                    }
                    break;
                case NKikimrSchemeOp::IAM_DELEGATION_ALTER_STAGE:
                    if (const auto error = ValidateIamDelegation(requested)) {
                        result->SetError(NKikimrScheme::StatusInvalidParameter, *error);
                        return result;
                    }
                    if (const auto holder = context.SS->FindIamDelegationReferrer(requested.GetReferrerId())) {
                        result->SetError(NKikimrScheme::StatusInvalidParameter, TStringBuilder()
                            << "IAM delegation " << requested.GetReferrerId() << " is already named by " << *holder);
                        return result;
                    }
                    if (current.HasPendingIamDelegation()
                        && TInstant::MicroSeconds(current.GetPendingIamDelegationStagedAt()) + StagedIamDelegationLease > context.Ctx.Now())
                    {
                        result->SetError(NKikimrScheme::StatusMultipleModifications, TStringBuilder()
                            << "IAM delegation " << current.GetPendingIamDelegation().GetReferrerId()
                            << " is being set up for the secret by another ALTER; retry once it has completed");
                        return result;
                    }
                    break;
                case NKikimrSchemeOp::IAM_DELEGATION_ALTER_PROMOTE:
                case NKikimrSchemeOp::IAM_DELEGATION_ALTER_CANCEL:
                    if (!current.HasPendingIamDelegation()
                        || current.GetPendingIamDelegation().GetReferrerId() != requested.GetReferrerId())
                    {
                        result->SetError(NKikimrScheme::StatusPreconditionFailed, TStringBuilder()
                            << "IAM delegation " << requested.GetReferrerId() << " is not staged for the secret");
                        return result;
                    }
                    break;
                case NKikimrSchemeOp::IAM_DELEGATION_ALTER_CONFIRM:
                    if (current.GetIamDelegation().GetReferrerId() != requested.GetReferrerId()) {
                        result->SetError(NKikimrScheme::StatusPreconditionFailed, TStringBuilder()
                            << "IAM delegation " << requested.GetReferrerId() << " is not the delegation of the secret");
                        return result;
                    }
                    break;
            }
        }

        context.MemChanges.GrabPath(context.SS, secretPath.Base()->PathId);
        context.MemChanges.GrabSecret(context.SS, secretPath.Base()->PathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);

        context.DbChanges.PersistPath(secretPath.Base()->PathId);
        context.DbChanges.PersistAlterSecret(secretPath.Base()->PathId);
        context.DbChanges.PersistTxState(OperationId);

        // InheritPermissions is set only by CREATE OR REPLACE SECRET over an existing secret:
        // reapply the ACL so that the result matches a freshly created secret (DROP + CREATE).
        // Keep the same precedence as TCreateSecret: an explicit ACL wins over InheritPermissions.
        if (alterSecretProto.HasInheritPermissions()) {
            const TString acl = Transaction.GetModifyACL().GetDiffACL();
            if (!acl.empty()) {
                secretPath.Base()->ApplyACL(acl);
            } else {
                if (alterSecretProto.GetInheritPermissions()) {
                    // Inherit from the parent: no ACL of its own.
                    secretPath.Base()->ACL.clear();
                } else {
                    secretPath.Base()->ACL = InterruptInheritanceExceptDescribe(parentPath.GetEffectiveACL());
                }
                ++secretPath.Base()->ACLVersion;
            }
        }

        auto alterData = secretInfo->CreateNextVersion();
        if (!storedDelegation) {
            alterData->Description.SetValue(alterSecretProto.GetValue());
        } else {
            // CreateNextVersion copied the current and the staged delegation
            switch (delegationAlter) {
                case NKikimrSchemeOp::IAM_DELEGATION_ALTER_NONE:
                    break;
                case NKikimrSchemeOp::IAM_DELEGATION_ALTER_STAGE:
                    alterData->Description.MutablePendingIamDelegation()->CopyFrom(alterSecretProto.GetIamDelegation());
                    alterData->Description.SetPendingIamDelegationStagedAt(context.Ctx.Now().MicroSeconds());
                    break;
                case NKikimrSchemeOp::IAM_DELEGATION_ALTER_PROMOTE:
                    alterData->Description.MutableIamDelegation()->CopyFrom(secretInfo->Description.GetPendingIamDelegation());
                    alterData->Description.SetIamDelegationNamedAt(secretInfo->Description.GetPendingIamDelegationStagedAt());
                    alterData->Description.SetIamDelegationSetUp(true);
                    alterData->Description.ClearPendingIamDelegation();
                    alterData->Description.ClearPendingIamDelegationStagedAt();
                    break;
                case NKikimrSchemeOp::IAM_DELEGATION_ALTER_CANCEL:
                    alterData->Description.ClearPendingIamDelegation();
                    alterData->Description.ClearPendingIamDelegationStagedAt();
                    break;
                case NKikimrSchemeOp::IAM_DELEGATION_ALTER_CONFIRM:
                    alterData->Description.SetIamDelegationSetUp(true);
                    break;
            }
        }
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
        YDB_LOG_NOTICE_CTX(context.Ctx, "");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        YDB_LOG_NOTICE_CTX(context.Ctx, "TAlterSecret AbortUnsafe",
            {"opId", OperationId},
            {"forceDropId", forceDropTxId},
            {"schemeshard", context.SS->SelfTabletId()},
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

#undef YDB_LOG_THIS_FILE_COMPONENT
