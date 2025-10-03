#include "schemeshard__op_traits.h"
#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->SelfTabletId() << "] " << stream)
#define LOG_I(stream) LOG_INFO_S  (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->SelfTabletId() << "] " << stream)
#define LOG_D(stream) LOG_DEBUG_S (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->SelfTabletId() << "] " << stream)

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

TString InterruptInheritanceExceptDescribe(const TString& initialAcl) {
    NACLib::TACL secObj(initialAcl);
    NACLib::TACL resultSecObj;
    resultSecObj.SetInterruptInheritance(true);
    for (auto& ace : *secObj.MutableACE()) {
        if (ace.GetAccessRight() & NACLib::EAccessRights::DescribeSchema) {
            resultSecObj.AddAccess(
                static_cast<NACLib::EAccessType>(ace.GetAccessType()),
                NACLib::EAccessRights::DescribeSchema,
                ace.GetSID(),
                ace.GetInheritanceType()
            );
        }
    }

    TString resultAcl;
    Y_ABORT_UNLESS(resultSecObj.SerializeToString(&resultAcl));
    return resultAcl;
}

class TPropose : public TSubOperationState {
private:
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TCreateSecret::TPropose"
            << ", opId: " << OperationId;
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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateSecret);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const auto step = TStepId(ev->Get()->StepId);

        LOG_I(DebugHint() << "HandleReply TEvOperationPlan"
            << ": step# " << step);

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateSecret);

        context.SS->TabletCounters->Simple()[COUNTER_SECRET_COUNT].Add(1);

        const auto& secretPathId = txState->TargetPathId;
        Y_ABORT_UNLESS(context.SS->PathsById.contains(secretPathId));
        auto secretPath = context.SS->PathsById.at(secretPathId);

        Y_ABORT_UNLESS(context.SS->Secrets.contains(secretPathId));
        auto secretInfo = context.SS->Secrets.at(secretPathId);

        auto alterData = secretInfo->AlterData;
        Y_ABORT_UNLESS(alterData);

        NIceDb::TNiceDb db(context.GetDB());

        secretPath->StepCreated = step;
        context.SS->PersistCreateStep(db, secretPathId, step);

        context.SS->Secrets[secretPathId] = alterData;
        context.SS->PersistSecretAlterRemove(db, secretPathId);
        context.SS->PersistSecret(db, secretPathId, *alterData);

        Y_ABORT_UNLESS(context.SS->PathsById.contains(secretPath->ParentPathId));
        auto parentPath = context.SS->PathsById.at(secretPath->ParentPathId);

        ++parentPath->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentPath);

        context.SS->ClearDescribePathCaches(parentPath);
        context.OnComplete.PublishToSchemeBoard(OperationId, parentPath->PathId);

        context.SS->ClearDescribePathCaches(secretPath);
        context.OnComplete.PublishToSchemeBoard(OperationId, secretPathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }
};

class TCreateSecret : public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::Propose;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
            case TTxState::Waiting:
            case TTxState::Propose:
                return TTxState::Done;
            default:
                return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
            case TTxState::Waiting:
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

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto acceptExisting = !Transaction.GetFailOnExist();
        const TString& parentPathStr = Transaction.GetWorkingDir();
        const auto& createSecretProto = Transaction.GetCreateSecret();

        const TString& secretName = createSecretProto.GetName();

        LOG_N("TCreateSecret Propose"
            << ", path: " << parentPathStr << "/" << secretName
            << ", opId: " << OperationId
        );

        auto secretDescrWithoutSecretParts = createSecretProto;
        secretDescrWithoutSecretParts.ClearValue();
        LOG_D("TCreateSecret Propose"
            << ", path: " << parentPathStr << "/" << secretName
            << ", opId: " << OperationId
            << ", secretDescription (without secret parts): " << secretDescrWithoutSecretParts.ShortDebugString()
        );

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        const auto parentPath = NSchemeShard::TPath::Resolve(parentPathStr, context.SS);
        {
            const auto checks = parentPath.Check();
            checks
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsCommonSensePath()
                .IsLikeDirectory()
                .FailOnRestrictedCreateInTempZone();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        const TString acl = Transaction.GetModifyACL().GetDiffACL();

        NSchemeShard::TPath dstPath = parentPath.Child(secretName);
        {
            const auto checks = dstPath.Check();
            checks.IsAtLocalSchemeShard();
            if (dstPath.IsResolved()) {
                checks
                    .NotUnderDeleting()
                    .FailOnExist(TPathElement::EPathType::EPathTypeSecret, acceptExisting);
            } else {
                checks
                    .NotEmpty();
            }

            if (checks) {
                checks
                    .IsValidLeafName(context.UserToken.Get())
                    .DepthLimit()
                    .PathsLimit()
                    .DirChildrenLimit()
                    .IsValidACL(acl);
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (dstPath.IsResolved()) {
                    result->SetPathCreateTxId(ui64(dstPath.Base()->CreateTxId));
                    result->SetPathId(dstPath.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        const auto secretPathId = context.SS->AllocatePathId();
        context.MemChanges.GrabNewPath(context.SS, secretPathId);
        context.MemChanges.GrabPath(context.SS, parentPath->PathId);
        context.MemChanges.GrabNewSecret(context.SS, secretPathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);

        context.DbChanges.PersistPath(secretPathId);
        context.DbChanges.PersistPath(parentPath->PathId);
        context.DbChanges.PersistSecret(secretPathId);
        context.DbChanges.PersistTxState(OperationId);

        dstPath.MaterializeLeaf(owner, secretPathId);
        dstPath.DomainInfo()->IncPathsInside(context.SS);
        IncAliveChildrenSafeWithUndo(OperationId, parentPath, context); // for correct discard of ChildrenExist prop

        result->SetPathId(secretPathId.LocalPathId);

        TPathElement::TPtr secretPath = dstPath.Base();
        secretPath->PathState = TPathElement::EPathState::EPathStateCreate;
        secretPath->PathType = TPathElement::EPathType::EPathTypeSecret;
        secretPath->CreateTxId = OperationId.GetTxId();
        secretPath->LastTxId = OperationId.GetTxId();

        if (!acl.empty()) {
            secretPath->ApplyACL(acl);
        } else {
            /** By default, secrets should not inherit permissions from their parent object, except for the DescribeSchema grant.
              * This is done to prevent users from accidentally granting permissions to such sensitive objects.
              * However, the DescribeSchema grant is quite harmless and allows users to view the object's ACL to request access from the owner.
              * There is also an inherit_permissions flag, which, when set, allows grant inheritance similarly to all other schema objects.
              */
            if (!createSecretProto.GetInheritPermissions()) {
                secretPath->ACL = InterruptInheritanceExceptDescribe(dstPath.GetEffectiveACL());
                secretPath->ACLVersion++;
            }
        }

        NKikimrSchemeOp::TSecretDescription secretDescription;
        secretDescription.SetName(createSecretProto.GetName());
        secretDescription.SetValue(createSecretProto.GetValue());

        const auto secretInfo = TSecretInfo::Create(std::move(secretDescription));
        context.SS->Secrets[secretPathId] = secretInfo;

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->PersistPath(db, dstPath->PathId);
        context.SS->PersistSecret(db, dstPath->PathId, *secretInfo);
        context.SS->PersistSecretAlter(db, dstPath->PathId, *secretInfo->AlterData);
        context.SS->IncrementPathDbRefCount(dstPath->PathId);

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateSecret, secretPathId);
        txState.State = TTxState::Propose;
        context.OnComplete.ActivateTx(OperationId);

        if (parentPath.Base()->HasActiveChanges()) {
            TTxId parentTxId = parentPath.Base()->PlannedToCreate() ? parentPath.Base()->CreateTxId : parentPath.Base()->LastTxId;
            context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
        }

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TCreateSecret AbortPropose" << ", opId: " << OperationId);
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TCreateSecret AbortUnsafe" << ", opId: " << OperationId << ", forceDropId: " << forceDropTxId);

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

using TTag = TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateSecret>;

namespace NOperation {

template <>
std::optional<TString> GetTargetName<TTag>(TTag, const TTxTransaction& tx) {
    return tx.GetCreateSecret().GetName();
}

template <>
bool SetName<TTag>(TTag, TTxTransaction& tx, const TString& name) {
    tx.MutableCreateSecret()->SetName(name);
    return true;
}

}

ISubOperation::TPtr CreateNewSecret(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateSecret>(id, tx);
}

ISubOperation::TPtr CreateNewSecret(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TCreateSecret>(id, state);
}

}
