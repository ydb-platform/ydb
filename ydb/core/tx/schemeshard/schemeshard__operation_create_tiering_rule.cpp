#include "schemeshard__operation_common_tiering_rule.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"


namespace NKikimr::NSchemeShard {

namespace {

class TPropose : public TSubOperationState {
public:
    explicit TPropose(TOperationId id)
        : OperationId(std::move(id))
    {}

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const TStepId step = TStepId(ev->Get()->StepId);
        LOG_I(DebugHint() << "HandleReply TEvOperationPlan: step# " << step);

        const TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateTieringRule);

        const TPathId& pathId = txState->TargetPathId;
        const TPath& path = TPath::Init(pathId, context.SS);
        const TPathElement::TPtr pathPtr = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        context.SS->TabletCounters->Simple()[COUNTER_TIERING_RULE_COUNT].Add(1);

        IncParentDirAlterVersionWithRepublish(OperationId, path, context);
        context.SS->ClearDescribePathCaches(pathPtr);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << "ProgressState");

        const TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateTieringRule);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

private:
    TString DebugHint() const override {
        return TStringBuilder() << "TCreateTieringRule TPropose, operationId: " << OperationId << ", ";
    }

private:
    const TOperationId OperationId;
};

class TCreateTieringRule : public TSubOperation {
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

    static bool IsDestinationPathValid(const THolder<TProposeResponse>& result, const TPath& dstPath, const TString& acl, bool acceptExisted) {
        const auto checks = dstPath.Check();
        checks.IsAtLocalSchemeShard();
        if (dstPath.IsResolved()) {
            checks
                .IsResolved()
                .NotUnderDeleting()
                .FailOnExist(TPathElement::EPathType::EPathTypeTieringRule, acceptExisted);
        } else {
            checks
                .NotEmpty()
                .NotResolved();
        }

        if (checks) {
            checks
                .IsValidLeafName()
                .DepthLimit()
                .PathsLimit()
                .DirChildrenLimit()
                .IsValidACL(acl);
        }

        if (!checks) {
            result->SetError(checks.GetStatus(), checks.GetError());
            if (dstPath.IsResolved()) {
                result->SetPathCreateTxId(static_cast<ui64>(dstPath.Base()->CreateTxId));
                result->SetPathId(dstPath.Base()->PathId.LocalPathId);
            }
        }

        return static_cast<bool>(checks);
    }

    static void AddPathInSchemeShard(const THolder<TProposeResponse>& result, TPath& dstPath, const TString& owner) {
        dstPath.MaterializeLeaf(owner);
        result->SetPathId(dstPath.Base()->PathId.LocalPathId);
    }

    TPathElement::TPtr CreateTieringRulePathElement(const TPath& dstPath) const {
        TPathElement::TPtr tieringRule = dstPath.Base();

        tieringRule->CreateTxId = OperationId.GetTxId();
        tieringRule->PathType = TPathElement::EPathType::EPathTypeTieringRule;
        tieringRule->PathState = TPathElement::EPathState::EPathStateCreate;
        tieringRule->LastTxId  = OperationId.GetTxId();

        return tieringRule;
    }

    static void UpdatePathSizeCounts(const TPath& parentPath, const TPath& dstPath) {
        dstPath.DomainInfo()->IncPathsInside();
        parentPath.Base()->IncAliveChildren();
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        const TString& parentPathStr = Transaction.GetWorkingDir();
        const auto& tieringRuleDescription = Transaction.GetCreateTieringRule();
        const TString& name = tieringRuleDescription.GetName();
        LOG_N("TCreateTieringRule Propose: opId# " << OperationId << ", path# " << parentPathStr << "/" << name);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted,
                                                   static_cast<ui64>(OperationId.GetTxId()),
                                                   static_cast<ui64>(context.SS->SelfTabletId()));

        const TPath& parentPath = TPath::Resolve(parentPathStr, context.SS);
        RETURN_RESULT_UNLESS(NTieringRule::IsParentPathValid(result, parentPath));

        TPath dstPath = parentPath.Child(name);
        const TString& acl = Transaction.GetModifyACL().GetDiffACL();
        RETURN_RESULT_UNLESS(IsDestinationPathValid(result, dstPath, acl, !Transaction.GetFailOnExist()));
        RETURN_RESULT_UNLESS(NTieringRule::IsApplyIfChecksPassed(Transaction, result, context));
        RETURN_RESULT_UNLESS(NTieringRule::IsDescriptionValid(result, tieringRuleDescription));

        const TTieringRuleInfo::TPtr tieringRuleInfo = NTieringRule::CreateTieringRule(tieringRuleDescription, 1);
        Y_ABORT_UNLESS(tieringRuleInfo);
        RETURN_RESULT_UNLESS(NTieringRule::IsTieringRuleInfoValid(result, tieringRuleInfo));

        AddPathInSchemeShard(result, dstPath, owner);
        const TPathElement::TPtr tieringRule = CreateTieringRulePathElement(dstPath);
        NTieringRule::CreateTransaction(OperationId, context, tieringRule->PathId, TTxState::TxCreateTieringRule);
        NTieringRule::RegisterParentPathDependencies(OperationId, context, parentPath);

        NIceDb::TNiceDb db(context.GetDB());
        NTieringRule::AdvanceTransactionStateToPropose(OperationId, context, db);
        NTieringRule::PersistTieringRule(OperationId, context, db, tieringRule, tieringRuleInfo, acl);

        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, dstPath, context.SS, context.OnComplete);

        UpdatePathSizeCounts(parentPath, dstPath);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TCreateTieringRule AbortPropose: opId# " << OperationId);
        Y_ABORT("no AbortPropose for TCreateTieringRule");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TCreateTieringRule AbortUnsafe: opId# " << OperationId << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

}  // anonymous namespace

ISubOperation::TPtr CreateNewTieringRule(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateTieringRule>(id, tx);
}

ISubOperation::TPtr CreateNewTieringRule(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return MakeSubOperation<TCreateTieringRule>(id, state);
}

}  // namespace NKikimr::NSchemeShard
