#include "schemeshard__op_traits.h"
#include "schemeshard__operation_common.h"
#include "schemeshard__operation_common_streaming_query.h"
#include "schemeshard_impl.h"


namespace NKikimr::NSchemeShard {

namespace NStreamingQuery {

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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateStreamingQuery);

        const TPathId& pathId = txState->TargetPathId;
        const TPath& path = TPath::Init(pathId, context.SS);
        const TPathElement::TPtr pathPtr = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        context.SS->TabletCounters->Simple()[COUNTER_STREAMING_QUERY_COUNT].Add(1);

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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateStreamingQuery);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

private:
    TString DebugHint() const override {
        return TStringBuilder() << "TCreateStreamingQuery TPropose, operationId: " << OperationId << ", ";
    }

private:
    const TOperationId OperationId;
};

class TCreateStreamingQuery : public TSubOperation {
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

    static bool IsDestinationPathValid(const THolder<TProposeResponse>& result, const TOperationContext& context, const TPath& dstPath, const TString& acl, bool acceptExisted) {
        const auto checks = dstPath.Check();

        checks.IsAtLocalSchemeShard();

        if (dstPath.IsResolved()) {
            checks.IsResolved()
                .NotUnderDeleting()
                .FailOnExist(TPathElement::EPathType::EPathTypeStreamingQuery, acceptExisted);
        } else {
            checks.NotEmpty()
                .NotResolved();
        }

        if (checks) {
            checks.IsValidLeafName(context.UserToken.Get())
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

    TPathElement::TPtr CreateStreamingQueryPathElement(const TPath& dstPath) const {
        TPathElement::TPtr streamingQuery = dstPath.Base();

        streamingQuery->CreateTxId = OperationId.GetTxId();
        streamingQuery->PathType = TPathElement::EPathType::EPathTypeStreamingQuery;
        streamingQuery->PathState = TPathElement::EPathState::EPathStateCreate;
        streamingQuery->LastTxId  = OperationId.GetTxId();

        return streamingQuery;
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        const TString& parentPathStr = Transaction.GetWorkingDir();
        const auto& streamingQueryDescription = Transaction.GetCreateStreamingQuery();
        const TString& name = streamingQueryDescription.GetName();
        LOG_N("TCreateStreamingQuery Propose: opId# " << OperationId << ", path# " << parentPathStr << "/" << name);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted,
                                                   static_cast<ui64>(OperationId.GetTxId()),
                                                   static_cast<ui64>(context.SS->SelfTabletId()));

        const TPath& parentPath = TPath::Resolve(parentPathStr, context.SS);
        RETURN_RESULT_UNLESS(IsParentPathValid(result, parentPath, /* isCreate */ true));

        TPath dstPath = parentPath.Child(name);
        const TString& acl = Transaction.GetModifyACL().GetDiffACL();
        RETURN_RESULT_UNLESS(IsDestinationPathValid(result, context, dstPath, acl, !Transaction.GetFailOnExist()));
        RETURN_RESULT_UNLESS(IsApplyIfChecksPassed(result, Transaction, context));
        RETURN_RESULT_UNLESS(IsDescriptionValid(result, streamingQueryDescription));

        const auto streamingQueryInfo = CreateNewStreamingQuery(streamingQueryDescription, 1);
        Y_ABORT_UNLESS(streamingQueryInfo);

        AddPathInSchemeShard(result, dstPath, owner);
        const auto streamingQuery = CreateStreamingQueryPathElement(dstPath);
        CreateTransaction(OperationId, context, streamingQuery->PathId, TTxState::TxCreateStreamingQuery);
        RegisterParentPathDependencies(OperationId, context, parentPath);

        NIceDb::TNiceDb db(context.GetDB());
        AdvanceTransactionStateToPropose(OperationId, context, db);
        PersistStreamingQuery(OperationId, context, db, streamingQuery, streamingQueryInfo, acl);

        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, dstPath, context.SS, context.OnComplete);

        dstPath.DomainInfo()->IncPathsInside(context.SS);
        IncAliveChildrenDirect(OperationId, parentPath, context); // for correct discard of ChildrenExist prop

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TCreateStreamingQuery AbortPropose: opId# " << OperationId);
        Y_ABORT("no AbortPropose for TCreateStreamingQuery");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TCreateStreamingQuery AbortUnsafe: opId# " << OperationId << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

using TTag = TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateStreamingQuery>;

}  // anonymous namespace

}  // namespace NStreamingQuery


namespace NOperation {

template <>
std::optional<TString> GetTargetName<NStreamingQuery::TTag>(NStreamingQuery::TTag, const TTxTransaction& tx) {
    return tx.GetCreateStreamingQuery().GetName();
}

template <>
bool SetName<NStreamingQuery::TTag>(NStreamingQuery::TTag, TTxTransaction& tx, const TString& name) {
    tx.MutableCreateStreamingQuery()->SetName(name);
    return true;
}

} // namespace NOperation

ISubOperation::TPtr CreateNewStreamingQuery(TOperationId id, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateStreamingQuery);

    LOG_I("CreateNewStreamingQuery, opId# " << id  << ", tx# " << tx.ShortDebugString());

    const TPath parentPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);
    if (const auto checks = NStreamingQuery::IsParentPathValid(parentPath, /* isCreate */ true); !checks) {
        return CreateReject(id, checks.GetStatus(), TStringBuilder() << "Invalid CreateStreamingQuery request: " << checks.GetError());
    }

    if (const auto& operation = tx.GetCreateStreamingQuery(); operation.GetReplaceIfExists()) {
        const TPath dstPath = parentPath.Child(operation.GetName());
        const auto isAlreadyExists = dstPath.Check()
            .IsResolved()
            .NotUnderDeleting();

        if (isAlreadyExists) {
            return CreateAlterStreamingQuery(id, tx);
        }
    }

    return MakeSubOperation<NStreamingQuery::TCreateStreamingQuery>(id, tx);
}

ISubOperation::TPtr CreateNewStreamingQuery(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return MakeSubOperation<NStreamingQuery::TCreateStreamingQuery>(id, state);
}

}  // namespace NKikimr::NSchemeShard
