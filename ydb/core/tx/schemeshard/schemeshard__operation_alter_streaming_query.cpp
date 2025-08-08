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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterStreamingQuery);

        const TPathId& pathId = txState->TargetPathId;
        const TPath& path = TPath::Init(pathId, context.SS);
        const TPathElement::TPtr pathPtr = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterStreamingQuery);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

private:
    TString DebugHint() const override {
        return TStringBuilder() << "TAlterStreamingQuery TPropose, operationId: " << OperationId << ", ";
    }

private:
    const TOperationId OperationId;
};

class TAlterStreamingQuery : public TSubOperation {
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

    static bool IsDestinationPathValid(const THolder<TProposeResponse>& result, const TPath& dstPath) {
        const auto checks = dstPath.Check();

        checks.IsAtLocalSchemeShard()
            .IsResolved()
            .NotUnderDeleting()
            .NotUnderOperation()
            .FailOnWrongType(TPathElement::EPathType::EPathTypeStreamingQuery);

        if (!checks) {
            result->SetError(checks.GetStatus(), checks.GetError());
            if (dstPath.IsResolved()) {
                result->SetPathCreateTxId(static_cast<ui64>(dstPath.Base()->CreateTxId));
                result->SetPathId(dstPath.Base()->PathId.LocalPathId);
            }
        }

        return static_cast<bool>(checks);
    }

    TPathElement::TPtr ReplaceStreamingQueryPathElement(const TPath& dstPath) const {
        TPathElement::TPtr streamingQuery = dstPath.Base();

        streamingQuery->PathState = TPathElement::EPathState::EPathStateAlter;
        streamingQuery->LastTxId  = OperationId.GetTxId();

        return streamingQuery;
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        Y_UNUSED(owner);

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const auto& streamingQueryDescription = Transaction.GetCreateStreamingQuery();
        const TString& name = streamingQueryDescription.GetName();
        LOG_N("TAlterStreamingQuery Propose: opId# " << OperationId << ", path# " << parentPathStr << "/" << name);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted,
                                                   static_cast<ui64>(OperationId.GetTxId()),
                                                   static_cast<ui64>(context.SS->SelfTabletId()));

        const TPath& parentPath = TPath::Resolve(parentPathStr, context.SS);
        RETURN_RESULT_UNLESS(IsParentPathValid(result, parentPath, /* isCreate */ false));

        const TPath& dstPath = parentPath.Child(name);
        RETURN_RESULT_UNLESS(IsDestinationPathValid(result, dstPath));
        RETURN_RESULT_UNLESS(IsApplyIfChecksPassed(result, Transaction, context));
        RETURN_RESULT_UNLESS(IsDescriptionValid(result, streamingQueryDescription));

        const auto& oldStreamingQueryInfo = context.SS->StreamingQueries.Value(dstPath->PathId, nullptr);
        Y_ABORT_UNLESS(oldStreamingQueryInfo);
        const auto streamingQueryInfo = CreateModifyStreamingQuery(streamingQueryDescription, oldStreamingQueryInfo);
        Y_ABORT_UNLESS(streamingQueryInfo);

        result->SetPathId(dstPath.Base()->PathId.LocalPathId);
        const TPathElement::TPtr streamingQuery = ReplaceStreamingQueryPathElement(dstPath);
        CreateTransaction(OperationId, context, streamingQuery->PathId, TTxState::TxAlterStreamingQuery);
        RegisterParentPathDependencies(OperationId, context, parentPath);

        NIceDb::TNiceDb db(context.GetDB());
        AdvanceTransactionStateToPropose(OperationId, context, db);
        PersistStreamingQuery(OperationId, context, db, streamingQuery, streamingQueryInfo, /* acl */ TString());

        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, dstPath, context.SS, context.OnComplete);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TAlterStreamingQuery AbortPropose: opId# " << OperationId);
        Y_ABORT("no AbortPropose for TAlterStreamingQuery");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TAlterStreamingQuery AbortUnsafe: opId# " << OperationId << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

}  // anonymous namespace

}  // namespace NStreamingQuery

ISubOperation::TPtr CreateAlterStreamingQuery(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<NStreamingQuery::TAlterStreamingQuery>(id, tx);
}

ISubOperation::TPtr CreateAlterStreamingQuery(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<NStreamingQuery::TAlterStreamingQuery>(id, state);
}

}  // namespace NKikimr::NSchemeShard
