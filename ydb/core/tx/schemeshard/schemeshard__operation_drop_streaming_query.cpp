#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#define LOG_I(stream) LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define RETURN_RESULT_UNLESS(x) if (!(x)) return result;

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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropStreamingQuery);

        context.SS->TabletCounters->Simple()[COUNTER_STREAMING_QUERY_COUNT].Sub(1);

        const TPathId& pathId = txState->TargetPathId;
        const auto pathPtr = context.SS->PathsById.at(pathId);
        const auto parentDirPtr = context.SS->PathsById.at(pathPtr->ParentPathId);
        NIceDb::TNiceDb db(context.GetDB());

        Y_ABORT_UNLESS(!pathPtr->Dropped());
        pathPtr->SetDropped(step, OperationId.GetTxId());
        context.SS->PersistDropStep(db, pathId, step, OperationId);
        context.SS->PersistRemoveStreamingQuery(db, pathId);

        context.SS->ResolveDomainInfo(pathId)->DecPathsInside(context.SS);
        DecAliveChildrenDirect(OperationId, parentDirPtr, context);

        ++parentDirPtr->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDirPtr);
        context.SS->ClearDescribePathCaches(parentDirPtr);
        context.SS->ClearDescribePathCaches(pathPtr);

        if (!context.SS->DisablePublicationsOfDropping) {
            context.OnComplete.PublishToSchemeBoard(OperationId, parentDirPtr->PathId);
            context.OnComplete.PublishToSchemeBoard(OperationId, pathId);
        }

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << "ProgressState");

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropStreamingQuery);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

private:
    TString DebugHint() const override {
        return TStringBuilder() << "TDropStreamingQuery TPropose, operationId: " << OperationId << ", ";
    }

private:
    const TOperationId OperationId;
};

class TDropStreamingQuery : public TSubOperation {
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

    static bool IsDestinationPathValid(const THolder<TProposeResponse>& result, const TPath& dstPath, const TOperationContext& context) {
        const auto checks = dstPath.Check();
        checks.NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .NotUnderDeleting()
            .IsStreamingQuery()
            .NotUnderOperation()
            .IsCommonSensePath();

        if (checks) {
            const auto streamingQuery = context.SS->StreamingQueries.Value(dstPath->PathId, nullptr);
            if (!streamingQuery) {
                result->SetError(NKikimrScheme::StatusPathDoesNotExist, "Streaming query doesn't exist");
                return false;
            }
        }

        if (!checks) {
            result->SetError(checks.GetStatus(), checks.GetError());
            if (dstPath.IsResolved() && dstPath.Base()->IsStreamingQuery() && (dstPath.Base()->PlannedToDrop() || dstPath.Base()->Dropped())) {
                result->SetPathDropTxId(ui64(dstPath.Base()->DropTxId));
                result->SetPathId(dstPath.Base()->PathId.LocalPathId);
            }
        }

        return static_cast<bool>(checks);
    }

    bool IsApplyIfChecksPassed(const THolder<TProposeResponse>& result, const TOperationContext& context) const {
        if (TString errorStr; !context.SS->CheckApplyIf(Transaction, errorStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errorStr);
            return false;
        }

        return true;
    }

    void PersistDropStreamingQuery(const TOperationContext& context, const TPath& dstPath) const {
        const TPathId& pathId = dstPath.Base()->PathId;

        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.MemChanges.GrabPath(context.SS, pathId);
        context.MemChanges.GrabPath(context.SS, dstPath->ParentPathId);
        context.MemChanges.GrabStreamingQuery(context.SS, pathId);

        context.DbChanges.PersistTxState(OperationId);
        context.DbChanges.PersistPath(pathId);
        context.DbChanges.PersistPath(dstPath->ParentPathId);
    }

    void CreateTransaction(const TOperationContext& context, const TPathId& streamingQueryPathId) const {
        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxDropStreamingQuery, streamingQueryPathId);
        txState.Shards.clear();
        txState.State = TTxState::Propose;
        txState.MinStep = TStepId(1);
        context.OnComplete.ActivateTx(OperationId);
    }

    void DropStreamingQueryPathElement(const TPath& dstPath) const {
        TPathElement::TPtr streamingQuery = dstPath.Base();

        streamingQuery->PathState = TPathElement::EPathState::EPathStateDrop;
        streamingQuery->DropTxId = OperationId.GetTxId();
        streamingQuery->LastTxId = OperationId.GetTxId();
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        Y_UNUSED(owner);

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const auto& dropDescription = Transaction.GetDrop();
        const TString& name = dropDescription.GetName();
        LOG_N("TDropStreamingQuery Propose: opId# " << OperationId << ", path# " << parentPathStr << "/" << name);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted,
                                                   static_cast<ui64>(OperationId.GetTxId()),
                                                   static_cast<ui64>(context.SS->SelfTabletId()));

        const TPath& dstPath = dropDescription.HasId()
            ? TPath::Init(context.SS->MakeLocalId(dropDescription.GetId()), context.SS)
            : TPath::Resolve(parentPathStr, context.SS).Dive(name);
        RETURN_RESULT_UNLESS(IsDestinationPathValid(result, dstPath, context));
        RETURN_RESULT_UNLESS(IsApplyIfChecksPassed(result, context));

        result->SetPathId(dstPath.Base()->PathId.LocalPathId);

        const auto guard = context.DbGuard();
        PersistDropStreamingQuery(context, dstPath);
        CreateTransaction(context, dstPath.Base()->PathId);
        DropStreamingQueryPathElement(dstPath);

        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, dstPath, context.SS, context.OnComplete);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TDropStreamingQuery AbortPropose: opId# " << OperationId);
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TDropStreamingQuery AbortUnsafe: opId# " << OperationId << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

}  // anonymous namespace

}  // namespace NStreamingQuery

ISubOperation::TPtr CreateDropStreamingQuery(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<NStreamingQuery::TDropStreamingQuery>(id, tx);
}

ISubOperation::TPtr CreateDropStreamingQuery(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<NStreamingQuery::TDropStreamingQuery>(id, state);
}

}  // namespace NKikimr::NSchemeShard
