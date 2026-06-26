#include <ydb/core/tx/schemeshard/schemeshard__operation_common.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/schemeshard_path_element.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD


namespace {

using namespace NKikimr;
using namespace NSchemeShard;

void DropPath(NIceDb::TNiceDb& db, TOperationContext& context,
              TOperationId operationId, const TTxState& txState, TPath& path)
{
    if (path->Dropped()) {
        return;
    }

    context.SS->TabletCounters->Simple()[COUNTER_TABLE_INDEXES_COUNT].Sub(1);

    Y_ABORT_UNLESS(txState.PlanStep);
    path->SetDropped(txState.PlanStep, operationId.GetTxId());
    context.SS->PersistDropStep(db, path->PathId, txState.PlanStep, operationId);
    context.SS->PersistRemoveTableIndex(db, path->PathId);
    context.SS->Indexes.erase(path->PathId);

    context.SS->TabletCounters->Simple()[COUNTER_USER_ATTRIBUTES_COUNT].Sub(path->UserAttrs->Size());
    context.SS->PersistUserAttributes(db, path->PathId, path->UserAttrs, nullptr);

    auto domainInfo = context.SS->ResolveDomainInfo(path->PathId);
    domainInfo->DecPathsInside(context.SS);

    auto parentDir = path.Parent();
    DecAliveChildrenDirect(operationId, parentDir.Base(), context);
    ++parentDir->DirAlterVersion;
    context.SS->PersistPathDirAlterVersion(db, parentDir.Base());

    context.SS->ClearDescribePathCaches(parentDir.Base());
    context.SS->ClearDescribePathCaches(path.Base());

    if (!context.SS->DisablePublicationsOfDropping) {
        context.OnComplete.PublishToSchemeBoard(operationId, parentDir->PathId);
        context.OnComplete.PublishToSchemeBoard(operationId, path->PathId);
    }
}

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TDropLocalIndex TPropose"
            << ", operationId: " << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);

        YDB_LOG_INFO_CTX(context.Ctx, "HandleReply TEvOperationPlan",
            {"debugHint", DebugHint()},
            {"step", step},
            {"atSchemeshard", context.SS->TabletID()});

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropLocalIndex);
        Y_ABORT_UNLESS(txState->State == TTxState::Propose);

        NIceDb::TNiceDb db(context.GetDB());

        txState->PlanStep = step;
        context.SS->PersistTxPlanStep(db, OperationId, step);

        Y_VERIFY_S(context.SS->PathsById.contains(txState->TargetPathId), "Unknown pathId: " << txState->TargetPathId);
        TPath path = TPath::Init(txState->TargetPathId, context.SS);
        Y_ABORT_UNLESS(path.IsResolved());

        DropPath(db, context, OperationId, *txState, path);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        YDB_LOG_INFO_CTX(context.Ctx, "ProgressState",
            {"debugHint", DebugHint()},
            {"atSchemeshard", context.SS->TabletID()});

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropLocalIndex);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

class TDropLocalIndex: public TSubOperation {
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

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        if (!Transaction.HasDrop()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "Drop is not present");
            return result;
        }

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = Transaction.GetDrop().GetName();

        YDB_LOG_NOTICE_CTX(context.Ctx, "TDropLocalIndex Propose /",
            {"path", parentPathStr},
            {"name", name},
            {"pathId", Transaction.GetDrop().GetId()},
            {"operationId", OperationId},
            {"atSchemeshard", ssId});

        TPath index = Transaction.GetDrop().HasId()
            ? TPath::Init(context.SS->MakeLocalId(Transaction.GetDrop().GetId()), context.SS)
            : TPath::Resolve(parentPathStr, context.SS).Dive(name);

        {
            TPath::TChecker checks = index.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsTableIndex()
                .NotUnderDeleting()
                .NotUnderOperation();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        TPath parentColumnTable = index.Parent();
        {
            TPath::TChecker checks = parentColumnTable.Check();
            checks
                .NotEmpty()
                .IsResolved()
                .NotDeleted()
                .IsColumnTable()
                .IsUnderOperation()
                .IsUnderTheSameOperation(OperationId.GetTxId());

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        Y_ABORT_UNLESS(context.SS->Indexes.contains(index.Base()->PathId));

        auto guard = context.DbGuard();
        context.MemChanges.GrabPath(context.SS, index.Base()->PathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);

        context.DbChanges.PersistPath(index.Base()->PathId);
        context.DbChanges.PersistTxState(OperationId);

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxDropLocalIndex, index.Base()->PathId);
        txState.MinStep = TStepId(1);
        txState.State = TTxState::Propose;

        index.Base()->PathState = TPathElement::EPathState::EPathStateDrop;
        index.Base()->DropTxId = OperationId.GetTxId();
        index.Base()->LastTxId = OperationId.GetTxId();

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        YDB_LOG_NOTICE_CTX(context.Ctx, "TDropLocalIndex AbortPropose",
            {"opId", OperationId},
            {"atSchemeshard", context.SS->TabletID()});
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        YDB_LOG_NOTICE_CTX(context.Ctx, "TDropLocalIndex AbortUnsafe",
            {"opId", OperationId},
            {"forceDropId", forceDropTxId},
            {"atSchemeshard", context.SS->TabletID()});

        context.OnComplete.DoneOperation(OperationId);
    }
};

} // anonymous namespace

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateDropColumnTableLocalIndex(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TDropLocalIndex>(id, tx);
}

ISubOperation::TPtr CreateDropColumnTableLocalIndex(TOperationId id, TTxState::ETxState state) {
    return MakeSubOperation<TDropLocalIndex>(id, state);
}

} // namespace NKikimr::NSchemeShard
