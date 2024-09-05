#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_path_element.h"

#include "schemeshard_impl.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/table_vector_index.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

void DropPath(NIceDb::TNiceDb& db, TOperationContext& context,
              TOperationId operationId, const TTxState& txState, TPath& path)
{
    if (path->Dropped()) {
        // it might be dropped
        // when rolling update goes
        // old code drop the path early at TDropParts
        // and new code try do it once more time for sure
        return;
    }

    context.SS->TabletCounters->Simple()[COUNTER_TABLE_INDEXES_COUNT].Sub(1);

    Y_ABORT_UNLESS(txState.PlanStep);
    path->SetDropped(txState.PlanStep, operationId.GetTxId());
    context.SS->PersistDropStep(db, path->PathId, txState.PlanStep, operationId);
    context.SS->PersistRemoveTableIndex(db, path->PathId);

    context.SS->TabletCounters->Simple()[COUNTER_USER_ATTRIBUTES_COUNT].Sub(path->UserAttrs->Size());
    context.SS->PersistUserAttributes(db, path->PathId, path->UserAttrs, nullptr);

    auto domainInfo = context.SS->ResolveDomainInfo(path->PathId);
    domainInfo->DecPathsInside();

    auto parentDir = path.Parent();
    parentDir->DecAliveChildren();
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
    TTxState::ETxState& NextState;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TDropTableIndex TPropose"
            << ", operationId: " << OperationId;
    }

public:
    TPropose(TOperationId id, TTxState::ETxState& nextState)
        : OperationId(id)
        , NextState(nextState)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", step: " << step
                               << ", at schemeshard: " << context.SS->TabletID());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropTableIndex);
        Y_ABORT_UNLESS(txState->State == TTxState::Propose);

        NIceDb::TNiceDb db(context.GetDB());

        txState->PlanStep = step;
        context.SS->PersistTxPlanStep(db, OperationId, step);

        Y_VERIFY_S(context.SS->PathsById.contains(txState->TargetPathId), "Unknown pathId: " << txState->TargetPathId);
        TPath path = TPath::Init(txState->TargetPathId, context.SS);
        Y_ABORT_UNLESS(path.IsResolved());


        NextState = TTxState::WaitShadowPathPublication;
        context.SS->ChangeTxState(db, OperationId, TTxState::WaitShadowPathPublication);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << context.SS->TabletID());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropTableIndex);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

class TWaitRenamedPathPublication: public TSubOperationState {
private:
    TOperationId OperationId;

    TPathId ActivePathId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TDropTableIndex TWaitRenamedPathPublication"
                << " operationId: " << OperationId;
    }

public:
    TWaitRenamedPathPublication(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvPrivate::TEvOperationPlan::EventType});
    }

    bool HandleReply(TEvPrivate::TEvCompletePublication::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvPrivate::TEvCompletePublication"
                               << ", msg: " << ev->Get()->ToString()
                               << ", at tablet" << ssId);

        Y_ABORT_UNLESS(ActivePathId == ev->Get()->PathId);

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->ChangeTxState(db, OperationId, TTxState::DeletePathBarrier);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        context.OnComplete.RouteByTabletsFromOperation(OperationId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        Y_ABORT_UNLESS(txState->PlanStep);

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", operation type: " << TTxState::TypeName(txState->TxType)
                               << ", at tablet" << ssId);

        TPath path = TPath::Init(txState->TargetPathId, context.SS);
        if (path.IsActive()) {
            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " ProgressState"
                                    << ", no renaming has been detected for this operation");

            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::DeletePathBarrier);
            return true;
        }

        auto activePath = TPath::Resolve(path.PathString(), context.SS);
        Y_ABORT_UNLESS(activePath.IsResolved());

        Y_ABORT_UNLESS(activePath != path);

        ActivePathId = activePath->PathId;
        context.OnComplete.PublishAndWaitPublication(OperationId, activePath->PathId);

        return false;
    }
};

class TDeletePathBarrier: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TDropTableIndex TDeletePathBarrier"
                << " operationId: " << OperationId;
    }

public:
    TDeletePathBarrier(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType, TEvDataShard::TEvProposeTransactionResult::EventType, TEvPrivate::TEvOperationPlan::EventType});
    }

    bool HandleReply(TEvPrivate::TEvCompleteBarrier::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvPrivate::TEvCompleteBarrier"
                               << ", msg: " << ev->Get()->ToString()
                               << ", at tablet" << ssId);

        NIceDb::TNiceDb db(context.GetDB());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        TPath path = TPath::Init(txState->TargetPathId, context.SS);

        DropPath(db, context, OperationId, *txState, path);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        context.OnComplete.RouteByTabletsFromOperation(OperationId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", operation type: " << TTxState::TypeName(txState->TxType)
                               << ", at tablet" << ssId);

        context.OnComplete.Barrier(OperationId, "RenamePathBarrier");

        return false;
    }
};

class TDropTableIndex: public TSubOperation {
    TTxState::ETxState AfterPropose = TTxState::Invalid;

    static TTxState::ETxState NextState() {
        return TTxState::Propose;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Propose:
            return AfterPropose;
        case TTxState::WaitShadowPathPublication:
            return TTxState::DeletePathBarrier;
        case TTxState::DeletePathBarrier:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId, AfterPropose);
        case TTxState::WaitShadowPathPublication:
            return MakeHolder<TWaitRenamedPathPublication>(OperationId);
        case TTxState::DeletePathBarrier:
            return MakeHolder<TDeletePathBarrier>(OperationId);
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

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = Transaction.GetDrop().GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TDropTableIndex Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", pathId: " << Transaction.GetDrop().GetId()
                         << ", operationId: " << OperationId
                         << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        if (!Transaction.HasDrop()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "Drop is not present");
            return result;
        }

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

        TPath parentTable = index.Parent();
        {
            TPath::TChecker checks = parentTable.Check();
            checks
                .NotEmpty()
                .IsResolved()
                .NotDeleted()
                .IsTable()
                .IsUnderOperation()
                .IsUnderTheSameOperation(OperationId.GetTxId()); // allowed only as part of consistent operations

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

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxDropTableIndex, index.Base()->PathId);
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
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TDropTableIndex AbortPropose"
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << context.SS->TabletID());
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TDropTableIndex AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateDropTableIndex(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TDropTableIndex>(id, tx);
}

ISubOperation::TPtr CreateDropTableIndex(TOperationId id, TTxState::ETxState state) {
    return MakeSubOperation<TDropTableIndex>(id, state);
}

TVector<ISubOperation::TPtr> CreateDropIndexedTable(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpDropTable);

    auto dropOperation = tx.GetDrop();

    const TString parentPathStr = tx.GetWorkingDir();
    const TString name = dropOperation.GetName();

    TPath table = dropOperation.HasId()
        ? TPath::Init(TPathId(context.SS->TabletID(), dropOperation.GetId()), context.SS)
        : TPath::Resolve(parentPathStr, context.SS).Dive(name);

    {
        TPath::TChecker checks = table.Check();
        checks
            .NotEmpty()
            .IsResolved()
            .NotDeleted();

        if (checks) {
            if (table.Base()->IsColumnTable()) {
                checks
                    .IsColumnTable()
                    .NotUnderDeleting()
                    .NotUnderOperation()
                    .IsCommonSensePath();

                if (checks) {
                    // DROP TABLE statement has no info is it a drop of row or column table
                    return {CreateDropColumnTable(nextId, tx)};
                }
            } else {
                checks
                    .IsTable()
                    .NotUnderDeleting()
                    .NotUnderOperation()
                    .IsCommonSensePath();
            }
        }

        if (!checks) {
            auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, 0, 0);
            result->SetError(checks.GetStatus(), checks.GetError());
            if (table.IsResolved() && table.Base()->IsTable() && (table.Base()->PlannedToDrop() || table.Base()->Dropped())) {
                result->SetPathDropTxId(ui64(table.Base()->DropTxId));
                result->SetPathId(table.Base()->PathId.LocalPathId);
            }

            return {CreateReject(nextId, std::move(result))};
        }
    }

    TVector<ISubOperation::TPtr> result;
    result.push_back(CreateDropTable(NextPartId(nextId, result), tx));
    if (auto reject = CascadeDropTableChildren(result, nextId, table)) {
        return {reject};
    }

    return result;
}

}
