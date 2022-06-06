#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_path_element.h"

#include "schemeshard_impl.h"

#include <ydb/core/base/path.h>
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

    Y_VERIFY(txState.PlanStep);
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
    context.OnComplete.PublishToSchemeBoard(operationId, parentDir->PathId);

    context.SS->ClearDescribePathCaches(path.Base());
    context.OnComplete.PublishToSchemeBoard(operationId, path->PathId);


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
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxDropTableIndex);
        Y_VERIFY(txState->State == TTxState::Propose);

        NIceDb::TNiceDb db(context.GetDB());

        txState->PlanStep = step;
        context.SS->PersistTxPlanStep(db, OperationId, step);

        Y_VERIFY_S(context.SS->PathsById.contains(txState->TargetPathId), "Unknown pathId: " << txState->TargetPathId);
        TPath path = TPath::Init(txState->TargetPathId, context.SS);
        Y_VERIFY(path.IsResolved());

        if (context.SS->EnableSchemeTransactionsAtSchemeShard) {
            // only persist step, but do not set it for the path
            NextState = TTxState::WaitShadowPathPublication;
            context.SS->ChangeTxState(db, OperationId, TTxState::WaitShadowPathPublication);
            return true;
        }

        DropPath(db, context, OperationId, *txState, path);

        NextState = TTxState::Done;
        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << context.SS->TabletID());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxDropTableIndex);

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

        Y_VERIFY(ActivePathId == ev->Get()->PathId);

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->ChangeTxState(db, OperationId, TTxState::DeletePathBarrier);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        context.OnComplete.RouteByTabletsFromOperation(OperationId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);

        Y_VERIFY(txState->PlanStep);

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
        Y_VERIFY(activePath.IsResolved());

        Y_VERIFY(activePath != path);

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
        Y_VERIFY(txState);

        TPath path = TPath::Init(txState->TargetPathId, context.SS);

        DropPath(db, context, OperationId, *txState, path);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        context.OnComplete.RouteByTabletsFromOperation(OperationId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", operation type: " << TTxState::TypeName(txState->TxType)
                               << ", at tablet" << ssId);

        context.OnComplete.Barrier(OperationId, "RenamePathBarrier");

        return false;
    }
};


class TDropTableIndex: public TSubOperation {
    const TOperationId OperationId;
    const TTxTransaction Transaction;
    TTxState::ETxState State = TTxState::Invalid;
    TTxState::ETxState AfterPropose = TTxState::Invalid;

    TTxState::ETxState NextState() {
        return TTxState::Propose;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) {
        switch(state) {
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

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) {
        switch(state) {
        case TTxState::Propose:
            return THolder(new TPropose(OperationId, AfterPropose));
        case TTxState::WaitShadowPathPublication:
            return THolder(new TWaitRenamedPathPublication(OperationId));
        case TTxState::DeletePathBarrier:
            return THolder(new TDeletePathBarrier(OperationId));
        case TTxState::Done:
            return THolder(new TDone(OperationId));
        default:
            return nullptr;
        }
    }

    void StateDone(TOperationContext& context) override {
        State = NextState(State);

        if (State != TTxState::Invalid) {
            SetState(SelectStateFunc(State));
            context.OnComplete.ActivateTx(OperationId);
        }
    }

public:
    TDropTableIndex(TOperationId id, const TTxTransaction& tx)
        : OperationId(id)
        , Transaction(tx)
    {
    }

    TDropTableIndex(TOperationId id, TTxState::ETxState state)
        : OperationId(id)
        , State(state)
    {
        SetState(SelectStateFunc(state));
    }

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
                TString explain = TStringBuilder() << "path table index fail checks"
                                                   << ", path: " << index.PathString();
                auto status = checks.GetStatus(&explain);
                result->SetError(status, explain);
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
                TString explain = TStringBuilder() << "path table index fail checks"
                                                   << ", path: " << parentTable.PathString();
                auto status = checks.GetStatus(&explain);
                result->SetError(status, explain);
                return result;
            }
        }

        Y_VERIFY(context.SS->Indexes.contains(index.Base()->PathId));

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

        State = NextState();
        SetState(SelectStateFunc(State));
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

namespace NKikimr {
namespace NSchemeShard {

ISubOperationBase::TPtr CreateDropTableIndex(TOperationId id, const TTxTransaction& tx) {
    return new TDropTableIndex(id, tx);
}

ISubOperationBase::TPtr CreateDropTableIndex(TOperationId id, TTxState::ETxState state) {
    return new TDropTableIndex(id, state);
}


TVector<ISubOperationBase::TPtr> CreateDropIndexedTable(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    Y_VERIFY(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpDropTable);

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
            .NotDeleted()
            .IsTable()
            .NotUnderDeleting()
            .NotUnderOperation()
            .IsCommonSensePath();

        if (!checks) {
            TString explain = TStringBuilder() << "path table fail checks"
                                               << ", path: " << table.PathString();
            auto status = checks.GetStatus(&explain);

            THolder<TProposeResponse> result = MakeHolder<TEvSchemeShard::TEvModifySchemeTransactionResult>(
                NKikimrScheme::StatusAccepted, 0, 0);

            result->SetError(status, explain);

            if (table.IsResolved() && table.Base()->IsTable() && (table.Base()->PlannedToDrop() || table.Base()->Dropped())) {
                result->SetPathDropTxId(ui64(table.Base()->DropTxId));
                result->SetPathId(table.Base()->PathId.LocalPathId);
            }

            return {CreateReject(nextId, std::move(result))};
        }
    }

    TVector<ISubOperationBase::TPtr> result;
    result.push_back(CreateDropTable(NextPartId(nextId, result), tx));

    for (const auto& [childName, childPathId] : table.Base()->GetChildren()) {
        TPath child = table.Child(childName);
        {
            TPath::TChecker checks = child.Check();
            checks
                .NotEmpty()
                .IsResolved();

            if (checks) {
                if (child.IsDeleted()) {
                    continue;
                }
            }

            if (child.IsTableIndex()) {
                checks.IsTableIndex();
            } else if (child.IsCdcStream()) {
                checks.IsCdcStream();
            } else if (child.IsSequence()) {
                checks.IsSequence();
            }

            checks.NotDeleted()
                .NotUnderDeleting()
                .NotUnderOperation();

            if (!checks) {
                TString explain = TStringBuilder() << "path index fail checks"
                                                   << ", index path: " << child.PathString();
                auto status = checks.GetStatus(&explain);
                return {CreateReject(nextId, status, explain)};
            }
        }
        Y_VERIFY(child.Base()->PathId == childPathId);

        if (child.IsSequence()) {
            auto dropSequence = TransactionTemplate(table.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropSequence);
            dropSequence.MutableDrop()->SetName(ToString(child->Name));

            result.push_back(CreateDropSequence(NextPartId(nextId, result), dropSequence));
            continue;
        } else if (child.IsTableIndex()) {
            auto dropIndex = TransactionTemplate(table.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropTableIndex);
            dropIndex.MutableDrop()->SetName(ToString(child.Base()->Name));

            result.push_back(CreateDropTableIndex(NextPartId(nextId, result), dropIndex));
        } else if (child.IsCdcStream()) {
            auto dropStream = TransactionTemplate(table.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStreamImpl);
            dropStream.MutableDrop()->SetName(ToString(child.Base()->Name));

            result.push_back(CreateDropCdcStreamImpl(NextPartId(nextId, result), dropStream));
        }

        Y_VERIFY(child.Base()->GetChildren().size() == 1);
        for (auto& [implName, implPathId] : child.Base()->GetChildren()) {
            Y_VERIFY(implName == "indexImplTable" || implName == "streamImpl",
                "unexpected name %s", implName.c_str());

            TPath implPath = child.Child(implName);
            {
                TPath::TChecker checks = implPath.Check();
                checks
                    .NotEmpty()
                    .IsResolved()
                    .NotDeleted()
                    .NotUnderDeleting()
                    .NotUnderOperation();

                if (checks) {
                    if (implPath.Base()->IsTable()) {
                        checks
                            .IsTable()
                            .IsInsideTableIndexPath();
                    } else if (implPath.Base()->IsPQGroup()) {
                        checks
                            .IsPQGroup()
                            .IsInsideCdcStreamPath();
                    }
                }

                if (!checks) {
                    TString explain = TStringBuilder() << "path impl table fail checks"
                                                       << ", index path: " << implPath.PathString();
                    auto status = checks.GetStatus(&explain);
                    return {CreateReject(nextId, status, explain)};
                }
            }
            Y_VERIFY(implPath.Base()->PathId == implPathId);

            if (implPath.Base()->IsTable()) {
                auto dropIndexTable = TransactionTemplate(child.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropTable);
                dropIndexTable.MutableDrop()->SetName(ToString(implPath.Base()->Name));

                result.push_back(CreateDropTable(NextPartId(nextId, result), dropIndexTable));
            } else if (implPath.Base()->IsPQGroup()) {
                auto dropPQGroup = TransactionTemplate(child.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropPersQueueGroup);
                dropPQGroup.MutableDrop()->SetName(ToString(implPath.Base()->Name));

                result.push_back(CreateDropPQ(NextPartId(nextId, result), dropPQGroup));
            }
        }
    }

    return result;
}

}
}
