#include <ydb/core/tx/schemeshard/schemeshard__operation_common.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TPropose: public TSubOperationState {
private:
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TCreateLocalIndex TPropose"
            << " operationId# " << OperationId;
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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateLocalIndex);
        Y_ABORT_UNLESS(txState->State == TTxState::Propose);

        NIceDb::TNiceDb db(context.GetDB());

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);
        path->StepCreated = step;
        context.SS->PersistCreateStep(db, path->PathId, step);

        context.SS->TabletCounters->Simple()[COUNTER_TABLE_INDEXES_COUNT].Add(1);

        Y_ABORT_UNLESS(context.SS->Indexes.contains(path->PathId));
        TTableIndexInfo::TPtr indexData = context.SS->Indexes.at(path->PathId);
        Y_ABORT_UNLESS(indexData->AlterData, "AlterData must be valid after TTableIndexInfo::Create");
        context.SS->PersistTableIndex(db, path->PathId);
        context.SS->Indexes[path->PathId] = indexData->AlterData;

        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, path->PathId);

        TPathElement::TPtr parentDir = context.SS->PathsById.at(path->ParentPathId);
        ++parentDir->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDir);
        context.SS->ClearDescribePathCaches(parentDir);
        context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        YDB_LOG_INFO_CTX(context.Ctx, "ProgressState",
            {"debugHint", DebugHint()},
            {"atSchemeshard", context.SS->TabletID()});

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateLocalIndex);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

class TCreateLocalIndex: public TSubOperation {
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

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        if (!Transaction.HasCreateTableIndex()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "CreateTableIndex is not present");
            return result;
        }

        const auto& tableIndexCreation = Transaction.GetCreateTableIndex();
        const TString& parentPathStr = Transaction.GetWorkingDir();

        if (!tableIndexCreation.HasName()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "Name is not present in CreateTableIndex");
            return result;
        }

        const TString& name = tableIndexCreation.GetName();

        YDB_LOG_NOTICE_CTX(context.Ctx, "TCreateLocalIndex Propose /",
            {"path", parentPathStr},
            {"name", name},
            {"operationId", OperationId},
            {"atSchemeshard", ssId});

        NSchemeShard::TPath parentPath = NSchemeShard::TPath::Resolve(parentPathStr, context.SS);
        {
            NSchemeShard::TPath::TChecker checks = parentPath.Check();
            checks
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsColumnTable();

            if (tableIndexCreation.GetState() == NKikimrSchemeOp::EIndexState::EIndexStateReady) {
                if (Transaction.HasInternal() && Transaction.GetInternal()) {
                    // Internal callers: composite create/alter (parent under our same op)
                    // or the local-index migrator (parent steady). Reject only foreign ops.
                    if (parentPath.IsUnderOperation()) {
                        checks.IsUnderTheSameOperation(OperationId.GetTxId());
                    }
                } else {
                    checks
                        .IsUnderCreating(NKikimrScheme::StatusNameConflict)
                        .IsUnderTheSameOperation(OperationId.GetTxId());
                }
            } else {
                // For non-Ready states (e.g., standalone CreateLocalIndex requests),
                // validate that the parent table is not under another operation to prevent conflicts
                checks.NotUnderOperation();
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        NSchemeShard::TPath dstPath = parentPath.Child(name);
        {
            NSchemeShard::TPath::TChecker checks = dstPath.Check();
            if (dstPath.IsResolved()) {
                checks
                    .IsResolved()
                    .NotUnderDeleting()
                    .FailOnExist(TPathElement::EPathType::EPathTypeTableIndex, false);
            } else {
                checks
                    .NotEmpty()
                    .NotResolved();
            }

            if (checks) {
                checks
                    .PathsLimit()
                    .IsValidLeafName(context.UserToken.Get());
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
        TTableIndexInfo::TPtr newIndexData = TTableIndexInfo::Create(tableIndexCreation, errStr);
        if (!newIndexData) {
            result->SetError(TEvSchemeShard::EStatus::StatusInvalidParameter, errStr);
            return result;
        }

        auto guard = context.DbGuard();
        TPathId allocatedPathId = context.SS->AllocatePathId();
        context.MemChanges.GrabNewPath(context.SS, allocatedPathId);
        context.MemChanges.GrabPath(context.SS, parentPath.Base()->PathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.MemChanges.GrabNewIndex(context.SS, allocatedPathId);

        context.DbChanges.PersistPath(allocatedPathId);
        context.DbChanges.PersistPath(parentPath.Base()->PathId);
        context.DbChanges.PersistAlterIndex(allocatedPathId);
        context.DbChanges.PersistTxState(OperationId);

        dstPath.MaterializeLeaf(owner, allocatedPathId);
        result->SetPathId(dstPath.Base()->PathId.LocalPathId);

        auto newIndexPath = dstPath.Base();

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateLocalIndex, newIndexPath->PathId);
        txState.State = TTxState::Propose;

        newIndexPath->PathState = NKikimrSchemeOp::EPathStateCreate;
        newIndexPath->CreateTxId = OperationId.GetTxId();
        newIndexPath->LastTxId = OperationId.GetTxId();
        newIndexPath->PathType = TPathElement::EPathType::EPathTypeTableIndex;

        context.SS->Indexes[newIndexPath->PathId] = newIndexData;
        context.SS->IncrementPathDbRefCount(newIndexPath->PathId);

        context.OnComplete.ActivateTx(OperationId);

        dstPath.DomainInfo()->IncPathsInside(context.SS);
        IncAliveChildrenSafeWithUndo(OperationId, parentPath, context);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        YDB_LOG_NOTICE_CTX(context.Ctx, "TCreateLocalIndex AbortPropose",
            {"opId", OperationId},
            {"atSchemeshard", context.SS->TabletID()});
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        YDB_LOG_NOTICE_CTX(context.Ctx, "TCreateLocalIndex AbortUnsafe",
            {"opId", OperationId},
            {"forceDropId", forceDropTxId},
            {"atSchemeshard", context.SS->TabletID()});

        context.OnComplete.DoneOperation(OperationId);
    }
};

} // anonymous namespace

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateNewColumnTableLocalIndex(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateLocalIndex>(id, tx);
}

ISubOperation::TPtr CreateNewColumnTableLocalIndex(TOperationId id, TTxState::ETxState state) {
    return MakeSubOperation<TCreateLocalIndex>(id, state);
}

} // namespace NKikimr::NSchemeShard
