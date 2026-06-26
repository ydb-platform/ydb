#include <ydb/core/tx/schemeshard/schemeshard__operation_common.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace {

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NTableIndex;

class TPropose: public TSubOperationState {
private:
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TCreateTableIndex TPropose"
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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateTableIndex);
        Y_ABORT_UNLESS(txState->State == TTxState::Propose);

        NIceDb::TNiceDb db(context.GetDB());

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);
        path->StepCreated = step;
        context.SS->PersistCreateStep(db, path->PathId, step);

        context.SS->TabletCounters->Simple()[COUNTER_TABLE_INDEXES_COUNT].Add(1);

        Y_ABORT_UNLESS(context.SS->Indexes.contains(path->PathId));
        TTableIndexInfo::TPtr indexData = context.SS->Indexes.at(path->PathId);
        context.SS->PersistTableIndex(db, path->PathId);
        context.SS->Indexes[path->PathId] = indexData->AlterData;

        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, path->PathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        YDB_LOG_INFO_CTX(context.Ctx, "ProgressState",
            {"debugHint", DebugHint()},
            {"atSchemeshard", context.SS->TabletID()});

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateTableIndex);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

class TCreateTableIndex: public TSubOperation {
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

        const auto acceptExisted = !Transaction.GetFailOnExist();
        const auto& tableIndexCreation = Transaction.GetCreateTableIndex();
        const bool internal = Transaction.HasInternal() && Transaction.GetInternal();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = tableIndexCreation.GetName();

        YDB_LOG_NOTICE_CTX(context.Ctx, "TCreateTableIndex Propose /",
            {"path", parentPathStr},
            {"name", name},
            {"operationId", OperationId},
            {"transaction", Transaction},
            {"atSchemeshard", ssId});

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        if (!Transaction.HasCreateTableIndex()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "CreateTableIndex is not present");
            return result;
        }

        if (!tableIndexCreation.HasName()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "Name is not present in CreateTableIndex");
            return result;
        }

        NSchemeShard::TPath parentPath = NSchemeShard::TPath::Resolve(parentPathStr, context.SS);
        {
            NSchemeShard::TPath::TChecker checks = parentPath.Check();
            checks
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsCommonSensePath()
                .IsTable();

            if (!internal) {
                checks.NotAsyncReplicaTable();
            }

            if (tableIndexCreation.GetState() == NKikimrSchemeOp::EIndexState::EIndexStateReady) {
                if (internal && TTableIndexInfo::IsLocalIndex(tableIndexCreation.GetType())) {
                    // Local indexes have no impl table. The parent may be under this same
                    // operation (CREATE/ALTER) or steady (migration), so only reject
                    // foreign concurrent operations.
                    if (parentPath.IsUnderOperation()) {
                        checks.IsUnderTheSameOperation(OperationId.GetTxId());
                    }
                } else {
                    checks
                        .IsUnderCreating(NKikimrScheme::StatusNameConflict)
                        .IsUnderTheSameOperation(OperationId.GetTxId()); //allow only as part of creating base table
                }
            } else {
                checks.NotBackupTable(); // allow to create backup table with index, but not to build index on a backup table
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        const TString acl = Transaction.GetModifyACL().GetDiffACL();

        NSchemeShard::TPath dstPath = parentPath.Child(name);
        {
            NSchemeShard::TPath::TChecker checks = dstPath.Check();
            if (dstPath.IsResolved()) {
                checks
                    .IsResolved()
                    .NotUnderDeleting()
                    .FailOnExist(TPathElement::EPathType::EPathTypeTableIndex, acceptExisted);
            } else {
                checks
                    .NotEmpty()
                    .NotResolved();
            }

            if (checks) {
                checks.PathsLimit();
                if (!internal) {
                    // Internal operations (e.g. index build auto-provisioning the unique index over
                    // __ydb_row_id) may legitimately create system-prefixed index names that the
                    // user-facing reserved-name check would reject.
                    checks.IsValidLeafName(context.UserToken.Get());
                }
                checks.IsValidACL(acl);
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

        if (!context.SS->CheckLocks(parentPath.Base()->PathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }

        TTableIndexInfo::TPtr newIndexData = nullptr;
        {
            newIndexData = TTableIndexInfo::Create(tableIndexCreation, errStr);
            if (!newIndexData) {
                result->SetError(TEvSchemeShard::EStatus::StatusInvalidParameter, errStr);
                return result;
            }
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

        // store table index description

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateTableIndex, newIndexPath->PathId);
        txState.State = TTxState::Propose;

        newIndexPath->PathState = NKikimrSchemeOp::EPathStateCreate;
        newIndexPath->CreateTxId = OperationId.GetTxId();
        newIndexPath->LastTxId = OperationId.GetTxId();
        newIndexPath->PathType = TPathElement::EPathType::EPathTypeTableIndex;

        context.SS->Indexes[newIndexPath->PathId] = newIndexData;
        context.SS->IncrementPathDbRefCount(newIndexPath->PathId);

        if (!acl.empty()) {
            newIndexPath->ApplyACL(acl);
        }

        context.OnComplete.ActivateTx(OperationId);

        dstPath.DomainInfo()->IncPathsInside(context.SS);
        IncAliveChildrenSafeWithUndo(OperationId, parentPath, context); // for correct discard of ChildrenExist prop

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        YDB_LOG_NOTICE_CTX(context.Ctx, "TCreateTableIndex AbortPropose",
            {"opId", OperationId},
            {"atSchemeshard", context.SS->TabletID()});
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        YDB_LOG_NOTICE_CTX(context.Ctx, "TCreateTableIndex AbortUnsafe",
            {"opId", OperationId},
            {"forceDropId", forceDropTxId},
            {"atSchemeshard", context.SS->TabletID()});

        context.OnComplete.DoneOperation(OperationId);
    }
};

} // anonymous namespace

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateNewTableIndex(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateTableIndex>(id, tx);
}

ISubOperation::TPtr CreateNewTableIndex(TOperationId id, TTxState::ETxState state) {
    return MakeSubOperation<TCreateTableIndex>(id, state);
}

}
