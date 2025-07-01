#include "schemeshard__backup_collection_common.h"
#include "schemeshard__op_traits.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#define LOG_I(stream) LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)

namespace NKikimr::NSchemeShard {

namespace {

class TPropose: public TSubOperationState {
private:
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TCreateBackupCollection TPropose"
            << ", operationId: " << OperationId;
    }

public:
    explicit TPropose(TOperationId id)
        : OperationId(std::move(id))
    {
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << "ProgressState");

        const TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateBackupCollection);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const TStepId step = TStepId(ev->Get()->StepId);

        LOG_I(DebugHint() << "HandleReply TEvOperationPlan"
            << ": step# " << step);

        const TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateBackupCollection);

        const auto pathId = txState->TargetPathId;
        const auto path = TPath::Init(pathId, context.SS);
        const TPathElement::TPtr pathPtr = context.SS->PathsById.at(pathId);

        context.SS->TabletCounters->Simple()[COUNTER_BACKUP_COLLECTION_COUNT].Add(1);

        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        IncParentDirAlterVersionWithRepublish(OperationId, path, context);

        context.SS->ClearDescribePathCaches(pathPtr);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }
};

class TCreateBackupCollection : public TSubOperation {
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

    static void AddPathInSchemeShard(const THolder<TProposeResponse>& result, TPath& dstPath, const TString& owner) {
        dstPath.MaterializeLeaf(owner);
        result->SetPathId(dstPath.Base()->PathId.LocalPathId);
    }

    TPathElement::TPtr CreateBackupCollectionPathElement(const TPath& dstPath) const {
        TPathElement::TPtr backupCollection = dstPath.Base();

        backupCollection->CreateTxId = OperationId.GetTxId();
        backupCollection->PathType = TPathElement::EPathType::EPathTypeBackupCollection;
        backupCollection->PathState = TPathElement::EPathState::EPathStateCreate;
        backupCollection->LastTxId  = OperationId.GetTxId();

        return backupCollection;
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        const TString& rootPathStr = Transaction.GetWorkingDir();
        const auto& desc = Transaction.GetCreateBackupCollection();
        const TString& name = desc.GetName();
        const TString acl = Transaction.GetModifyACL().GetDiffACL();

        LOG_N("TCreateBackupCollection Propose: opId# " << OperationId << ", path# " << rootPathStr << "/" << name);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted,
                                                    static_cast<ui64>(OperationId.GetTxId()),
                                                    static_cast<ui64>(context.SS->SelfTabletId()));

        auto bcPaths = NBackup::ResolveBackupCollectionPaths(rootPathStr, name, true, context, result, false);
        if (!bcPaths) {
            return result;
        }

        auto& [rootPath, dstPath] = *bcPaths;

        {
            const auto checks = dstPath.Check();
            checks.IsAtLocalSchemeShard();
            if (dstPath.IsResolved()) {
                checks
                    .IsResolved()
                    .NotUnderDeleting()
                    .FailOnExist(TPathElement::EPathType::EPathTypeBackupCollection, false);
            } else {
                checks
                    .NotEmpty()
                    .NotResolved();
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
                return result;
            }
        }


        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        AddPathInSchemeShard(result, dstPath, owner);
        auto pathEl = CreateBackupCollectionPathElement(dstPath);

        IncAliveChildrenDirect(OperationId, rootPath, context); // for correct discard of ChildrenExist prop
        rootPath.DomainInfo()->IncPathsInside(context.SS);

        auto backupCollection = TBackupCollectionInfo::Create(desc);
        context.SS->BackupCollections[dstPath->PathId] = backupCollection;
        context.SS->TabletCounters->Simple()[COUNTER_BACKUP_COLLECTION_COUNT].Add(1);
        context.SS->CreateTx(
            OperationId,
            TTxState::TxCreateBackupCollection,
            pathEl->PathId);

        NIceDb::TNiceDb db(context.GetDB());

        if (rootPath.Base()->HasActiveChanges()) {
            const TTxId parentTxId = rootPath.Base()->PlannedToCreate()
                                         ? rootPath.Base()->CreateTxId
                                         : rootPath.Base()->LastTxId;
            context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
        }

        context.SS->ChangeTxState(db, OperationId, TTxState::Propose);
        context.OnComplete.ActivateTx(OperationId);

        const auto& backupCollectionPathId = pathEl->PathId;

        context.SS->BackupCollections[dstPath->PathId] = backupCollection;
        context.SS->IncrementPathDbRefCount(backupCollectionPathId);

        if (!acl.empty()) {
            pathEl->ApplyACL(acl);
        }
        context.SS->PersistPath(db, backupCollectionPathId);

        context.SS->PersistBackupCollection(db,
                                            backupCollectionPathId,
                                            backupCollection);
        context.SS->PersistTxState(db, OperationId);

        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId,
                                                          dstPath,
                                                          context.SS,
                                                          context.OnComplete);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TCreateBackupCollection AbortPropose: opId# " << OperationId);
        Y_ABORT("no AbortPropose for TCreateBackupCollection");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TCreateBackupCollection AbortUnsafe: opId# " << OperationId << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

}  // anonymous namespace

using TTag = TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateBackupCollection>;

namespace NOperation {

template <>
std::optional<TString> GetTargetName<TTag>(
    TTag,
    const TTxTransaction& tx)
{
    return tx.GetCreateBackupCollection().GetName();
}

template <>
bool SetName<TTag>(
    TTag,
    TTxTransaction& tx,
    const TString& name)
{
    tx.MutableCreateBackupCollection()->SetName(name);
    return true;
}

} // namespace NOperation

ISubOperation::TPtr CreateNewBackupCollection(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateBackupCollection>(id, tx);
}

ISubOperation::TPtr CreateNewBackupCollection(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return MakeSubOperation<TCreateBackupCollection>(id, state);
}

}  // namespace NKikimr::NSchemeShard
