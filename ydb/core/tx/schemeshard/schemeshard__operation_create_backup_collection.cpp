#include "schemeshard__operation_common.h"
#include "schemeshard__operation_common_resource_pool.h"
#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

namespace {

TPath::TChecker IsParentPathValid(const TPath& parentPath) {
    auto checks = parentPath.Check();
    checks.NotUnderDomainUpgrade()
        .IsAtLocalSchemeShard()
        .IsResolved()
        .NotDeleted()
        .NotUnderDeleting()
        .IsCommonSensePath()
        .IsLikeDirectory();

    return std::move(checks);
}

bool IsParentPathValid(const THolder<TProposeResponse>& result, const TPath& parentPath) {
    const auto checks = IsParentPathValid(parentPath);
    if (!checks) {
        result->SetError(checks.GetStatus(), checks.GetError());
    }

    return static_cast<bool>(checks);
}

///

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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateResourcePool);

        const TPathId& pathId = txState->TargetPathId;
        const TPath& path = TPath::Init(pathId, context.SS);
        const TPathElement::TPtr pathPtr = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        context.SS->TabletCounters->Simple()[COUNTER_RESOURCE_POOL_COUNT].Add(1);

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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateResourcePool);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

private:
    TString DebugHint() const override {
        return TStringBuilder() << "TCreateResourcePool TPropose, operationId: " << OperationId << ", ";
    }

private:
    const TOperationId OperationId;
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

    static bool IsDestinationPathValid(const THolder<TProposeResponse>& result, const TPath& dstPath, const TString& acl, bool acceptExisted) {
        const auto checks = dstPath.Check();
        checks.IsAtLocalSchemeShard();
        if (dstPath.IsResolved()) {
            checks
                .IsResolved()
                .NotUnderDeleting()
                .FailOnExist(TPathElement::EPathType::EPathTypeBackupCollection, acceptExisted);
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

    TPathElement::TPtr CreateBackupCollectionPathElement(const TPath& dstPath) const {
        TPathElement::TPtr backupCollection = dstPath.Base();

        backupCollection->CreateTxId = OperationId.GetTxId();
        backupCollection->PathType = TPathElement::EPathType::EPathTypeBackupCollection;
        backupCollection->PathState = TPathElement::EPathState::EPathStateCreate;
        backupCollection->LastTxId  = OperationId.GetTxId();

        return backupCollection;
    }

    static void UpdatePathSizeCounts(const TPath& parentPath, const TPath& dstPath) {
        dstPath.DomainInfo()->IncPathsInside();
        parentPath.Base()->IncAliveChildren();
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        const TString& rootPathStr = Transaction.GetWorkingDir();
        const auto& desc = Transaction.GetCreateBackupCollection();
        const TString& name = desc.GetName();
        LOG_N("TCreateBackupCollection Propose: opId# " << OperationId << ", path# " << rootPathStr << "/" << name);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted,
                                                   static_cast<ui64>(OperationId.GetTxId()),
                                                   static_cast<ui64>(context.SS->SelfTabletId()));

        bool backupServiceEnabled = AppData()->FeatureFlags.GetEnableBackupService();
        if (!backupServiceEnabled) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, "Backup collections are disabled. Please contact your system administrator to enable it");
            return result;
        }

        const TPath& rootPath = TPath::Resolve(rootPathStr, context.SS);
        {
            const auto checks = rootPath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsCommonSensePath()
                .IsLikeDirectory()
                .FailOnRestrictedCreateInTempZone();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        TPath dstPath = rootPath.Child(name);

        const TString& backupCollectionsDir = JoinPath({rootPath.GetDomainPathString(), ".backups/collections"});

        if (!dstPath.PathString().StartsWith(backupCollectionsDir + "/")) {
            result->SetError(NKikimrScheme::EStatus::StatusSchemeError, TStringBuilder() << "Backup collections must be placed in " << backupCollectionsDir);
            return result;
        }

        const TPath& backupCollectionsPath = TPath::Resolve(backupCollectionsDir, context.SS);

        // FIXME
        if (backupCollectionsPath.IsResolved()) {
            RETURN_RESULT_UNLESS(IsParentPathValid(result, backupCollectionsPath));
        }

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        AddPathInSchemeShard(result, dstPath, owner);
        auto pathEl = CreateBackupCollectionPathElement(dstPath);

        context.SS->IncrementPathDbRefCount(dstPath->PathId);
        rootPath->IncAliveChildren();
        rootPath.DomainInfo()->IncPathsInside();

        auto backupCollection = TBackupCollectionInfo::Create(desc);
        context.SS->BackupCollections[dstPath->PathId] = backupCollection;
        context.SS->TabletCounters->Simple()[COUNTER_BACKUP_COLLECTION_COUNT].Add(1);

        // in progress

        TTxState& txState = context.SS->CreateTx(
            OperationId,
            TTxState::TxCreateBackupCollection,
            pathEl->PathId);
        txState.Shards.clear();

        NIceDb::TNiceDb db(context.GetDB());

        const TString& acl = Transaction.GetModifyACL().GetDiffACL();
        if (!acl.empty()) {
            dstPath->ApplyACL(acl);
        }

        // context.SS->PersistPath(db, dstPath->PathId);
        ////
        // RegisterParentPathDependencies(context, parentPath);
        if (rootPath.Base()->HasActiveChanges()) {
            const TTxId parentTxId = rootPath.Base()->PlannedToCreate()
                                         ? rootPath.Base()->CreateTxId
                                         : rootPath.Base()->LastTxId;
            context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
        }

        // AdvanceTransactionStateToPropose(context, db);

        // PersistExternalDataSource(context, db, externalDataSource,
        //                           externalDataSourceInfo, acl);

        // IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId,
        //                                                   dstPath,
        //                                                   context.SS,
        //                                                   context.OnComplete);

        // UpdatePathSizeCounts(parentPath, dstPath);

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

ISubOperation::TPtr CreateNewBackupCollection(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateBackupCollection>(id, tx);
}

ISubOperation::TPtr CreateNewBackupCollection(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return MakeSubOperation<TCreateBackupCollection>(id, state);
}

}  // namespace NKikimr::NSchemeShard
