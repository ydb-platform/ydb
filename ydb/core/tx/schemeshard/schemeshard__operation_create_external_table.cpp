#include "schemeshard__operation_common_external_table.h"
#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <utility>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TPropose: public TSubOperationState {
private:
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TCreateExternalTable TPropose"
            << ", operationId: " << OperationId;
    }

    void IncrementExternalTableCounter(const TOperationContext& context) const {
        context.SS->TabletCounters->Simple()[COUNTER_EXTERNAL_TABLE_COUNT].Add(1);
    }

    void SetAndPersistCreateStep(const TOperationContext& context,
                                 NIceDb::TNiceDb& db,
                                 const TPath& path,
                                 const TPathId& pathId,
                                 const TStepId& step) const {
        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);
    }

    void ClearDescribePathCaches(const TOperationContext& context,
                                 const TPathElement::TPtr& pathPtr,
                                 const TPathElement::TPtr& dataSourcePathPtr) const {
        context.SS->ClearDescribePathCaches(pathPtr);
        context.SS->ClearDescribePathCaches(dataSourcePathPtr);
    }

    void PublishToSchemeBoard(const TOperationContext& context,
                              const TPathId& pathId,
                              const TPathId& dataSourcePathId) const {
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);
        context.OnComplete.PublishToSchemeBoard(OperationId, dataSourcePathId);
    }

public:
    explicit TPropose(TOperationId id)
        : OperationId(std::move(id)) { }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const TStepId step = TStepId(ev->Get()->StepId);

        LOG_I(DebugHint() << " HandleReply TEvOperationPlan"
            << ": step# " << step);

        const TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateExternalTable);

        const auto pathId                = txState->TargetPathId;
        const auto dataSourcePathId      = txState->SourcePathId;
        const auto path                  = TPath::Init(pathId, context.SS);
        const TPathElement::TPtr pathPtr = context.SS->PathsById.at(pathId);
        const TPathElement::TPtr dataSourcePathPtr =
            context.SS->PathsById.at(dataSourcePathId);

        IncrementExternalTableCounter(context);

        NIceDb::TNiceDb db(context.GetDB());

        SetAndPersistCreateStep(context, db, path, pathId, step);

        IncParentDirAlterVersionWithRepublish(OperationId, path, context);

        ClearDescribePathCaches(context, pathPtr, dataSourcePathPtr);
        PublishToSchemeBoard(context, pathId, dataSourcePathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << " ProgressState");

        const TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateExternalTable);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};


class TCreateExternalTable: public TSubOperation {
private:
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

    static bool IsDestinationPathValid(const THolder<TProposeResponse>& result,
                                       const TPath& dstPath,
                                       const TString& acl,
                                       bool acceptExisted) {
        const auto checks = dstPath.Check();
        checks.IsAtLocalSchemeShard();
        if (dstPath.IsResolved()) {
            checks
                .IsResolved()
                .NotUnderDeleting()
                .FailOnExist(TPathElement::EPathType::EPathTypeExternalTable, acceptExisted);
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

    static bool IsDataSourcePathValid(const THolder<TProposeResponse>& result, const TPath& dataSourcePath) {
        const auto checks = dataSourcePath.Check();
        checks
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .NotUnderDeleting()
            .IsCommonSensePath()
            .IsExternalDataSource()
            .NotUnderOperation();

        if (!checks) {
            result->SetError(checks.GetStatus(), checks.GetError());
        }

        return static_cast<bool>(checks);
    }

    bool IsApplyIfChecksPassed(const THolder<TProposeResponse>& result,
                               const TOperationContext& context) const {
        TString errorMessage;
        if (!context.SS->CheckApplyIf(Transaction, errorMessage)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errorMessage);
            return false;
        }

        return true;
    }

    static bool IsDataSourceValid(const THolder<TProposeResponse>& result,
                                  const TExternalDataSourceInfo::TPtr& externalDataSource) {
        if (!externalDataSource) {
            result->SetError(NKikimrScheme::StatusSchemeError, "Data source doesn't exist");
            return false;
        }
        return true;
    }

    static bool IsExternalTableDescriptionValid(
        const THolder<TProposeResponse>& result,
        const TString& sourceType,
        const NKikimrSchemeOp::TExternalTableDescription& desc) {
        if (TString errorMessage; !NExternalTable::Validate(sourceType, desc, errorMessage)) {
            result->SetError(NKikimrScheme::StatusSchemeError, errorMessage);
            return false;
        }

        return true;
    }

    static void AddPathInSchemeShard(const THolder<TProposeResponse> &result,
                                     TPath &dstPath, const TString &owner) {
        dstPath.MaterializeLeaf(owner);
        result->SetPathId(dstPath.Base()->PathId.LocalPathId);
    }

    TPathElement::TPtr CreateExternalTablePathElement(const TPath& dstPath) const {
        TPathElement::TPtr externalTable = dstPath.Base();
        externalTable->CreateTxId        = OperationId.GetTxId();
        externalTable->PathType  = TPathElement::EPathType::EPathTypeExternalTable;
        externalTable->PathState = TPathElement::EPathState::EPathStateCreate;
        externalTable->LastTxId  = OperationId.GetTxId();

        return externalTable;
    }

    void CreateTransaction(const TOperationContext &context,
                           const TPathId &externalTablePathId,
                           const TPathId &externalDataSourcePathId) const {
      TTxState &txState =
          context.SS->CreateTx(OperationId, TTxState::TxCreateExternalTable,
                               externalTablePathId, externalDataSourcePathId);
      txState.Shards.clear();
    }

    void RegisterParentPathDependencies(const TOperationContext& context,
                                        const TPath& parentPath) const {
        if (parentPath.Base()->HasActiveChanges()) {
            const auto parentTxId = parentPath.Base()->PlannedToCreate()
                                   ? parentPath.Base()->CreateTxId
                                   : parentPath.Base()->LastTxId;
            context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
        }
    }

    void AdvanceTransactionStateToPropose(const TOperationContext& context,
                                          NIceDb::TNiceDb& db) const {
        context.SS->ChangeTxState(db, OperationId, TTxState::Propose);
        context.OnComplete.ActivateTx(OperationId);
    }

    static void LinkExternalDataSourceWithExternalTable(
        const TExternalDataSourceInfo::TPtr& externalDataSource,
        const TPathElement::TPtr& externalTable,
        const TPath& dstPath) {
        auto& reference = *externalDataSource->ExternalTableReferences.AddReferences();
        reference.SetPath(dstPath.PathString());
        PathIdFromPathId(externalTable->PathId, reference.MutablePathId());
    }

    void PersistExternalTable(
        const TOperationContext& context,
        NIceDb::TNiceDb& db,
        const TPathElement::TPtr& externalTable,
        const TExternalTableInfo::TPtr& externalTableInfo,
        const TPathId& externalDataSourcePathId,
        const TExternalDataSourceInfo::TPtr& externalDataSource,
        const TString& acl) const {
        context.SS->ExternalTables[externalTable->PathId] = externalTableInfo;
        context.SS->IncrementPathDbRefCount(externalTable->PathId);


        if (!acl.empty()) {
            externalTable->ApplyACL(acl);
        }
        context.SS->PersistPath(db, externalTable->PathId);

        context.SS->PersistExternalDataSource(db, externalDataSourcePathId, externalDataSource);
        context.SS->PersistExternalTable(db, externalTable->PathId, externalTableInfo);
        context.SS->PersistTxState(db, OperationId);
    }

    static void UpdatePathSizeCounts(const TPath& parentPath,
                                     const TPath& dstPath) {
        dstPath.DomainInfo()->IncPathsInside();
        parentPath.Base()->IncAliveChildren();
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        const auto ssId = context.SS->SelfTabletId();

        const auto acceptExisted = !Transaction.GetFailOnExist();
        const TString& parentPathStr = Transaction.GetWorkingDir();
        const auto& externalTableDescription = Transaction.GetCreateExternalTable();
        const TString& name = externalTableDescription.GetName();

        LOG_N("TCreateExternalTable Propose"
              << ": opId# " << OperationId << ", path# " << parentPathStr << "/" << name);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted,
                                                   static_cast<ui64>(OperationId.GetTxId()),
                                                   static_cast<ui64>(ssId));

        const auto parentPath = TPath::Resolve(parentPathStr, context.SS);
        RETURN_RESULT_UNLESS(NExternalTable::IsParentPathValid(result, parentPath));

        const TString acl = Transaction.GetModifyACL().GetDiffACL();
        TPath dstPath     = parentPath.Child(name);
        RETURN_RESULT_UNLESS(IsDestinationPathValid(
            result, dstPath, acl, acceptExisted));

        const auto dataSourcePath =
            TPath::Resolve(externalTableDescription.GetDataSourcePath(), context.SS);
        RETURN_RESULT_UNLESS(IsDataSourcePathValid(result, dataSourcePath));

        const auto externalDataSource =
            context.SS->ExternalDataSources.Value(dataSourcePath->PathId, nullptr);
        RETURN_RESULT_UNLESS(IsDataSourceValid(result, externalDataSource));

        RETURN_RESULT_UNLESS(IsApplyIfChecksPassed(result, context));

        RETURN_RESULT_UNLESS(IsExternalTableDescriptionValid(result,
                                                             externalDataSource->SourceType,
                                                             externalTableDescription));

        auto [externalTableInfo, maybeError] =
            NExternalTable::CreateExternalTable(externalDataSource->SourceType,
                                                externalTableDescription,
                                                context.SS->ExternalSourceFactory,
                                                1);
        if (maybeError) {
            result->SetError(NKikimrScheme::StatusSchemeError, *maybeError);
            return result;
        }
        Y_ABORT_UNLESS(externalTableInfo);

        AddPathInSchemeShard(result, dstPath, owner);

        const auto externalTable = CreateExternalTablePathElement(dstPath);
        CreateTransaction(context, externalTable->PathId, dataSourcePath->PathId);

        NIceDb::TNiceDb db(context.GetDB());

        RegisterParentPathDependencies(context, parentPath);
        AdvanceTransactionStateToPropose(context, db);

        LinkExternalDataSourceWithExternalTable(externalDataSource,
                                                externalTable,
                                                dstPath);

        PersistExternalTable(context,
                             db,
                             externalTable,
                             externalTableInfo,
                             dataSourcePath->PathId,
                             externalDataSource,
                             acl);

        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId,
                                                          dstPath,
                                                          context.SS,
                                                          context.OnComplete);

        UpdatePathSizeCounts(parentPath, dstPath);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TCreateExternalTable AbortPropose"
            << ": opId# " << OperationId);
        Y_ABORT("no AbortPropose for TCreateExternalTable");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TCreateExternalTable AbortUnsafe"
            << ": opId# " << OperationId
            << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

TVector<ISubOperation::TPtr> CreateNewExternalTable(TOperationId id, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateExternalTable);

    LOG_I("CreateNewExternalTable, opId " << id << ", feature flag EnableReplaceIfExistsForExternalEntities "
                                               << context.SS->EnableReplaceIfExistsForExternalEntities << ", tx "
                                               << tx.ShortDebugString());

    auto errorResult = [&id](NKikimrScheme::EStatus status, const TStringBuf& msg) -> TVector<ISubOperation::TPtr> {
        return {CreateReject(id, status, TStringBuilder() << "Invalid TCreateExternalTable request: " << msg)};
    };

    const auto &operation = tx.GetCreateExternalTable();
    const auto replaceIfExists = operation.GetReplaceIfExists();
    const TString &name = operation.GetName();

    if (replaceIfExists && !context.SS->EnableReplaceIfExistsForExternalEntities) {
        return errorResult(NKikimrScheme::StatusPreconditionFailed, "Unsupported: feature flag EnableReplaceIfExistsForExternalEntities is off");
    }

    const TString& parentPathStr = tx.GetWorkingDir();
    const TPath parentPath = TPath::Resolve(parentPathStr, context.SS);

    {
        const auto checks = NExternalTable::IsParentPathValid(parentPath);
        if (!checks) {
            return errorResult(checks.GetStatus(), checks.GetError());
        }
    }

    if (replaceIfExists) {
        const TPath dstPath = parentPath.Child(name);
        const auto isAlreadyExists =
            dstPath.Check()
                .IsResolved()
                .NotUnderDeleting();
        if (isAlreadyExists) {
            return {CreateAlterExternalTable(id, tx)};
        }
    }

    return {MakeSubOperation<TCreateExternalTable>(id, tx)};
}

ISubOperation::TPtr CreateNewExternalTable(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TCreateExternalTable>(std::move(id), state);
}

}
