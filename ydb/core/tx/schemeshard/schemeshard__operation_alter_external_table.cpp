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
    bool IsSameDataSource = false;
    TPathId OldSourcePathId = InvalidPathId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TAlterExternalTable TPropose"
            << ", operationId: " << OperationId;
    }

    void ClearDescribePathCaches(const TOperationContext& context,
                                 const TPathElement::TPtr& pathPtr,
                                 const TPathElement::TPtr& dataSourcePathPtr,
                                 const TPathElement::TPtr& oldDataSourcePathPtr) const {
        context.SS->ClearDescribePathCaches(pathPtr);
        if (!IsSameDataSource) {
            context.SS->ClearDescribePathCaches(dataSourcePathPtr);
            context.SS->ClearDescribePathCaches(oldDataSourcePathPtr);
        }
    }

    void PublishToSchemeBoard(const TOperationContext& context,
                              const TPathId& pathId,
                              const TPathId& dataSourcePathId) const {
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);
        if (!IsSameDataSource) {
            context.OnComplete.PublishToSchemeBoard(OperationId, dataSourcePathId);
            context.OnComplete.PublishToSchemeBoard(OperationId, OldSourcePathId);
        }
    }

public:
    explicit TPropose(TOperationId id,
                      bool isSameDataSource,
                      const TPathId oldSourcePathId)
        : OperationId(std::move(id))
        , IsSameDataSource(isSameDataSource)
        , OldSourcePathId(oldSourcePathId) { }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const TStepId step = TStepId(ev->Get()->StepId);

        LOG_I(DebugHint() << " HandleReply TEvOperationPlan"
            << ": step# " << step);

        const TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterExternalTable);

        const auto pathId                = txState->TargetPathId;
        const auto dataSourcePathId      = txState->SourcePathId;
        const auto path                  = TPath::Init(pathId, context.SS);
        const TPathElement::TPtr pathPtr = context.SS->PathsById.at(pathId);
        const TPathElement::TPtr dataSourcePathPtr =
            context.SS->PathsById.at(dataSourcePathId);
        TPathElement::TPtr oldDataSourcePathPtr;
        if (!IsSameDataSource) {
            oldDataSourcePathPtr = context.SS->PathsById.at(OldSourcePathId);
        }

        NIceDb::TNiceDb db(context.GetDB());

        IncParentDirAlterVersionWithRepublish(OperationId, path, context);

        ClearDescribePathCaches(context, pathPtr, dataSourcePathPtr, oldDataSourcePathPtr);
        PublishToSchemeBoard(context, pathId, dataSourcePathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << " ProgressState");

        const TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterExternalTable);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};


class TAlterExternalTable: public TSubOperation {
private:
    bool IsSameDataSource = true;
    TPathId OldDataSourcePathId = InvalidPathId;

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
            return MakeHolder<TPropose>(OperationId, IsSameDataSource, OldDataSourcePathId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

    static bool IsDestinationPathValid(const THolder<TProposeResponse>& result,
                                       const TPath& dstPath,
                                       const TString& acl) {
        const auto checks = dstPath.Check();
        checks.IsAtLocalSchemeShard()
            .IsResolved()
            .NotUnderDeleting()
            .FailOnWrongType(TPathElement::EPathType::EPathTypeExternalTable)
            .IsValidLeafName()
            .DepthLimit()
            .PathsLimit()
            .DirChildrenLimit()
            .IsValidACL(acl);

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

    static void AddPathInSchemeShard(const THolder<TProposeResponse>& result,
                                     TPath& dstPath) {
        result->SetPathId(dstPath.Base()->PathId.LocalPathId);
    }

    TPathElement::TPtr ReplaceExternalTablePathElement(const TPath& dstPath) const {
        TPathElement::TPtr externalTable = dstPath.Base();
        externalTable->PathState = TPathElement::EPathState::EPathStateAlter;
        externalTable->LastTxId  = OperationId.GetTxId();

        return externalTable;
    }

    void CreateTransaction(const TOperationContext& context,
                           const TPathId& externalTablePathId,
                           const TPathId& externalDataSourcePathId) const {
        TTxState& txState = context.SS->CreateTx(OperationId,
                                                 TTxState::TxAlterExternalTable,
                                                 externalTablePathId,
                                                 externalDataSourcePathId);
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
        const TPath& dstPath,
        const TExternalDataSourceInfo::TPtr& oldDataSource,
        bool isSameDataSource) {

        if (!isSameDataSource) {
            auto& reference = *externalDataSource->ExternalTableReferences.AddReferences();
            reference.SetPath(dstPath.PathString());
            PathIdFromPathId(externalTable->PathId, reference.MutablePathId());

            EraseIf(*oldDataSource->ExternalTableReferences.MutableReferences(),
                    [pathId = externalTable->PathId](
                        const NKikimrSchemeOp::TExternalTableReferences::TReference& reference) {
                        return PathIdFromPathId(reference.GetPathId()) == pathId;
                    });
        }
    }

    void PersistExternalTable(
        const TOperationContext& context,
        NIceDb::TNiceDb& db,
        const TPathElement::TPtr& externalTable,
        const TExternalTableInfo::TPtr& externalTableInfo,
        const TPathId& externalDataSourcePathId,
        const TExternalDataSourceInfo::TPtr& externalDataSource,
        const TPathId& oldExternalDataSourcePathId,
        const TExternalDataSourceInfo::TPtr& oldExternalDataSource,
        const TString& acl,
        bool isSameDataSource) const {
        context.SS->ExternalTables[externalTable->PathId] = externalTableInfo;


        if (!acl.empty()) {
            externalTable->ApplyACL(acl);
        }
        context.SS->PersistPath(db, externalTable->PathId);

        if (!isSameDataSource) {
            context.SS->PersistExternalDataSource(db, externalDataSourcePathId, externalDataSource);
            context.SS->PersistExternalDataSource(db, oldExternalDataSourcePathId, oldExternalDataSource);
        }
        context.SS->PersistExternalTable(db, externalTable->PathId, externalTableInfo);
        context.SS->PersistTxState(db, OperationId);
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        Y_UNUSED(owner);
        const auto ssId = context.SS->SelfTabletId();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const auto& externalTableDescription = Transaction.GetCreateExternalTable();
        const TString& name = externalTableDescription.GetName();

        LOG_N("TAlterExternalTable Propose"
            << ": opId# " << OperationId
            << ", path# " << parentPathStr << "/" << name << ", ReplaceIfExists:" << externalTableDescription.GetReplaceIfExists());

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted,
                                                   static_cast<ui64>(OperationId.GetTxId()),
                                                   static_cast<ui64>(ssId));

        const auto parentPath = TPath::Resolve(parentPathStr, context.SS);
        RETURN_RESULT_UNLESS(NExternalTable::IsParentPathValid(result, parentPath));

        const TString acl = Transaction.GetModifyACL().GetDiffACL();
        TPath dstPath     = parentPath.Child(name);
        RETURN_RESULT_UNLESS(IsDestinationPathValid(result, dstPath, acl));

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

        /// Extract old data source
        TExternalDataSourceInfo::TPtr oldDataSource;
        {
            const auto oldExternalTableRecord = context.SS->ExternalTables.Value(dstPath->PathId, nullptr);
            Y_ABORT_UNLESS(oldExternalTableRecord);
            const auto oldDataSourcePath = TPath::Resolve(oldExternalTableRecord->DataSourcePath, context.SS);
            RETURN_RESULT_UNLESS(IsDataSourcePathValid(result, oldDataSourcePath));

            OldDataSourcePathId = oldDataSourcePath->PathId;
            IsSameDataSource = oldDataSourcePath.PathString() == dataSourcePath.PathString();
            if (!IsSameDataSource) {
                oldDataSource = context.SS->ExternalDataSources.Value(oldDataSourcePath->PathId, nullptr);
                Y_ABORT_UNLESS(oldDataSource);
            }
        }
        /// Extract old data source end

        const auto oldExternalTableInfo =
            context.SS->ExternalTables.Value(dstPath->PathId, nullptr);
        Y_ABORT_UNLESS(oldExternalTableInfo);
        auto [externalTableInfo, maybeError] =
            NExternalTable::CreateExternalTable(externalDataSource->SourceType,
                                                externalTableDescription,
                                                context.SS->ExternalSourceFactory,
                                                oldExternalTableInfo->AlterVersion + 1);
        if (maybeError) {
            result->SetError(NKikimrScheme::StatusSchemeError, *maybeError);
            return result;
        }
        Y_ABORT_UNLESS(externalTableInfo);

        AddPathInSchemeShard(result, dstPath);

        const auto externalTable = ReplaceExternalTablePathElement(dstPath);
        CreateTransaction(context, externalTable->PathId, dataSourcePath->PathId);

        NIceDb::TNiceDb db(context.GetDB());

        RegisterParentPathDependencies(context, parentPath);
        AdvanceTransactionStateToPropose(context, db);

        LinkExternalDataSourceWithExternalTable(externalDataSource,
                                                externalTable,
                                                dstPath,
                                                oldDataSource,
                                                IsSameDataSource);

        PersistExternalTable(context, db, externalTable, externalTableInfo,
                             dataSourcePath->PathId, externalDataSource,
                             OldDataSourcePathId, oldDataSource, acl,
                             IsSameDataSource);

        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId,
                                                          dstPath,
                                                          context.SS,
                                                          context.OnComplete);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TAlterExternalTable AbortPropose"
            << ": opId# " << OperationId);
        Y_ABORT("no AbortPropose for TAlterExternalTable");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TAlterExternalTable AbortUnsafe"
            << ": opId# " << OperationId
            << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateAlterExternalTable(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAlterExternalTable>(std::move(id), tx);
}

ISubOperation::TPtr CreateAlterExternalTable(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TAlterExternalTable>(std::move(id), state);
}

}
