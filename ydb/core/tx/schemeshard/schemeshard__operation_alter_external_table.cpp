#include "schemeshard__operation_common.h"
#include "schemeshard__operation_common_external_table.h"
#include "schemeshard__operation_part.h"
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

        const auto pathId = txState->TargetPathId;
        const auto dataSourcePathId = txState->SourcePathId;
        const auto path = TPath::Init(pathId, context.SS);
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
                                       const TPath& dstPath) {
        const auto checks = dstPath.Check();
        checks.IsAtLocalSchemeShard()
            .IsResolved()
            .NotUnderDeleting()
            .NotUnderOperation()
            .FailOnWrongType(TPathElement::EPathType::EPathTypeExternalTable)
            ;

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

    static void LinkExternalDataSourceWithExternalTable(
        const TExternalDataSourceInfo::TPtr& externalDataSource,
        const TPathElement::TPtr& externalTable,
        const TPath& dstPath,
        const TExternalDataSourceInfo::TPtr& oldDataSource,
        bool isSameDataSource) {

        if (!isSameDataSource) {
            auto& reference = *externalDataSource->ExternalTableReferences.AddReferences();
            reference.SetPath(dstPath.PathString());
            externalTable->PathId.ToProto(reference.MutablePathId());

            EraseIf(*oldDataSource->ExternalTableReferences.MutableReferences(),
                    [pathId = externalTable->PathId](
                        const NKikimrSchemeOp::TExternalTableReferences::TReference& reference) {
                        return TPathId::FromProto(reference.GetPathId()) == pathId;
                    });
        }
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
            << ", path# " << parentPathStr << "/" << name << ", ReplaceIfExists: " << Transaction.GetReplaceIfExists());

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted,
                                                   static_cast<ui64>(OperationId.GetTxId()),
                                                   static_cast<ui64>(ssId));

        if (context.SS->IsServerlessDomain(TPath::Init(context.SS->RootPathId(), context.SS))) {
            if (!context.SS->EnableExternalDataSourcesOnServerless) {
                result->SetError(NKikimrScheme::StatusPreconditionFailed, "External data sources are disabled for serverless domains. Please contact your system administrator to enable it");
                return result;
            }
        }

        const auto parentPath = TPath::Resolve(parentPathStr, context.SS);
        RETURN_RESULT_UNLESS(NExternalTable::IsParentPathValid(result, parentPath));

        TPath dstPath     = parentPath.Child(name);
        RETURN_RESULT_UNLESS(IsDestinationPathValid(result, dstPath));

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

        auto guard = context.DbGuard();

        context.MemChanges.GrabPath(context.SS, externalTable->PathId);
        context.MemChanges.GrabPath(context.SS, parentPath.Base()->PathId);
        context.MemChanges.GrabExternalTable(context.SS, externalTable->PathId);
        context.MemChanges.GrabExternalDataSource(context.SS, dataSourcePath.Base()->PathId);
        if (!IsSameDataSource) {
            context.MemChanges.GrabPath(context.SS, dataSourcePath.Base()->PathId);
            context.MemChanges.GrabExternalDataSource(context.SS, OldDataSourcePathId);
        }
        context.MemChanges.GrabNewTxState(context.SS, OperationId);

        context.DbChanges.PersistPath(externalTable->PathId);
        context.DbChanges.PersistPath(parentPath.Base()->PathId);
        context.DbChanges.PersistExternalTable(externalTable->PathId);
        if (!IsSameDataSource) {
            context.DbChanges.PersistExternalDataSource(dataSourcePath.Base()->PathId);
            context.DbChanges.PersistExternalDataSource(OldDataSourcePathId);
        }
        context.DbChanges.PersistTxState(OperationId);

        LinkExternalDataSourceWithExternalTable(externalDataSource,
                                                externalTable,
                                                dstPath,
                                                oldDataSource,
                                                IsSameDataSource);

        // Carry over removed columns with DeleteVersion set (soft-delete, like regular tables)
        for (const auto& [oldColId, oldCol] : oldExternalTableInfo->Columns) {
            if (!externalTableInfo->Columns.contains(oldColId)) {
                auto deletedCol = oldCol;
                deletedCol.DeleteVersion = externalTableInfo->AlterVersion;
                externalTableInfo->Columns[oldColId] = deletedCol;
            }
        }

        context.SS->ExternalTables[externalTable->PathId] = externalTableInfo;

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterExternalTable,
                                                  externalTable->PathId, dataSourcePath.Base()->PathId);
        txState.Shards.clear();
        txState.State = TTxState::Propose;
        context.OnComplete.ActivateTx(OperationId);

        RegisterParentPathDependencies(OperationId, context, parentPath);

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
