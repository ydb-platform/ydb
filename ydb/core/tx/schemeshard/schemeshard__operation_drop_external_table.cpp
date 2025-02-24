#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#define LOG_I(stream) LOG_INFO_S  (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TPropose: public TSubOperationState {
    TString DebugHint() const override {
        return TStringBuilder()
            << "TDropExternalTable TPropose"
            << " opId# " << OperationId << " ";
    }

public:
    explicit TPropose(TOperationId id)
        : OperationId(id)
    { }

    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << "ProgressState");

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropExternalTable);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const auto step = TStepId(ev->Get()->StepId);

        LOG_I(DebugHint() << "HandleReply TEvOperationPlan"
            << ": step# " << step);

        NIceDb::TNiceDb db(context.GetDB());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        TPathId pathId = txState->TargetPathId;
        TPathId dataSourcePathId = txState->SourcePathId;
        auto path = context.SS->PathsById.at(pathId);
        auto dataSourcePath = context.SS->PathsById.at(dataSourcePathId);
        auto parentDir = context.SS->PathsById.at(path->ParentPathId);

        Y_ABORT_UNLESS(!path->Dropped());
        path->SetDropped(step, OperationId.GetTxId());
        context.SS->PersistDropStep(db, pathId, step, OperationId);
        auto domainInfo = context.SS->ResolveDomainInfo(pathId);
        domainInfo->DecPathsInside(context.SS);
        DecAliveChildrenDirect(OperationId, parentDir, context); // for correct discard of ChildrenExist prop

        TExternalDataSourceInfo::TPtr externalDataSourceInfo = context.SS->ExternalDataSources.Value(dataSourcePathId, nullptr);
        Y_ABORT_UNLESS(externalDataSourceInfo);
        EraseIf(*externalDataSourceInfo->ExternalTableReferences.MutableReferences(), [pathId](const NKikimrSchemeOp::TExternalTableReferences::TReference& reference) { return TPathId::FromProto(reference.GetPathId()) == pathId; });

        context.SS->TabletCounters->Simple()[COUNTER_EXTERNAL_TABLE_COUNT].Sub(1);
        context.SS->PersistExternalDataSource(db, dataSourcePathId, externalDataSourceInfo);
        context.SS->PersistRemoveExternalTable(db, pathId);

        ++parentDir->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDir);
        context.SS->ClearDescribePathCaches(parentDir);
        context.SS->ClearDescribePathCaches(path);
        context.SS->ClearDescribePathCaches(dataSourcePath);

        if (!context.SS->DisablePublicationsOfDropping) {
            context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);
            context.OnComplete.PublishToSchemeBoard(OperationId, pathId);
            context.OnComplete.PublishToSchemeBoard(OperationId, dataSourcePathId);
        }

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);

        return true;
    }

private:
    const TOperationId OperationId;
}; // TPropose

class TDropExternalTable: public TSubOperation {
    TTxState::ETxState NextState() const {
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
        const ui64 ssId = context.SS->TabletID();
        const auto& drop = Transaction.GetDrop();

        const TString& workingDir = Transaction.GetWorkingDir();
        const TString& name = drop.GetName();

        LOG_N("TDropExternalTable Propose"
            << ": opId# " << OperationId
            << ", path# " << workingDir << "/" << name);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ssId);

        TPath path = drop.HasId()
            ? TPath::Init(context.SS->MakeLocalId(drop.GetId()), context.SS)
            : TPath::Resolve(workingDir, context.SS).Dive(name);
        {
            auto checks = path.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsExternalTable()
                .NotUnderOperation()
                .IsCommonSensePath();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (path.IsResolved() && path.Base()->IsExternalTable() && (path.Base()->PlannedToDrop() || path.Base()->Dropped())) {
                    result->SetPathDropTxId(ui64(path.Base()->DropTxId));
                    result->SetPathId(path.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        const auto pathId = path.Base()->PathId;
        result->SetPathId(pathId.LocalPathId);

        TExternalTableInfo::TPtr externalTableInfo = context.SS->ExternalTables.Value(pathId, nullptr);
        if (!externalTableInfo) {
            result->SetError(NKikimrScheme::StatusSchemeError, "External table info doesn't exist");
            return result;
        }
        TPath dataSourcePath = TPath::Resolve(externalTableInfo->DataSourcePath, context.SS);
        {
            auto checks = dataSourcePath.Check();
            checks
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsCommonSensePath()
                .IsExternalDataSource();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        auto guard = context.DbGuard();
        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.MemChanges.GrabPath(context.SS, pathId);
        context.MemChanges.GrabPath(context.SS, path->ParentPathId);
        context.MemChanges.GrabPath(context.SS, dataSourcePath->PathId);
        context.MemChanges.GrabExternalTable(context.SS, pathId);
        context.MemChanges.GrabExternalDataSource(context.SS, dataSourcePath->PathId);

        context.DbChanges.PersistTxState(OperationId);
        context.DbChanges.PersistPath(pathId);
        context.DbChanges.PersistPath(path->ParentPathId);
        context.DbChanges.PersistPath(dataSourcePath->PathId);

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxDropExternalTable, path.Base()->PathId, dataSourcePath->PathId);
        txState.State = TTxState::Propose;
        txState.MinStep = TStepId(1);

        path.Base()->PathState = TPathElement::EPathState::EPathStateDrop;
        path.Base()->DropTxId = OperationId.GetTxId();
        path.Base()->LastTxId = OperationId.GetTxId();

        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, path, context.SS, context.OnComplete);

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TDropExternalTable AbortPropose"
            << ": opId# " << OperationId);
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TDropExternalTable AbortUnsafe"
            << ": opId# " << OperationId
            << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateDropExternalTable(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TDropExternalTable>(id, tx);
}

ISubOperation::TPtr CreateDropExternalTable(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TDropExternalTable>(id, state);
}

}
