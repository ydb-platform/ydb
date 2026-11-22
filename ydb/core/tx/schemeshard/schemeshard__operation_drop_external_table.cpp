#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TPropose: public TSubOperationState {
    virtual const char* Name() const override final { return "TPropose"; }

public:
    explicit TPropose(TOperationId id)
        : OperationId(id)
    { }

    bool ProgressState(TOperationContext& context) override {
        YDB_LOG_INFO_CTX(context.Ctx, "Propose to coordinator");

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropExternalTable);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const auto step = TStepId(ev->Get()->StepId);

        YDB_LOG_INFO_CTX(context.Ctx, "Operation plan received",
            {"step", step},
        );

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
        context.SS->PersistExternalDataSource(db, dataSourcePathId);
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
    virtual const char* Name() const override final { return "TDropExternalTable"; }

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

    THolder<TProposeResponse> Propose(const TString&, TProposeContext& context) override {
        const TString& workingDir = Transaction.GetWorkingDir();
        const auto& drop = Transaction.GetDrop();

        const TString& name = drop.GetName();

        YDB_LOG_NOTICE_CTX(context.Ctx, "Drop external table",
            {"path", workingDir + "/" + name},
        );

        const ui64 ssId = context.SS->TabletID();
        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ssId);

        const bool abortCreate = Transaction.HasExternalTableAbortCreateKey();
        const bool commitCreate = Transaction.HasExternalTableCommitCreateKey();
        const bool releaseCreate = Transaction.HasExternalTableReleaseCreateKey();
        const bool finishCreate = abortCreate || commitCreate || releaseCreate;
        const auto& operationKey = abortCreate    ? Transaction.GetExternalTableAbortCreateKey()
                                   : commitCreate ? Transaction.GetExternalTableCommitCreateKey()
                                                  : Transaction.GetExternalTableReleaseCreateKey();
        if (drop.HasExpectedExternalTable() || finishCreate) {
            const auto& expected = drop.GetExpectedExternalTable();
            if (drop.HasId() || !drop.HasName() || (drop.HasExpectedExternalTable() &&
                (finishCreate || !expected.HasPathId() || !expected.HasVersion())) ||
                (finishCreate && (operationKey.empty() || operationKey.size() > 128 || Transaction.ApplyIfSize())))
            {
                result->SetError(NKikimrScheme::StatusInvalidParameter, "Invalid external table drop guard or create ticket");
                return result;
            }
            if (!context.UserToken || context.UserToken->GetUserSID().empty()) {
                result->SetError(NKikimrScheme::StatusAccessDenied, "An external table drop guard or create ticket requires authentication");
                return result;
            }
        }

        TPath path = drop.HasId()
            ? TPath::Init(context.SS->MakeLocalId(drop.GetId()), context.SS)
            : TPath::Resolve(workingDir, context.SS).Dive(name);
        TExternalTableCreateTicketKey key;
        NKikimrSchemeOp::TExternalTableCreateTicket ticket;
        if (finishCreate) {
            const auto parentPath = TPath::Resolve(workingDir, context.SS);
            const auto checks = parentPath.Check();
            checks.IsAtLocalSchemeShard().IsResolved().NotDeleted().NotUnderDeleting().IsLikeDirectory();
            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }

            const auto pathChecks = path.Check();
            pathChecks.IsAtLocalSchemeShard().IsValidLeafName(context.UserToken.Get());
            if (!pathChecks) {
                result->SetError(pathChecks.GetStatus(), pathChecks.GetError());
                return result;
            }

            key = {context.UserToken->GetUserSID(), operationKey};
            const auto* previous = context.SS->ExternalTableCreateTickets.FindPtr(key);
            const ui64 deadline = Transaction.GetExternalTableCreateLeaseDeadlineMs();
            if (previous && (previous->GetTargetPath() != path.PathString() || previous->GetLeaseDeadlineMs() != deadline)) {
                result->SetError(NKikimrScheme::StatusPreconditionFailed, "The external table create ticket belongs to another path");
                return result;
            }
            if (releaseCreate) {
                if (!previous && deadline <= context.SS->ExternalTableCreateLeaseCutoffMs) {
                    result->SetStatus(NKikimrScheme::StatusSuccess);
                } else if (!previous || (!previous->GetAborted() && !previous->GetCommitted())) {
                    result->SetError(NKikimrScheme::StatusPreconditionFailed, "The external table create lease is not terminal");
                } else if (previous->GetAborted() && previous->HasTable() && path.IsResolved() && !path.Base()->Dropped() &&
                           path.Base()->PathId == TPathId::FromProto(previous->GetTable().GetPathId()))
                {
                    result->SetError(NKikimrScheme::StatusMultipleModifications, "The aborted external table is being dropped");
                } else {
                    if (!previous->GetReleased()) {
                        ticket = *previous;
                        ticket.SetReleased(true);
                        context.SS->StageExternalTableCreateTicket(context, key, std::move(ticket));
                        context.OnComplete.Send(context.SS->SelfId(), new TEvPrivate::TEvCleanupExternalTableCreateTickets);
                    }
                    result->SetStatus(NKikimrScheme::StatusSuccess);
                }
                return result;
            }

            if (previous) {
                ticket = *previous;
            } else {
                ticket.SetTargetPath(path.PathString());
                if (abortCreate && deadline) {
                    const ui64 now = context.Ctx.Now().MilliSeconds();
                    if (deadline <= Max(now, context.SS->ExternalTableCreateLeaseCutoffMs)) {
                        context.SS->StageExternalTableCreateLeaseCutoff(context, deadline);
                        result->SetStatus(NKikimrScheme::StatusSuccess);
                        return result;
                    }
                    if (deadline - now > TSchemeShard::MaxExternalTableCreateLease.MilliSeconds()) {
                        result->SetError(NKikimrScheme::StatusInvalidParameter, "The external table create lease is too long");
                        return result;
                    }
                    if (context.SS->LeasedExternalTableCreateTicketCount >= TSchemeShard::MaxLeasedExternalTableCreateTickets) {
                        result->SetError(NKikimrScheme::StatusQuotaExceeded, "Too many external table create leases");
                        return result;
                    }
                    ticket.SetLeaseDeadlineMs(deadline);
                }
            }

            if (commitCreate && (!ticket.HasTable() || ticket.GetAborted())) {
                result->SetError(NKikimrScheme::StatusPreconditionFailed, "The external table create ticket cannot be committed");
                return result;
            }
            if (ticket.GetCommitted()) {
                if (abortCreate) {
                    result->SetError(NKikimrScheme::StatusPreconditionFailed, "The external table create ticket is committed");
                } else {
                    result->SetStatus(NKikimrScheme::StatusSuccess);
                }
                return result;
            }

            if (!ticket.HasTable() || !path.IsResolved() || path.Base()->Dropped() ||
                path.Base()->PathId != TPathId::FromProto(ticket.GetTable().GetPathId()))
            {
                if (commitCreate) {
                    result->SetError(NKikimrScheme::StatusPreconditionFailed, "The external table created by the ticket no longer exists");
                    return result;
                }
                if (!ticket.GetAborted()) {
                    ticket.SetAborted(true);
                    ticket.ClearDefinition();
                    context.SS->StageExternalTableCreateTicket(context, key, std::move(ticket));
                }
                result->SetStatus(NKikimrScheme::StatusSuccess);
                return result;
            }
        }
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
        const auto& expected = finishCreate ? ticket.GetTable() : drop.GetExpectedExternalTable();
        if ((finishCreate || drop.HasExpectedExternalTable()) &&
            (TPathId::FromProto(expected.GetPathId()) != pathId || expected.GetVersion() != externalTableInfo->AlterVersion))
        {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, "External table identity does not match the drop guard");
            return result;
        }
        if (commitCreate) {
            ticket.SetCommitted(true);
            context.SS->StageExternalTableCreateTicket(context, key, std::move(ticket));
            result->SetStatus(NKikimrScheme::StatusSuccess);
            return result;
        }
        if (!abortCreate && context.SS->PreparedExternalTables.contains(pathId)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, "The external table has a prepared create ticket");
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
        if (abortCreate) {
            ticket.SetAborted(true);
            ticket.ClearDefinition();
            context.SS->StageExternalTableCreateTicket(context, key, std::move(ticket));
        }

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

    void AbortPropose(TProposeContext& context) override {
        YDB_LOG_NOTICE_CTX(context.Ctx, "");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        YDB_LOG_NOTICE_CTX(context.Ctx, "TDropExternalTable AbortUnsafe",
            {"operationId", OperationId},
            {"txId", forceDropTxId},
            {"schemeshard", context.SS->TabletID()},
        );
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

#undef YDB_LOG_THIS_FILE_COMPONENT
