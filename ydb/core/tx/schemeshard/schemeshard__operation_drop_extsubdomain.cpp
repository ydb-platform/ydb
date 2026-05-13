#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/hive.h>
#include <ydb/core/base/subdomain.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TDeleteSubdomainSystemShards: public TSubOperationState {
protected:
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder() << "TDeleteSubdomainSystemShards" << " opId# " << OperationId << " ";
    }

public:
    explicit TDeleteSubdomainSystemShards(const TOperationId& id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), AllIncomingEvents());
    }

    bool ProgressState(TOperationContext& context) override {
        YDB_LOG_CTX_INFO(context.Ctx, "ProgressState",
            {"#_context.SS->SelfTabletId()", context.SS->SelfTabletId()},
            {"DebugHint", DebugHint()});

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxForceDropExtSubDomain);

        auto subdomain = context.SS->SubDomains.at(txState->TargetPathId);
        Y_ABORT_UNLESS(subdomain);

        // Initiate asynchronous deletion of system shards
        if (subdomain->GetSharedHive()) {
            for (const auto& shard : txState->Shards) {
                context.OnComplete.DeleteShard(shard.Idx);
            }
        } else {
            for (const auto& shard : txState->Shards) {
                context.OnComplete.DeleteSystemShard(shard.Idx);
            }
        }

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }
};

class TDeleteExternalShards: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TDropExtSubdomain TDeleteExternalShards"
            << ", operationId: " << OperationId;
    }

public:
    TDeleteExternalShards(TOperationId id)
        : OperationId(id)
    {
        TSet<ui32> toIgnore = AllIncomingEvents();
        toIgnore.erase(TEvHive::TEvDeleteOwnerTabletsReply::EventType);

        IgnoreMessages(DebugHint(), toIgnore);
    }

    void FinishState(TTxState* txState, TOperationContext& context) {
        auto targetPath = context.SS->PathsById.at(txState->TargetPathId);

        NIceDb::TNiceDb db(context.GetDB());

        // We are done with the extsubdomain's tablets, now its a good time
        // to make extsubdomain root unresolvable to any external observer
        context.SS->DropNode(targetPath, txState->PlanStep, OperationId.GetTxId(), db, context.Ctx);

        {
            auto parentDir = context.SS->PathsById.at(targetPath->ParentPathId);
            ++parentDir->DirAlterVersion;
            context.SS->PersistPathDirAlterVersion(db, parentDir);
            context.SS->ClearDescribePathCaches(parentDir);
            context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);
        }

        context.OnComplete.PublishToSchemeBoard(OperationId, targetPath->PathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::DeletePrivateShards);
    }

    bool HandleReply(TEvHive::TEvDeleteOwnerTabletsReply::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        NKikimrHive::TEvDeleteOwnerTabletsReply record = ev->Get()->Record;

        YDB_LOG_CTX_INFO(context.Ctx, "HandleReply TDeleteExternalShards",
            {"#_DebugHint()", DebugHint()},
            {"Status", NKikimrProto::EReplyStatus_Name(record.GetStatus())},
            {"from_Hive", record.GetOrigin()},
            {"Owner", record.GetOwner()},
            {"at_schemeshard", ssId});

        if (record.GetStatus() != NKikimrProto::EReplyStatus::OK && record.GetStatus() != NKikimrProto::EReplyStatus::ALREADY) {
            TStringBuilder errMsg;
            errMsg << DebugHint()
                   << " Unexpected answer status from hive "
                   << ", msg: " << record.ShortDebugString()
                   << ", at schemeshard: " << ssId;
            YDB_LOG_CTX_ERROR(context.Ctx, "",
                {"errMsg", errMsg});
            Y_VERIFY_DEBUG_S(false, errMsg);
            return false;
        }

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxForceDropExtSubDomain);

        TTabletId hive = TTabletId(record.GetOrigin());
        context.OnComplete.UnbindMsgFromPipe(OperationId, hive, TPipeMessageId(0, 0));

        FinishState(txState, context);

        return true;
    }


    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        YDB_LOG_CTX_INFO(context.Ctx, "ProgressState",
            {"#_DebugHint()", DebugHint()},
            {"at_schemeshard", ssId});

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxForceDropExtSubDomain);

        TSubDomainInfo::TPtr domainInfo = context.SS->SubDomains.at(txState->TargetPathId);
        domainInfo->Initialize(context.SS->ShardInfos);

        TTabletId tenantSchemeshard = domainInfo->GetTenantSchemeShardID();

        if (!tenantSchemeshard) {
            // extsubdomain was't altered at all, there are no tenantSchemeshard,
            // nothing to do
            FinishState(txState, context);
            return true;
        }

        TTabletId hiveToRequest = context.SS->ResolveHive(txState->TargetPathId, TSchemeShard::EHiveSelection::IGNORE_TENANT);

        auto event = MakeHolder<TEvHive::TEvDeleteOwnerTablets>(ui64(tenantSchemeshard), ui64(OperationId.GetTxId()));
        context.OnComplete.BindMsgToPipe(OperationId, hiveToRequest, TPipeMessageId(0, 0), event.Release());

        return false;
    }
};

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TDropExtSubdomain TPropose"
            << ", operationId: " << OperationId;
    }
public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        TSet<ui32> toIgnore = AllIncomingEvents();
        toIgnore.erase(TEvPrivate::TEvOperationPlan::EventType);

        IgnoreMessages(DebugHint(), toIgnore);
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        YDB_LOG_CTX_INFO(context.Ctx, "HandleReply TEvOperationPlan",
            {"#_DebugHint()", DebugHint()},
            {"step", step},
            {"at_schemeshard", ssId});

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxForceDropExtSubDomain);

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        txState->PlanStep = step;
        context.SS->PersistTxPlanStep(db, OperationId, step);

        //NOTE: drop entire extsubdomain path tree except the extsubdomain root itself,
        // root should stay alive (but marked for deletion) until operation is done.
        // Or at least until we are done with deleting tablets via extsubdomain's hive.
        // In a configuration with dedicated nodes extsubdomain's hive runs on
        // extsubdomain's nodes and nodes can't reconnect to the extsubdomain
        // if its root is not resolvable. And nodes could go away right in the middle of anything --
        // -- being able to reconnect node any time until extsubdomain is actually gone
        // is a good thing.
        auto paths = context.SS->ListSubTree(pathId, context.Ctx);
        paths.erase(pathId);
        context.SS->DropPaths(paths, step, OperationId.GetTxId(), db, context.Ctx);

        for (auto id: paths) {
            context.OnComplete.PublishToSchemeBoard(OperationId, id);
        }

        context.SS->ChangeTxState(db, OperationId, TTxState::DeleteExternalShards);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        YDB_LOG_CTX_INFO(context.Ctx, "ProgressState",
            {"#_DebugHint()", DebugHint()},
            {"at_schemeshard", ssId});

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxForceDropExtSubDomain);

        auto targetPath = context.SS->PathsById.at(txState->TargetPathId);

        auto paths = context.SS->ListSubTree(targetPath->PathId, context.Ctx);
        NForceDrop::ValidateNoTransactionOnPaths(OperationId, paths, context);
        NForceDrop::CollectShards(paths, OperationId, txState, context);

        context.SS->MarkAsDropping(targetPath, OperationId.GetTxId(), context.Ctx);

        context.OnComplete.ProposeToCoordinator(OperationId, targetPath->PathId, TStepId(0));
        return false;
    }
};

class TDropExtSubdomain: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::Propose;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Propose:
            return TTxState::DeleteExternalShards;
        case TTxState::DeleteExternalShards:
            return TTxState::DeletePrivateShards;
        case TTxState::DeletePrivateShards:
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
        case TTxState::DeleteExternalShards:
            return MakeHolder<TDeleteExternalShards>(OperationId);
        case TTxState::DeletePrivateShards:
            return MakeHolder<TDeleteSubdomainSystemShards>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto& drop = Transaction.GetDrop();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = drop.GetName();

        YDB_LOG_CTX_NOTICE(context.Ctx, "TDropExtSubdomain Propose /",
            {"path", parentPathStr},
            {"name", name},
            {"pathId", drop.GetId()},
            {"opId", OperationId},
            {"at_schemeshard", ssId});

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        TPath path = drop.HasId()
            ? TPath::Init(context.SS->MakeLocalId(drop.GetId()), context.SS)
            : TPath::Resolve(parentPathStr, context.SS).Dive(name);

        {
            TPath::TChecker checks = path.Check();
            checks
                .NotEmpty()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotRoot()
                .NotDeleted()
                .IsCommonSensePath()
                .IsExternalSubDomain();

            if (checks) {
                if (path.IsUnderCreating()) {
                    TPath parent = path.Parent();
                    if (parent.IsUnderCreating()) {
                        checks
                            .NotUnderTheSameOperation(parent.ActiveOperation(), NKikimrScheme::StatusMultipleModifications);
                    }
                }
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

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxForceDropExtSubDomain, path.Base()->PathId);
        txState.State = TTxState::Waiting;
        txState.MinStep = TStepId(1);

        NIceDb::TNiceDb db(context.GetDB());

        auto relatedTx = context.SS->GetRelatedTransactions({path.Base()->PathId}, context.Ctx);

        for (auto otherTxId: relatedTx) {
            if (otherTxId == OperationId.GetTxId()) {
                continue;
            }

            YDB_LOG_CTX_NOTICE(context.Ctx, "TDropExtSubdomain Propose dependence has found, dependent, parent",
                {"transaction", OperationId.GetTxId()},
                {"transaction", otherTxId},
                {"at_schemeshard", ssId});

            context.OnComplete.Dependence(otherTxId, OperationId.GetTxId());

            Y_ABORT_UNLESS(context.SS->Operations.contains(otherTxId));
            auto otherOperation = context.SS->Operations.at(otherTxId);
            for (ui32 partId = 0; partId < otherOperation->Parts.size(); ++partId) {
                if (auto part = otherOperation->Parts.at(partId)) {
                    part->AbortUnsafe(OperationId.GetTxId(), context);
                }
            }
        }

        context.SS->MarkAsDropping(path.Base(), OperationId.GetTxId(), context.Ctx);

        txState.State = TTxState::Propose;
        context.OnComplete.ActivateTx(OperationId);

        context.SS->PersistTxState(db, OperationId);

        auto parentDir = path.Parent();
        ++parentDir.Base()->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDir.Base());
        context.SS->ClearDescribePathCaches(parentDir.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, parentDir.Base()->PathId);

        context.SS->ClearDescribePathCaches(path.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, path.Base()->PathId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TDropExtSubdomain");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        AbortUnsafeDropOperation(OperationId, forceDropTxId, context);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateForceDropExtSubDomain(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TDropExtSubdomain>(id, tx);
}

ISubOperation::TPtr CreateForceDropExtSubDomain(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TDropExtSubdomain>(id, state);
}

}
