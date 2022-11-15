#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TDeletePrivateShards: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TDropExtSubdomain TDeletePrivateShards"
            << ", operationId: " << OperationId;
    }

public:
    TDeletePrivateShards(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), AllIncomingEvents());
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);

        // Initiate asynchonous deletion of all shards
        for (auto shard : txState->Shards) {
            context.OnComplete.DeleteShard(shard.Idx);
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

    bool HandleReply(TEvHive::TEvDeleteOwnerTabletsReply::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        NKikimrHive::TEvDeleteOwnerTabletsReply record =  ev->Get()->Record;

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TDeleteExternalShards"
                               << ", Status: " << NKikimrProto::EReplyStatus_Name(record.GetStatus())
                               << ", from Hive: " << record.GetOrigin()
                               << ", Owner: " << record.GetOwner()
                               << ", at schemeshard: " << ssId);

        if (record.GetStatus() != NKikimrProto::EReplyStatus::OK && record.GetStatus() != NKikimrProto::EReplyStatus::ALREADY) {
            TStringBuilder errMsg;
            errMsg << DebugHint()
                   << " Unexpected answer status from hive "
                   << ", msg: " << record.ShortDebugString()
                   << ", at schemeshard: " << ssId;
            LOG_ERROR_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, errMsg);
            Y_VERIFY_DEBUG_S(false, errMsg);
            return false;
        }

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxForceDropExtSubDomain);

        TTabletId hive = TTabletId(record.GetOrigin());
        context.OnComplete.UnbindMsgFromPipe(OperationId, hive, TPipeMessageId(0, 0));

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->ChangeTxState(db, OperationId, TTxState::DeletePrivateShards);

        return true;
    }


    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxForceDropExtSubDomain);

        TSubDomainInfo::TPtr domainInfo = context.SS->SubDomains.at(txState->TargetPathId);
        domainInfo->Initialize(context.SS->ShardInfos);

        TTabletId tenantSchemeshard = domainInfo->GetTenantSchemeShardID();

        if (!tenantSchemeshard) { // ext_subdomain was't altered at all, and there has't been added tenantSchemeshard
            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::DeletePrivateShards);
            return true;
        }

        TTabletId hiveToRequest = context.SS->ResolveHive(txState->TargetPathId, context.Ctx);

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

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", step: " << step
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState->TxType == TTxState::TxForceDropExtSubDomain);

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        auto pathes = context.SS->ListSubThee(pathId, context.Ctx);
        context.SS->DropPathes(pathes, step, OperationId.GetTxId(), db, context.Ctx);

        auto parentDir = context.SS->PathsById.at(path->ParentPathId);
        ++parentDir->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDir);
        context.SS->ClearDescribePathCaches(parentDir);
        context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);

        for (auto id: pathes) {
            context.OnComplete.PublishToSchemeBoard(OperationId, id);
        }

        context.SS->ChangeTxState(db, OperationId, TTxState::DeleteExternalShards);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxForceDropExtSubDomain);

        auto pathes = context.SS->ListSubThee(txState->TargetPathId, context.Ctx);
        NForceDrop::ValidateNoTrasactionOnPathes(OperationId, pathes, context);
        context.SS->MarkAsDroping({txState->TargetPathId}, OperationId.GetTxId(), context.Ctx);
        NForceDrop::CollectShards(pathes, OperationId, txState, context);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
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
            return THolder(new TPropose(OperationId));
        case TTxState::DeleteExternalShards:
            return THolder(new TDeleteExternalShards(OperationId));
        case TTxState::DeletePrivateShards:
            return THolder(new TDeletePrivateShards(OperationId));
        case TTxState::Done:
            return THolder(new TDone(OperationId));
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

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TDropExtSubdomain Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", pathId: " << drop.GetId()
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

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
        if (!context.SS->CheckInFlightLimit(TTxState::TxForceDropExtSubDomain, errStr)) {
            result->SetError(NKikimrScheme::StatusResourceExhausted, errStr);
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

            LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TDropExtSubdomain Propose dependence has found"
                             << ", dependent transaction: " << OperationId.GetTxId()
                             << ", parent transaction: " << otherTxId
                             << ", at schemeshard: " << ssId);

            context.OnComplete.Dependence(otherTxId, OperationId.GetTxId());

            Y_VERIFY(context.SS->Operations.contains(otherTxId));
            auto otherOperation = context.SS->Operations.at(otherTxId);
            for (ui32 partId = 0; partId < otherOperation->Parts.size(); ++partId) {
                if (auto part = otherOperation->Parts.at(partId)) {
                    part->AbortUnsafe(OperationId.GetTxId(), context);
                }
            }
        }

        context.SS->MarkAsDroping({path.Base()->PathId}, OperationId.GetTxId(), context.Ctx);

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
        Y_FAIL("no AbortPropose for TDropExtSubdomain");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TDropExtSubdomain AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);

        TPathId pathId = txState->TargetPathId;
        Y_VERIFY(context.SS->PathsById.contains(pathId));
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);
        Y_VERIFY(path);

        if (path->Dropped()) {
            for (auto shard : txState->Shards) {
                context.OnComplete.DeleteShard(shard.Idx);
            }
        }

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperationBase::TPtr CreateFroceDropExtSubDomain(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TDropExtSubdomain>(id, tx);
}

ISubOperationBase::TPtr CreateFroceDropExtSubDomain(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return MakeSubOperation<TDropExtSubdomain>(id, state);
}

}
