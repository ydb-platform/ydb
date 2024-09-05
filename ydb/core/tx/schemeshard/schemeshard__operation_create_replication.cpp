#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/tx/replication/controller/public_events.h>

#define LOG_D(stream) LOG_DEBUG_S (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_I(stream) LOG_INFO_S  (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_W(stream) LOG_WARN_S  (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)

namespace NKikimr::NSchemeShard {

namespace {

class TConfigureParts: public TSubOperationState {
    TString DebugHint() const override {
        return TStringBuilder()
            << "TCreateReplication TConfigureParts"
            << " opId# " << OperationId << " ";
    }

public:
    explicit TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {
            TEvHive::TEvCreateTabletReply::EventType,
        });
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << "ProgressState");

        auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateReplication);
        const auto& pathId = txState->TargetPathId;

        Y_ABORT_UNLESS(context.SS->Replications.contains(pathId));
        auto alterData = context.SS->Replications.at(pathId)->AlterData;
        Y_ABORT_UNLESS(alterData);

        txState->ClearShardsInProgress();

        for (const auto& shard : txState->Shards) {
            Y_ABORT_UNLESS(shard.TabletType == ETabletType::ReplicationController);

            Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shard.Idx));
            const auto tabletId = context.SS->ShardInfos.at(shard.Idx).TabletID;

            if (tabletId == InvalidTabletId) {
                LOG_D(DebugHint() << "Shard is not created yet"
                    << ": shardIdx# " << shard.Idx);
                context.OnComplete.WaitShardCreated(shard.Idx, OperationId);
            } else {
                auto ev = MakeHolder<NReplication::TEvController::TEvCreateReplication>();
                PathIdFromPathId(pathId, ev->Record.MutablePathId());
                ev->Record.MutableOperationId()->SetTxId(ui64(OperationId.GetTxId()));
                ev->Record.MutableOperationId()->SetPartId(ui32(OperationId.GetSubTxId()));
                ev->Record.MutableConfig()->CopyFrom(alterData->Description.GetConfig());

                LOG_D(DebugHint() << "Send TEvCreateReplication to controller"
                    << ": tabletId# " << tabletId
                    << ", ev# " << ev->ToString());
                context.OnComplete.BindMsgToPipe(OperationId, tabletId, pathId, ev.Release());
            }

            txState->ShardsInProgress.insert(shard.Idx);
        }

        return false;
    }

    bool HandleReply(NReplication::TEvController::TEvCreateReplicationResult::TPtr& ev, TOperationContext& context) override {
        LOG_I(DebugHint() << "HandleReply " << ev->Get()->ToString());

        const auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
        const auto status = ev->Get()->Record.GetStatus();

        switch (status) {
        case NKikimrReplication::TEvCreateReplicationResult::SUCCESS:
        case NKikimrReplication::TEvCreateReplicationResult::ALREADY_EXISTS:
            break;
        default:
            LOG_W(DebugHint() << "Ignoring unexpected TEvCreateReplicationResult"
                << " tabletId# " << tabletId
                << " status# " << static_cast<int>(status));
            return false;
        }

        auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateReplication);
        Y_ABORT_UNLESS(txState->State == TTxState::ConfigureParts);

        const auto shardIdx = context.SS->MustGetShardIdx(tabletId);
        if (!txState->ShardsInProgress.erase(shardIdx)) {
            LOG_W(DebugHint() << "Ignoring duplicate TEvCreateReplicationResult");
            return false;
        }

        context.OnComplete.UnbindMsgFromPipe(OperationId, tabletId, txState->TargetPathId);

        if (!txState->ShardsInProgress.empty()) {
            return false;
        }

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->ChangeTxState(db, OperationId, TTxState::Propose);
        context.OnComplete.ActivateTx(OperationId);

        return true;
    }

private:
    const TOperationId OperationId;

}; // TConfigureParts

class TPropose: public TSubOperationState {
    TString DebugHint() const override {
        return TStringBuilder()
            << "TCreateReplication TPropose"
            << " opId# " << OperationId << " ";
    }

public:
    explicit TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {
            TEvHive::TEvCreateTabletReply::EventType,
            NReplication::TEvController::TEvCreateReplicationResult::EventType,
        });
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << "ProgressState");

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateReplication);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const auto step = TStepId(ev->Get()->StepId);

        LOG_I(DebugHint() << "HandleReply TEvOperationPlan"
            << ": step# " << step);

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateReplication);
        const auto& pathId = txState->TargetPathId;

        Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
        auto path = context.SS->PathsById.at(pathId);

        Y_ABORT_UNLESS(context.SS->Replications.contains(pathId));
        auto replication = context.SS->Replications.at(pathId);

        auto alterData = replication->AlterData;
        Y_ABORT_UNLESS(alterData);

        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        context.SS->Replications[pathId] = alterData;
        context.SS->PersistReplicationAlterRemove(db, pathId);
        context.SS->PersistReplication(db, pathId, *alterData);

        Y_ABORT_UNLESS(context.SS->PathsById.contains(path->ParentPathId));
        auto parentPath = context.SS->PathsById.at(path->ParentPathId);

        ++parentPath->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentPath);

        context.SS->ClearDescribePathCaches(parentPath);
        context.OnComplete.PublishToSchemeBoard(OperationId, parentPath->PathId);

        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

private:
   const TOperationId OperationId;

}; // TPropose

class TCreateReplication: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::CreateParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return TTxState::ConfigureParts;
        case TTxState::ConfigureParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return MakeHolder<TCreateParts>(OperationId);
        case TTxState::ConfigureParts:
            return MakeHolder<TConfigureParts>(OperationId);
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
        const auto& workingDir = Transaction.GetWorkingDir();
        auto desc = Transaction.GetReplication();
        const auto& name = desc.GetName();
        const auto& acl = Transaction.GetModifyACL().GetDiffACL();
        const auto acceptExisted = !Transaction.GetFailOnExist();

        LOG_N("TCreateReplication Propose"
            << ": opId# " << OperationId
            << ", path# " << workingDir << "/" << name);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(context.SS->SelfTabletId()));

        const auto parentPath = TPath::Resolve(workingDir, context.SS);
        {
            const auto checks = parentPath.Check();
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

        auto path = parentPath.Child(name);
        {
            const auto checks = path.Check();
            checks
                .IsAtLocalSchemeShard();

            if (path.IsResolved()) {
                checks
                    .IsResolved()
                    .NotUnderDeleting()
                    .FailOnExist(TPathElement::EPathType::EPathTypeReplication, acceptExisted);
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
                    .ShardsLimit(1)
                    .IsValidACL(acl);
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (path.IsResolved()) {
                    result->SetPathCreateTxId(ui64(path->CreateTxId));
                    result->SetPathId(path->PathId.LocalPathId);
                }

                return result;
            }
        }

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        TChannelsBindings channelsBindings;
        if (!context.SS->ResolveTabletChannels(0, parentPath.GetPathIdForDomain(), channelsBindings)) {
            result->SetError(NKikimrScheme::StatusInvalidParameter,
                "Unable to construct channel binding for replication controller with the storage pool");
            return result;
        }

        if (desc.HasState()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "Cannot create replication with explicit state");
            return result;
        }

        path.MaterializeLeaf(owner);
        path->CreateTxId = OperationId.GetTxId();
        path->LastTxId = OperationId.GetTxId();
        path->PathState = TPathElement::EPathState::EPathStateCreate;
        path->PathType = TPathElement::EPathType::EPathTypeReplication;
        result->SetPathId(path->PathId.LocalPathId);

        context.SS->IncrementPathDbRefCount(path->PathId);
        parentPath->IncAliveChildren();
        parentPath.DomainInfo()->IncPathsInside();

        if (desc.GetConfig().GetSrcConnectionParams().GetCredentialsCase() == NKikimrReplication::TConnectionParams::CREDENTIALS_NOT_SET) {
            desc.MutableConfig()->MutableSrcConnectionParams()->MutableOAuthToken()->SetToken(BUILTIN_ACL_ROOT);
        }

        desc.MutableState()->MutableStandBy();
        auto replication = TReplicationInfo::Create(std::move(desc));
        context.SS->Replications[path->PathId] = replication;
        context.SS->TabletCounters->Simple()[COUNTER_REPLICATION_COUNT].Add(1);

        replication->AlterData->ControllerShardIdx = context.SS->RegisterShardInfo(
            TShardInfo::ReplicationControllerInfo(OperationId.GetTxId(), path->PathId)
                .WithBindedChannels(channelsBindings));
        context.SS->TabletCounters->Simple()[COUNTER_REPLICATION_CONTROLLER_COUNT].Add(1);

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        auto& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateReplication, path->PathId);
        txState.Shards.emplace_back(replication->AlterData->ControllerShardIdx,
            ETabletType::ReplicationController, TTxState::CreateParts);
        txState.State = TTxState::CreateParts;

        path->IncShardsInside();
        parentPath.DomainInfo()->AddInternalShards(txState);

        if (parentPath->HasActiveChanges()) {
            const auto parentTxId = parentPath->PlannedToCreate() ? parentPath->CreateTxId : parentPath->LastTxId;
            context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
        }

        NIceDb::TNiceDb db(context.GetDB());

        if (!acl.empty()) {
            path->ApplyACL(acl);
        }
        context.SS->PersistPath(db, path->PathId);

        context.SS->PersistReplication(db, path->PathId, *replication);
        context.SS->PersistReplicationAlter(db, path->PathId, *replication->AlterData);

        Y_ABORT_UNLESS(txState.Shards.size() == 1);
        for (const auto& shard : txState.Shards) {
            Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shard.Idx));
            const TShardInfo& shardInfo = context.SS->ShardInfos.at(shard.Idx);

            if (shard.Operation == TTxState::CreateParts) {
                context.SS->PersistShardMapping(db, shard.Idx, InvalidTabletId, path->PathId, OperationId.GetTxId(), shard.TabletType);
                context.SS->PersistChannelsBinding(db, shard.Idx, shardInfo.BindedChannels);
            }
        }

        context.SS->ChangeTxState(db, OperationId, txState.State);
        context.SS->PersistTxState(db, OperationId);
        context.SS->PersistUpdateNextPathId(db);
        context.SS->PersistUpdateNextShardIdx(db);

        ++parentPath->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentPath.Base());

        context.SS->ClearDescribePathCaches(parentPath.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, parentPath->PathId);

        context.SS->ClearDescribePathCaches(path.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, path->PathId);

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TCreateReplication");
    }

    void AbortUnsafe(TTxId txId, TOperationContext& context) override {
        LOG_N("TCreateReplication AbortUnsafe"
            << ": opId# " << OperationId
            << ", txId# " << txId);
        context.OnComplete.DoneOperation(OperationId);
    }

}; // TCreateReplication

} // anonymous

ISubOperation::TPtr CreateNewReplication(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateReplication>(id, tx);
}

ISubOperation::TPtr CreateNewReplication(TOperationId id, TTxState::ETxState state) {
    return MakeSubOperation<TCreateReplication>(id, state);
}

}
