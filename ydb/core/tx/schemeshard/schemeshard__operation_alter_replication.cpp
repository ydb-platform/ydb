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
            << "TAlterReplication TConfigureParts"
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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterReplication);
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
                auto ev = MakeHolder<NReplication::TEvController::TEvAlterReplication>();
                PathIdFromPathId(pathId, ev->Record.MutablePathId());
                ev->Record.MutableOperationId()->SetTxId(ui64(OperationId.GetTxId()));
                ev->Record.MutableOperationId()->SetPartId(ui32(OperationId.GetSubTxId()));
                ev->Record.MutableSwitchState()->CopyFrom(alterData->Description.GetState());

                LOG_D(DebugHint() << "Send TEvAlterReplication to controller"
                    << ": tabletId# " << tabletId
                    << ", ev# " << ev->ToString());
                context.OnComplete.BindMsgToPipe(OperationId, tabletId, pathId, ev.Release());
            }

            txState->ShardsInProgress.insert(shard.Idx);
        }

        return false;
    }

    bool HandleReply(NReplication::TEvController::TEvAlterReplicationResult::TPtr& ev, TOperationContext& context) override {
        LOG_I(DebugHint() << "HandleReply " << ev->Get()->ToString());

        const auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
        const auto status = ev->Get()->Record.GetStatus();

        switch (status) {
        case NKikimrReplication::TEvAlterReplicationResult::SUCCESS:
            break;
        default:
            LOG_W(DebugHint() << "Ignoring unexpected TEvAlterReplicationResult"
                << " tabletId# " << tabletId
                << " status# " << static_cast<int>(status));
            return false;
        }

        auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterReplication);
        Y_ABORT_UNLESS(txState->State == TTxState::ConfigureParts);

        const auto shardIdx = context.SS->MustGetShardIdx(tabletId);
        if (!txState->ShardsInProgress.erase(shardIdx)) {
            LOG_W(DebugHint() << "Ignoring duplicate TEvAlterReplicationResult");
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
            << "TAlterReplication TPropose"
            << " opId# " << OperationId << " ";
    }

public:
    explicit TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {
            TEvHive::TEvCreateTabletReply::EventType,
            NReplication::TEvController::TEvAlterReplicationResult::EventType,
        });
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << "ProgressState");

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterReplication);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const auto step = TStepId(ev->Get()->StepId);

        LOG_I(DebugHint() << "HandleReply TEvOperationPlan"
            << ": step# " << step);

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterReplication);
        const auto& pathId = txState->TargetPathId;

        Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
        auto path = context.SS->PathsById.at(pathId);

        Y_ABORT_UNLESS(context.SS->Replications.contains(pathId));
        auto replication = context.SS->Replications.at(pathId);

        auto alterData = replication->AlterData;
        Y_ABORT_UNLESS(alterData);

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->Replications[pathId] = alterData;
        context.SS->PersistReplicationAlterRemove(db, pathId);
        context.SS->PersistReplication(db, pathId, *alterData);

        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

private:
   const TOperationId OperationId;

}; // TPropose

class TAlterReplication: public TSubOperation {
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

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const auto& workingDir = Transaction.GetWorkingDir();
        const auto& op = Transaction.GetAlterReplication();
        const auto& name = op.GetName();
        const auto pathId = op.HasPathId()
            ? PathIdFromPathId(op.GetPathId())
            : InvalidPathId;

        LOG_N("TAlterReplication Propose"
            << ": opId# " << OperationId
            << ", path# " << workingDir << "/" << name
            << ", pathId# " << pathId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(context.SS->SelfTabletId()));

        if (!op.HasName() && !op.HasPathId()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "Neither name nor pathId in Alter");
            return result;
        }

        const auto path = pathId
            ? TPath::Init(pathId, context.SS)
            : TPath::Resolve(workingDir, context.SS).Dive(name);
        {
            const auto checks = path.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsReplication()
                .NotUnderOperation();

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

        Y_ABORT_UNLESS(context.SS->Replications.contains(path.Base()->PathId));
        auto replication = context.SS->Replications.at(path.Base()->PathId);

        if (replication->AlterVersion == 0) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, "Replication is not created yet");
            return result;
        }

        if (replication->AlterData) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, "There's another Alter in flight");
            return result;
        }

        if (op.HasConfig()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "Cannot alter replication config");
            return result;
        }

        if (!op.HasState()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "Empty alter");
            return result;
        }

        using TState = NKikimrReplication::TReplicationState;
        switch (replication->Description.GetState().GetStateCase()) {
        case NKikimrReplication::TReplicationState::kStandBy:
            if (op.GetState().GetStateCase() != TState::kDone) {
                result->SetError(NKikimrScheme::StatusInvalidParameter, "Cannot switch state");
                return result;
            }
            break;
        case NKikimrReplication::TReplicationState::kPaused:
            if (!THashSet<TState::StateCase>{TState::kStandBy, TState::kDone}.contains(op.GetState().GetStateCase())) {
                result->SetError(NKikimrScheme::StatusInvalidParameter, "Cannot switch state");
                return result;
            }
            break;
        case NKikimrReplication::TReplicationState::kDone:
            result->SetError(NKikimrScheme::StatusInvalidParameter, "Cannot switch state");
            return result;
        default:
            result->SetError(NKikimrScheme::StatusInvalidParameter, "State not set");
            return result;
        }

        auto alterData = replication->CreateNextVersion();
        alterData->Description.MutableState()->CopyFrom(op.GetState());

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        auto& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterReplication, path.Base()->PathId);
        txState.Shards.emplace_back(replication->AlterData->ControllerShardIdx,
            ETabletType::ReplicationController, TTxState::ConfigureParts);
        txState.State = TTxState::CreateParts;

        path.Base()->LastTxId = OperationId.GetTxId();
        path.Base()->PathState = TPathElement::EPathState::EPathStateAlter;

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->PersistReplicationAlter(db, path.Base()->PathId, *replication->AlterData);
        context.SS->PersistTxState(db, OperationId);

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TAlterReplication");
    }

    void AbortUnsafe(TTxId txId, TOperationContext& context) override {
        LOG_N("TAlterReplication AbortUnsafe"
            << ": opId# " << OperationId
            << ", txId# " << txId);
        context.OnComplete.DoneOperation(OperationId);
    }

}; // TAlterReplication

} // anonymous

ISubOperation::TPtr CreateAlterReplication(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAlterReplication>(id, tx);
}

ISubOperation::TPtr CreateAlterReplication(TOperationId id, TTxState::ETxState state) {
    return MakeSubOperation<TAlterReplication>(id, state);
}

}
