#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>
#include <ydb/core/persqueue/config/config.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

void PrepareChanges(TOperationId operationId,
                    TPathElement::TPtr item,
                    TKesusInfo::TPtr kesus,
                    TOperationContext& context)
{
    NIceDb::TNiceDb db(context.GetDB());

    item->LastTxId = operationId.GetTxId();
    item->PathState = TPathElement::EPathState::EPathStateAlter;

    TTxState& txState = context.SS->CreateTx(operationId, TTxState::TxAlterKesus, item->PathId);
    txState.State = TTxState::ConfigureParts;

    txState.Shards.reserve(1);
    {
        TShardIdx shardIdx = kesus->KesusShardIdx;
        TTabletId tabletId = kesus->KesusTabletId;

        Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shardIdx));
        auto& shardInfo = context.SS->ShardInfos[shardIdx];
        Y_ABORT_UNLESS(shardInfo.TabletID == tabletId);
        txState.Shards.emplace_back(shardIdx, ETabletType::Kesus, TTxState::ConfigureParts);
        shardInfo.CurrentTxId = operationId.GetTxId();
        context.SS->PersistShardTx(db, shardIdx, operationId.GetTxId());
    }

    LOG_DEBUG(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "AlterKesus txid# %" PRIu64 ", AlterVersion %" PRIu64,
            operationId.GetTxId(), kesus->AlterVersion);

    context.SS->PersistAddKesusAlter(db, item->PathId, kesus);

    context.SS->PersistTxState(db, operationId);
    context.OnComplete.ActivateTx(operationId);
}

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TAlterKesus TConfigureParts"
                << " operationId#" << OperationId;
    }

public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool HandleReply(NKesus::TEvKesus::TEvSetConfigResult::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TCreateKesus TConfigureParts HandleReply TEvSetConfigResult"
                    << ", operationId: " << OperationId
                    << ", at schemeshard: " << ssId);

        TTabletId tabletId =TTabletId(ev->Get()->Record.GetTabletId());
        auto status = ev->Get()->Record.GetError().GetStatus();

        // SetConfig may fail if schemeshard tries to downgrade configuration
        // That likely means this is a very outdated version
        Y_VERIFY_S(status == Ydb::StatusIds::SUCCESS,
                   "Unexpected error in SetConfigResult."
                       << " status: " << Ydb::StatusIds::StatusCode_Name(status)
                       << " txId: " << OperationId
                       << " tablet: " << tabletId
                       << " at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterKesus);
        Y_ABORT_UNLESS(txState->State == TTxState::ConfigureParts);

        TShardIdx idx = context.SS->MustGetShardIdx(tabletId);
        txState->ShardsInProgress.erase(idx);

        context.OnComplete.UnbindMsgFromPipe(OperationId, tabletId, idx);

        if (txState->ShardsInProgress.empty()) {
            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::Propose);
            context.OnComplete.ActivateTx(OperationId);
            return true;
        }

        return false;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TCreateKesus TConfigureParts ProgressState"
                    << ", operationId: " << OperationId
                    << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterKesus);
        Y_ABORT_UNLESS(!txState->Shards.empty());

        txState->ClearShardsInProgress();

        TKesusInfo::TPtr kesus = context.SS->KesusInfos[txState->TargetPathId];
        Y_VERIFY_S(kesus, "kesus is null. PathId: " << txState->TargetPathId);

        TPath kesusPath = TPath::Init(txState->TargetPathId, context.SS);
        Y_ABORT_UNLESS(kesusPath.IsResolved());

        Y_ABORT_UNLESS(txState->Shards.size() == 1);
        for (auto shard : txState->Shards) {
            auto shardIdx = shard.Idx;
            auto tabletId = context.SS->ShardInfos[shardIdx].TabletID;
            Y_ABORT_UNLESS(shard.TabletType == ETabletType::Kesus);

            auto event = MakeHolder<NKesus::TEvKesus::TEvSetConfig>(ui64(OperationId.GetTxId()), *kesus->AlterConfig, kesus->AlterVersion);
            event->Record.MutableConfig()->set_path(kesusPath.PathString()); // TODO: remove legacy field eventually
            event->Record.SetPath(kesusPath.PathString());

            context.OnComplete.BindMsgToPipe(OperationId, tabletId, shardIdx, event.Release());

            // Wait for results from this shard
            txState->ShardsInProgress.insert(shardIdx);
        }

        return false;
    }
};

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TAlterKesus TPropose"
                << " operationId#" << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr&, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "TAlterKesus TPropose HandleReply TEvOperationPlan"
                       << ", operationId: " << OperationId
                       << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        if (!txState) {
            return false;
        }
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterKesus);

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        path->PathState = TPathElement::EPathState::EPathStateNoChanges;

        TKesusInfo::TPtr kesus = context.SS->KesusInfos.at(pathId);
        kesus->FinishAlter();
        context.SS->PersistKesusInfo(db, pathId, kesus);
        context.SS->PersistRemoveKesusAlter(db, pathId);

        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        context.OnComplete.DoneOperation(OperationId);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "TAlterKesus TPropose ProgressState"
                       << ", operationId: " << OperationId
                       << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterKesus);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

class TAlterKesus: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::ConfigureParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::ConfigureParts:
            return TTxState::Propose;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::ConfigureParts:
            return MakeHolder<TConfigureParts>(OperationId);
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    const Ydb::Coordination::Config* ParseParams(
            const NKikimrSchemeOp::TKesusDescription& alter,
            TString& errStr) {
        if (alter.HasKesusTabletId() || alter.HasVersion()) {
            errStr = "Setting schemeshard owned properties is not allowed";
            return nullptr;
        }
        if (!alter.HasConfig()) {
            errStr = "Missing changes to coordination node config";
            return nullptr;
        }

        const auto& config = alter.GetConfig();
        if (!config.path().empty()) {
            errStr = "Setting path is not allowed";
            return nullptr;
        }

        return &config;
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto& alter = Transaction.GetKesus();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = alter.GetName();
        const TPathId pathId = alter.HasPathId() ? context.SS->MakeLocalId(alter.GetPathId()) : InvalidPathId;

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterKesus Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", pathId: " << pathId
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        TString errStr;
        if (!alter.HasName() && !alter.HasPathId()) {
            errStr = "Neither kesus name nor pathId in Kesus";
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        TPath path = alter.HasPathId()
            ? TPath::Init(pathId, context.SS)
            : TPath::Resolve(parentPathStr, context.SS).Dive(name);

        {
            TPath::TChecker checks = path.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsKesus()
                .NotUnderOperation()
                .IsCommonSensePath();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        const auto* alterConfig = ParseParams(alter, errStr);
        if (!alterConfig) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        TKesusInfo::TPtr kesus = context.SS->KesusInfos.at(path.Base()->PathId);
        Y_ABORT_UNLESS(kesus);
        Y_ABORT_UNLESS(path.Base()->IsCreateFinished()); // checks.NotUnderOperation checks that path not under creation

        if (kesus->AlterConfig) {
            result->SetError(
                NKikimrScheme::StatusMultipleModifications,
                "There's another alter in flight");
            return result;
        }

        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        kesus->AlterConfig.Reset(new Ydb::Coordination::Config);
        kesus->AlterConfig->CopyFrom(kesus->Config);
        kesus->AlterConfig->MergeFrom(*alterConfig);
        kesus->AlterVersion = kesus->Version + 1;
        PrepareChanges(OperationId, path.Base(), kesus, context);

        context.SS->ClearDescribePathCaches(path.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, path.Base()->PathId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TAlterKesus");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterKesus AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateAlterKesus(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAlterKesus>(id, tx);
}

ISubOperation::TPtr CreateAlterKesus(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TAlterKesus>(id, state);
}

}
