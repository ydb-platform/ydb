#include "continue.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

namespace {
class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder() << "TAlterColumnTable TConfigureParts operationId#" << OperationId;
    }

public:
    TConfigureParts(TOperationId id)
        : OperationId(id) {
        IgnoreMessages(DebugHint(), { TEvHive::TEvCreateTabletReply::EventType });
    }

    bool HandleReply(TEvColumnShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override {
        return NTableState::CollectProposeTransactionResults(OperationId, ev, context);
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            DebugHint() << " ProgressState"
            << " at tabletId# " << ssId);

        TTxState* txState = context.SS->FindTxSafe(OperationId, TTxState::TxAlterColumnTable);

        TPathId pathId = txState->TargetPathId;
        TPath path = TPath::Init(pathId, context.SS);
        TString pathString = path.PathString();

        auto entity = ISSEntity::GetPathEntityVerified(context, path);
        auto evolution = entity->GetCurrentEvolution();
        AFL_VERIFY(!!evolution);

        txState->ClearShardsInProgress();
        auto seqNo = context.SS->StartRound(*txState);

        for (auto& shard : txState->Shards) {
            const TTabletId tabletId = context.SS->ShardInfos[shard.Idx].TabletID;
            AFL_VERIFY(shard.TabletType == ETabletType::ColumnShard);
            auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(
                evolution->GetShardTransactionKind(),
                context.SS->TabletID(),
                context.Ctx.SelfID,
                ui64(OperationId.GetTxId()),
                evolution->GetShardTxBody(pathId, (ui64)tabletId, seqNo),
                context.SS->SelectProcessingParams(txState->TargetPathId));

            context.OnComplete.BindMsgToPipe(OperationId, tabletId, shard.Idx, event.release());

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                DebugHint() << " ProgressState"
                << " Propose modify scheme on shard"
                << " tabletId: " << tabletId);
        }

        txState->UpdateShardsInProgress();
        return false;
    }
};

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder() << "TAlterColumnTable TPropose operationId#" << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id) {
        IgnoreMessages(DebugHint(), { TEvHive::TEvCreateTabletReply::EventType });
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            DebugHint() << " ProgressState"
            << " at tabletId# " << ssId);

        TTxState* txState = context.SS->FindTxSafe(OperationId, TTxState::TxAlterColumnTable);

        TPathId pathId = txState->TargetPathId;
        TPath path = TPath::Init(pathId, context.SS);
        TString pathString = path.PathString();

        auto entity = ISSEntity::GetPathEntityVerified(context, path);
        auto evolution = entity->ExtractCurrentEvolution();
        AFL_VERIFY(!!evolution);

        txState->ClearShardsInProgress();
        txState->UpdateShardsInProgress();

        NIceDb::TNiceDb db(context.GetDB());
        TEvolutionFinishContext fContext(&path, &context, &db);
        evolution->FinishEvolution(fContext);
        entity->Persist(fContext);
        return false;
    }
};

}

THolder<TProposeResponse> TEvoluteAlterColumnTable::Propose(const TString&, TOperationContext& context) {
    const TTabletId ssId = context.SS->SelfTabletId();

    auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

    const TString& parentPathStr = Transaction.GetWorkingDir();
    const TString& name = Transaction.HasAlterColumnTable() ? Transaction.GetAlterColumnTable().GetName() : Transaction.GetAlterTable().GetName();
    LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "TAlterColumnTable Propose"
        << ", path: " << parentPathStr << "/" << name
        << ", opId: " << OperationId
        << ", at schemeshard: " << ssId);

    TPath path = TPath::Resolve(parentPathStr, context.SS).Dive(name);
    auto originalEntity = ISSEntity::GetPathEntityVerified(context, path);

    TProposeErrorCollector errors(*result);

    std::shared_ptr<ISSEntityEvolution> evolution = originalEntity->GetCurrentEvolution();
    if (!evolution) {
        errors.AddError("no evolution for step");
        return result;
    }

    NIceDb::TNiceDb db(context.GetDB());

    {
        TEvolutionStartContext startContext(&path, &context, &db);
        auto status = evolution->StartEvolution(startContext);
        if (status.IsFail()) {
            errors.AddError(status.GetErrorMessage());
            return result;
        }
    }

    TTxState* txState = context.SS->FindTxSafe(OperationId, TTxState::TxAlterColumnTable);

    // TODO: we need to know all shards where this table is currently active
    for (ui64 columnShardId : evolution->GetShardIds()) {
        auto tabletId = TTabletId(columnShardId);
        auto shardIdx = context.SS->TabletIdToShardIdx.at(tabletId);

        Y_VERIFY_S(context.SS->ShardInfos.contains(shardIdx), "Unknown shardIdx " << shardIdx);
        txState->Shards.emplace_back(shardIdx, context.SS->ShardInfos[shardIdx].TabletType, TTxState::ConfigureParts);

        context.SS->ShardInfos[shardIdx].CurrentTxId = OperationId.GetTxId();
        context.SS->PersistShardTx(db, shardIdx, OperationId.GetTxId());
    }

    if (evolution->GetShardIds().size()) {
        txState->State = TTxState::ConfigureParts;
        SetState(TTxState::ConfigureParts);
    } else {
        SetState(TTxState::Done);
    }
    return result;
}

TSubOperationState::TPtr TEvoluteAlterColumnTable::SelectStateFunc(TTxState::ETxState state) {
    switch (state) {
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

void TEvoluteAlterColumnTable::AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) {
    LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "TAlterColumnTable AbortUnsafe"
        << ", opId: " << OperationId
        << ", forceDropId: " << forceDropTxId
        << ", at schemeshard: " << context.SS->TabletID());

    context.OnComplete.DoneOperation(OperationId);
}

}
