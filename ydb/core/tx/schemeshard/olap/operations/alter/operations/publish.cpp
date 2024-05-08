#include "publish.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {
bool TPropose::HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) {
    TStepId step = TStepId(ev->Get()->StepId);
    TTabletId ssId = context.SS->SelfTabletId();

    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        DebugHint() << " HandleReply TEvOperationPlan"
        << " at tablet: " << ssId
        << ", stepId: " << step);

    TTxState* txState = context.SS->FindTxSafe(OperationId, TTxState::TxAlterColumnTable);

    const TPathId pathId = txState->TargetPathId;
    const TPath path = TPath::Init(pathId, context.SS);

    NIceDb::TNiceDb db(context.GetDB());

    auto tableInfo = context.SS->ColumnTables.TakeVerified(pathId);
    context.SS->PersistColumnTable(db, pathId, *tableInfo);

    auto parentDir = context.SS->PathsById.at(path->ParentPathId);
    if (parentDir->IsLikeDirectory()) {
        ++parentDir->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDir);
    }
    context.SS->ClearDescribePathCaches(parentDir);
    context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);

    ++path->DirAlterVersion;
    context.SS->PersistPathDirAlterVersion(db, path.Base());
    context.SS->ClearDescribePathCaches(path.Base());
    context.OnComplete.PublishToSchemeBoard(OperationId, path->PathId);

    context.SS->ChangeTxState(db, OperationId, TTxState::ProposedWaitParts);
    return true;
}

bool TPropose::ProgressState(TOperationContext& context) {
    TTabletId ssId = context.SS->SelfTabletId();

    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        DebugHint() << " HandleReply ProgressState"
        << " at tablet: " << ssId);

    TTxState* txState = context.SS->FindTxSafe(OperationId, TTxState::TxAlterColumnTable);

    TSet<TTabletId> shardSet;
    for (const auto& shard : txState->Shards) {
        TShardIdx idx = shard.Idx;
        TTabletId tablet = context.SS->ShardInfos.at(idx).TabletID;
        shardSet.insert(tablet);
    }

    context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, txState->MinStep, shardSet);
    return false;
}

THolder<NKikimr::NSchemeShard::TProposeResponse> TPublishAlterColumnTable::Propose(const TString&, TOperationContext& context) {
    const TTabletId ssId = context.SS->SelfTabletId();

    TTxState* txState = context.SS->FindTxSafe(OperationId, TTxState::TxAlterColumnTable);
    const TPathId pathId = txState->TargetPathId;
    const TPath path = TPath::Init(pathId, context.SS);

    auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

    NIceDb::TNiceDb db(context.GetDB());

    auto entity = ISSEntity::GetPathEntityVerified(context, path);
    AFL_VERIFY(entity->GetShardIds().size());
    for (ui64 columnShardId : entity->GetShardIds()) {
        auto tabletId = TTabletId(columnShardId);
        auto shardIdx = context.SS->TabletIdToShardIdx.at(tabletId);

        Y_VERIFY_S(context.SS->ShardInfos.contains(shardIdx), "Unknown shardIdx " << shardIdx);
        txState->Shards.emplace_back(shardIdx, context.SS->ShardInfos[shardIdx].TabletType, TTxState::ConfigureParts);

        context.SS->ShardInfos[shardIdx].CurrentTxId = OperationId.GetTxId();
        context.SS->PersistShardTx(db, shardIdx, OperationId.GetTxId());
    }

    SetState(TTxState::Propose);
    return result;
}

void TPublishAlterColumnTable::AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) {
    LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "TAlterColumnTable AbortUnsafe"
        << ", opId: " << OperationId
        << ", forceDropId: " << forceDropTxId
        << ", at schemeshard: " << context.SS->TabletID());

    context.OnComplete.DoneOperation(OperationId);
}

}
