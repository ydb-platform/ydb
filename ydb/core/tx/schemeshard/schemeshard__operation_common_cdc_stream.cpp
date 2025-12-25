#include "schemeshard__operation_common.h"
#include "schemeshard_cdc_stream_common.h"
#include "schemeshard_private.h"

#include <ydb/core/base/hive.h>
#include <ydb/core/tx/datashard/datashard.h>


namespace NKikimr::NSchemeShard::NCdcStreamState {

namespace {

constexpr const char* CONTINUOUS_BACKUP_SUFFIX = "_continuousBackupImpl";

bool IsExpectedTxType(TTxState::ETxType txType) {
    switch (txType) {
    case TTxState::TxCreateCdcStreamAtTable:
    case TTxState::TxCreateCdcStreamAtTableWithInitialScan:
    case TTxState::TxAlterCdcStreamAtTable:
    case TTxState::TxAlterCdcStreamAtTableDropSnapshot:
    case TTxState::TxDropCdcStreamAtTable:
    case TTxState::TxDropCdcStreamAtTableDropSnapshot:
    case TTxState::TxRotateCdcStreamAtTable:
        return true;
    default:
        return false;
    }
}

bool IsContinuousBackupStream(const TString& streamName) {
    return streamName.EndsWith(CONTINUOUS_BACKUP_SUFFIX);
}

struct TTableVersionContext {
    TPathId PathId;
    TPathId ParentPathId;
    TPathId GrandParentPathId;
    bool IsIndexImplTable = false;
    bool IsContinuousBackupStream = false;
    bool IsPartOfContinuousBackup = false;
};

bool DetectContinuousBackupStream(const TTxState& txState, TOperationContext& context) {
    if (!txState.CdcPathId || !context.SS->PathsById.contains(txState.CdcPathId)) {
        return false;
    }

    auto cdcPath = context.SS->PathsById.at(txState.CdcPathId);
    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Checking CDC stream name"
                << ", cdcPathId: " << txState.CdcPathId
                << ", streamName: " << cdcPath->Name
                << ", at schemeshard: " << context.SS->SelfTabletId());

    return IsContinuousBackupStream(cdcPath->Name);
}

bool DetectIndexImplTable(TPathElement::TPtr path, TOperationContext& context, TPathId& outGrandParentPathId) {
    const TPathId& parentPathId = path->ParentPathId;
    if (!parentPathId || !context.SS->PathsById.contains(parentPathId)) {
        return false;
    }

    auto parentPath = context.SS->PathsById.at(parentPathId);
    if (parentPath->IsTableIndex()) {
        outGrandParentPathId = parentPath->ParentPathId;
        return true;
    }

    return false;
}

bool HasParentContinuousBackup(const TPathId& grandParentPathId, TOperationContext& context) {
    if (!grandParentPathId || !context.SS->PathsById.contains(grandParentPathId)) {
        return false;
    }

    auto grandParentPath = context.SS->PathsById.at(grandParentPathId);
    for (const auto& [childName, childPathId] : grandParentPath->GetChildren()) {
        auto childPath = context.SS->PathsById.at(childPathId);
        if (childPath->IsCdcStream() && IsContinuousBackupStream(childName)) {
            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "Detected continuous backup via parent table CDC stream"
                        << ", parentTablePathId: " << grandParentPathId
                        << ", cdcStreamName: " << childName
                        << ", at schemeshard: " << context.SS->SelfTabletId());
            return true;
        }
    }

    return false;
}

TTableVersionContext BuildTableVersionContext(
    const TTxState& txState,
    TPathElement::TPtr path,
    TOperationContext& context)
{
    TTableVersionContext ctx;
    ctx.PathId = txState.TargetPathId;
    ctx.ParentPathId = path->ParentPathId;
    ctx.IsContinuousBackupStream = DetectContinuousBackupStream(txState, context);
    ctx.IsIndexImplTable = DetectIndexImplTable(path, context, ctx.GrandParentPathId);
    
    // Check if impl table is part of continuous backup
    if (ctx.IsIndexImplTable) {
        ctx.IsPartOfContinuousBackup = HasParentContinuousBackup(ctx.GrandParentPathId, context);
    } else {
        ctx.IsPartOfContinuousBackup = ctx.IsContinuousBackupStream;
    }

    return ctx;
}

}  // namespace anonymous


TConfigurePartsAtTable::TConfigurePartsAtTable(TOperationId id)
    : OperationId(id)
{
    IgnoreMessages(DebugHint(), {});
}

bool TConfigurePartsAtTable::ProgressState(TOperationContext& context) {
    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                DebugHint() << " ProgressState"
                            << ", at schemeshard: " << context.SS->SelfTabletId());

    auto* txState = context.SS->FindTx(OperationId);
    Y_ABORT_UNLESS(txState);
    Y_ABORT_UNLESS(IsExpectedTxType(txState->TxType));
    const auto& pathId = txState->TargetPathId;

    if (NTableState::CheckPartitioningChangedForTableModification(*txState, context)) {
        NTableState::UpdatePartitioningForTableModification(OperationId, *txState, context);
    }

    NKikimrTxDataShard::TFlatSchemeTransaction tx;
    context.SS->FillSeqNo(tx, context.SS->StartRound(*txState));
    FillNotice(pathId, tx, context);

    txState->ClearShardsInProgress();
    Y_ABORT_UNLESS(txState->Shards.size());

    for (ui32 i = 0; i < txState->Shards.size(); ++i) {
        const auto& idx = txState->Shards[i].Idx;
        const auto datashardId = context.SS->ShardInfos[idx].TabletID;
        auto ev = context.SS->MakeDataShardProposal(pathId, OperationId, tx.SerializeAsString(), context.Ctx);
        context.OnComplete.BindMsgToPipe(OperationId, datashardId, idx, ev.Release());
    }

    txState->UpdateShardsInProgress(TTxState::ConfigureParts);
    return false;
}

bool TConfigurePartsAtTable::HandleReply(TEvDataShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) {
    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                DebugHint() << " HandleReply " << ev->Get()->ToString()
                            << ", at schemeshard: " << context.SS->SelfTabletId());

    if (!NTableState::CollectProposeTransactionResults(OperationId, ev, context)) {
        return false;
    }

    return true;
}


TProposeAtTable::TProposeAtTable(TOperationId id)
    : OperationId(id)
{
    IgnoreMessages(DebugHint(), {TEvDataShard::TEvProposeTransactionResult::EventType});
}

bool TProposeAtTable::ProgressState(TOperationContext& context) {
    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                DebugHint() << " ProgressState"
                            << ", at schemeshard: " << context.SS->SelfTabletId());

    const auto* txState = context.SS->FindTx(OperationId);
    Y_ABORT_UNLESS(txState);
    Y_ABORT_UNLESS(IsExpectedTxType(txState->TxType));

    TSet<TTabletId> shardSet;
    for (const auto& shard : txState->Shards) {
        Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shard.Idx));
        shardSet.insert(context.SS->ShardInfos.at(shard.Idx).TabletID);
    }

    context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, txState->MinStep, shardSet);
    return false;
}

bool TProposeAtTable::HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) {
    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                DebugHint() << " HandleReply TEvOperationPlan"
                            << ", step: " << ev->Get()->StepId
                            << ", at schemeshard: " << context.SS->SelfTabletId());

    const auto* txState = context.SS->FindTx(OperationId);
    Y_ABORT_UNLESS(txState);
    Y_ABORT_UNLESS(IsExpectedTxType(txState->TxType));
    const auto& pathId = txState->TargetPathId;

    Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
    auto path = context.SS->PathsById.at(pathId);

    Y_ABORT_UNLESS(context.SS->Tables.contains(pathId));
    auto table = context.SS->Tables.at(pathId);

    NIceDb::TNiceDb db(context.GetDB());

    // Use coordinated version if available (from backup operations)
    // Otherwise, increment by 1 for backward compatibility
    if (txState->CoordinatedSchemaVersion.Defined()) {
        table->AlterVersion = *txState->CoordinatedSchemaVersion;
    } else {
        table->AlterVersion += 1;
    }

    // Update parent index entity version if this is an index impl table with coordinated version
    auto versionCtx = BuildTableVersionContext(*txState, path, context);
    if (versionCtx.IsIndexImplTable && txState->CoordinatedSchemaVersion.Defined()) {
        if (context.SS->Indexes.contains(versionCtx.ParentPathId)) {
            auto index = context.SS->Indexes.at(versionCtx.ParentPathId);
            if (index->AlterVersion < *txState->CoordinatedSchemaVersion) {
                index->AlterVersion = *txState->CoordinatedSchemaVersion;
                context.SS->PersistTableIndexAlterVersion(db, versionCtx.ParentPathId, index);
                context.OnComplete.PublishToSchemeBoard(OperationId, versionCtx.ParentPathId);
            }
        }
    }

    context.SS->PersistTableAlterVersion(db, pathId, table);
    context.SS->ClearDescribePathCaches(path);
    context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

    context.SS->ChangeTxState(db, OperationId, TTxState::ProposedWaitParts);
    return true;
}

bool TProposeAtTable::HandleReply(TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) {
    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                DebugHint() << " TEvDataShard::TEvSchemaChanged"
                            << " triggers early, save it"
                            << ", at schemeshard: " << context.SS->SelfTabletId());

    NTableState::CollectSchemaChanged(OperationId, ev, context);
    return false;
}


bool TProposeAtTableDropSnapshot::HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) {
    TProposeAtTable::HandleReply(ev, context);

    const auto* txState = context.SS->FindTx(OperationId);
    Y_ABORT_UNLESS(txState);
    const auto& pathId = txState->TargetPathId;

    Y_ABORT_UNLESS(context.SS->TablesWithSnapshots.contains(pathId));
    const auto snapshotTxId = context.SS->TablesWithSnapshots.at(pathId);

    auto it = context.SS->SnapshotTables.find(snapshotTxId);
    if (it != context.SS->SnapshotTables.end()) {
        it->second.erase(pathId);
        if (it->second.empty()) {
            context.SS->SnapshotTables.erase(it);
        }
    }

    context.SS->SnapshotsStepIds.erase(snapshotTxId);
    context.SS->TablesWithSnapshots.erase(pathId);

    NIceDb::TNiceDb db(context.GetDB());
    context.SS->PersistDropSnapshot(db, snapshotTxId, pathId);

    context.SS->TabletCounters->Simple()[COUNTER_SNAPSHOTS_COUNT].Sub(1);
    return true;
}

}  // NKikimr::NSchemeShard::NCdcStreamState
