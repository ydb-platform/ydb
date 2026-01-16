#include "schemeshard__operation_common.h"
#include "schemeshard_cdc_stream_common.h"
#include "schemeshard_private.h"

#include <ydb/core/base/hive.h>
#include <ydb/core/tx/datashard/datashard.h>


namespace NKikimr::NSchemeShard::NCdcStreamState {

namespace {

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

struct TTableVersionContext {
    TPathId PathId;
    TPathId ParentPathId;
    TPathId GrandParentPathId;
    bool IsIndexImplTable = false;
};

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

TTableVersionContext BuildTableVersionContext(
    const TTxState& txState,
    TPathElement::TPtr path,
    TOperationContext& context)
{
    TTableVersionContext ctx;
    ctx.PathId = txState.TargetPathId;
    ctx.ParentPathId = path->ParentPathId;
    ctx.IsIndexImplTable = DetectIndexImplTable(path, context, ctx.GrandParentPathId);
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

    // Don't call InitAlterData() here - it was already called in ConfigureParts,
    // and calling it again after another subop's Done() updated AlterVersion
    // would incorrectly create a new version.
    Y_ABORT_UNLESS(table->AlterData && table->AlterData->CoordinatedSchemaVersion,
        "AlterData with CoordinatedSchemaVersion must be set in ConfigureParts before Done phase");
    table->AlterVersion = Max(table->AlterVersion, *table->AlterData->CoordinatedSchemaVersion);

    // Persist updated AlterVersion so it survives restart
    context.SS->PersistTableAlterVersion(db, pathId, table);

    // After successful completion, clear AlterTableFull if this was the last user
    if (table->ReleaseAlterData(OperationId)) {
        context.SS->PersistClearAlterTableFull(db, pathId);
    }

    // Sync index versions before publishing (indexes must be published before parent table)
    auto versionCtx = BuildTableVersionContext(*txState, path, context);
    TVector<TPathId> indexesToPublish;
    TPathId mainTableToPublish;

    if (versionCtx.IsIndexImplTable) {
        if (context.SS->Indexes.contains(versionCtx.ParentPathId)) {
            auto index = context.SS->Indexes.at(versionCtx.ParentPathId);
            ui64 targetVersion = table->AlterVersion;
            if (index->AlterVersion < targetVersion) {
                index->AlterVersion = targetVersion;
                if (index->AlterData && index->AlterData->AlterVersion < targetVersion) {
                    index->AlterData->AlterVersion = targetVersion;
                    context.SS->PersistTableIndexAlterData(db, versionCtx.ParentPathId);
                }
                context.SS->PersistTableIndexAlterVersion(db, versionCtx.ParentPathId, index);
                indexesToPublish.push_back(versionCtx.ParentPathId);

                if (context.SS->PathsById.contains(versionCtx.ParentPathId)) {
                    auto indexPath = context.SS->PathsById.at(versionCtx.ParentPathId);
                    context.SS->ClearDescribePathCaches(indexPath);

                    if (indexPath->ParentPathId && context.SS->PathsById.contains(indexPath->ParentPathId)) {
                        auto mainTablePath = context.SS->PathsById.at(indexPath->ParentPathId);
                        if (mainTablePath->IsTable()) {
                            mainTableToPublish = indexPath->ParentPathId;
                            context.SS->ClearDescribePathCaches(mainTablePath);
                        }
                    }
                }
            }
        }
    } else {
        // For main tables, sync all child indexes
        // Note: We collect indexes to publish but don't publish here since we control ordering
        ui64 targetVersion = table->AlterVersion;
        for (const auto& [childName, childPathId] : path->GetChildren()) {
            if (!context.SS->PathsById.contains(childPathId)) {
                continue;
            }
            auto childPath = context.SS->PathsById.at(childPathId);
            if (!childPath->IsTableIndex() || childPath->Dropped()) {
                continue;
            }
            if (!context.SS->Indexes.contains(childPathId)) {
                continue;
            }
            auto index = context.SS->Indexes.at(childPathId);
            if (index->AlterVersion < targetVersion) {
                index->AlterVersion = targetVersion;
                if (index->AlterData && index->AlterData->AlterVersion < targetVersion) {
                    index->AlterData->AlterVersion = targetVersion;
                    context.SS->PersistTableIndexAlterData(db, childPathId);
                }
                context.SS->PersistTableIndexAlterVersion(db, childPathId, index);
                context.SS->ClearDescribePathCaches(childPath);
                indexesToPublish.push_back(childPathId);
            }
        }
        mainTableToPublish = pathId;
    }

    context.SS->PersistTableAlterVersion(db, pathId, table);
    context.SS->ClearDescribePathCaches(path);

    // Publish indexes first, then impl table, then main table
    for (const auto& indexPathId : indexesToPublish) {
        context.OnComplete.PublishToSchemeBoard(OperationId, indexPathId);
    }
    if (versionCtx.IsIndexImplTable) {
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);
    }
    if (mainTableToPublish) {
        context.OnComplete.PublishToSchemeBoard(OperationId, mainTableToPublish);
    }

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

namespace NKikimr::NSchemeShard::NTableIndexVersion {

TVector<TPathId> SyncChildIndexVersions(
    TPathElement::TPtr path,
    TTableInfo::TPtr table,
    ui64 targetVersion,
    TOperationId operationId,
    TOperationContext& context,
    NIceDb::TNiceDb& db,
    bool skipPlannedToDrop)
{
    Y_UNUSED(table);
    TVector<TPathId> publishedIndexes;

    for (const auto& [childName, childPathId] : path->GetChildren()) {
        if (!context.SS->PathsById.contains(childPathId)) {
            continue;
        }
        auto childPath = context.SS->PathsById.at(childPathId);
        if (!childPath->IsTableIndex() || childPath->Dropped()) {
            continue;
        }
        if (skipPlannedToDrop && childPath->PlannedToDrop()) {
            continue;
        }
        if (!context.SS->Indexes.contains(childPathId)) {
            continue;
        }
        auto index = context.SS->Indexes.at(childPathId);
        if (index->AlterVersion < targetVersion) {
            index->AlterVersion = targetVersion;
            // If there's ongoing alter operation, also update alterData version to converge
            if (index->AlterData && index->AlterData->AlterVersion < targetVersion) {
                index->AlterData->AlterVersion = targetVersion;
                context.SS->PersistTableIndexAlterData(db, childPathId);
            }
            context.SS->PersistTableIndexAlterVersion(db, childPathId, index);
            context.SS->ClearDescribePathCaches(childPath);
            context.OnComplete.PublishToSchemeBoard(operationId, childPathId);
            publishedIndexes.push_back(childPathId);
        }
    }

    return publishedIndexes;
}

}  // NKikimr::NSchemeShard::NTableIndexVersion
