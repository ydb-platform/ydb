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

// Synchronizes AlterVersion across index entities without modifying impl table versions.
// Uses lock-free-like helping coordination where each operation helps update sibling index entities.
void HelpSyncSiblingVersions(
    const TPathId& myImplTablePathId,
    const TPathId& myIndexPathId,
    const TPathId& parentTablePathId,
    ui64 myVersion,
    TOperationId operationId,
    TOperationContext& context,
    NIceDb::TNiceDb& db)
{
    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "HelpSyncSiblingVersions ENTRY"
                << ", myImplTablePathId: " << myImplTablePathId
                << ", myIndexPathId: " << myIndexPathId
                << ", parentTablePathId: " << parentTablePathId
                << ", myVersion: " << myVersion
                << ", operationId: " << operationId
                << ", at schemeshard: " << context.SS->SelfTabletId());
    
    TVector<TPathId> allIndexPathIds;
    TVector<TPathId> allImplTablePathIds;
    
    if (!context.SS->PathsById.contains(parentTablePathId)) {
        LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Parent table not found in PathsById"
                   << ", parentTablePathId: " << parentTablePathId
                   << ", at schemeshard: " << context.SS->SelfTabletId());
        return;
    }
    
    auto parentTablePath = context.SS->PathsById.at(parentTablePathId);
    
    for (const auto& [childName, childPathId] : parentTablePath->GetChildren()) {
        auto childPath = context.SS->PathsById.at(childPathId);
        
        if (!childPath->IsTableIndex() || childPath->Dropped()) {
            continue;
        }
        
        allIndexPathIds.push_back(childPathId);
        
        auto indexPath = context.SS->PathsById.at(childPathId);
        Y_ABORT_UNLESS(indexPath->GetChildren().size() == 1);
        auto [implTableName, implTablePathId] = *indexPath->GetChildren().begin();
        allImplTablePathIds.push_back(implTablePathId);
        
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Found index and impl table"
                    << ", indexPathId: " << childPathId
                    << ", implTablePathId: " << implTablePathId
                    << ", at schemeshard: " << context.SS->SelfTabletId());
    }
    
    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Collected index family"
                << ", indexCount: " << allIndexPathIds.size()
                << ", implTableCount: " << allImplTablePathIds.size()
                << ", at schemeshard: " << context.SS->SelfTabletId());
    
    ui64 maxVersion = myVersion;
    
    for (const auto& indexPathId : allIndexPathIds) {
        if (context.SS->Indexes.contains(indexPathId)) {
            auto index = context.SS->Indexes.at(indexPathId);
            maxVersion = Max(maxVersion, index->AlterVersion);
        }
    }
    
    for (const auto& implTablePathId : allImplTablePathIds) {
        if (context.SS->Tables.contains(implTablePathId)) {
            auto implTable = context.SS->Tables.at(implTablePathId);
            maxVersion = Max(maxVersion, implTable->AlterVersion);
        }
    }
    
    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                 "Computed maximum version across all siblings"
                 << ", myVersion: " << myVersion
                 << ", maxVersion: " << maxVersion
                 << ", at schemeshard: " << context.SS->SelfTabletId());
    
    // DO NOT update self to catch up - each impl table has already incremented its own version.
    // Changing our version based on other operations would cause datashard version mismatches.
    
    if (context.SS->Indexes.contains(myIndexPathId)) {
        auto myIndex = context.SS->Indexes.at(myIndexPathId);
        if (myIndex->AlterVersion < maxVersion) {
            myIndex->AlterVersion = maxVersion;
            context.SS->PersistTableIndexAlterVersion(db, myIndexPathId, myIndex);
            
            auto myIndexPath = context.SS->PathsById.at(myIndexPathId);
            context.SS->ClearDescribePathCaches(myIndexPath);
            context.OnComplete.PublishToSchemeBoard(operationId, myIndexPathId);
            
            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "Updated my index entity"
                        << ", myIndexPathId: " << myIndexPathId
                        << ", newVersion: " << maxVersion
                        << ", at schemeshard: " << context.SS->SelfTabletId());
        }
    }
    
    ui64 indexesUpdated = 0;
    for (const auto& indexPathId : allIndexPathIds) {
        if (indexPathId == myIndexPathId) {
            continue;
        }
        
        if (!context.SS->Indexes.contains(indexPathId)) {
            continue;
        }
        
        auto index = context.SS->Indexes.at(indexPathId);
        if (index->AlterVersion < maxVersion) {
            index->AlterVersion = maxVersion;
            context.SS->PersistTableIndexAlterVersion(db, indexPathId, index);
            
            auto indexPath = context.SS->PathsById.at(indexPathId);
            context.SS->ClearDescribePathCaches(indexPath);
            context.OnComplete.PublishToSchemeBoard(operationId, indexPathId);
            
            indexesUpdated++;
            
            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "Updated sibling index entity"
                        << ", indexPathId: " << indexPathId
                        << ", newVersion: " << maxVersion
                        << ", at schemeshard: " << context.SS->SelfTabletId());
        }
    }
    
    // CRITICAL: DO NOT help update sibling impl tables.
    // Impl tables have datashards that expect schema change transactions.
    // Bumping AlterVersion without TX_KIND_SCHEME_CHANGED causes "Wrong schema version" errors.
    // Each impl table must increment its own version when its CDC operation executes.
    
    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                 "HelpSyncSiblingVersions COMPLETE"
                 << ", maxVersion: " << maxVersion
                 << ", indexesUpdated: " << indexesUpdated
                 << ", totalIndexes: " << allIndexPathIds.size()
                 << ", totalImplTables: " << allImplTablePathIds.size()
                 << ", NOTE: Sibling impl tables NOT updated (they update themselves)"
                 << ", at schemeshard: " << context.SS->SelfTabletId());
}

}  // namespace anonymous

// Public functions for version synchronization (used by copy-table and other operations)
void SyncIndexEntityVersion(
    const TPathId& indexPathId,
    ui64 targetVersion,
    TOperationId operationId,
    TOperationContext& context,
    NIceDb::TNiceDb& db)
{
    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "SyncIndexEntityVersion ENTRY"
                << ", indexPathId: " << indexPathId
                << ", targetVersion: " << targetVersion
                << ", operationId: " << operationId
                << ", at schemeshard: " << context.SS->SelfTabletId());

    if (!context.SS->Indexes.contains(indexPathId)) {
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "SyncIndexEntityVersion EXIT - index not found"
                    << ", indexPathId: " << indexPathId
                    << ", at schemeshard: " << context.SS->SelfTabletId());
        return;
    }

    auto index = context.SS->Indexes.at(indexPathId);
    ui64 oldIndexVersion = index->AlterVersion;

    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "SyncIndexEntityVersion current state"
                << ", indexPathId: " << indexPathId
                << ", currentIndexVersion: " << oldIndexVersion
                << ", targetVersion: " << targetVersion
                << ", at schemeshard: " << context.SS->SelfTabletId());

    // Only update if we're increasing the version (prevent downgrade due to race conditions)
    if (targetVersion > oldIndexVersion) {
        index->AlterVersion = targetVersion;

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "SyncIndexEntityVersion UPDATING index->AlterVersion"
                    << ", indexPathId: " << indexPathId
                    << ", oldVersion: " << oldIndexVersion
                    << ", newVersion: " << index->AlterVersion
                    << ", at schemeshard: " << context.SS->SelfTabletId());

        context.SS->PersistTableIndexAlterVersion(db, indexPathId, index);

        auto indexPath = context.SS->PathsById.at(indexPathId);
        context.SS->ClearDescribePathCaches(indexPath);
        context.OnComplete.PublishToSchemeBoard(operationId, indexPathId);

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Synced index entity version"
                    << ", indexPathId: " << indexPathId
                    << ", oldVersion: " << oldIndexVersion
                    << ", newVersion: " << index->AlterVersion
                    << ", at schemeshard: " << context.SS->SelfTabletId());
    } else {
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Skipping index entity sync - already at higher version"
                    << ", indexPathId: " << indexPathId
                    << ", currentVersion: " << oldIndexVersion
                    << ", targetVersion: " << targetVersion
                    << ", at schemeshard: " << context.SS->SelfTabletId());
    }
}

void SyncChildIndexes(
    TPathElement::TPtr parentPath,
    ui64 targetVersion,
    TOperationId operationId,
    TOperationContext& context,
    NIceDb::TNiceDb& db)
{
    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "SyncChildIndexes ENTRY"
                << ", parentPath: " << parentPath->PathId
                << ", targetVersion: " << targetVersion
                << ", operationId: " << operationId
                << ", at schemeshard: " << context.SS->SelfTabletId());

    for (const auto& [childName, childPathId] : parentPath->GetChildren()) {
        auto childPath = context.SS->PathsById.at(childPathId);

        // Skip non-index children and deleted indexes
        if (!childPath->IsTableIndex() || childPath->Dropped()) {
            continue;
        }

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "SyncChildIndexes processing index"
                    << ", indexPathId: " << childPathId
                    << ", indexName: " << childName
                    << ", targetVersion: " << targetVersion
                    << ", at schemeshard: " << context.SS->SelfTabletId());

        NCdcStreamState::SyncIndexEntityVersion(childPathId, targetVersion, operationId, context, db);

        // NOTE: We intentionally do NOT sync the index impl table version here.
        // Bumping AlterVersion without sending a TX_KIND_SCHEME transaction to datashards
        // causes SCHEME_CHANGED errors because datashards still have the old version.
        // The version should only be incremented when there's an actual schema change.
        
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Synced parent index version with parent table"
                    << ", parentTable: " << parentPath->Name
                    << ", indexName: " << childName
                    << ", indexPathId: " << childPathId
                    << ", newVersion: " << targetVersion
                    << ", at schemeshard: " << context.SS->SelfTabletId());
    }

    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "SyncChildIndexes EXIT"
                << ", parentPath: " << parentPath->PathId
                << ", targetVersion: " << targetVersion
                << ", at schemeshard: " << context.SS->SelfTabletId());
}


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

    auto versionCtx = BuildTableVersionContext(*txState, path, context);
    
    bool isIndexImplTableCdc = versionCtx.IsPartOfContinuousBackup && versionCtx.IsIndexImplTable;
    
    if (isIndexImplTableCdc) {
        table->AlterVersion += 1;
        ui64 myIncrementedVersion = table->AlterVersion;
        
        HelpSyncSiblingVersions(
            pathId,
            versionCtx.ParentPathId,
            versionCtx.GrandParentPathId,
            myIncrementedVersion,
            OperationId,
            context,
            db);
    } else {
        table->AlterVersion += 1;
    }
    
    if (versionCtx.IsContinuousBackupStream && !versionCtx.IsIndexImplTable) {
        NCdcStreamState::SyncChildIndexes(path, table->AlterVersion, OperationId, context, db);
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
