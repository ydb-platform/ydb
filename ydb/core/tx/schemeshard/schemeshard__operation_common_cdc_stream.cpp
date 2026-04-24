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

    table->AlterVersion += 1;
    context.SS->PersistTableAlterVersion(db, pathId, table);
    context.SS->ClearDescribePathCaches(path);

    // If the table is an indexImplTable (parent is a TableIndex), keep the
    // parent index's AlterVersion in sync with the impl table AND bump the
    // grandparent main table's AlterVersion so its GeneralVersion advances.
    // Without this, the main table's scheme-board entry gets republished with
    // the same GeneralVersion as before, scheme-cache clients do not evict the
    // stale entry, and KQP sees:
    //   T.TableIndexes[idx].SchemaVersion (stale) != implTable.SchemaVersion (fresh)
    // producing "schema version mismatch during metadata loading".
    if (path->ParentPathId && context.SS->PathsById.contains(path->ParentPathId)) {
        auto indexPath = context.SS->PathsById.at(path->ParentPathId);
        if (indexPath->IsTableIndex() && context.SS->Indexes.contains(path->ParentPathId)) {
            const TPathId indexPathId = path->ParentPathId;
            auto index = context.SS->Indexes.at(indexPathId);
            if (index->AlterVersion < table->AlterVersion) {
                index->AlterVersion = table->AlterVersion;
                if (index->AlterData && index->AlterData->AlterVersion < table->AlterVersion) {
                    index->AlterData->AlterVersion = table->AlterVersion;
                    context.SS->PersistTableIndexAlterData(db, indexPathId);
                }
                context.SS->PersistTableIndexAlterVersion(db, indexPathId, index);
            }

            // Advance the parent index's DirAlterVersion (ChildrenVersion
            // component of GeneralVersion) so its scheme-board describe is
            // recognised as a new version on republish.
            ++indexPath->DirAlterVersion;
            context.SS->PersistPathDirAlterVersion(db, indexPath);
            context.SS->ClearDescribePathCaches(indexPath);

            // Also advance the main table's DirAlterVersion (ChildrenVersion
            // component of GeneralVersion) so scheme-cache clients see a new
            // version for the table path and evict the stale cached describe
            // (which embedded the old index SchemaVersion).
            // We bump DirAlterVersion rather than AlterVersion because
            // AlterVersion changes trigger schema-change transactions to
            // datashards; DirAlterVersion is purely a descriptor-staleness
            // counter that does not affect datashard schema negotiations.
            const TPathId mainTablePathId = indexPath->ParentPathId;
            if (mainTablePathId && context.SS->PathsById.contains(mainTablePathId)) {
                auto mainTablePath = context.SS->PathsById.at(mainTablePathId);
                if (mainTablePath->IsTable()) {
                    ++mainTablePath->DirAlterVersion;
                    context.SS->PersistPathDirAlterVersion(db, mainTablePath);
                    context.SS->ClearDescribePathCaches(mainTablePath);
                }
            }
        }
    }

    // Cascade publication republishes this path together with all its ancestors
    // up to the domain root, so that parent describes (which embed child
    // versions, e.g. TableIndex.SchemaVersion) reflect the current child state.
    context.OnComplete.PublishToSchemeBoardWithAncestors(OperationId, pathId, context.SS);

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
