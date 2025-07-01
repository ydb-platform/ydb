#include "create_datashard_streaming_unit.h"
#include "change_sender_incr_restore.h"
#include "execution_unit_ctors.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tablet_flat/flat_scan_spent.h>

#define STREAMING_LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "[DataShardStreaming] [" << LogPrefix() << "] " << stream)
#define STREAMING_LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "[DataShardStreaming] [" << LogPrefix() << "] " << stream)
#define STREAMING_LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "[DataShardStreaming] [" << LogPrefix() << "] " << stream)
#define STREAMING_LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "[DataShardStreaming] [" << LogPrefix() << "] " << stream)
#define STREAMING_LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "[DataShardStreaming] [" << LogPrefix() << "] " << stream)

namespace NKikimr {
namespace NDataShard {

using namespace NKikimrTxDataShard;

bool TCreateDataShardStreamingUnit::IsReadyToExecute(TOperation::TPtr op) const {
    if (IsWaiting(op)) {
        return false;
    }

    return !DataShard.IsAnyChannelYellowStop();
}

void TCreateDataShardStreamingUnit::Abort(TOperation::TPtr op, const TActorContext& ctx, const TString& error) {
    TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

    STREAMING_LOG_E(error);

    BuildResult(op)->AddError(NKikimrTxDataShard::TError::WRONG_SHARD_STATE, error);
    ResetWaiting(op);

    Cancel(tx, ctx);
}

THolder<NTable::IScan> TCreateDataShardStreamingUnit::CreateDataShardStreamingScan(
    const ::NKikimrSchemeOp::TCreateDataShardStreaming& streaming,
    ui64 txId)
{
    TPathId sourcePathId = TPathId::FromProto(streaming.GetSourcePathId());
    TPathId targetPathId = TPathId::FromProto(streaming.GetTargetPathId());
    const ui64 tableId = streaming.GetSourcePathId().GetLocalId();

    // Create a scan that will use the change exchange infrastructure
    // to stream changes to the target DataShard
    return CreateIncrementalRestoreScan(
        DataShard.SelfId(),
        [=, tabletID = DataShard.TabletID(), generation = DataShard.Generation(), tabletActor = DataShard.SelfId()]
        (const TActorContext& ctx, TActorId parent) {
            // Create a specialized change sender for DataShard-to-DataShard streaming
            return ctx.Register(
                CreateDataShardStreamingChangeSender(
                    parent,
                    NDataShard::TDataShardId{
                        .TabletId = tabletID,
                        .Generation = generation,
                        .ActorId = tabletActor,
                    },
                    sourcePathId,
                    targetPathId,
                    streaming.GetStreamingConfig()));
        },
        sourcePathId,
        DataShard.GetUserTables().at(tableId),
        targetPathId,
        txId,
        {} // Use default limits for now
    );
}

EExecutionStatus TCreateDataShardStreamingUnit::Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) {
    TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

    Y_ENSURE(tx->GetSchemeTx().HasCreateDataShardStreaming());
    const auto& streaming = tx->GetSchemeTx().GetCreateDataShardStreaming();

    const ui64 tableId = streaming.GetSourcePathId().GetLocalId();
    if (!DataShard.GetUserTables().contains(tableId)) {
        Abort(op, ctx, TStringBuilder() << "Table not found: " << tableId);
        return EExecutionStatus::Executed;
    }

    const ui32 localTableId = DataShard.GetUserTables().at(tableId)->LocalTid;
    if (!txc.DB.GetScheme().GetTableInfo(localTableId)) {
        Abort(op, ctx, TStringBuilder() << "Table schema not found: " << localTableId);
        return EExecutionStatus::Executed;
    }

    if (DataShard.IsAnyChannelYellowStop()) {
        SetWaiting(op);
        return EExecutionStatus::Continue;
    }

    if (!op->IsWaitingForScan()) {
        // Create and start the streaming scan
        auto scan = CreateDataShardStreamingScan(streaming, tx->GetTxId());
        if (!scan) {
            Abort(op, ctx, "Failed to create DataShard streaming scan");
            return EExecutionStatus::Executed;
        }

        DataShard.QueueScan(localTableId, std::move(scan), tx->GetTxId(), TRowVersion::Min());
        SetWaiting(op);

        STREAMING_LOG_I("Started DataShard streaming scan"
            << " from " << streaming.GetSourcePathId().ShortDebugString()
            << " to " << streaming.GetTargetPathId().ShortDebugString()
            << " txId: " << tx->GetTxId());
    }

    // Check if scan is completed
    if (op->IsWaitingForScan()) {
        return EExecutionStatus::Continue;
    }

    ResetWaiting(op);
    
    STREAMING_LOG_I("DataShard streaming completed successfully"
        << " txId: " << tx->GetTxId());

    return EExecutionStatus::Executed;
}

void TCreateDataShardStreamingUnit::Complete(TOperation::TPtr op, const TActorContext& ctx) {
    TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

    STREAMING_LOG_D("DataShard streaming unit completed"
        << " txId: " << tx->GetTxId());
}

// Factory function for creating the change sender specialized for DataShard streaming
IActor* CreateDataShardStreamingChangeSender(
    const TActorId& changeServerActor, 
    const TDataShardId& dataShard, 
    const TPathId& sourcePathId, 
    const TPathId& targetPathId,
    const TString& streamingConfig) 
{
    // For now, reuse the incremental restore change sender as the base
    // This can be extended later with DataShard-specific streaming logic
    return CreateIncrRestoreChangeSender(changeServerActor, dataShard, sourcePathId, targetPathId);
}

} // namespace NDataShard
} // namespace NKikimr
