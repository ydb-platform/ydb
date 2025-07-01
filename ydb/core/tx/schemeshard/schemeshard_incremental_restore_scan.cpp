#include "schemeshard_impl.h"
#include "schemeshard_incremental_restore_scan.h"

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/base/tablet_pipe.h>

#if defined LOG_D || \
    defined LOG_W || \
    defined LOG_N || \
    defined LOG_E
#error log macro redefinition
#endif

#define LOG_D(stream) LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[IncrementalRestore] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[IncrementalRestore] " << stream)
#define LOG_W(stream) LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[IncrementalRestore] " << stream)
#define LOG_E(stream) LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[IncrementalRestore] " << stream)

namespace NKikimr::NSchemeShard::NIncrementalRestoreScan {

class TTxProgress: public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
private:
    // Input params
    TEvPrivate::TEvRunIncrementalRestore::TPtr RunIncrementalRestore = nullptr;
    struct {
        TOperationId OperationId;
        TTabletId TabletId;
        explicit operator bool() const { return OperationId && TabletId; }
    } PipeRetry;

    // Side effects
    TDeque<std::tuple<TOperationId, TTabletId, THolder<IEventBase>>> RestoreRequests;
    TOperationId OperationToProgress;

public:
    TTxProgress() = delete;

    explicit TTxProgress(TSelf* self, TEvPrivate::TEvRunIncrementalRestore::TPtr& ev)
        : TTransactionBase(self)
        , RunIncrementalRestore(ev)
    {
    }

    explicit TTxProgress(TSelf* self, const TOperationId& operationId, TTabletId tabletId)
        : TTransactionBase(self)
        , PipeRetry({operationId, tabletId})
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_PROGRESS_INCREMENTAL_RESTORE;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        if (RunIncrementalRestore) {
            return OnRunIncrementalRestore(txc, ctx);
        } else if (PipeRetry) {
            return OnPipeRetry(txc, ctx);
        } else {
            Y_ABORT("unreachable");
        }
    }

    void Complete(const TActorContext& ctx) override {
        // Send restore requests to DataShards
        for (auto& [operationId, tabletId, ev] : RestoreRequests) {
            LOG_D("Sending restore request to DataShard"
                << ": operationId# " << operationId
                << ", tabletId# " << tabletId);
            
            // TODO: Implement dedicated pipe pool for incremental restore like CdcStreamScanPipes
            // For now, send directly to the DataShard
            auto pipe = NTabletPipe::CreateClient(ctx.SelfID, ui64(tabletId));
            auto pipeId = ctx.Register(pipe);
            NTabletPipe::SendData(ctx, pipeId, ev.Release());
        }

        // Schedule next progress check if needed
        if (OperationToProgress) {
            TPathId backupCollectionPathId;
            if (Self->LongIncrementalRestoreOps.contains(OperationToProgress)) {
                const auto& op = Self->LongIncrementalRestoreOps.at(OperationToProgress);
                backupCollectionPathId.OwnerId = op.GetBackupCollectionPathId().GetOwnerId();
                backupCollectionPathId.LocalPathId = op.GetBackupCollectionPathId().GetLocalId();
                
                LOG_D("Scheduling next progress check"
                    << ": operationId# " << OperationToProgress
                    << ", backupCollectionPathId# " << backupCollectionPathId);
                    
                ctx.Send(ctx.SelfID, new TEvPrivate::TEvRunIncrementalRestore(backupCollectionPathId));
            }
        }
    }

private:
    bool OnRunIncrementalRestore(TTransactionContext&, const TActorContext& ctx) {
        const auto& pathId = RunIncrementalRestore->Get()->BackupCollectionPathId;

        LOG_D("Run incremental restore"
            << ": backupCollectionPathId# " << pathId);

        // Find the backup collection
        if (!Self->PathsById.contains(pathId)) {
            LOG_W("Cannot run incremental restore"
                << ": backupCollectionPathId# " << pathId
                << ", reason# " << "backup collection doesn't exist");
            return true;
        }

        auto path = Self->PathsById.at(pathId);
        if (!path->IsBackupCollection()) {
            LOG_W("Cannot run incremental restore"
                << ": backupCollectionPathId# " << pathId
                << ", reason# " << "path is not a backup collection");
            return true;
        }

        // Find the corresponding incremental restore operation
        TOperationId operationId;
        bool operationFound = false;
        for (const auto& [opId, op] : Self->LongIncrementalRestoreOps) {
            TPathId opBackupCollectionPathId;
            opBackupCollectionPathId.OwnerId = op.GetBackupCollectionPathId().GetOwnerId();
            opBackupCollectionPathId.LocalPathId = op.GetBackupCollectionPathId().GetLocalId();
            
            if (opBackupCollectionPathId == pathId) {
                operationId = opId;
                operationFound = true;
                break;
            }
        }

        if (!operationFound) {
            LOG_W("Cannot run incremental restore"
                << ": backupCollectionPathId# " << pathId
                << ", reason# " << "incremental restore operation not found");
            return true;
        }

        const auto& op = Self->LongIncrementalRestoreOps.at(operationId);

        LOG_D("Found incremental restore operation"
            << ": operationId# " << operationId
            << ", txId# " << op.GetTxId()
            << ", tableCount# " << op.GetTablePathList().size());

        // Process each table in the restore operation
        for (const auto& tablePathString : op.GetTablePathList()) {
            TPath tablePath = TPath::Resolve(tablePathString, Self);
            if (!tablePath.IsResolved()) {
                LOG_W("Table path not resolved in restore operation"
                    << ": operationId# " << operationId
                    << ", tablePath# " << tablePathString);
                continue;
            }
            
            TPathId tablePathId = tablePath.Base()->PathId;
            
            if (!Self->Tables.contains(tablePathId)) {
                LOG_W("Table not found in restore operation"
                    << ": operationId# " << operationId
                    << ", tablePathId# " << tablePathId);
                continue;
            }

            auto table = Self->Tables.at(tablePathId);
            
            // Send restore request to each shard of the table
            for (const auto& shard : table->GetPartitions()) {
                Y_ABORT_UNLESS(Self->ShardInfos.contains(shard.ShardIdx));
                const auto tabletId = Self->ShardInfos.at(shard.ShardIdx).TabletID;

                auto ev = MakeHolder<TEvDataShard::TEvRestoreMultipleIncrementalBackups>();
                ev->Record.SetTxId(op.GetTxId());
                tablePathId.ToProto(ev->Record.MutablePathId());
                
                // Copy backup settings from the operation
                for (const auto& backup : op.GetIncrementalBackupTrimmedNames()) {
                    auto* incrementalBackup = ev->Record.AddIncrementalBackups();
                    incrementalBackup->SetBackupTrimmedName(backup);
                }

                RestoreRequests.emplace_back(operationId, tabletId, std::move(ev));
                
                LOG_D("Scheduled restore request"
                    << ": operationId# " << operationId
                    << ", tablePathId# " << tablePathId
                    << ", shardIdx# " << shard.ShardIdx
                    << ", tabletId# " << tabletId);
            }
        }

        LOG_N("Incremental restore operation initiated"
            << ": operationId# " << operationId
            << ", backupCollectionPathId# " << pathId
            << ", tableCount# " << op.GetTablePathList().size()
            << ", requestCount# " << RestoreRequests.size());

        return true;
    }

    bool OnPipeRetry(TTransactionContext&, const TActorContext& ctx) {
        LOG_D("Retrying incremental restore for pipe failure"
            << ": operationId# " << PipeRetry.OperationId
            << ", tabletId# " << PipeRetry.TabletId);

        // Find the operation and retry the request to this specific DataShard
        if (!Self->LongIncrementalRestoreOps.contains(PipeRetry.OperationId)) {
            LOG_W("Cannot retry incremental restore - operation not found"
                << ": operationId# " << PipeRetry.OperationId);
            return true;
        }

        const auto& op = Self->LongIncrementalRestoreOps.at(PipeRetry.OperationId);
        
        // Find the table and shard for this tablet
        for (const auto& tablePathString : op.GetTablePathList()) {
            TPath tablePath = TPath::Resolve(tablePathString, Self);
            if (!tablePath.IsResolved()) {
                continue;
            }
            
            TPathId tablePathId = tablePath.Base()->PathId;
            
            if (!Self->Tables.contains(tablePathId)) {
                continue;
            }

            auto table = Self->Tables.at(tablePathId);
            
            // Find the specific shard that matches this tablet
            for (const auto& shard : table->GetPartitions()) {
                Y_ABORT_UNLESS(Self->ShardInfos.contains(shard.ShardIdx));
                const auto tabletId = Self->ShardInfos.at(shard.ShardIdx).TabletID;

                if (tabletId == PipeRetry.TabletId) {
                    // Create retry request for this specific DataShard
                    auto ev = MakeHolder<TEvDataShard::TEvRestoreMultipleIncrementalBackups>();
                    ev->Record.SetTxId(op.GetTxId());
                    tablePathId.ToProto(ev->Record.MutablePathId());
                    
                    // Copy backup settings from the operation
                    for (const auto& backup : op.GetIncrementalBackupTrimmedNames()) {
                        auto* incrementalBackup = ev->Record.AddIncrementalBackups();
                        incrementalBackup->SetBackupTrimmedName(backup);
                    }

                    RestoreRequests.emplace_back(PipeRetry.OperationId, tabletId, std::move(ev));
                    
                    LOG_D("Scheduled retry restore request"
                        << ": operationId# " << PipeRetry.OperationId
                        << ", tablePathId# " << tablePathId
                        << ", shardIdx# " << shard.ShardIdx
                        << ", tabletId# " << tabletId);
                    
                    return true;
                }
            }
        }

        LOG_W("Cannot retry incremental restore - tablet not found in operation"
            << ": operationId# " << PipeRetry.OperationId
            << ", tabletId# " << PipeRetry.TabletId);
        
        return true;
    }
}; // TTxProgress

class TTxIncrementalRestoreResponse : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
private:
    TEvDataShard::TEvRestoreMultipleIncrementalBackupsResponse::TPtr Response;
    
public:
    explicit TTxIncrementalRestoreResponse(TSchemeShard* self, TEvDataShard::TEvRestoreMultipleIncrementalBackupsResponse::TPtr& response)
        : TTransactionBase(self)
        , Response(response)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_INCREMENTAL_RESTORE_RESPONSE;
    }

    bool Execute(TTransactionContext&, const TActorContext& ctx) override {
        LOG_D("Processing incremental restore response from DataShard");

        const auto& record = Response->Get()->Record;
        const auto txId = record.GetTxId();
        const auto tabletId = record.GetTabletId();
        const auto status = record.GetStatus();

        LOG_D("DataShard incremental restore response"
            << ": txId# " << txId
            << ", tabletId# " << tabletId
            << ", status# " << static_cast<ui32>(status));

        // Find the operation by TxId
        TOperationId operationId;
        bool operationFound = false;
        
        for (const auto& [opId, op] : Self->LongIncrementalRestoreOps) {
            if (op.GetTxId() == txId) {
                operationId = opId;
                operationFound = true;
                break;
            }
        }

        if (!operationFound) {
            LOG_W("Received response for unknown incremental restore operation"
                << ": txId# " << txId
                << ", tabletId# " << tabletId);
            return true;
        }

        // TODO: Update shard status in database
        // For now, just log the response details
        
        if (status == NKikimrTxDataShard::TEvRestoreMultipleIncrementalBackupsResponse::SUCCESS) {
            LOG_N("DataShard incremental restore completed successfully"
                << ": operationId# " << operationId
                << ", txId# " << txId
                << ", tabletId# " << tabletId
                << ", processedRows# " << record.GetProcessedRows()
                << ", processedBytes# " << record.GetProcessedBytes());
        } else {
            LOG_W("DataShard incremental restore failed"
                << ": operationId# " << operationId
                << ", txId# " << txId
                << ", tabletId# " << tabletId
                << ", status# " << static_cast<ui32>(status)
                << ", issueCount# " << record.IssuesSize());
                
            for (const auto& issue : record.GetIssues()) {
                LOG_W("DataShard restore issue: " << issue.message());
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        // TODO: Implement completion logic
        // This could include:
        // 1. Checking if all shards have completed for this operation
        // 2. Finalizing the operation if all shards are done
        // 3. Scheduling retries for failed shards
        // 4. Updating operation progress in database
        
        LOG_D("Incremental restore response transaction completed");
    }
};

} // namespace NKikimr::NSchemeShard::NIncrementalRestoreScan

namespace NKikimr::NSchemeShard {

using namespace NIncrementalRestoreScan;

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxProgressIncrementalRestore(TEvPrivate::TEvRunIncrementalRestore::TPtr& ev) {
    return new TTxProgress(this, ev);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreatePipeRetryIncrementalRestore(const TOperationId& operationId, TTabletId tabletId) {
    return new TTxProgress(this, operationId, tabletId);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxIncrementalRestoreResponse(TEvDataShard::TEvRestoreMultipleIncrementalBackupsResponse::TPtr& ev) {
    return new TTxIncrementalRestoreResponse(this, ev);
}

void TSchemeShard::Handle(TEvPrivate::TEvRunIncrementalRestore::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxProgressIncrementalRestore(ev), ctx);
}

void TSchemeShard::Handle(TEvDataShard::TEvRestoreMultipleIncrementalBackupsResponse::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxIncrementalRestoreResponse(ev), ctx);
}

} // namespace NKikimr::NSchemeShard
