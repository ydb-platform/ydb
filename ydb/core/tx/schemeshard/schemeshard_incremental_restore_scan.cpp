#include "schemeshard_impl.h"
#include "schemeshard_incremental_restore_scan.h"
#include "schemeshard_utils.h"

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_allocator_client/client.h>

#include <algorithm>  // for std::sort

#if defined LOG_D || \
    defined LOG_W || \
    defined LOG_N || \
    defined LOG_I || \
    defined LOG_E
#error log macro redefinition
#endif

#define LOG_D(stream) LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[IncrementalRestore] " << stream)
#define LOG_I(stream) LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[IncrementalRestore] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[IncrementalRestore] " << stream)
#define LOG_W(stream) LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[IncrementalRestore] " << stream)
#define LOG_E(stream) LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[IncrementalRestore] " << stream)

namespace NKikimr::NSchemeShard::NIncrementalRestoreScan {

// Propose function for incremental restore
THolder<TEvSchemeShard::TEvModifySchemeTransaction> IncrementalRestorePropose(
    TSchemeShard* ss,
    TTxId txId,
    const TPathId& sourcePathId,
    const TPathId& destPathId,
    const TString& srcTablePath,
    const TString& dstTablePath
) {
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), ss->TabletID());

    auto& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups);
    modifyScheme.SetInternal(true);
    
    // Set WorkingDir - use parent directory of destination table
    TString workingDir = "/";
    if (auto pos = dstTablePath.rfind('/'); pos != TString::npos && pos > 0) {
        workingDir = dstTablePath.substr(0, pos);
    }
    modifyScheme.SetWorkingDir(workingDir);

    auto& restore = *modifyScheme.MutableRestoreMultipleIncrementalBackups();
    restore.add_srctablepaths(srcTablePath);
    sourcePathId.ToProto(restore.add_srcpathids());
    restore.set_dsttablepath(dstTablePath);
    destPathId.ToProto(restore.mutable_dstpathid());

    return propose;
}

class TTxProgress: public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
private:
    // Input params
    TEvPrivate::TEvRunIncrementalRestore::TPtr RunIncrementalRestore = nullptr;

    // Transaction lifecycle support
    TEvTxAllocatorClient::TEvAllocateResult::TPtr AllocateResult = nullptr;
    TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr ModifyResult = nullptr;
    TTxId CompletedTxId = InvalidTxId;

    // Side effects
    TOperationId OperationToProgress;

public:
    TTxProgress() = delete;

    explicit TTxProgress(TSelf* self, TEvPrivate::TEvRunIncrementalRestore::TPtr& ev)
        : TTransactionBase(self)
        , RunIncrementalRestore(ev)
    {
    }

    // Transaction lifecycle constructors
    explicit TTxProgress(TSelf* self, TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev)
        : TTransactionBase(self)
        , AllocateResult(ev)
    {
    }

    explicit TTxProgress(TSelf* self, TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev)
        : TTransactionBase(self)
        , ModifyResult(ev)
    {
    }

    explicit TTxProgress(TSelf* self, TTxId completedTxId)
        : TTransactionBase(self)
        , CompletedTxId(completedTxId)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_PROGRESS_INCREMENTAL_RESTORE;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        if (AllocateResult) {
            return OnAllocateResult(txc, ctx);
        } else if (ModifyResult) {
            return OnModifyResult(txc, ctx);
        } else if (CompletedTxId) {
            return OnNotifyResult(txc, ctx);
        } else if (RunIncrementalRestore) {
            return OnRunIncrementalRestore(txc, ctx);
        } else {
            Y_ABORT("unreachable");
        }
    }

    void Complete(const TActorContext& ctx) override {
        // NOTE: Operations are now created and scheduled directly in Execute methods
        // using Self->Execute(CreateRestoreIncrementalBackupAtTable(newOperationId, newTx), ctx)
        // This ensures proper SchemeShard operation coordination with plan steps.
        
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

    bool OnRunIncrementalRestore(TTransactionContext&, const TActorContext& ctx);
    
    // Transaction lifecycle methods
    bool OnAllocateResult(TTransactionContext& txc, const TActorContext& ctx);
    bool OnModifyResult(TTransactionContext& txc, const TActorContext& ctx);
    bool OnNotifyResult(TTransactionContext& txc, const TActorContext& ctx);
}; // TTxProgress

bool NKikimr::NSchemeShard::NIncrementalRestoreScan::TTxProgress::OnRunIncrementalRestore(TTransactionContext&, const TActorContext& ctx) {
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

    LOG_D("Found incremental restore operation"
        << ": operationId# " << operationId
        << ", txId# " << Self->LongIncrementalRestoreOps.at(operationId).GetTxId()
        << ", tableCount# " << Self->LongIncrementalRestoreOps.at(operationId).GetTablePathList().size());

    // Process each table in the restore operation
    for (const auto& tablePathString : Self->LongIncrementalRestoreOps.at(operationId).GetTablePathList()) {
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

        // Create schema transaction for incremental restore once per table
        // (not per shard - the operation framework handles shard distribution)
        
        // Find the first incremental backup table
        TPathId firstIncrementalBackupPathId;
        auto tableName = tablePath.Base()->Name;
        auto backupCollectionPath = Self->PathsById.at(pathId);
        bool found = false;
        
        for (auto& [childName, childPathId] : backupCollectionPath->GetChildren()) {
            if (childName.Contains("_incremental")) {
                auto backupEntryPath = Self->PathsById.at(childPathId);
                for (auto& [tableNameInEntry, backupTablePathId] : backupEntryPath->GetChildren()) {
                    if (tableNameInEntry == tableName) {
                        firstIncrementalBackupPathId = backupTablePathId;
                        found = true;
                        break;
                    }
                }
                if (found) break;
            }
        }
        
        if (!found) {
            LOG_W("No incremental backup found for table"
                << ": operationId# " << operationId
                << ", tableName# " << tableName);
            continue;
        }

        // Create operation for single incremental restore
        ui64 newOperationId = ui64(Self->GetCachedTxId(ctx));
        // Store context for transaction lifecycle
        TSchemeShard::TIncrementalRestoreContext context;
        context.DestinationTablePathId = tablePathId;
        context.DestinationTablePath = tablePath.PathString();
        context.OriginalOperationId = ui64(operationId.GetTxId());
        context.BackupCollectionPathId = pathId;
        Self->IncrementalRestoreContexts[newOperationId] = context;

        // Request transaction allocation
        ctx.Send(Self->TxAllocatorClient, new TEvTxAllocatorClient::TEvAllocate(), 0, newOperationId);
        }

    LOG_N("Incremental restore operation initiated"
        << ": operationId# " << operationId
        << ", backupCollectionPathId# " << pathId);

    return true;
}

// Transaction lifecycle methods

bool NKikimr::NSchemeShard::NIncrementalRestoreScan::TTxProgress::OnAllocateResult(TTransactionContext& txc, const TActorContext& ctx) {
    Y_UNUSED(txc);
    Y_ABORT_UNLESS(AllocateResult);

    const auto txId = TTxId(AllocateResult->Get()->TxIds.front());
    const ui64 operationId = AllocateResult->Cookie;

    LOG_D("TTxProgress: OnAllocateResult"
        << ": txId# " << txId
        << ", operationId# " << operationId);

    if (!Self->IncrementalRestoreContexts.contains(operationId)) {
        LOG_E("TTxProgress: OnAllocateResult received unknown operationId"
            << ": operationId# " << operationId);
        return true;
    }

    const auto& context = Self->IncrementalRestoreContexts.at(operationId);
    
    // Re-collect and re-create the transaction with all incremental backups
    // (we need to do this again because we only stored simplified context)
    TVector<std::pair<TString, TPathId>> incrementalBackupEntries;
    auto backupCollectionPath = Self->PathsById.at(context.BackupCollectionPathId);
    for (auto& [childName, childPathId] : backupCollectionPath->GetChildren()) {
        if (childName.Contains("_incremental")) {
            auto backupEntryPath = Self->PathsById.at(childPathId);
            for (auto& [tableNameInEntry, tablePathId] : backupEntryPath->GetChildren()) {
                // Use the last segment of the destination table path for comparison
                TString expectedTableName = context.DestinationTablePath;
                if (auto pos = expectedTableName.rfind('/'); pos != TString::npos) {
                    expectedTableName = expectedTableName.substr(pos + 1);
                }
                if (tableNameInEntry == expectedTableName) {
                    // Extract timestamp from backup entry name
                    TString timestamp = childName;
                    if (timestamp.EndsWith("_incremental")) {
                        timestamp = timestamp.substr(0, timestamp.size() - 12);
                    }
                    incrementalBackupEntries.emplace_back(timestamp, tablePathId);
                }
            }
        }
    }
    
    // Sort incremental backups by timestamp to ensure correct order
    std::sort(incrementalBackupEntries.begin(), incrementalBackupEntries.end(),
        [](const auto& a, const auto& b) { return a.first < b.first; });
    
    // Create the transaction proposal manually with ALL incremental backup paths
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), Self->TabletID());
    auto& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups);
    modifyScheme.SetInternal(true);
    
    // Set WorkingDir - use parent directory of destination table
    TString workingDir = "/";
    if (auto pos = context.DestinationTablePath.rfind('/'); pos != TString::npos && pos > 0) {
        workingDir = context.DestinationTablePath.substr(0, pos);
    }
    modifyScheme.SetWorkingDir(workingDir);

    auto& restore = *modifyScheme.MutableRestoreMultipleIncrementalBackups();
    
    // Add ALL incremental backup paths in sorted order as sources
    for (const auto& entry : incrementalBackupEntries) {
        TPath backupTablePath = TPath::Init(entry.second, Self);
        restore.add_srctablepaths(backupTablePath.PathString());
        entry.second.ToProto(restore.add_srcpathids());
        
        LOG_D("TTxProgress: Added incremental backup path to OnAllocateResult transaction"
            << ": timestamp# " << entry.first
            << ", pathId# " << entry.second
            << ", path# " << backupTablePath.PathString());
    }
    
    // Set destination table
    restore.set_dsttablepath(context.DestinationTablePath);
    context.DestinationTablePathId.ToProto(restore.mutable_dstpathid());
    
    ctx.Send(Self->SelfId(), propose.Release());
    
    // Track transaction for completion handling
    Self->TxIdToIncrementalRestore[txId] = operationId;
    
    LOG_I("TTxProgress: Sent incremental restore propose for all incrementals"
        << ": txId# " << txId
        << ", operationId# " << operationId
        << ", dstPathId# " << context.DestinationTablePathId
        << ", dstTablePath# " << context.DestinationTablePath);
    
    return true;
}

bool NKikimr::NSchemeShard::NIncrementalRestoreScan::TTxProgress::OnModifyResult(TTransactionContext& txc, const TActorContext& ctx) {
    Y_UNUSED(txc);
    Y_UNUSED(ctx);
    Y_ABORT_UNLESS(ModifyResult);
    const auto& record = ModifyResult->Get()->Record;

    LOG_D("TTxProgress: OnModifyResult"
        << ": txId# " << record.GetTxId()
        << ", status# " << record.GetStatus());

    auto txId = TTxId(record.GetTxId());
    
    if (!Self->TxIdToIncrementalRestore.contains(txId)) {
        LOG_E("TTxProgress: OnModifyResult received unknown txId"
            << ": txId# " << txId);
        return true;
    }
    
    ui64 operationId = Self->TxIdToIncrementalRestore.at(txId);
    
    if (record.GetStatus() == NKikimrScheme::StatusAccepted) {
        LOG_I("TTxProgress: Incremental restore transaction accepted"
            << ": txId# " << txId
            << ", operationId# " << operationId);
        
        // Transaction subscription is automatic - when txId is added to TxInFlight
        // and tracked in Operations, completion notifications will be sent automatically
        // No explicit subscription needed since we have TxIdToIncrementalRestore mapping
    } else {
        LOG_W("TTxProgress: Incremental restore transaction rejected"
            << ": txId# " << txId
            << ", operationId# " << operationId
            << ", status# " << record.GetStatus());
        
        // Clean up tracking on rejection
        Self->TxIdToIncrementalRestore.erase(txId);
        Self->IncrementalRestoreContexts.erase(operationId);
    }

    return true;
}

bool NKikimr::NSchemeShard::NIncrementalRestoreScan::TTxProgress::OnNotifyResult(TTransactionContext& txc, const TActorContext& ctx) {
    Y_UNUSED(txc);
    LOG_D("TTxProgress: OnNotifyResult"
        << ": completedTxId# " << CompletedTxId);

    if (!Self->TxIdToIncrementalRestore.contains(CompletedTxId)) {
        LOG_W("TTxProgress: OnNotifyResult received unknown txId"
            << ": txId# " << CompletedTxId);
        return true;
    }
    
    ui64 operationId = Self->TxIdToIncrementalRestore.at(CompletedTxId);

    LOG_I("TTxProgress: Incremental restore transaction completed"
        << ": txId# " << CompletedTxId
        << ", operationId# " << operationId);

    // Check if context exists for logging
    if (Self->IncrementalRestoreContexts.contains(operationId)) {
        const auto& context = Self->IncrementalRestoreContexts.at(operationId);
        LOG_I("TTxProgress: All incremental backups completed for table"
            << ": operationId# " << operationId
            << ", dstTablePath# " << context.DestinationTablePath);
        
        // Clean up context
        Self->IncrementalRestoreContexts.erase(operationId);
    }

    // Clean up transaction tracking
    Self->TxIdToIncrementalRestore.erase(CompletedTxId);

    return true;
}

} // namespace NKikimr::NSchemeShard::NIncrementalRestoreScan

namespace NKikimr::NSchemeShard {

using namespace NIncrementalRestoreScan;

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxProgressIncrementalRestore(TEvPrivate::TEvRunIncrementalRestore::TPtr& ev) {
    return new TTxProgress(this, ev);
}

// Transaction lifecycle constructor functions
NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxProgressIncrementalRestore(TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev) {
    return new TTxProgress(this, ev);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxProgressIncrementalRestore(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev) {
    return new TTxProgress(this, ev);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxProgressIncrementalRestore(TTxId completedTxId) {
    return new TTxProgress(this, completedTxId);
}

void TSchemeShard::Handle(TEvPrivate::TEvRunIncrementalRestore::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxProgressIncrementalRestore(ev), ctx);
}

} // namespace NKikimr::NSchemeShard
