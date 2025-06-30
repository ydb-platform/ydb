#include "schemeshard_impl.h"
#include "schemeshard_incremental_restore_scan.h"

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/base/tablet_pipe.h>

namespace NKikimr::NSchemeShard::NIncrementalRestoreScan {

class TTxProgress: public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
private:
    TEvPrivate::TEvRunIncrementalRestore::TPtr RunIncrementalRestore = nullptr;

public:
    TTxProgress() = delete;

    explicit TTxProgress(TSelf* self, TEvPrivate::TEvRunIncrementalRestore::TPtr& ev)
        : TTransactionBase(self)
        , RunIncrementalRestore(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_PROGRESS_INCREMENTAL_RESTORE;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        Y_UNUSED(txc);
        Y_UNUSED(ctx);

        const auto& pathId = RunIncrementalRestore->Get()->BackupCollectionPathId;

        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "TTxProgress Execute, pathId: " << pathId);

        // Find the backup collection
        if (!Self->PathsById.contains(pathId)) {
            LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "TTxProgress Execute, backup collection not found, pathId: " << pathId);
            return true;
        }

        auto path = Self->PathsById.at(pathId);
        if (!path->IsBackupCollection()) {
            LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "TTxProgress Execute, path is not a backup collection, pathId: " << pathId);
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
            LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "TTxProgress Execute, incremental restore operation not found for backup collection, pathId: " << pathId);
            return true;
        }

        const auto& op = Self->LongIncrementalRestoreOps.at(operationId);

        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "TTxProgress Execute, found incremental restore operation, operationId: " << operationId
                   << ", txId: " << op.GetTxId()
                   << ", tableCount: " << op.GetTablePathList().size());

        // For now, just log the scan initiation
        // In a full implementation, this would coordinate with DataShards
        // similar to how CdcStreamScan works
        
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "Incremental restore scan initiated, operationId: " << operationId
                     << ", backupCollectionPathId: " << pathId
                     << ", tableCount: " << op.GetTablePathList().size());

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "TTxProgress Complete");
    }
}; // TTxProgress

} // namespace NKikimr::NSchemeShard::NIncrementalRestoreScan

namespace NKikimr::NSchemeShard {

using namespace NIncrementalRestoreScan;

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxProgressIncrementalRestore(TEvPrivate::TEvRunIncrementalRestore::TPtr& ev) {
    return new TTxProgress(this, ev);
}

void TSchemeShard::Handle(TEvPrivate::TEvRunIncrementalRestore::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxProgressIncrementalRestore(ev), ctx);
}

} // namespace NKikimr::NSchemeShard
