#include "datashard_impl.h"
#include "datashard_active_transaction.h"
#include "incr_restore_scan.h"
#include "change_sender_incr_restore.h"

#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

namespace NKikimr {
namespace NDataShard {

void TDataShard::Handle(TEvDataShard::TEvRestoreMultipleIncrementalBackups::TPtr& ev, const TActorContext& ctx) {
    using TEvRequest = TEvDataShard::TEvRestoreMultipleIncrementalBackups;
    using TEvResponse = TEvDataShard::TEvRestoreMultipleIncrementalBackupsResponse;

    const auto& record = ev->Get()->Record;
    
    LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
        "DataShard " << TabletID() << " received TEvRestoreMultipleIncrementalBackups"
        << " TxId: " << record.GetTxId()
        << " SrcPaths: " << record.SrcTablePathsSize()
        << " DstPath: " << (record.HasDstTablePath() ? record.GetDstTablePath() : "none"));

    auto response = MakeHolder<TEvResponse>();
    response->Record.SetTabletID(TabletID());
    response->Record.SetTxId(record.GetTxId());

    auto errorResponse = [&](NKikimrTxDataShard::TEvRestoreMultipleIncrementalBackupsResponse::EStatus status, const TString& error) {
        response->Record.SetStatus(status);
        response->Record.SetErrorDescription(error);
        
        LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD,
            "DataShard " << TabletID() << " restore error: " << error
            << " TxId: " << record.GetTxId());
            
        ctx.Send(ev->Sender, response.Release());
    };

    // Validate the tablet state
    if (!IsStateActive()) {
        errorResponse(
            NKikimrTxDataShard::TEvRestoreMultipleIncrementalBackupsResponse::WRONG_SHARD_STATE,
            TStringBuilder() << "DataShard is not active, state: " << State);
        return;
    }

    // Validate that we have source and destination paths
    if (record.SrcTablePathsSize() == 0) {
        errorResponse(
            NKikimrTxDataShard::TEvRestoreMultipleIncrementalBackupsResponse::BAD_REQUEST,
            "No source table paths specified");
        return;
    }

    if (!record.HasDstTablePath() && !record.HasDstPathId()) {
        errorResponse(
            NKikimrTxDataShard::TEvRestoreMultipleIncrementalBackupsResponse::BAD_REQUEST,
            "No destination table path or path ID specified");
        return;
    }

    // Extract path IDs - we need both source and destination to be local to this DataShard
    TVector<TPathId> srcPathIds;
    if (record.SrcPathIdsSize() > 0) {
        for (const auto& protoPathId : record.GetSrcPathIds()) {
            srcPathIds.push_back(TPathId::FromProto(protoPathId));
        }
    } else {
        // If no path IDs provided, we cannot proceed (we need local table IDs)
        errorResponse(
            NKikimrTxDataShard::TEvRestoreMultipleIncrementalBackupsResponse::BAD_REQUEST,
            "Source path IDs are required for DataShard-to-DataShard streaming");
        return;
    }

    TPathId dstPathId;
    if (record.HasDstPathId()) {
        dstPathId = TPathId::FromProto(record.GetDstPathId());
    } else {
        errorResponse(
            NKikimrTxDataShard::TEvRestoreMultipleIncrementalBackupsResponse::BAD_REQUEST,
            "Destination path ID is required for DataShard-to-DataShard streaming");
        return;
    }

    // Validate that all source tables exist on this DataShard
    for (const auto& srcPathId : srcPathIds) {
        const ui64 localTableId = srcPathId.LocalPathId;
        if (!GetUserTables().contains(localTableId)) {
            errorResponse(
                NKikimrTxDataShard::TEvRestoreMultipleIncrementalBackupsResponse::SCHEME_ERROR,
                TStringBuilder() << "Source table not found on this DataShard: " << srcPathId);
            return;
        }
    }

    // For DataShard-to-DataShard streaming, we start incremental restore scans
    // that will use the change exchange infrastructure to stream changes
    try {
        TVector<THolder<NTable::IScan>> scans;
        
        for (size_t i = 0; i < srcPathIds.size(); ++i) {
            const auto& srcPathId = srcPathIds[i];
            const ui64 localTableId = srcPathId.LocalPathId;
            
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "DataShard " << TabletID() << " starting incremental restore scan"
                << " from table: " << srcPathId << " to: " << dstPathId
                << " TxId: " << record.GetTxId());

            // Create incremental restore scan that will stream to target DataShard
            auto scan = CreateIncrementalRestoreScan(
                SelfId(),
                [=, tabletID = TabletID(), generation = Generation(), tabletActor = SelfId()]
                (const TActorContext& ctx, TActorId parent) {
                    // Create change sender for DataShard-to-DataShard streaming
                    return ctx.Register(
                        CreateIncrRestoreChangeSender(
                            parent,
                            NDataShard::TDataShardId{
                                .TabletId = tabletID,
                                .Generation = generation,
                                .ActorId = tabletActor,
                            },
                            srcPathId,
                            dstPathId));
                },
                srcPathId,
                GetUserTables().at(localTableId),
                dstPathId,
                record.GetTxId(),
                {} // Use default limits for now
            );

            if (!scan) {
                errorResponse(
                    NKikimrTxDataShard::TEvRestoreMultipleIncrementalBackupsResponse::INTERNAL_ERROR,
                    TStringBuilder() << "Failed to create incremental restore scan for table: " << srcPathId);
                return;
            }

            const ui32 localTid = GetUserTables().at(localTableId)->LocalTid;
            QueueScan(localTid, std::move(scan), record.GetTxId(), TRowVersion::Min());
        }

        // Success response
        response->Record.SetStatus(NKikimrTxDataShard::TEvRestoreMultipleIncrementalBackupsResponse::SUCCESS);
        
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
            "DataShard " << TabletID() << " successfully started " << srcPathIds.size() 
            << " incremental restore scans for TxId: " << record.GetTxId());
            
    } catch (const std::exception& ex) {
        errorResponse(
            NKikimrTxDataShard::TEvRestoreMultipleIncrementalBackupsResponse::INTERNAL_ERROR,
            TStringBuilder() << "Exception while starting incremental restore scans: " << ex.what());
        return;
    }

    ctx.Send(ev->Sender, response.Release());
}

} // namespace NDataShard
} // namespace NKikimr
