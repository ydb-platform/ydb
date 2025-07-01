#include "defs.h"
#include "datashard_impl.h"
#include "incr_restore_scan.h"
#include "change_exchange_impl.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tablet_flat/flat_scan_spent.h>

namespace NKikimr {
namespace NDataShard {

void TDataShard::Handle(TEvDataShard::TEvRestoreMultipleIncrementalBackups::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    const ui64 txId = record.GetTxId();
    const TPathId pathId = TPathId::FromProto(record.GetPathId());
    
    LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
        "DataShard " << TabletID() << " received incremental restore request"
        << " txId: " << txId
        << " pathId: " << pathId
        << " backups count: " << record.IncrementalBackupsSize());

    auto response = MakeHolder<TEvDataShard::TEvRestoreMultipleIncrementalBackupsResponse>();
    response->Record.SetTxId(txId);
    response->Record.SetTabletId(TabletID());
    response->Record.SetStatus(NKikimrTxDataShard::TEvRestoreMultipleIncrementalBackupsResponse::SUCCESS);

    try {
        // Find the table by path ID
        const ui64 tableId = pathId.LocalPathId;
        if (!GetUserTables().contains(tableId)) {
            throw yexception() << "Table not found: " << tableId;
        }

        const ui32 localTableId = GetUserTables().at(tableId)->LocalTid;

        // Create incremental restore scan using existing infrastructure
        // We use the same infrastructure as CreateIncrementalRestoreSrcUnit
        auto scan = CreateIncrementalRestoreScan(
            SelfId(),
            [=, tabletID = TabletID(), generation = Generation(), tabletActor = SelfId()]
            (const TActorContext& ctx, TActorId parent) {
                // Create change sender for DataShard-to-DataShard streaming
                // This will stream changes to the target DataShard
                return ctx.Register(
                    CreateIncrRestoreChangeSender(
                        parent,
                        NDataShard::TDataShardId{
                            .TabletId = tabletID,
                            .Generation = generation,
                            .ActorId = tabletActor,
                        },
                        pathId,
                        pathId  // For DataShard-to-DataShard streaming, source and target path are same table
                    )
                );
            },
            pathId,
            GetUserTables().at(tableId),
            pathId,  // Target path ID (same as source for DataShard streaming)
            txId,
            {} // Use default limits
        );

        if (!scan) {
            throw yexception() << "Failed to create incremental restore scan";
        }

        // Queue the scan for execution
        QueueScan(localTableId, std::move(scan), txId);

        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
            "DataShard " << TabletID() << " successfully started incremental restore scan"
            << " txId: " << txId
            << " pathId: " << pathId);

    } catch (const std::exception& e) {
        LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD,
            "DataShard " << TabletID() << " failed to start incremental restore: " << e.what()
            << " txId: " << txId
            << " pathId: " << pathId);
        
        response->Record.SetStatus(NKikimrTxDataShard::TEvRestoreMultipleIncrementalBackupsResponse::ERROR);
        
        // Add error to Issues field
        auto* issue = response->Record.AddIssues();
        issue->set_message(e.what());
        issue->set_severity(NYql::TSeverityIds::S_ERROR);
    }

    // Send response back to SchemeShard
    ctx.Send(ev->Sender, response.Release());
}

} // namespace NDataShard
} // namespace NKikimr
