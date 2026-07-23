#include "datashard_cdc_stream_common.h"
#include "datashard_impl.h"

namespace NKikimr {
namespace NDataShard {

TCdcStreamUnitBase::TCdcStreamUnitBase(EExecutionUnitKind kind, bool createCdcStream, TDataShard& self, TPipeline& pipeline)
    : TExecutionUnit(kind, createCdcStream, self, pipeline)
{
}

void TCdcStreamUnitBase::DropCdcStream(
    TTransactionContext& txc,
    const TPathId& tablePathId,
    const TPathId& streamPathId,
    const TUserTable& tableInfo)
{
    auto& scanManager = DataShard.GetCdcStreamScanManager();
    scanManager.Forget(txc.DB, tablePathId, streamPathId);

    if (const auto* info = scanManager.Get(streamPathId)) {
        DataShard.CancelScan(tableInfo.LocalTid, info->ScanId);
        scanManager.Complete(streamPathId);
    }

    DataShard.GetCdcStreamHeartbeatManager().DropCdcStream(txc.DB, tablePathId, streamPathId);
    RemoveSenders.emplace_back(new TEvChangeExchange::TEvRemoveSender(streamPathId));
}

void TCdcStreamUnitBase::AddCdcStream(
    TTransactionContext& txc,
    const TPathId& tablePathId,
    const TPathId& streamPathId,
    const NKikimrSchemeOp::TCdcStreamDescription& streamDesc,
    const TString& snapshotName,
    ui64 step,
    ui64 txId)
{
    if (!snapshotName.empty()) {
        Y_ENSURE(streamDesc.GetState() == NKikimrSchemeOp::ECdcStreamStateScan);
        Y_ENSURE(step != 0);

        DataShard.GetSnapshotManager().AddSnapshot(txc.DB,
            TSnapshotKey(tablePathId, step, txId),
            snapshotName, TSnapshot::FlagScheme, TDuration::Zero());

        DataShard.GetCdcStreamScanManager().Add(txc.DB,
            tablePathId, streamPathId, TRowVersion(step, txId));
    }

    if (streamDesc.GetState() == NKikimrSchemeOp::ECdcStreamStateReady) {
        if (const auto heartbeatInterval = TDuration::MilliSeconds(streamDesc.GetResolvedTimestampsIntervalMs())) {
            DataShard.GetCdcStreamHeartbeatManager().AddCdcStream(txc.DB, tablePathId, streamPathId, heartbeatInterval);
        }
    }

    AddSender.Reset(new TEvChangeExchange::TEvAddSender(
        tablePathId, TEvChangeExchange::ESenderType::CdcStream, streamPathId
    ));
}

void TCdcStreamUnitBase::Complete(TOperation::TPtr, const TActorContext& ctx) {
    if (const auto& changeSender = DataShard.GetChangeSender()) {
        for (auto& removeSender : RemoveSenders) {
            if (auto* event = removeSender.Release()) {
                ctx.Send(changeSender, event);
            }
        }
    }

    if (AddSender) {
        ctx.Send(DataShard.GetChangeSender(), AddSender.Release());
        DataShard.EmitHeartbeats();
    }
}

} // namespace NDataShard
} // namespace NKikimr