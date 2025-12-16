#pragma once

#include "datashard_impl.h"
#include "datashard_pipeline.h"

namespace NKikimr {
namespace NDataShard {

class TCdcStreamUnitBase : public TExecutionUnit {
protected:
    TVector<THolder<TEvChangeExchange::TEvRemoveSender>> RemoveSenders;

public:
    TCdcStreamUnitBase(EExecutionUnitKind kind, bool createCdcStream, TDataShard& self, TPipeline& pipeline)
        : TExecutionUnit(kind, createCdcStream, self, pipeline)
    {
    }

    void StopCdcStream(TTransactionContext& txc, const TPathId& tablePathId, const TPathId& streamPathId, const TUserTable::TPtr& tableInfo) {
        auto& scanManager = DataShard.GetCdcStreamScanManager();
        scanManager.Forget(txc.DB, tablePathId, streamPathId);

        if (const auto* info = scanManager.Get(streamPathId)) {
            if (tableInfo) {
                DataShard.CancelScan(tableInfo->LocalTid, info->ScanId);
            }
            scanManager.Complete(streamPathId);
        }

        DataShard.GetCdcStreamHeartbeatManager().DropCdcStream(txc.DB, tablePathId, streamPathId);

        RemoveSenders.emplace_back(new TEvChangeExchange::TEvRemoveSender(streamPathId));
    }

    void Complete(TOperation::TPtr, const TActorContext& ctx) override {
        if (!RemoveSenders.empty()) {
            const auto& changeSender = DataShard.GetChangeSender();
            if (changeSender) {
                for (auto& holder : RemoveSenders) {
                    ctx.Send(changeSender, holder.Release());
                }
            }
            RemoveSenders.clear();
        }
    }
};

} // namespace NDataShard
} // namespace NKikimr