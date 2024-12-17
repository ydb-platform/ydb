#include "long_tx_write.h"
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NColumnShard {

TLongTxTransactionOperator::TProposeResult TLongTxTransactionOperator::DoStartProposeOnExecute(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& /*txc*/) {
    if (WriteIds.empty()) {
        return TProposeResult(NKikimrTxColumnShard::EResultStatus::ERROR,
            TStringBuilder() << "Commit TxId# " << GetTxId() << " has an empty list of write ids");
    }

    for (auto&& writeId : WriteIds) {
        if (!owner.LongTxWrites.contains(writeId)) {
            return TProposeResult(NKikimrTxColumnShard::EResultStatus::ERROR,
                TStringBuilder() << "Commit TxId# " << GetTxId() << " references WriteId# " << (ui64)writeId << " that no longer exists");
        }
        auto& lw = owner.LongTxWrites[writeId];
        if (lw.PreparedTxId != 0) {
            return TProposeResult(NKikimrTxColumnShard::EResultStatus::ERROR,
                TStringBuilder() << "Commit TxId# " << GetTxId() << " references WriteId# " << (ui64)writeId << " that is already locked by TxId# " << lw.PreparedTxId);
        }

        if (auto* inserted = owner.InsertTable->GetInserted().GetOptional(writeId)) {
            auto granuleShardingInfo =
                owner.GetIndexAs<NOlap::TColumnEngineForLogs>().GetVersionedIndex().GetShardingInfoActual(inserted->GetPathId());
            if (granuleShardingInfo && lw.GranuleShardingVersionId && *lw.GranuleShardingVersionId != granuleShardingInfo->GetSnapshotVersion()) {
                return TProposeResult(NKikimrTxColumnShard::EResultStatus::ERROR,
                    TStringBuilder() << "Commit TxId# " << GetTxId() << " references WriteId# " << (ui64)writeId << " declined through sharding deprecated");
            }
        }
    }

    for (auto&& writeId : WriteIds) {
        owner.AddLongTxWrite(writeId, GetTxId());
    }
    return TProposeResult();
}

bool TLongTxTransactionOperator::DoParse(TColumnShard& /*owner*/, const TString& data) {
    NKikimrTxColumnShard::TCommitTxBody commitTxBody;
    if (!commitTxBody.ParseFromString(data)) {
        return false;
    }

    for (auto& id : commitTxBody.GetWriteIds()) {
        WriteIds.insert(TInsertWriteId{ id });
    }
    return true;
}

void TLongTxTransactionOperator::DoSendReply(TColumnShard& owner, const TActorContext& ctx) {
    const auto& txInfo = GetTxInfo();
    ctx.Send(txInfo.Source, BuildProposeResultEvent(owner).release());
}

}
