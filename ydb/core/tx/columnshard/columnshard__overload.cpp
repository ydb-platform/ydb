#include "columnshard_impl.h"

namespace NKikimr::NColumnShard {

void TColumnShard::OnYellowChannelsChanged() {
    if (!IsAnyChannelYellowStop()) {
        // TODO: notify overload subscribers
    }
}

TColumnShard::EOverloadStatus TColumnShard::CheckOverloadedImmediate(const TInternalPathId /* pathId */) const {
    if (IsAnyChannelYellowStop()) {
        return EOverloadStatus::Disk;
    }
    const ui64 txLimit = Settings.OverloadTxInFlight;

    if (txLimit && Executor()->GetStats().TxInFly > txLimit) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "shard_overload")("reason", "tx_in_fly")("sum", Executor()->GetStats().TxInFly)(
            "limit", txLimit);
        return EOverloadStatus::ShardTxInFly;
    }
    return EOverloadStatus::None;
}

void TColumnShard::Handle(TEvColumnShard::TEvOverloadUnsubscribe::TPtr& ev, const TActorContext&) {
    Send(NOverload::TOverloadManagerServiceOperator::MakeServiceId(), new NOverload::TEvOverloadUnsubscribe({.ColumnShardId = SelfId(), .TabletId = TabletID()}, {.PipeServerId = ev->Recipient, .OverloadSubscriberId = ev->Sender, .SeqNo = ev->Get()->Record.GetSeqNo()}));
}

} // namespace NKikimr::NColumnShard
