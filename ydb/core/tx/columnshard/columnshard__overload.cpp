#include "columnshard_impl.h"

namespace NKikimr::NColumnShard {

namespace {

constexpr ui64 DEFAULT_WRITES_IN_FLY_LIMIT = 1000000;
constexpr ui64 DEFAULT_WRITES_SIZE_IN_FLY_LIMIT = (((ui64)1) << 30);

} // namespace

void TColumnShard::OnYellowChannelsChanged() {
    if (!IsAnyChannelYellowStop()) {
        OverloadSubscribers.NotifyOverloadSubscribers(NOverload::ERejectReason::YellowChannels, SelfId(), TabletID());
    }
}

ui64 TColumnShard::GetShardWritesInFlyLimit() const {
    return HasAppData() ? AppDataVerified().ColumnShardConfig.GetWritingInFlightRequestsCountLimit() : DEFAULT_WRITES_IN_FLY_LIMIT;
}

ui64 TColumnShard::GetShardWritesSizeInFlyLimit() const {
    return HasAppData() ? AppDataVerified().ColumnShardConfig.GetWritingInFlightRequestBytesLimit() : DEFAULT_WRITES_SIZE_IN_FLY_LIMIT;
}

TColumnShard::EOverloadStatus TColumnShard::CheckOverloadedImmediate(const TInternalPathId /* pathId */) const {
    if (IsAnyChannelYellowStop()) {
        return EOverloadStatus::Disk;
    }
    const ui64 txLimit = Settings.OverloadTxInFlight;
    const ui64 writesLimit = GetShardWritesInFlyLimit();
    const ui64 writesSizeLimit = GetShardWritesSizeInFlyLimit();
    if (txLimit && Executor()->GetStats().TxInFly > txLimit) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "shard_overload")("reason", "tx_in_fly")("sum", Executor()->GetStats().TxInFly)(
            "limit", txLimit);
        return EOverloadStatus::ShardTxInFly;
    }
    if (writesLimit && Counters.GetWritesMonitor()->GetWritesInFlight() > writesLimit) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "shard_overload")("reason", "writes_in_fly")(
            "sum", Counters.GetWritesMonitor()->GetWritesInFlight())("limit", writesLimit);
        return EOverloadStatus::ShardWritesInFly;
    }
    if (writesSizeLimit && Counters.GetWritesMonitor()->GetWritesSizeInFlight() > writesSizeLimit) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "shard_overload")("reason", "writes_size_in_fly")(
            "sum", Counters.GetWritesMonitor()->GetWritesSizeInFlight())("limit", writesSizeLimit);
        return EOverloadStatus::ShardWritesSizeInFly;
    }
    return EOverloadStatus::None;
}

void TColumnShard::UpdateOverloadsStatus() {
    if (Counters.GetWritesMonitor()->GetWritesInFlight() < GetShardWritesInFlyLimit()) {
        OverloadSubscribers.NotifyOverloadSubscribers(NOverload::ERejectReason::OverloadByShardWritesInFly, SelfId(), TabletID());
    }
    if (Counters.GetWritesMonitor()->GetWritesSizeInFlight() < GetShardWritesSizeInFlyLimit()) {
        OverloadSubscribers.NotifyOverloadSubscribers(NOverload::ERejectReason::OverloadByShardWritesSizeInFly, SelfId(), TabletID());
    }
}

void TColumnShard::Handle(TEvColumnShard::TEvOverloadUnsubscribe::TPtr& ev, const TActorContext&) {
    OverloadSubscribers.RemoveOverloadSubscriber(ev->Get()->Record.GetSeqNo(), ev->Recipient, ev->Sender);
}

} // namespace NKikimr::NColumnShard
