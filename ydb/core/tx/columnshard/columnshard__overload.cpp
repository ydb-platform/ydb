#include "columnshard_impl.h"

namespace NKikimr::NColumnShard {

TColumnShard::EOverloadStatus TColumnShard::ResourcesStatusToOverloadStatus(const NOverload::EResourcesStatus status) const {
    switch (status) {
        case NOverload::EResourcesStatus::Ok:
            return EOverloadStatus::None;
        case NOverload::EResourcesStatus::WritesInFlyLimitReached:
            return EOverloadStatus::ShardWritesInFly;
        case NOverload::EResourcesStatus::WritesSizeInFlyLimitReached:
            return EOverloadStatus::ShardWritesSizeInFly;
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

    if (AppData()->FeatureFlags.GetEnableOlapRejectProbability()) {
        const float rejectProbabilty = Executor()->GetRejectProbability();
        if (rejectProbabilty > 0) {
            const float rnd = TAppData::RandomProvider->GenRandReal2();
            if (rnd < rejectProbabilty) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "shard_overload")("reason", "reject_probality")("RP", rejectProbabilty);
                return EOverloadStatus::RejectProbability;
            }
        }
    }

    return EOverloadStatus::None;
}

void TColumnShard::Handle(TEvColumnShard::TEvOverloadUnsubscribe::TPtr& ev, const TActorContext&) {
    Send(NOverload::TOverloadManagerServiceOperator::MakeServiceId(),
        std::make_unique<NOverload::TEvOverloadUnsubscribe>(NOverload::TColumnShardInfo{.ColumnShardId = SelfId(), .TabletId = TabletID()},
            NOverload::TOverloadSubscriberInfo{.PipeServerId = ev->Recipient, .OverloadSubscriberId = ev->Sender, .SeqNo = ev->Get()->Record.GetSeqNo()}));
}

} // namespace NKikimr::NColumnShard
