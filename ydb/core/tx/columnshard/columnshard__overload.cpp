#include "columnshard_impl.h"

namespace NKikimr::NColumnShard {

TColumnShard::TOverloadStatus TColumnShard::ResourcesStatusToOverloadStatus(const NOverload::EResourcesStatus status) const {
    switch (status) {
        case NOverload::EResourcesStatus::Ok:
            return TOverloadStatus{EOverloadStatus::None, {}};
        case NOverload::EResourcesStatus::WritesInFlyLimitReached:
            return TOverloadStatus{EOverloadStatus::ShardWritesInFly, "The limit on the number of in-flight write requests to a shard has been exceeded. Please add more resources or reduce the database load."};
        case NOverload::EResourcesStatus::WritesSizeInFlyLimitReached:
            return TOverloadStatus{EOverloadStatus::ShardWritesSizeInFly, "The limit on the total size of in-flight write requests to the shard has been exceeded. Please add more resources or reduce the database load."};
    }
}

TColumnShard::TOverloadStatus TColumnShard::CheckOverloadedImmediate(const TInternalPathId /* pathId */) const {
    if (IsAnyChannelYellowStop()) {
        return TOverloadStatus{EOverloadStatus::Disk, "Channels are overloaded (yellow), please rebalance groups or add new ones"};
    }
    const ui64 txLimit = Settings.OverloadTxInFlight;

    if (txLimit && Executor()->GetStats().TxInFly > txLimit) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "shard_overload")("reason", "tx_in_fly")("sum", Executor()->GetStats().TxInFly)(
            "limit", txLimit);
        return TOverloadStatus{EOverloadStatus::ShardTxInFly, TStringBuilder{} << "The local transaction limit has been exceeded " << Executor()->GetStats().TxInFly << " of " << txLimit << ". Please add more resources or reduce the database load."};
    }

    if (AppData()->FeatureFlags.GetEnableOlapRejectProbability()) {
        const float rejectProbabilty = Executor()->GetRejectProbability();
        if (rejectProbabilty > 0) {
            const float rnd = TAppData::RandomProvider->GenRandReal2();
            if (rnd < rejectProbabilty) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "shard_overload")("reason", "reject_probality")("RP", rejectProbabilty);
                return TOverloadStatus{EOverloadStatus::RejectProbability, "The local database is overloaded. Please add more resources or reduce the database load."};
            }
        }
    }

    return TOverloadStatus{EOverloadStatus::None, {}};
}

void TColumnShard::Handle(TEvColumnShard::TEvOverloadUnsubscribe::TPtr& ev, const TActorContext&) {
    Send(NOverload::TOverloadManagerServiceOperator::MakeServiceId(),
        std::make_unique<NOverload::TEvOverloadUnsubscribe>(NOverload::TColumnShardInfo{.ColumnShardId = SelfId(), .TabletId = TabletID()},
            NOverload::TOverloadSubscriberInfo{.PipeServerId = ev->Recipient, .OverloadSubscriberId = ev->Sender, .SeqNo = ev->Get()->Record.GetSeqNo()}));
}

} // namespace NKikimr::NColumnShard
