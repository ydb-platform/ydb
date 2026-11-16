#include "cdc_aggregator_heartbeat_delivery_proto.h"

#include <util/string/builder.h>

namespace NKikimr::NCdcAggregator {

THolder<TEvCdcAggregator::TEvWriteHeartbeats> TCdcAggregatorHeartbeatDeliveryProtoConverter::SerializeBatch(const TCdcAggregatorHeartbeatBatch& batch) const {
    auto ev = MakeHolder<TEvCdcAggregator::TEvWriteHeartbeats>();
    for (const auto& item : batch) {
        auto& proto = *ev->Record.AddHeartbeats();
        proto.SetSourceDataShardId(item.SourceDataShardId);
        item.Heartbeat.Serialize(*proto.MutableHeartbeat());
    }
    return ev;
}

std::pair<std::optional<THashMap<ui64, TCdcAggregatorHeartbeatDeliveryReply>>, TString> TCdcAggregatorHeartbeatDeliveryProtoConverter::ParseBatchResult(const NKikimrCdcAggregator::TEvWriteHeartbeatsResult& proto) const {
    using TProto = NKikimrCdcAggregator::TEvWriteHeartbeatsResult;

    if (proto.GetStatus() != TProto::SUCCESS) {
        return {std::nullopt, TStringBuilder() << TProto::EStatus_Name(proto.GetStatus()) << ": " << proto.GetError()};
    }

    using EReplyStatus = TCdcAggregatorHeartbeatDeliveryReply::EStatus;

    THashMap<ui64, TCdcAggregatorHeartbeatDeliveryReply> tabletRepliesBySourceDataShardId;
    for (const auto& item : proto.GetResults()) {
        const ui64 sourceDataShardId = item.GetSourceDataShardId();
        switch (item.GetStatus()) {
        case TProto::THeartbeatResult::SUCCESS:
            if (item.HasWrittenVersion()) {
                tabletRepliesBySourceDataShardId[sourceDataShardId] = {
                    .SourceDataShardId = sourceDataShardId,
                    .Status = EReplyStatus::Written,
                    .WrittenVersion = TRowVersion::FromProto(item.GetWrittenVersion()),
                };
            }
            break;
        case TProto::THeartbeatResult::UNKNOWN_SOURCE:
        case TProto::THeartbeatResult::BAD_REQUEST:
            tabletRepliesBySourceDataShardId[sourceDataShardId] = {
                .SourceDataShardId = sourceDataShardId,
                .Status = EReplyStatus::Rejected,
                .Error = TProto::THeartbeatResult::EStatus_Name(item.GetStatus()),
            };
            break;
        default:
            break;
        }
    }

    return {std::move(tabletRepliesBySourceDataShardId), TString()};
}

THolder<TEvCdcAggregatorService::TEvWriteHeartbeatResult> TCdcAggregatorHeartbeatDeliveryProtoConverter::SerializeReply(const TCdcAggregatorHeartbeatDeliveryReply& reply) const {
    using EStatus = TEvCdcAggregatorService::TEvWriteHeartbeatResult::EStatus;

    auto ev = MakeHolder<TEvCdcAggregatorService::TEvWriteHeartbeatResult>();
    switch (reply.Status) {
    case TCdcAggregatorHeartbeatDeliveryReply::EStatus::Written:
        ev->Status = EStatus::Written;
        break;
    case TCdcAggregatorHeartbeatDeliveryReply::EStatus::Retry:
        ev->Status = EStatus::Retry;
        break;
    case TCdcAggregatorHeartbeatDeliveryReply::EStatus::Rejected:
        ev->Status = EStatus::Rejected;
        break;
    }
    ev->WrittenVersion = reply.WrittenVersion;
    ev->Error = reply.Error;
    return ev;
}

} // namespace NKikimr::NCdcAggregator
