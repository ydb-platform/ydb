#include "cdc_aggregator_heartbeat_delivery_proto.h"

#include <util/string/builder.h>

namespace NKikimr::NCdcAggregator {

THolder<TEvCdcAggregator::TEvWriteHeartbeats> TCdcAggregatorHeartbeatDeliveryProtoConverter::SerializeBatch(const TCdcAggregatorHeartbeatBatch& batch) const {
    auto ev = MakeHolder<TEvCdcAggregator::TEvWriteHeartbeats>();
    for (const auto& [sourceDataShardId, heartbeat] : batch) {
        auto& proto = *ev->Record.AddHeartbeats();
        proto.SetSourceDataShardId(sourceDataShardId);
        heartbeat.Serialize(*proto.MutableHeartbeat());
    }
    return ev;
}

std::pair<std::optional<TCdcAggregatorHeartbeatBatchResult>, TString> TCdcAggregatorHeartbeatDeliveryProtoConverter::ParseBatchResult(const NKikimrCdcAggregator::TEvWriteHeartbeatsResult& proto) const {
    using TProto = NKikimrCdcAggregator::TEvWriteHeartbeatsResult;

    if (proto.GetStatus() != TProto::SUCCESS) {
        return {std::nullopt, TStringBuilder() << TProto::EStatus_Name(proto.GetStatus()) << ": " << proto.GetError()};
    }

    TCdcAggregatorHeartbeatBatchResult tabletBatchResult;
    for (const auto& item : proto.GetResults()) {
        const ui64 sourceDataShardId = item.GetSourceDataShardId();
        switch (item.GetStatus()) {
        case TProto::THeartbeatResult::SUCCESS:
            if (item.HasWrittenVersion()) {
                tabletBatchResult.WrittenVersionsBySourceDataShardId[sourceDataShardId] = TRowVersion::FromProto(item.GetWrittenVersion());
            }
            break;
        case TProto::THeartbeatResult::UNKNOWN_SOURCE:
        case TProto::THeartbeatResult::BAD_REQUEST:
            tabletBatchResult.RejectionErrorsBySourceDataShardId[sourceDataShardId] = TProto::THeartbeatResult::EStatus_Name(item.GetStatus());
            break;
        default:
            break;
        }
    }

    return {std::move(tabletBatchResult), TString()};
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
