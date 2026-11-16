#include "cdc_aggregator_heartbeat_delivery_proto.h"

#include <util/string/builder.h>

namespace NKikimr::NCdcAggregator {

THolder<TEvCdcAggregator::TEvWriteHeartbeats> TCdcAggregatorHeartbeatDeliveryProtoConverter::SerializeBatch(const TVector<TCdcAggregatorHeartbeatDeliveryItem>& batch) const {
    auto ev = MakeHolder<TEvCdcAggregator::TEvWriteHeartbeats>();
    for (const auto& item : batch) {
        auto& proto = *ev->Record.AddHeartbeats();
        proto.SetSourceDataShardId(item.SourceDataShardId);
        item.Heartbeat.Serialize(*proto.MutableHeartbeat());
    }
    return ev;
}

std::pair<std::optional<TCdcAggregatorHeartbeatDeliveryResult>, TString> TCdcAggregatorHeartbeatDeliveryProtoConverter::ParseBatchResult(const NKikimrCdcAggregator::TEvWriteHeartbeatsResult& proto) const {
    using TProto = NKikimrCdcAggregator::TEvWriteHeartbeatsResult;

    if (proto.GetStatus() != TProto::SUCCESS) {
        return {std::nullopt, TStringBuilder() << TProto::EStatus_Name(proto.GetStatus()) << ": " << proto.GetError()};
    }

    TCdcAggregatorHeartbeatDeliveryResult result;
    for (const auto& item : proto.GetResults()) {
        switch (item.GetStatus()) {
        case TProto::THeartbeatResult::SUCCESS:
            if (item.HasWrittenVersion()) {
                result.WrittenVersions[item.GetSourceDataShardId()] = TRowVersion::FromProto(item.GetWrittenVersion());
            }
            break;
        case TProto::THeartbeatResult::UNKNOWN_SOURCE:
        case TProto::THeartbeatResult::BAD_REQUEST:
            result.RejectionErrors[item.GetSourceDataShardId()] = TProto::THeartbeatResult::EStatus_Name(item.GetStatus());
            break;
        default:
            break;
        }
    }

    return {std::move(result), TString()};
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
