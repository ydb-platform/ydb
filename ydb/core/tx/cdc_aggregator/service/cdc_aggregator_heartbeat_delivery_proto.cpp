#include "cdc_aggregator_heartbeat_delivery_proto.h"

#include <util/generic/overloaded.h>
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

std::pair<std::optional<TCdcAggregatorWriteHeartbeatsResult>, TString> TCdcAggregatorHeartbeatDeliveryProtoConverter::ParseWriteHeartbeatsResult(const NKikimrCdcAggregator::TEvWriteHeartbeatsResult& proto) const {
    using TProto = NKikimrCdcAggregator::TEvWriteHeartbeatsResult;

    if (proto.GetStatus() != TProto::SUCCESS) {
        return {std::nullopt, TStringBuilder() << TProto::EStatus_Name(proto.GetStatus()) << ": " << proto.GetError()};
    }

    TCdcAggregatorWriteHeartbeatsResult writeHeartbeatsResult;
    for (const auto& item : proto.GetResults()) {
        const ui64 sourceDataShardId = item.GetSourceDataShardId();
        switch (item.GetStatus()) {
        case TProto::THeartbeatResult::SUCCESS:
            if (item.HasWrittenVersion()) {
                writeHeartbeatsResult.WrittenVersionsBySourceDataShardId[sourceDataShardId] = TRowVersion::FromProto(item.GetWrittenVersion());
            }
            break;
        case TProto::THeartbeatResult::UNKNOWN_SOURCE:
        case TProto::THeartbeatResult::BAD_REQUEST:
            writeHeartbeatsResult.RejectionErrorsBySourceDataShardId[sourceDataShardId] = TProto::THeartbeatResult::EStatus_Name(item.GetStatus());
            break;
        default:
            break;
        }
    }

    return {std::move(writeHeartbeatsResult), TString()};
}

THolder<TEvCdcAggregatorService::TEvWriteHeartbeatResult> TCdcAggregatorHeartbeatDeliveryProtoConverter::SerializeReply(const TCdcAggregatorHeartbeatDeliveryReply& reply) const {
    using EStatus = TEvCdcAggregatorService::TEvWriteHeartbeatResult::EStatus;

    auto ev = MakeHolder<TEvCdcAggregatorService::TEvWriteHeartbeatResult>();
    std::visit(TOverloaded{
        [&](const TCdcAggregatorHeartbeatWritten& written) {
            ev->Status = EStatus::Written;
            ev->WrittenVersion = written.Version;
        },
        [&](const TCdcAggregatorHeartbeatRetry& retry) {
            ev->Status = EStatus::Retry;
            ev->Error = retry.Error;
        },
        [&](const TCdcAggregatorHeartbeatRejected& rejected) {
            ev->Status = EStatus::Rejected;
            ev->Error = rejected.Error;
        },
    }, reply);
    return ev;
}

} // namespace NKikimr::NCdcAggregator
