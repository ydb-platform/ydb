#pragma once

#include "cdc_aggregator_heartbeat_delivery.h"

#include <ydb/core/tx/cdc_aggregator/public/events.h>

#include <optional>
#include <utility>

namespace NKikimr::NCdcAggregator {

class TCdcAggregatorHeartbeatDeliveryProtoConverter {
public:
    THolder<TEvCdcAggregator::TEvWriteHeartbeats> SerializeBatch(const TCdcAggregatorHeartbeatBatch& batch) const;
    std::pair<std::optional<TCdcAggregatorHeartbeatBatchResult>, TString> ParseBatchResult(const NKikimrCdcAggregator::TEvWriteHeartbeatsResult& proto) const;
    THolder<TEvCdcAggregatorService::TEvWriteHeartbeatResult> SerializeReply(const TCdcAggregatorHeartbeatDeliveryReply& reply) const;
};

} // namespace NKikimr::NCdcAggregator
