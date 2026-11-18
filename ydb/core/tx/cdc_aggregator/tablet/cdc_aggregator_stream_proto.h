#pragma once

#include "cdc_aggregator_stream.h"

#include <optional>
#include <utility>

namespace NKikimrCdcAggregator {
    class TTrackingConfig;
    class TCdcAggregatorStreamTopic;
    class TEvPartitioningChanged;
    class TEvWriteHeartbeats;
    class TEvWriteHeartbeatsResult;
    class TEvDescribeTrackingResult;
}

namespace NKikimr::NCdcAggregator {

template <typename T>
using TParseResult = std::pair<std::optional<T>, TString>;

struct TCdcAggregatorStreamHeartbeatsBatch {
    THashMap<ui64, NPQ::THeartbeat> HeartbeatsBySourceDataShardId;
    THashSet<ui64> InvalidSourceDataShardIds;
};

class TCdcAggregatorStreamProtoConverter {
public:
    TParseResult<TCdcAggregatorStreamConfig> ParseConfig(const NKikimrCdcAggregator::TTrackingConfig& proto) const;
    TParseResult<TCdcAggregatorStreamTopic> ParseTopic(const NKikimrCdcAggregator::TCdcAggregatorStreamTopic& proto) const;
    TParseResult<TCdcAggregatorStreamPartitioningChange> ParsePartitioningChange(const NKikimrCdcAggregator::TEvPartitioningChanged& proto) const;
    TCdcAggregatorStreamHeartbeatsBatch ParseHeartbeatsBatch(const NKikimrCdcAggregator::TEvWriteHeartbeats& proto) const;
    TString SerializeTopic(const TCdcAggregatorStreamTopic& topic) const;
    NKikimrCdcAggregator::TEvWriteHeartbeatsResult SerializeWriteHeartbeatsResult(
        const TCdcAggregatorStreamHeartbeatsBatch& batch, const TCdcAggregatorStreamHeartbeatsDiff& diff) const;
    NKikimrCdcAggregator::TEvDescribeTrackingResult SerializeDescribeTrackingResult(const TCdcAggregatorStream& stream) const;
};

} // namespace NKikimr::NCdcAggregator
