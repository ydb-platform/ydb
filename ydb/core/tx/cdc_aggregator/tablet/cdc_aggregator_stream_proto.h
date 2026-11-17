#pragma once

#include "cdc_aggregator_stream.h"

#include <optional>
#include <utility>

namespace NKikimrCdcAggregator {
    class TTrackingConfig;
    class TCdcAggregatorStreamTopic;
    class TEvPartitioningChanged;
}

namespace NKikimr::NCdcAggregator {

template <typename T>
using TParseResult = std::pair<std::optional<T>, TString>;

class TCdcAggregatorStreamProtoConverter {
public:
    TParseResult<TCdcAggregatorStreamConfig> ParseConfig(const NKikimrCdcAggregator::TTrackingConfig& proto) const;
    TParseResult<TCdcAggregatorStreamTopic> ParseTopic(const NKikimrCdcAggregator::TCdcAggregatorStreamTopic& proto) const;
    TParseResult<TCdcAggregatorStreamPartitioningChange> ParsePartitioningChange(const NKikimrCdcAggregator::TEvPartitioningChanged& proto) const;
    TString SerializeTopic(const TCdcAggregatorStreamTopic& topic) const;
};

} // namespace NKikimr::NCdcAggregator
