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

class TCdcAggregatorStreamProtoConverter {
public:
    std::pair<std::optional<TCdcAggregatorStreamConfig>, TString> ParseConfig(const NKikimrCdcAggregator::TTrackingConfig& proto) const;
    std::pair<std::optional<TCdcAggregatorStreamTopic>, TString> ParseTopic(const NKikimrCdcAggregator::TCdcAggregatorStreamTopic& proto) const;
    std::pair<std::optional<TCdcAggregatorStreamPartitioningChange>, TString> ParsePartitioningChange(const NKikimrCdcAggregator::TEvPartitioningChanged& proto) const;
    TString SerializeTopic(const TCdcAggregatorStreamTopic& topic) const;
};

} // namespace NKikimr::NCdcAggregator
