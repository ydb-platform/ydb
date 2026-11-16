#pragma once

#include "cdc_aggregator_stream.h"

#include <ydb/core/protos/cdc_aggregator.pb.h>

#include <optional>
#include <utility>

namespace NKikimr::NCdcAggregator {

class TCdcAggregatorStreamProtoConverter {
public:
    std::pair<std::optional<TCdcAggregatorStreamConfig>, TString> ParseConfig(const NKikimrCdcAggregator::TTrackingConfig& proto) const;
    std::pair<std::optional<TCdcAggregatorStreamTopic>, TString> ParseTopic(const NKikimrCdcAggregator::TCdcAggregatorStreamTopic& proto) const;
    TString SerializeTopic(const TCdcAggregatorStreamTopic& topic) const;
};

} // namespace NKikimr::NCdcAggregator
