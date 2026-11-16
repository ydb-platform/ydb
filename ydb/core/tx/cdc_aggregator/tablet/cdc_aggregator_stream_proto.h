#pragma once

#include "cdc_aggregator_stream.h"

#include <ydb/core/protos/cdc_aggregator.pb.h>

#include <optional>

namespace NKikimr::NCdcAggregator {

template <typename T>
struct TParseResult {
    std::optional<T> Value;
    TString Error;
};

class TCdcAggregatorStreamProtoConverter {
public:
    TParseResult<TCdcAggregatorStreamConfig> ParseConfig(const NKikimrCdcAggregator::TTrackingConfig& proto) const;
    TParseResult<TCdcAggregatorStreamTopic> ParseTopic(const NKikimrCdcAggregator::TTrackingConfig& proto) const;
    TString SerializeTopic(const TCdcAggregatorStreamTopic& topic) const;
};

} // namespace NKikimr::NCdcAggregator
