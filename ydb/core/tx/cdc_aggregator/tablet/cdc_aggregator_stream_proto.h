#pragma once

#include "cdc_aggregator_stream.h"

#include <ydb/core/protos/cdc_aggregator.pb.h>

#include <optional>

namespace NKikimr::NCdcAggregator {

std::optional<TCdcAggregatorStreamTopic> ParseCdcAggregatorStreamTopic(const NKikimrCdcAggregator::TTrackingConfig& proto, TString& error);
std::optional<TCdcAggregatorStreamConfig> ParseCdcAggregatorStreamConfig(const NKikimrCdcAggregator::TTrackingConfig& proto, TString& error);
TString SerializeCdcAggregatorStreamTopic(const TCdcAggregatorStreamTopic& topic);

} // namespace NKikimr::NCdcAggregator
