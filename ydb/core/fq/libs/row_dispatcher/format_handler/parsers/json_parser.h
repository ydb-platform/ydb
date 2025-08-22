#pragma once

#include "parser_abstract.h"

#include <util/generic/size_literals.h>

#include <ydb/core/protos/config.pb.h>

namespace NFq::NRowDispatcher {

struct TJsonParserConfig {
    ui64 BatchSize = 1_MB;
    TDuration LatencyLimit;
    ui64 BufferCellCount = 1000000;  // (number rows) * (number columns) limit
};

TValueStatus<ITopicParser::TPtr> CreateJsonParser(IParsedDataConsumer::TPtr consumer, const TJsonParserConfig& config, const TCountersDesc& counters);
TJsonParserConfig CreateJsonParserConfig(const NKikimrConfig::TSharedReadingConfig::TJsonParserConfig& parserConfig);

}  // namespace NFq::NRowDispatcher
