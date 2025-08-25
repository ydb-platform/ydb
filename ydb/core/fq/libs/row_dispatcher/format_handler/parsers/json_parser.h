#pragma once

#include "parser_abstract.h"

#include <util/generic/size_literals.h>

namespace NFq::NConfig {
    class TJsonParserConfig;
} // namespace NFq::NConfig

namespace NFq::NRowDispatcher {

struct TJsonParserConfig {
    ui64 BatchSize = 1_MB;
    TDuration LatencyLimit;
    ui64 BufferCellCount = 1000000;  // (number rows) * (number columns) limit
};

TValueStatus<ITopicParser::TPtr> CreateJsonParser(IParsedDataConsumer::TPtr consumer, const TJsonParserConfig& config, const TCountersDesc& counters);
TJsonParserConfig CreateJsonParserConfig(const NConfig::TJsonParserConfig& parserConfig);

}  // namespace NFq::NRowDispatcher
