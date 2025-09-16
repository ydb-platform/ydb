#pragma once

#include "parser_abstract.h"

#include <util/generic/size_literals.h>

namespace NKikimrConfig {
class TSharedReadingConfig_TJsonParserConfig;
} // namespace NKikimrConfig

namespace NFq::NRowDispatcher {

struct TJsonParserConfig {
    ui64 BatchSize = 1_MB;
    TDuration LatencyLimit;
    ui64 BufferCellCount = 1000000;  // (number rows) * (number columns) limit
};

TValueStatus<ITopicParser::TPtr> CreateJsonParser(IParsedDataConsumer::TPtr consumer, const TJsonParserConfig& config, const TCountersDesc& counters);
TJsonParserConfig CreateJsonParserConfig(const NKikimrConfig::TSharedReadingConfig_TJsonParserConfig& parserConfig);

}  // namespace NFq::NRowDispatcher
