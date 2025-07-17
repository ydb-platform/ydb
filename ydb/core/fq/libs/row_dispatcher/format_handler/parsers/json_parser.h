#pragma once

#include "parser_abstract.h"

#include <ydb/core/fq/libs/config/protos/row_dispatcher.pb.h>

#include <util/generic/size_literals.h>

namespace NFq::NRowDispatcher {

struct TJsonParserConfig {
    ui64 BatchSize = 1_MB;
    TDuration LatencyLimit;
    ui64 BufferCellCount = 1000000;  // (number rows) * (number columns) limit
    ui64 MaxMessageSizeBytes = 15_MB;
};

TValueStatus<ITopicParser::TPtr> CreateJsonParser(IParsedDataConsumer::TPtr consumer, const TJsonParserConfig& config, const TCountersDesc& counters);
TJsonParserConfig CreateJsonParserConfig(const NConfig::TJsonParserConfig& parserConfig);

}  // namespace NFq::NRowDispatcher
