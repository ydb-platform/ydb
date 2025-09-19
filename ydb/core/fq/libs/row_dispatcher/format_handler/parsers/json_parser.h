#pragma once

#include "parser_abstract.h"

#include <util/generic/size_literals.h>

namespace NKikimrConfig {
class TSharedReadingConfig_TJsonParserConfig;
} // namespace NKikimrConfig

namespace NKikimr::NMiniKQL {
    class IFunctionRegistry;
} // namespace NKikimr::NMiniKQL

namespace NFq::NRowDispatcher {

struct TJsonParserConfig {
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;
    ui64 BatchSize = 1_MB;
    TDuration LatencyLimit;
    ui64 BufferCellCount = 1000000;  // (number rows) * (number columns) limit, amount memory size is O(BufferCellCount * log(BufferCellCount))
};

TValueStatus<ITopicParser::TPtr> CreateJsonParser(IParsedDataConsumer::TPtr consumer, const TJsonParserConfig& config, const TCountersDesc& counters);
TJsonParserConfig CreateJsonParserConfig(const NKikimrConfig::TSharedReadingConfig_TJsonParserConfig& parserConfig, const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry);

}  // namespace NFq::NRowDispatcher
