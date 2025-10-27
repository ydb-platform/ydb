#pragma once

#include "parser_abstract.h"

#include <ydb/core/fq/libs/row_dispatcher/common/row_dispatcher_settings.h>

#include <util/generic/size_literals.h>

namespace NKikimr::NMiniKQL {
    class IFunctionRegistry;
} // namespace NKikimr::NMiniKQL

namespace NFq::NRowDispatcher {

struct TJsonParserConfig {
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;
    ui64 BatchSize = 1_MB;
    TDuration LatencyLimit;
    ui64 BufferCellCount = 1000000;  // (number rows) * (number columns) limit
};

TValueStatus<ITopicParser::TPtr> CreateJsonParser(IParsedDataConsumer::TPtr consumer, const TJsonParserConfig& config, const TCountersDesc& counters);
TJsonParserConfig CreateJsonParserConfig(const TRowDispatcherSettings::TJsonParserSettings& parserConfig, const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry);

}  // namespace NFq::NRowDispatcher
