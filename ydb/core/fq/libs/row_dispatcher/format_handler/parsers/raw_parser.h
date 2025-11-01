#pragma once

#include "parser_abstract.h"

namespace NKikimr::NMiniKQL {
    class IFunctionRegistry;
} // namespace NKikimr::NMiniKQL

namespace NFq::NRowDispatcher {

TValueStatus<ITopicParser::TPtr> CreateRawParser(IParsedDataConsumer::TPtr consumer, const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry, const TCountersDesc& counters);

}  // namespace NFq::NRowDispatcher
