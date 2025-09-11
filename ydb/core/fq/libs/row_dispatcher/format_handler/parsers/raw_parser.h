#pragma once

#include "parser_abstract.h"

#include <yql/essentials/minikql/mkql_function_registry.h>

namespace NFq::NRowDispatcher {

TValueStatus<ITopicParser::TPtr> CreateRawParser(IParsedDataConsumer::TPtr consumer, const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry, const TCountersDesc& counters);

}  // namespace NFq::NRowDispatcher
