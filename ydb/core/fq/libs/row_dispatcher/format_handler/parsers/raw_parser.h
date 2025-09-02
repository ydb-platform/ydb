#pragma once

#include "parser_abstract.h"

namespace NFq::NRowDispatcher {

TValueStatus<ITopicParser::TPtr> CreateRawParser(IParsedDataConsumer::TPtr consumer, const TCountersDesc& counters);

}  // namespace NFq::NRowDispatcher
