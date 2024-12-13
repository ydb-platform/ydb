#pragma once

#include "parser_abstract.h"

namespace NFq::NRowDispatcher {

TValueStatus<ITopicParser::TPtr> CreateRawParser(IParsedDataConsumer::TPtr consumer);

}  // namespace NFq::NRowDispatcher
