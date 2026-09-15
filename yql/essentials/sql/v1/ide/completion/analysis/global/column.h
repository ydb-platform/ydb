#pragma once

#include "global.h"
#include "input.h"
#include <yql/essentials/sql/v1/ide/analysis/named_node_resolution.h>

namespace NSQLComplete {

TMaybe<TColumnContext> InferColumnContext(TParsedInput input, const INamedNodes& nodes);

} // namespace NSQLComplete
