#pragma once

#include "global.h"
#include "input.h"
#include "named_node.h"

namespace NSQLComplete {

    TMaybe<TColumnContext> InferColumnContext(TParsedInput input, const TNamedNodes& nodes);

} // namespace NSQLComplete
