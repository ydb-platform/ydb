#pragma once

#include "global.h"
#include "input.h"
#include "named_node.h"

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NSQLComplete {

    TMaybe<TFunctionContext> EnclosingFunction(TParsedInput input, const TNamedNodes& nodes);

} // namespace NSQLComplete
