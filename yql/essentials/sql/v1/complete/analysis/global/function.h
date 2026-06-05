#pragma once

#include "global.h"
#include "input.h"
#include "named_node_resolution.h"

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NSQLComplete {

TMaybe<TFunctionContext> EnclosingFunction(TParsedInput input, const INamedNodes& nodes);

TMaybe<TFunctionContext> GetFunction(SQLv1::Table_refContext* ctx, const INamedNodes& nodes);

} // namespace NSQLComplete
