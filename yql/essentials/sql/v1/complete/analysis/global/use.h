#pragma once

#include "global.h"
#include "input.h"
#include "named_node.h"

#include <util/generic/ptr.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NSQLComplete {

    TMaybe<TUseContext> FindUseStatement(TParsedInput input, const TNamedNodes& nodes);

} // namespace NSQLComplete
