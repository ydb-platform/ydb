#pragma once

#include "global.h"
#include "input.h"

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NSQLComplete {

    TMaybe<TFunctionContext> EnclosingFunction(TParsedInput input);

} // namespace NSQLComplete
