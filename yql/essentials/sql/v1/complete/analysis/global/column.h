#pragma once

#include "global.h"
#include "input.h"

namespace NSQLComplete {

    TMaybe<TColumnContext> InferColumnContext(TParsedInput input);

} // namespace NSQLComplete
