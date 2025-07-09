#pragma once

#include "input.h"

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NSQLComplete {

    TMaybe<TString> EnclosingFunction(TParsedInput input);

} // namespace NSQLComplete
