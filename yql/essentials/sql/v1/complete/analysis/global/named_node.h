#pragma once

#include "input.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NSQLComplete {

    TVector<TString> CollectNamedNodes(TParsedInput input);

} // namespace NSQLComplete
