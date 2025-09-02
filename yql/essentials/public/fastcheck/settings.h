#pragma once

#include <util/generic/hash_set.h>
#include <util/generic/string.h>

namespace NYql::NFastCheck {

    THashSet<TString> TranslationFlags();

} // namespace NYql::NFastCheck
