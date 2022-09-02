#pragma once

#include <util/generic/string.h>

namespace NYql::NConfig {

template <class TActivation>
bool Allow(const TActivation& activation, const TString& userName);

} // namespace
