#pragma once

#include <util/generic/string.h>

namespace NYql::NConfig {

template <class TActivation>
ui32 GetPercentage(const TActivation& activation, const TString& userName);

template <class TActivation>
bool Allow(const TActivation& activation, const TString& userName);

} // namespace
