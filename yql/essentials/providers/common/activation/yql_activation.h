#pragma once

#include <util/generic/string.h>
#include <unordered_set>

namespace NYql::NConfig {

template <class TActivation>
ui32 GetPercentage(const TActivation& activation, const TString& userName, const std::unordered_set<std::string_view>& groups);

template <class TActivation>
bool Allow(const TActivation& activation, const TString& userName, const std::unordered_set<std::string_view>& groups);

} // namespace
