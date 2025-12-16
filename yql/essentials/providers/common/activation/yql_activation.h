#pragma once

#include <util/generic/string.h>
#include <unordered_set>

namespace NYql::NConfig {

template <class TActivation>
ui32 GetPercentage(const TActivation& activation, const TString& userName, bool userIsRobot, const std::unordered_set<std::string_view>& groups);

// TODO: remove after changing DQ provider
template <class TActivation>
ui32 GetPercentage(const TActivation& activation, const TString& userName, const std::unordered_set<std::string_view>& groups) {
    return GetPercentage(activation, userName, userName.StartsWith("robot-") || userName.StartsWith("zomb-"), groups);
}

template <class TActivation>
bool Allow(const TActivation& activation, const TString& userName, bool userIsRobot, const std::unordered_set<std::string_view>& groups);

// TODO: remove after changing DQ provider
template <class TActivation>
bool Allow(const TActivation& activation, const TString& userName, const std::unordered_set<std::string_view>& groups) {
    return Allow(activation, userName, userName.StartsWith("robot-") || userName.StartsWith("zomb-"), groups);
}

} // namespace NYql::NConfig
