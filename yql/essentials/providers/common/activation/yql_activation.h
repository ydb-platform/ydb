#pragma once

#include <yql/essentials/core/credentials/yql_credentials.h>

#include <util/generic/string.h>

#include <unordered_set>

namespace NYql::NConfig {

template <class TActivation>
ui32 GetPercentage(const TActivation& activation, const TString& userName, bool userIsRobot, const std::unordered_set<std::string_view>& groups);

template <class TActivation>
bool Allow(const TActivation& activation, const TString& userName, bool userIsRobot, const std::unordered_set<std::string_view>& groups);

template <typename ConfigFeature>
std::function<bool(const ConfigFeature&)> MakeActivationFilter(const TString& userName,
                                                               TCredentials::TPtr credentials,
                                                               std::function<void(const TString&)> onActivated)
{
    std::unordered_set<std::string_view> groups;
    bool isRobot = false;
    if (credentials != nullptr) {
        groups.insert(credentials->GetGroups().begin(), credentials->GetGroups().end());
        isRobot = credentials->IsRobot();
    }
    return [userName, groups = std::move(groups), isRobot, onActivated = std::move(onActivated)](const ConfigFeature& attr) -> bool {
        if (!attr.HasActivation()) {
            return true;
        }
        if (NConfig::Allow(attr.GetActivation(), userName, isRobot, groups)) {
            onActivated(attr.GetName());
            return true;
        }
        return false;
    };
}

} // namespace NYql::NConfig
