#pragma once

#include <yql/essentials/core/credentials/yql_credentials.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

#include <functional>
#include <unordered_set>
#include <utility>

namespace NYql {

class TActivationPercentage;
class TGatewaysConfig;

namespace NConfig {

class TActivationGroupRegistry {
public:
    explicit TActivationGroupRegistry(const TGatewaysConfig& gateways);

    const TActivationPercentage& Resolve(const TActivationPercentage& activation) const;

private:
    const TActivationPercentage& Get(TStringBuf name) const;

    THashMap<TString, const TActivationPercentage*> Groups_;
};

template <class TActivation>
ui32 GetPercentage(const TActivation& activation, const TString& userName, bool userIsRobot, const std::unordered_set<std::string_view>& groups);

template <class TActivation>
bool Allow(const TActivation& activation, const TString& userName, bool userIsRobot, const std::unordered_set<std::string_view>& groups);

template <typename ConfigFeature>
std::function<bool(const ConfigFeature&)> MakeActivationFilter(const TString& userName,
                                                               TCredentials::TPtr credentials)
{
    std::unordered_set<std::string_view> groups;
    bool isRobot = false;
    if (credentials != nullptr) {
        groups.insert(credentials->GetGroups().begin(), credentials->GetGroups().end());
        isRobot = credentials->IsRobot();
    }
    return [userName, groups = std::move(groups), isRobot](const ConfigFeature& attr) -> bool {
        return !attr.HasActivation() || NConfig::Allow(attr.GetActivation(), userName, isRobot, groups);
    };
}

template <typename ConfigFeature>
std::function<bool(const ConfigFeature&)> MakeActivationFilter(const TString& userName,
                                                               TCredentials::TPtr credentials,
                                                               std::function<void(const TString&)> onActivated)
{
    auto filter = MakeActivationFilter<ConfigFeature>(userName, std::move(credentials));
    return [filter = std::move(filter), onActivated = std::move(onActivated)](const ConfigFeature& attr) -> bool {
        const bool allowed = filter(attr);
        if (allowed && attr.HasActivation()) {
            onActivated(attr.GetName());
        }
        return allowed;
    };
}

} // namespace NConfig
} // namespace NYql
