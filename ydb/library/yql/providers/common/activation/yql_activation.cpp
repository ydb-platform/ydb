#include "yql_activation.h"

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>

#include <library/cpp/svnversion/svnversion.h>

#include <util/random/random.h>
#include <util/generic/algorithm.h>
#include <util/datetime/base.h>
#include <util/datetime/systime.h>

namespace NYql::NConfig {

template <class TActivation>
ui32 GetPercentage(const TActivation& activation, const TString& userName, const std::unordered_set<std::string_view>& groups) {
    if (AnyOf(activation.GetIncludeUsers(), [&](const auto& user) { return user == userName; })) {
        return 100;
    }
    if (!groups.empty() && AnyOf(activation.GetIncludeGroups(), [&](const auto& includeGroup) { return groups.contains(includeGroup); })) {
        return 100;
    }
    const auto currentRev =  GetProgramCommitId();
    if (currentRev && AnyOf(activation.GetIncludeRevisions(), [&](const auto& rev) { return rev == currentRev; })) {
        return 100;
    }
    if (AnyOf(activation.GetExcludeUsers(), [&](const auto& user) { return user == userName; })) {
        return 0;
    }
    if (!groups.empty() && AnyOf(activation.GetExcludeGroups(), [&](const auto& excludeGroup) { return groups.contains(excludeGroup); })) {
        return 0;
    }
    if (currentRev && AnyOf(activation.GetExcludeRevisions(), [&](const auto& rev) { return rev == currentRev; })) {
        return 0;
    }
    if ((userName.StartsWith("robot-") || userName.StartsWith("zomb-")) && activation.GetExcludeRobots()) {
        return 0;
    }

    ui32 percent = activation.GetPercentage();
    if (activation.ByHourSize()) {
        auto now = TInstant::Now();
        struct tm local = {};
        now.LocalTime(&local);
        const auto hour = ui32(local.tm_hour);

        for (auto& byHour: activation.GetByHour()) {
            if (byHour.GetHour() == hour) {
                percent = byHour.GetPercentage();
                break;
            }
        }
    }

    return percent;
}

template <class TActivation>
bool Allow(const TActivation& activation, const TString& userName, const std::unordered_set<std::string_view>& groups) {
    ui32 percent = GetPercentage(activation, userName, groups);
    const auto random = RandomNumber<ui8>(100);
    return random < percent;
}

template ui32 GetPercentage<NYql::TActivationPercentage>(const NYql::TActivationPercentage& activation, const TString& userName, const std::unordered_set<std::string_view>& groups);
template bool Allow<NYql::TActivationPercentage>(const NYql::TActivationPercentage& activation, const TString& userName, const std::unordered_set<std::string_view>& groups);

} // namespace
