#include "yql_activation.h"

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/svnversion/svnversion.h>

#include <util/random/random.h>
#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/datetime/base.h>
#include <util/datetime/systime.h>

namespace NYql::NConfig {

namespace {

void EnsureActivationGroupIsNotCombinedWithDirectActivation(const TActivationPercentage& activation) {
    TVector<const google::protobuf::FieldDescriptor*> fields;
    activation.GetReflection()->ListFields(activation, &fields);

    const auto* activationGroupField = activation.GetDescriptor()->FindFieldByName("ActivationGroup");
    YQL_ENSURE(
        AllOf(fields, [activationGroupField](const auto* field) { return field == activationGroupField; }),
        "Activation group '" << activation.GetActivationGroup() << "' cannot be combined with a direct activation");
}

} // namespace

TActivationGroupRegistry::TActivationGroupRegistry(const TGatewaysConfig& gateways) {
    for (const auto& group : gateways.GetActivationGroup()) {
        const auto& name = group.GetName();
        YQL_ENSURE(!name.empty(), "Activation group name must not be empty");
        YQL_ENSURE(group.HasActivation(), "Activation group '" << name << "' has no activation");
        YQL_ENSURE(
            !group.GetActivation().HasActivationGroup(),
            "Activation group '" << name << "' cannot reference another activation group");
        YQL_ENSURE(Groups_.emplace(name, &group.GetActivation()).second, "Duplicate activation group '" << name << "'");
    }
}

const TActivationPercentage& TActivationGroupRegistry::Resolve(const TActivationPercentage& activation) const {
    if (!activation.HasActivationGroup()) {
        return activation;
    }

    EnsureActivationGroupIsNotCombinedWithDirectActivation(activation);
    return Get(activation.GetActivationGroup());
}

const TActivationPercentage& TActivationGroupRegistry::Get(TStringBuf name) const {
    const auto it = Groups_.find(name);
    YQL_ENSURE(it != Groups_.end(), "Unknown activation group '" << name << "'");
    return *it->second;
}

template <class TActivation>
ui32 GetPercentage(const TActivation& activation, const TString& userName, bool isRobot, const std::unordered_set<std::string_view>& groups) {
    YQL_ENSURE(
        !activation.HasActivationGroup(),
        "Activation group '" << activation.GetActivationGroup() << "' must be resolved before evaluating activation");

    if (AnyOf(activation.GetExcludeUsers(), [&](const auto& user) { return user == userName; })) {
        return 0;
    }
    if (AnyOf(activation.GetIncludeUsers(), [&](const auto& user) { return user == userName; })) {
        return 100;
    }
    if (!groups.empty()) {
        if (AnyOf(activation.GetExcludeGroups(), [&](const auto& excludeGroup) { return groups.contains(excludeGroup); })) {
            return 0;
        }
        if (AnyOf(activation.GetIncludeGroups(), [&](const auto& includeGroup) { return groups.contains(includeGroup); })) {
            return 100;
        }
    }
    if (const auto currentRev = GetProgramCommitId()) {
        if (AnyOf(activation.GetExcludeRevisions(), [&](const auto& rev) { return rev == currentRev; })) {
            return 0;
        }
        if (AnyOf(activation.GetIncludeRevisions(), [&](const auto& rev) { return rev == currentRev; })) {
            return 100;
        }
    }
    if (isRobot && activation.GetExcludeRobots()) {
        return 0;
    }

    ui32 percent = activation.GetPercentage();
    if (activation.ByHourSize()) {
        auto now = TInstant::Now();
        struct tm local = {};
        now.LocalTime(&local);
        const auto hour = ui32(local.tm_hour);

        for (auto& byHour : activation.GetByHour()) {
            if (byHour.GetHour() == hour) {
                percent = byHour.GetPercentage();
                break;
            }
        }
    }

    return percent;
}

template <class TActivation>
bool Allow(const TActivation& activation, const TString& userName, bool isRobot, const std::unordered_set<std::string_view>& groups) {
    ui32 percent = GetPercentage(activation, userName, isRobot, groups);
    const auto random = RandomNumber<ui8>(100);
    return random < percent;
}

template ui32 GetPercentage<NYql::TActivationPercentage>(const NYql::TActivationPercentage& activation, const TString& userName, bool isRobot, const std::unordered_set<std::string_view>& groups);
template bool Allow<NYql::TActivationPercentage>(const NYql::TActivationPercentage& activation, const TString& userName, bool isRobot, const std::unordered_set<std::string_view>& groups);

} // namespace NYql::NConfig
