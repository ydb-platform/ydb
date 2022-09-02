#include "yql_activation.h"

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>

#include <util/random/random.h>
#include <util/generic/algorithm.h>
#include <util/datetime/base.h>
#include <util/datetime/systime.h>

namespace NYql::NConfig {


template <class TActivation>
bool Allow(const TActivation& activation, const TString& userName) {
    if (AnyOf(activation.GetIncludeUsers(), [&](const auto& user) { return user == userName; })) {
        return true;
    }
    if (AnyOf(activation.GetExcludeUsers(), [&](const auto& user) { return user == userName; })) {
        return false;
    }
    if (userName.StartsWith("robot") && activation.GetExcludeRobots()) {
        return false;
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
    const auto random = RandomNumber<ui8>(100);
    return random < percent;
}

template bool Allow<NYql::TActivationPercentage>(const NYql::TActivationPercentage& activation, const TString& userName);

} // namespace
