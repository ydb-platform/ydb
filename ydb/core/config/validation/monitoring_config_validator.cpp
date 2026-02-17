#include "validators.h"
#include <ydb/core/protos/config.pb.h>
#include <util/generic/string.h>
#include <vector>


namespace NKikimr::NConfig {

namespace {

bool GetEnforceUserToken(const NKikimrConfig::TAppConfig& config) {
    if (!config.HasDomainsConfig()) {
        return false;
    }

    if (!config.GetDomainsConfig().HasSecurityConfig()) {
        return false;
    }

    const auto& securityConfig = config.GetDomainsConfig().GetSecurityConfig();
    return securityConfig.GetEnforceUserTokenRequirement();
}

} // namespace

EValidationResult ValidateMonitoringConfig(const NKikimrConfig::TAppConfig& config, std::vector<TString>& msg) {
    const auto& monitoringConfig = config.GetMonitoringConfig();

    // Custom monitoring authentication may be enabled only if user authentication is mandatory
    if (monitoringConfig.GetRequireCountersAuthentication() || monitoringConfig.GetRequireHealthcheckAuthentication()) {
        const bool enforceUserToken = GetEnforceUserToken(config);
        if (!enforceUserToken && monitoringConfig.GetRequireCountersAuthentication()) {
            msg.push_back("Setting EnforceUserTokenRequirement is disabled, but RequireCountersAuthentication is enabled");
            return EValidationResult::Error;
        }

        if (!enforceUserToken && monitoringConfig.GetRequireHealthcheckAuthentication()) {
            msg.push_back("Setting EnforceUserTokenRequirement is disabled, but RequireHealthcheckAuthentication is enabled");
            return EValidationResult::Error;
        }
    }

    return EValidationResult::Ok;
}

} // NKikimr::NConfig
