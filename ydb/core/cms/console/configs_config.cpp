#include "configs_config.h"

#include <util/string/printf.h>

namespace NKikimr::NConsole {

TConfigsConfig::TConfigsConfig(const NKikimrConsole::TConfigsConfig &config)
{
    Parse(config);
}

void TConfigsConfig::Clear()
{
    AllowedNodeIdScopeKinds.clear();
    AllowedHostScopeKinds.clear();
    DisallowedDomainScopeKinds.clear();
}

void TConfigsConfig::Parse(const NKikimrConsole::TConfigsConfig &config)
{
    Clear();

    for (auto &kind : config.GetUsageScopeRestrictions().GetAllowedNodeIdUsageScopeKinds())
        AllowedNodeIdScopeKinds.insert(kind);
    for (auto &kind : config.GetUsageScopeRestrictions().GetAllowedHostUsageScopeKinds())
        AllowedHostScopeKinds.insert(kind);
    for (auto &kind : config.GetUsageScopeRestrictions().GetDisallowedDomainUsageScopeKinds())
        DisallowedDomainScopeKinds.insert(kind);

    ValidationLevel = config.GetValidationOptions().GetValidationLevel();
    MaxConfigChecksPerModification = config.GetValidationOptions().GetMaxConfigChecksPerModification();
    FailOnExceededConfigChecksLimit = config.GetValidationOptions().GetFailOnExceededConfigChecksLimit();
    EnableValidationOnNodeConfigRequest = config.GetValidationOptions().GetEnableValidationOnNodeConfigRequest();
    TreatWarningAsError = config.GetValidationOptions().GetTreatWarningAsError();
}

bool TConfigsConfig::Check(const NKikimrConsole::TConfigsConfig &config,
                           TString &error)
{
    TConfigsConfig newConfig(config);

    if (newConfig.MaxConfigChecksPerModification > MAX_CONFIG_CHECKS_LIMIT) {
        error = Sprintf("MaxConfigChecksPerModification exceeds limit value %" PRIu64,
                        MAX_CONFIG_CHECKS_LIMIT);
        return false;
    }

    return true;
}


} // namespace NKikimr::NConsole
