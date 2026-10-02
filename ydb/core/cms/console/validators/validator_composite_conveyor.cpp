#include "validator_composite_conveyor.h"

#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/config/validation/validators.h>

namespace NKikimr::NConsole {

TCompositeConveyorConfigValidator::TCompositeConveyorConfigValidator()
    : IConfigValidator("composite_conveyor", NKikimrConsole::TConfigItem::CompositeConveyorConfigItem) {
}

TString TCompositeConveyorConfigValidator::GetDescription() const {
    return "Validate composite conveyor configuration";
}

bool TCompositeConveyorConfigValidator::CheckConfig(const NKikimrConfig::TAppConfig& oldConfig,
    const NKikimrConfig::TAppConfig& newConfig,
    TVector<Ydb::Issue::IssueMessage>& issues) const {
    if (oldConfig.HasCompositeConveyorConfig() && !newConfig.HasCompositeConveyorConfig()) {
        AddError(issues, "removing CompositeConveyorConfig is not supported");
        return false;
    }

    if (!newConfig.HasCompositeConveyorConfig()) {
        return true;
    }

    if (oldConfig.GetCompositeConveyorConfig().GetEnabled() != newConfig.GetCompositeConveyorConfig().GetEnabled()) {
        AddError(issues, "changing CompositeConveyorConfig.Enabled is not supported");
        return false;
    }

    std::vector<TString> errors;
    if (NConfig::ValidateCompositeConveyorConfig(newConfig.GetCompositeConveyorConfig(), errors) == NConfig::EValidationResult::Error) {
        AddError(issues, errors.front());
        return false;
    }
    return true;
}

}   // namespace NKikimr::NConsole
