#include "runtime_settings_configuration.h"

namespace NYql {

#define REGISTER_RUNTIME_SETTING(setting) \
    (*this).AddSetting(#setting, setting.Setting_)

TRuntimeSettingsConfiguration::TRuntimeSettingsConfiguration()
    : TRuntimeSettingsConfiguration(TRuntimeSettings())
{
}

TRuntimeSettingsConfiguration::TRuntimeSettingsConfiguration(const TRuntimeSettings& settings)
    : TRuntimeSettings(settings)
{
    REGISTER_RUNTIME_SETTING(DatumValidation);
}

} // namespace NYql
