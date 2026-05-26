#include "runtime_settings_configuration.h"

namespace NYql {

#define REGISTER_RUNTIME_SETTING(setting) \
    (*this).AddSetting(#setting, setting.Setting_)

TRuntimeSettingsConfiguration::TRuntimeSettingsConfiguration()
    : TRuntimeSettingsConfiguration(TRuntimeSettings())
{
}

TRuntimeSettingsConfiguration::TRuntimeSettingsConfiguration(const TQContext& QContext)
    : TRuntimeSettingsConfiguration(TRuntimeSettings(), QContext)
{
}

TRuntimeSettingsConfiguration::TRuntimeSettingsConfiguration(const TRuntimeSettings& settings, const TQContext& QContext)
    : TSettingDispatcher("runtime_settings", QContext)
    , TRuntimeSettings(settings)
{
    REGISTER_RUNTIME_SETTING(DatumValidation);
    REGISTER_RUNTIME_SETTING(TestHostSetting);
}

} // namespace NYql
