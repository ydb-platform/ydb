#include "runtime_settings_configuration.h"

namespace NYql {

namespace {

auto MakeDatumValidationModeParser() {
    return [](const TString& str) -> EDatumValidationMode {
        if (str == "None") {
            return EDatumValidationMode::None;
        }
        if (str == "Cheap") {
            return EDatumValidationMode::Cheap;
        }
        if (str == "Expensive") {
            return EDatumValidationMode::Expensive;
        }
        ythrow yexception() << "Unknown EDatumValidationMode value: " << str;
    };
}

auto MakeDatumValidationModeSerializer() {
    return [](const EDatumValidationMode& mode) -> TString {
        switch (mode) {
            case EDatumValidationMode::None:
                return "None";
            case EDatumValidationMode::Cheap:
                return "Cheap";
            case EDatumValidationMode::Expensive:
                return "Expensive";
        }
        ythrow yexception() << "Unknown EDatumValidationMode value: " << static_cast<int>(mode);
    };
}

} // namespace

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
    REGISTER_RUNTIME_SETTING(DatumValidation)
        .Parser(MakeDatumValidationModeParser())
        .Serializer(MakeDatumValidationModeSerializer());
    REGISTER_RUNTIME_SETTING(TestHostSetting);
}

} // namespace NYql
