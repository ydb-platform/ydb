#include "runtime_settings_configuration.h"

#include <library/cpp/hyperloglog/hyperloglog.h>

#include <util/generic/singleton.h>

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
    AddSetting(#setting, [](auto& settings) -> auto& { return settings.setting.Setting_; })

const TRuntimeSettingsConfiguration& TRuntimeSettingsConfiguration::Get() {
    struct TInstance {
        const TRuntimeSettingsConfiguration Configuration;
    };
    return Singleton<TInstance>()->Configuration;
}

TRuntimeSettingsConfiguration::TRuntimeSettingsConfiguration()
    : NCommon::TDispatcher<TRuntimeSettings>("runtime_settings")
{
    REGISTER_RUNTIME_SETTING(DatumValidation)
        .Parser(MakeDatumValidationModeParser())
        .Serializer(MakeDatumValidationModeSerializer());
    REGISTER_RUNTIME_SETTING(TestHostSetting);
    REGISTER_RUNTIME_SETTING(UdfProfileEnable);
    REGISTER_RUNTIME_SETTING(UdfProfileMinTimeUs);
    REGISTER_RUNTIME_SETTING(UdfProfileGraceCount);
    REGISTER_RUNTIME_SETTING(UdfProfileExcludeModules);
    REGISTER_RUNTIME_SETTING(UdfProfileHLLPrecision)
        .Lower(THyperLogLogBase::PRECISION_MIN)
        .Upper(THyperLogLogBase::PRECISION_MAX);
}

} // namespace NYql
