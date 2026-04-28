#include "runtime_settings_serialization.h"

#include <yql/essentials/providers/common/config/yql_dispatch.h>
#include <yql/essentials/providers/common/activation/yql_activation.h>

namespace NYql {

namespace {

void FillUdfSettings(
    TRuntimeSettingsConfiguration& config,
    const google::protobuf::RepeatedPtrField<NProto::TUdfSettings>& udfSettings,
    const std::function<bool(const NProto::TRuntimeSetting&)>& filter)
{
    for (const auto& udf : udfSettings) {
        for (const auto& setting : udf.GetRuntimeSettings()) {
            if (filter(setting)) {
                config.SetUdfSetting(udf.GetModule(), setting.GetName(), setting.GetValue());
            }
        }
    }
}

TRuntimeSettings::TPtr CreateRuntimeSettingsFromProtoImpl(
    const NProto::TRuntimeSettings& proto,
    const TString& userName,
    TCredentials::TPtr credentials,
    bool allowActivation)
{
    auto config = MakeRuntimeSettingsConfigurationMutable();
    auto filter = NConfig::MakeActivationFilter<NProto::TRuntimeSetting>(userName, credentials, [&](const TString&) {
        YQL_ENSURE(allowActivation, "Activation is not allowed. "
                                    "Seems you are trying to load runtime settings with activation but all settings must be already activated.");
    });
    config->Dispatch(proto.GetHostSettings(), filter);
    FillUdfSettings(*config, proto.GetUdfSettings(), filter);
    return config;
}

TRuntimeSettings::TPtr CreateRuntimeSettingsFromStringImpl(
    const TString& data,
    const TString& userName,
    TCredentials::TPtr credentials,
    bool allowActivation)
{
    NProto::TRuntimeSettings proto;
    proto.ParseFromStringOrThrow(data);
    return CreateRuntimeSettingsFromProtoImpl(proto, userName, credentials, allowActivation);
}

} // namespace

TRuntimeSettings::TPtr CreateRuntimeSettingsFromProto(
    const NProto::TRuntimeSettings& proto,
    const TString& userName,
    TCredentials::TPtr credentials)
{
    return CreateRuntimeSettingsFromProtoImpl(proto, userName, credentials, /*allowActivation=*/true);
}

NProto::TRuntimeSettings SerializeRuntimeSettingsToProto(
    const TRuntimeSettings& config)
{
    NProto::TRuntimeSettings proto;
    TRuntimeSettingsConfiguration configuration(config);
    configuration.SerializeStaticSettings([&proto](const TString& name, const TString& value) {
        auto* setting = proto.AddHostSettings();
        setting->SetName(name);
        setting->SetValue(value);
    });

    for (const auto& [moduleName, settingsMap] : config.GetUdfSettings()) {
        auto* udfSettings = proto.AddUdfSettings();
        udfSettings->SetModule(moduleName);
        for (const auto& [name, value] : settingsMap) {
            auto* setting = udfSettings->AddRuntimeSettings();
            setting->SetName(name);
            setting->SetValue(value);
        }
    }

    return proto;
}

TString SerializeRuntimeSettingsToString(const TRuntimeSettings& config) {
    TString result;
    Y_ENSURE(SerializeRuntimeSettingsToProto(config).SerializeToString(&result));
    return result;
}

TRuntimeSettings::TPtr CreateRuntimeSettingsFromString(
    const TString& data,
    const TString& userName,
    TCredentials::TPtr credentials)
{
    return CreateRuntimeSettingsFromStringImpl(data, userName, credentials, /*allowActivation=*/true);
}

TRuntimeSettings::TPtr CreateRuntimeSettingsFromString(
    const TString& data)
{
    NProto::TRuntimeSettings proto;
    proto.ParseFromStringOrThrow(data);
    return CreateRuntimeSettingsFromStringImpl(data, "", nullptr, /*allowActivation=*/false);
}

} // namespace NYql
