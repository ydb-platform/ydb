#include "runtime_settings_serialization.h"

#include <yql/essentials/providers/common/config/yql_dispatch.h>
#include <yql/essentials/providers/common/activation/yql_activation.h>

namespace NYql {

namespace {

void FillUdfSettings(
    TRuntimeSettingsConfiguration& config,
    const google::protobuf::RepeatedPtrField<NProto::TUdfSettings>& udfSettings,
    const std::function<bool(const NProto::TRuntimeSetting&)>& filter,
    const TQContext& qContext)
{
    for (const auto& udf : udfSettings) {
        TString activationLabel = TStringBuilder() << "runtime_settings_udf_" << udf.GetModule();
        auto flags = NCommon::SelectAndSaveActivatedFlags<NProto::TRuntimeSetting>(
            activationLabel, qContext, udf.GetRuntimeSettings(), filter, /*hasProviderName=*/true);
        for (const auto& setting : flags) {
            config.SetUdfSetting(udf.GetModule(), setting.GetName(), setting.GetValue());
        }
    }
}

TRuntimeSettings::TPtr CreateRuntimeSettingsFromProtoImpl(
    const NProto::TRuntimeSettings& proto,
    const TString& userName,
    TCredentials::TPtr credentials,
    bool allowActivation,
    const TQContext& qContext,
    const std::function<void(const TString&)>& onPartialFeatureActivation)
{
    auto config = MakeRuntimeSettingsConfigurationMutable(qContext);
    auto filter = NConfig::MakeActivationFilter<NProto::TRuntimeSetting>(userName, credentials, [&](const TString& name) {
        YQL_ENSURE(allowActivation, "Activation is not allowed. "
                                    "Seems you are trying to load runtime settings with activation but all settings must be already activated.");
        if (onPartialFeatureActivation) {
            onPartialFeatureActivation(name);
        }
    });
    config->Dispatch(proto.GetHostSettings(), filter);
    FillUdfSettings(*config, proto.GetUdfSettings(), filter, qContext);
    return config;
}

TRuntimeSettings::TPtr CreateRuntimeSettingsFromStringImpl(const TString& data)
{
    NProto::TRuntimeSettings proto;
    proto.ParseFromStringOrThrow(data);
    return CreateRuntimeSettingsFromProtoImpl(proto, "", nullptr, /*allowActivation=*/false, TQContext(), {});
}

} // namespace

TRuntimeSettings::TPtr CreateRuntimeSettingsFromProto(
    const NProto::TRuntimeSettings& proto,
    const TString& userName,
    TCredentials::TPtr credentials,
    const TQContext& qContext,
    std::function<void(const TString&)> onPartialFeatureActivation)
{
    return CreateRuntimeSettingsFromProtoImpl(proto, userName, credentials, /*allowActivation=*/true, qContext, onPartialFeatureActivation);
}

TRuntimeSettings::TPtr DeserializeRuntimeSettingsFromProto(
    const NProto::TRuntimeSettings& proto)
{
    return CreateRuntimeSettingsFromProtoImpl(proto, "", nullptr, /*allowActivation=*/false, TQContext(), {});
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
    const TString& data)
{
    return CreateRuntimeSettingsFromStringImpl(data);
}

} // namespace NYql
