#include "runtime_settings_serialization.h"
#include "runtime_settings_configuration.h"

#include <yql/essentials/providers/common/config/yql_config_qplayer.h>
#include <yql/essentials/providers/common/activation/yql_activation.h>

#include <util/generic/singleton.h>

#include <functional>

namespace NYql {

namespace {

const auto& GetDeserializationFilter() {
    struct TInstance {
        const std::function<bool(const NProto::TRuntimeSetting&)> Filter =
            NConfig::MakeActivationFilter<NProto::TRuntimeSetting>("", nullptr, [](const TString&) {
                YQL_ENSURE(false, "Activation is not allowed. "
                                  "Seems you are trying to load runtime settings with activation but all settings must be already activated.");
            });
    };
    return Singleton<TInstance>()->Filter;
}

void FillUdfSettings(
    TRuntimeSettings& config,
    const google::protobuf::RepeatedPtrField<NProto::TUdfSettings>& udfSettings,
    const std::function<bool(const NProto::TRuntimeSetting&)>& filter,
    const TQContext& qContext)
{
    for (const auto& udf : udfSettings) {
        NCommon::ForEachActivatedFlag<NProto::TRuntimeSetting>(
            udf.GetRuntimeSettings(), filter, qContext,
            [&]() -> TString { return TStringBuilder() << "runtime_settings_udf_" << udf.GetModule(); },
            /*hasProviderName=*/true,
            [&](const auto& setting) {
                config.SetUdfSetting(udf.GetModule(), setting.GetName(), setting.GetValue());
            });
    }
}

TRuntimeSettings::TPtr CreateRuntimeSettingsFromProtoImpl(
    const NProto::TRuntimeSettings& proto,
    const std::function<bool(const NProto::TRuntimeSetting&)>& filter,
    const TQContext& qContext)
{
    auto config = MakeRuntimeSettingsMutable();
    TRuntimeSettingsConfiguration::Get().Dispatch(*config, proto.GetHostSettings(), filter, qContext);
    FillUdfSettings(*config, proto.GetUdfSettings(), filter, qContext);
    return config;
}

TRuntimeSettings::TPtr CreateRuntimeSettingsFromStringImpl(const TString& data)
{
    NProto::TRuntimeSettings proto;
    proto.ParseFromStringOrThrow(data);
    return DeserializeRuntimeSettingsFromProto(proto);
}

} // namespace

TRuntimeSettings::TPtr CreateRuntimeSettingsFromProto(
    const NProto::TRuntimeSettings& proto,
    const TString& userName,
    TCredentials::TPtr credentials,
    const TQContext& qContext,
    std::function<void(const TString&)> onPartialFeatureActivation)
{
    auto filter = NConfig::MakeActivationFilter<NProto::TRuntimeSetting>(userName, credentials, [&](const TString& name) {
        if (onPartialFeatureActivation) {
            onPartialFeatureActivation(name);
        }
    });
    return CreateRuntimeSettingsFromProtoImpl(proto, filter, qContext);
}

TRuntimeSettings::TPtr DeserializeRuntimeSettingsFromProto(
    const NProto::TRuntimeSettings& proto)
{
    return CreateRuntimeSettingsFromProtoImpl(proto, GetDeserializationFilter(), TQContext());
}

NProto::TRuntimeSettings SerializeRuntimeSettingsToProto(
    const TRuntimeSettings& config)
{
    NProto::TRuntimeSettings proto;
    TRuntimeSettingsConfiguration::Get().SerializeStaticSettings(config, [&proto](const TString& name, const TString& value) {
        auto* setting = proto.AddHostSettings();
        setting->SetName(name);
        setting->SetValue(value);
    });

    proto.MutableUdfSettings()->Reserve(config.GetUdfSettings().size());
    for (const auto& [moduleName, settingsMap] : config.GetUdfSettings()) {
        auto* udfSettings = proto.AddUdfSettings();
        udfSettings->SetModule(moduleName);
        udfSettings->MutableRuntimeSettings()->Reserve(settingsMap.size());
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
