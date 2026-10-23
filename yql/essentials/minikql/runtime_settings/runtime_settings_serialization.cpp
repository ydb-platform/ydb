#include "runtime_settings_serialization.h"
#include "runtime_settings_configuration.h"

#include <yql/essentials/providers/common/config/yql_config_qplayer.h>
#include <yql/essentials/providers/common/activation/yql_activation.h>

#include <util/system/mutex.h>

#include <array>
#include <list>
#include <unordered_map>

namespace NYql {

namespace {

void FillUdfSettings(
    TRuntimeSettings& config,
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
    auto config = MakeRuntimeSettingsMutable();
    auto filter = NConfig::MakeActivationFilter<NProto::TRuntimeSetting>(userName, credentials, [&](const TString& name) {
        YQL_ENSURE(allowActivation, "Activation is not allowed. "
                                    "Seems you are trying to load runtime settings with activation but all settings must be already activated.");
        if (onPartialFeatureActivation) {
            onPartialFeatureActivation(name);
        }
    });
    TRuntimeSettingsConfiguration::Get().Dispatch(*config, proto.GetHostSettings(), filter, qContext);
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

std::shared_ptr<const TResolvedRuntimeSettings> ResolveRuntimeSettings(
    const NProto::TRuntimeSettings& proto)
{
    // Key by the complete wire representation, including activation/unknown fields.
    // Equivalent encodings can miss this cache but retain the same pattern hash.
    // Changing settings never reuses metadata from the previous plan. The program
    // and language version remain part of the computation-pattern cache key.
    struct TShard {
        TMutex Mutex;
        std::list<TString> Lru;
        struct TEntry {
            std::shared_ptr<const TResolvedRuntimeSettings> Value;
            std::list<TString>::iterator Position;
        };
        std::unordered_map<TString, TEntry> Entries;
        size_t Bytes = 0;
    };
    static std::array<TShard, 16> cache;
    constexpr size_t MaxEntriesPerShard = 64;
    constexpr size_t MaxBytesPerShard = 64 * 1024;
    constexpr size_t MaxEntryBytes = 16 * 1024;

    const auto build = [&] {
        const auto mutableSettings = DeserializeRuntimeSettingsFromProto(proto);
        // Copy the plain settings, stripping the mutable configuration dispatcher.
        auto settings = MakeRuntimeSettings(*mutableSettings);
        auto hash = StableHashRuntimeSettings(*settings);
        return std::make_shared<const TResolvedRuntimeSettings>(
            TResolvedRuntimeSettings{std::move(settings), std::move(hash)});
    };
    // Never cache activation decisions. Even malformed/unresolved task settings
    // must retain the deserializer's request-local activation/error behavior.
    bool hasActivation = false;
    for (const auto& setting : proto.GetHostSettings()) {
        hasActivation |= setting.HasActivation();
    }
    for (const auto& udf : proto.GetUdfSettings()) {
        for (const auto& setting : udf.GetRuntimeSettings()) {
            hasActivation |= setting.HasActivation();
        }
    }
    if (hasActivation || proto.ByteSizeLong() > MaxEntryBytes) {
        return build();
    }
    TString key;
    Y_ENSURE(proto.SerializeToString(&key));
    auto& shard = cache[THash<TString>()(key) % cache.size()];
    {
        const TGuard<TMutex> guard(shard.Mutex);
        if (const auto it = shard.Entries.find(key); it != shard.Entries.end()) {
            shard.Lru.splice(shard.Lru.begin(), shard.Lru, it->second.Position);
            return it->second.Value;
        }
    }
    // Configuration dispatch (which can throw) happens outside the cache lock.
    // Activation is still rejected on this path; failures are never cached.
    auto value = build();
    const TGuard<TMutex> guard(shard.Mutex);
    if (const auto it = shard.Entries.find(key); it != shard.Entries.end()) {
        shard.Lru.splice(shard.Lru.begin(), shard.Lru, it->second.Position);
        return it->second.Value;
    }
    shard.Bytes += key.size();
    shard.Lru.push_front(key);
    shard.Entries.emplace(std::move(key), TShard::TEntry{value, shard.Lru.begin()});
    while (shard.Entries.size() > MaxEntriesPerShard || shard.Bytes > MaxBytesPerShard) {
        shard.Bytes -= shard.Lru.back().size();
        shard.Entries.erase(shard.Lru.back());
        shard.Lru.pop_back();
    }
    return value;
}

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
    TRuntimeSettingsConfiguration::Get().SerializeStaticSettings(config, [&proto](const TString& name, const TString& value) {
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
