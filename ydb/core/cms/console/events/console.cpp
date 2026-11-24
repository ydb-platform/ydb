#include "console.h"

#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NConsole {

const NKikimrConfig::TAppConfig& TEvConsole::TEvConfigNotificationRequest::GetConfig() const {
    return Record.GetConfig();
}

TEvConsole::TEvConfigSubscriptionNotification::TEvConfigSubscriptionNotification(
    ui64 generation,
    NKikimrConfig::TAppConfig &&config,
    const THashSet<ui32> &affectedKinds,
    const TString &yamlConfig,
    const TMap<ui64, TString> &volatileYamlConfigs)
{
    Record.SetGeneration(generation);
    Record.MutableConfig()->Swap(&config);
    for (ui32 kind : affectedKinds)
        Record.AddAffectedKinds(kind);

    if (!yamlConfig.empty()) {
        Record.SetMainYamlConfig(yamlConfig);
        for (auto &[id, yaml] : volatileYamlConfigs) {
            auto *volatileConfig = Record.AddVolatileConfigs();
            volatileConfig->SetId(id);
            volatileConfig->SetConfig(yaml);
        }
    }
}

TEvConsole::TEvConfigSubscriptionNotification::TEvConfigSubscriptionNotification(
    ui64 generation,
    const NKikimrConfig::TAppConfig &config,
    const THashSet<ui32> &affectedKinds,
    const TString &yamlConfig,
    const TMap<ui64, TString> &volatileYamlConfigs)
    : TEvConfigSubscriptionNotification(generation, config, affectedKinds, yamlConfig, volatileYamlConfigs, NKikimrConfig::TAppConfig{})
{
}

TEvConsole::TEvConfigSubscriptionNotification::TEvConfigSubscriptionNotification(
    ui64 generation,
    const NKikimrConfig::TAppConfig &config,
    const THashSet<ui32> &affectedKinds,
    const TString &yamlConfig,
    const TMap<ui64, TString> &volatileYamlConfigs,
    const NKikimrConfig::TAppConfig &rawConfig)
    : TEvConfigSubscriptionNotification(generation, config, affectedKinds, yamlConfig, volatileYamlConfigs, rawConfig, Nothing())
{
}

TEvConsole::TEvConfigSubscriptionNotification::TEvConfigSubscriptionNotification(
    ui64 generation,
    const NKikimrConfig::TAppConfig &config,
    const THashSet<ui32> &affectedKinds,
    const TString &yamlConfig,
    const TMap<ui64, TString> &volatileYamlConfigs,
    const NKikimrConfig::TAppConfig &rawConfig,
    const TMaybe<TString> databaseYamlConfig)
{
    Record.SetGeneration(generation);
    Record.MutableConfig()->CopyFrom(config);
    Record.MutableRawConsoleConfig()->CopyFrom(rawConfig);
    for (ui32 kind : affectedKinds)
        Record.AddAffectedKinds(kind);

    if (!yamlConfig.empty()) {
        Record.SetMainYamlConfig(yamlConfig);
        for (auto &[id, yaml] : volatileYamlConfigs) {
            auto *volatileConfig = Record.AddVolatileConfigs();
            volatileConfig->SetId(id);
            volatileConfig->SetConfig(yaml);
        }
    }
    if (databaseYamlConfig) {
        Record.SetDatabaseYamlConfig(*databaseYamlConfig);
    }
}

} // namespace NKikimr::NConsole
