#pragma once

#include "defs.h"

#include <ydb/core/protos/config.pb.h>

#include <util/generic/string.h>
#include <util/generic/map.h>

namespace NKikimr::NConsole {

/**
 * Configuration source type enumeration.
 * Indicates how the system was initialized.
 */
enum class EConfigSource {
    SeedNodes,
    DynamicConfig,
    Unknown
};

/**
 * State snapshot of ConfigsDispatcher.
 * Used for observability, monitoring, and testing.
 */
struct TConfigsDispatcherState {
    EConfigSource ConfigSource = EConfigSource::Unknown;
    TString ConfigSourceLabel;
    TString ConfigurationVersion;
    bool HasStorageYaml = false;
    size_t StorageYamlSize = 0;
    bool YamlConfigEnabled = false;
    size_t SubscriptionsCount = 0;
    
    bool LastReplayUsedSeedNodesPath = false;
    bool LastReplayUsedDynamicConfigPath = false;
    
    TMap<TString, TString> Labels;
    
    TString ToDebugString() const {
        TStringStream ss;
        ss << "ConfigSource: " << ConfigSourceLabel;
        switch (ConfigSource) {
            case EConfigSource::SeedNodes:
                ss << " (seed nodes - uses ConfigClient)";
                break;
            case EConfigSource::DynamicConfig:
                ss << " (dynamic config - uses DynConfigClient)";
                break;
            case EConfigSource::Unknown:
                ss << " (unknown)";
                break;
        }
        ss << "\nConfigurationVersion: " << ConfigurationVersion;
        ss << "\nHasStorageYaml: " << (HasStorageYaml ? "yes" : "no");
        if (HasStorageYaml) {
            ss << " (" << StorageYamlSize << " bytes)";
        }
        ss << "\nYamlConfigEnabled: " << YamlConfigEnabled;
        ss << "\nSubscriptionsCount: " << SubscriptionsCount;
        return ss.Str();
    }
};

} // namespace NKikimr::NConsole

