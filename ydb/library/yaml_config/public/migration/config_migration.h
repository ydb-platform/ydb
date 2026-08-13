#pragma once

#include <ydb/library/yaml_config/public/yaml_config.h>

namespace NKikimr::NYamlConfig {

    struct TMigrationConfigMergeResult {
        TString Config;
        bool HasConflicts = false;
    };

    /** Merges V1 configs conservatively and emits Git-style markers for ambiguous sections and representations. */
    TMigrationConfigMergeResult MergeConfigsForMigration(const TString& staticConfig, const TString& dynamicConfig,
                                                         TStringBuf staticConfigName = "static config",
                                                         TStringBuf dynamicConfigName = "dynamic config");

    /** Sets the feature flag used for the first migration switch. */
    NFyaml::TDocument SetConfigV2FeatureFlag(const TString& config, bool enabled);

    /** Sets distributed self-management after the first migration switch. */
    NFyaml::TDocument SetSelfManagement(const TString& config, bool enabled);

    /** Removes legacy static-group topology and State Storage definitions after their management has switched to V2. */
    NFyaml::TDocument CleanupConfigV2Migration(const TString& config);

} // namespace NKikimr::NYamlConfig
