#pragma once

#include <ydb/library/yaml_config/public/yaml_config.h>

namespace NKikimr::NYamlConfig {

    struct TMigrationConfigMergeResult {
        TString Config;
        bool HasConflicts = false;
    };

    enum class EStaticGroupLayoutCheckResult {
        NotApplicable,
        Incorrect,
        Mirror3dc,
        Mirror3dc3Nodes,
        Block42,
    };

    /** Merges V1 configs conservatively and emits Git-style markers for ambiguous sections and representations. */
    TMigrationConfigMergeResult MergeConfigsForMigration(const TString& staticConfig, const TString& dynamicConfig,
                                                         TStringBuf staticConfigName = "static config",
                                                         TStringBuf dynamicConfigName = "dynamic config");

    /** Sets the feature flag used for the first migration switch. */
    NFyaml::TDocument SetConfigV2FeatureFlag(const TString& config, bool enabled);

    /** Sets distributed self-management after the first migration switch. */
    NFyaml::TDocument SetSelfManagement(const TString& config, bool enabled);

    /** Returns the current distributed self-management state. */
    bool IsSelfManagementEnabled(const TString& config);

    /** Sets the simple-format failure-domain type to disk. */
    void SetDiskFailDomainType(NFyaml::TDocument& config);

    /** Returns whether the main config uses disk failure domains. */
    bool HasDiskFailDomainType(NFyaml::TDocument& config);

    /** Classifies a static-group layout supported by automatic self-management migration. */
    EStaticGroupLayoutCheckResult CheckStaticGroupLayout(NFyaml::TDocument& config);

    /** Removes legacy static-group topology and State Storage definitions after their management has switched to V2. */
    NFyaml::TDocument CleanupConfigV2Migration(const TString& config);

} // namespace NKikimr::NYamlConfig
