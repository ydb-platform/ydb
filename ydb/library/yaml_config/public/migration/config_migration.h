#pragma once

#include <ydb/library/yaml_config/public/yaml_config.h>

namespace NKikimr::NYamlConfig {

    struct TMigrationConfigMergeResult {
        NFyaml::TDocument Config;
        TVector<TString> StaticConfigPrecedencePaths;
    };

    /** Merges static V1 storage sections with a dynamic MainConfig. */
    TMigrationConfigMergeResult MergeConfigsForMigration(const TString& staticConfig, const TString& dynamicConfig);

    /** Promotes V2 values and removes only provably redundant legacy sections. */
    NFyaml::TDocument CleanupConfigV2Migration(const TString& config);

} // namespace NKikimr::NYamlConfig
