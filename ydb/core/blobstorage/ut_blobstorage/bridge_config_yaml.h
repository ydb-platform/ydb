#pragma once

#include "bridge_common.h"

#include <contrib/libs/yaml-cpp/include/yaml-cpp/yaml.h>
#include <library/cpp/yaml/as/tstring.h>

// helpers for bridge tests changing YAML config of the cluster
namespace NBridgeTest {

    // main config YAML of self-managed cluster
    inline TString FetchMainConfigYaml(TEnvironmentSetup& env) {
        const auto res = InvokeDistconf(env, [](auto& record) {
            record.MutableFetchStorageConfig()->SetMainConfig(true);
        });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, res.GetErrorReason());
        UNIT_ASSERT(res.GetFetchStorageConfig().HasYAML());
        return res.GetFetchStorageConfig().GetYAML();
    }

    // replace main config of self-managed cluster with the current one modified by the callback (it gets the config
    // section of the YAML), the version is advanced automatically
    inline NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult ReplaceMainConfig(TEnvironmentSetup& env,
            const std::function<void(YAML::Node&)>& modify) {
        YAML::Node yaml = YAML::Load(FetchMainConfigYaml(env));
        yaml["metadata"]["version"] = yaml["metadata"]["version"].as<ui64>() + 1;
        yaml["metadata"]["cluster"] = ""; // test environment has no cluster name
        YAML::Node config = yaml["config"];
        modify(config);
        const TString text(YAML::Dump(yaml));
        return InvokeDistconf(env, [&](auto& record) {
            auto *cmd = record.MutableReplaceStorageConfig();
            cmd->SetYAML(text);
            cmd->SetSkipConsoleValidation(true);
        });
    }

} // NBridgeTest
