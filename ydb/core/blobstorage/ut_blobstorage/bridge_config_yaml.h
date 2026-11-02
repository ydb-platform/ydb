#pragma once

#include "bridge_common.h"

#include <contrib/libs/yaml-cpp/include/yaml-cpp/yaml.h>
#include <library/cpp/yaml/as/tstring.h>

// helpers for bridge tests changing YAML config of the cluster
namespace NBridgeTest {

    // main config YAML of self-managed cluster
    inline TString FetchMainConfigYaml(TEnvironmentSetup& env, ui32 nodeId = 1) {
        const auto res = InvokeDistconf(env, [](auto& record) {
            record.MutableFetchStorageConfig()->SetMainConfig(true);
        }, nodeId);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, res.GetErrorReason());
        UNIT_ASSERT(res.GetFetchStorageConfig().HasYAML());
        return res.GetFetchStorageConfig().GetYAML();
    }

    // replace main config of self-managed cluster with the current one modified by the callback (it gets the config
    // section of the YAML), the version is advanced automatically
    inline NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult ReplaceMainConfig(TEnvironmentSetup& env,
            const std::function<void(YAML::Node&)>& modify, ui32 nodeId = 1) {
        YAML::Node yaml = YAML::Load(FetchMainConfigYaml(env, nodeId));
        yaml["metadata"]["version"] = yaml["metadata"]["version"].as<ui64>() + 1;
        yaml["metadata"]["cluster"] = ""; // test environment has no cluster name
        YAML::Node config = yaml["config"];
        modify(config);
        const TString text(YAML::Dump(yaml));
        return InvokeDistconf(env, [&](auto& record) {
            auto *cmd = record.MutableReplaceStorageConfig();
            cmd->SetYAML(text);
            cmd->SetSkipConsoleValidation(true);
        }, nodeId);
    }

    // two-pile cluster which may get the third pile of nodes 17-24 added to its config
    inline TEnvironmentSetup::TSettings MakeExpandableSettings() {
        return {
            .NodeCount = 24,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
            .InitialConfiguredNodeCount = 16,
        };
    }

    constexpr ui32 FirstThirdPileNodeId = 17;
    constexpr ui32 LastThirdPileNodeId = 24;

    // add hosts of nodes firstNodeId..lastNodeId (located as GetBridgedLocation does, but in the designated pile) to the
    // config section of main YAML; hosts get the designated host config, if any
    inline void AddPileHosts(YAML::Node& config, const TString& pileName, ui32 firstNodeId, ui32 lastNodeId,
            std::optional<ui32> hostConfigId = std::nullopt) {
        for (ui32 nodeId = firstNodeId; nodeId <= lastNodeId; ++nodeId) {
            YAML::Node host;
            host["host"] = "host_" + std::to_string(nodeId);
            if (hostConfigId) {
                host["host_config_id"] = *hostConfigId;
            }
            YAML::Node location = host["location"];
            location["data_center"] = "my_dc";
            location["rack"] = "rack_" + std::to_string(nodeId);
            location["unit"] = "unit_" + std::to_string(nodeId);
            location["bridge_pile_name"] = pileName;
            config["hosts"].push_back(host);
        }
    }

    // add the pile with its hosts (see AddPileHosts) to the main config of the cluster
    inline NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult AddPileToConfig(TEnvironmentSetup& env, TString name,
            ui32 firstNodeId, ui32 lastNodeId, std::optional<ui32> hostConfigId = std::nullopt) {
        return ReplaceMainConfig(env, [&](YAML::Node& config) {
            YAML::Node pile;
            pile["name"] = name;
            config["bridge_config"]["piles"].push_back(pile);
            AddPileHosts(config, name, firstNodeId, lastNodeId, hostConfigId);
        });
    }

    // drop the pile from the bridge config of the cluster along with its hosts
    inline NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult DropPileFromConfig(TEnvironmentSetup& env,
            const TString& name) {
        return ReplaceMainConfig(env, [&](YAML::Node& config) {
            auto filter = [](const YAML::Node& items, auto&& keep) {
                YAML::Node res(YAML::NodeType::Sequence);
                for (const YAML::Node& item : items) {
                    if (keep(item)) {
                        res.push_back(item);
                    }
                }
                return res;
            };
            config["bridge_config"]["piles"] = filter(config["bridge_config"]["piles"], [&](const YAML::Node& pile) {
                return pile["name"].as<TString>() != name;
            });
            config["hosts"] = filter(config["hosts"], [&](const YAML::Node& host) {
                // hosts of the implicit pile may have neither pile name nor location
                const YAML::Node location = host["location"];
                return !location || !location["bridge_pile_name"] || location["bridge_pile_name"].as<TString>() != name;
            });
        });
    }

    // add the third pile (named as GetBridgedLocation does) with its nodes to the main config of the cluster
    inline NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult AddThirdPileToConfig(TEnvironmentSetup& env) {
        return AddPileToConfig(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId);
    }

    // pile names and states as recorded in the storage config
    inline TString GetPileStates(const NKikimrBlobStorage::TStorageConfig& config) {
        TStringBuilder res;
        const auto& registry = config.GetClusterStateDetails().GetPileRegistry();
        for (size_t i = 0; i < registry.PilesSize(); ++i) {
            res << (res.empty() ? "" : " ") << (registry.GetPiles(i).HasName() ? registry.GetPiles(i).GetName() : "-")
                << ':' << NKikimrBridge::TClusterState::EPileState_Name(config.GetClusterState().GetPerPileState(i));
        }
        return res;
    }

} // NBridgeTest
