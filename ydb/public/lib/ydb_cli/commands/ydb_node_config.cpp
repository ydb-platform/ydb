#include "ydb_node_config.h"
#include <util/system/fs.h>
#include <util/folder/dirut.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/config/config.h>

namespace NYdb::NConsoleClient::NNodeConfig {

constexpr const char* CONFIG_FILE_NAME = "config.yaml";
constexpr const char* STORAGE_CONFIG_FILE_NAME = "storage.yaml";

TCommandNodeConfig::TCommandNodeConfig()
    : TClientCommandTree("config", {}, "Node-wide configuration")
{
    AddCommand(std::make_unique<TCommandNodeConfigInit>());
}

TCommandNodeConfigInit::TCommandNodeConfigInit()
    : TYdbCommand("init", {}, "Initialize node configuration")
{
}

void TCommandNodeConfigInit::PropagateFlags(const TCommandFlags& flags) {
    TYdbCommand::PropagateFlags(flags);
    Dangerous = false;
}

void TCommandNodeConfigInit::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption('f', "from-config", "Path to the initial configuration file.")
        .RequiredArgument("[config.yaml]").StoreResult(&ConfigYamlPath);
    config.Opts->AddLongOption('d', "config-dir", "Path to the directory for storing configuration files.")
        .Required().RequiredArgument("[directory]").StoreResult(&ConfigDirPath);
    config.Opts->AddLongOption('s', "seed-node", "Endpoint of the seed node from which the configuration will be fetched.")
        .RequiredArgument("[HOST:PORT]").StoreResult(&SeedNodeEndpoint);
    config.Opts->MutuallyExclusive("from-config", "seed-node");
    config.SetFreeArgsNum(0);
    config.AllowEmptyDatabase = true;
    config.AllowEmptyAddress = true;
}

bool TCommandNodeConfigInit::SaveConfig(const TString& config, const TString& configName, const TString& configDirPath) {
    try {
        TString tempPath = TStringBuilder() << configDirPath << "/temp_" << configName;
        TString configPath = TStringBuilder() << configDirPath << "/" << configName;

        {
            TFileOutput tempFile(tempPath);
            tempFile << config;
            tempFile.Flush();

            if (Chmod(tempPath.c_str(), S_IRUSR | S_IRGRP | S_IROTH) != 0) {
                return false;
            }
        }

        return NFs::Rename(tempPath, configPath);
    } catch (const std::exception& e) {
        return false;
    }
}

int TCommandNodeConfigInit::Run(TConfig& config) {
    MakePathIfNotExist(ConfigDirPath.c_str());
    TString configYaml;

    if (SeedNodeEndpoint) {
        config.Address = SeedNodeEndpoint;
        auto driver = std::make_unique<NYdb::TDriver>(CreateDriver(config));
        auto client = NYdb::NConfig::TConfigClient(*driver);

        auto result = client.FetchAllConfigs().GetValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
        TString clusterConfig;
        TString storageConfig;
        for (const auto& entry : result.GetConfigs()) {
            std::visit([&](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, NYdb::NConfig::TMainConfigIdentity>) {
                    clusterConfig = entry.Config;
                } else if constexpr (std::is_same_v<T, NYdb::NConfig::TStorageConfigIdentity>) {
                    storageConfig = entry.Config;
                }
            }, entry.Identity);
        }
        bool hasCluster = !clusterConfig.empty();
        bool hasStorage = !storageConfig.empty();
        bool clusterSaved = false;
        bool storageSaved = false;

        if (hasCluster && hasStorage) {
            clusterSaved = SaveConfig(clusterConfig, CONFIG_FILE_NAME, ConfigDirPath);
            storageSaved = SaveConfig(storageConfig, STORAGE_CONFIG_FILE_NAME, ConfigDirPath);

            if (clusterSaved && storageSaved) {
                Cout << "Initialized main and storage configs in " << ConfigDirPath << "/" 
                     << CONFIG_FILE_NAME << " and " << STORAGE_CONFIG_FILE_NAME << Endl;
                return EXIT_SUCCESS;
            }
            Cerr << "Failed to save configs: " 
                 << (clusterSaved ? "" : "main config")
                 << (clusterSaved && storageSaved ? ", " : "") 
                 << (storageSaved ? "" : "storage config") 
                 << Endl;
            return EXIT_FAILURE;
        }

        if (hasCluster) {
            clusterSaved = SaveConfig(clusterConfig, CONFIG_FILE_NAME, ConfigDirPath);
            if (clusterSaved) {
                Cout << "Initialized config in " << ConfigDirPath << "/" << CONFIG_FILE_NAME << Endl;
                return EXIT_SUCCESS;
            }
            Cerr << "Failed to save config to " << ConfigDirPath << Endl;
            return EXIT_FAILURE;
        }

        Cerr << "No main config found" << Endl;
        return EXIT_FAILURE;
    }

    configYaml = TFileInput(ConfigYamlPath).ReadAll();
    if (SaveConfig(configYaml, CONFIG_FILE_NAME, ConfigDirPath)){
        Cout << "Initialized cluster config in " << ConfigDirPath << "/" << CONFIG_FILE_NAME << Endl;
        return EXIT_SUCCESS;
    } else {
        Cout << "Failed to initialize cluster config in " << ConfigDirPath << "/" << CONFIG_FILE_NAME << Endl;
        return EXIT_FAILURE;
    }

}

} // namespace NYdb::NConsoleClient::NNodeConfig
