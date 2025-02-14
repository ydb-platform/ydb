#include "ydb_storage_config.h"

#include <ydb-cpp-sdk/client/bsconfig/storage_config.h>
#include <ydb/library/yaml_config/public/yaml_config.h>

#include <openssl/sha.h>

#include <util/folder/path.h>
#include <util/string/hex.h>

using namespace NKikimr;

namespace NYdb::NConsoleClient::NStorageConfig {

TString WrapYaml(const TString& yaml) {
    auto doc = NFyaml::TDocument::Parse(yaml);

    TStringStream out;
    out << (doc.HasExplicitDocumentStart() ? "" : "---\n")
        << doc << (yaml[yaml.size() - 1] != '\n' ? "\n" : "");

    return out.Str();
}

TCommandStorageConfig::TCommandStorageConfig(std::optional<bool> overrideOnlyExplicitProfile)
    : TClientCommandTree("storage", {}, "Storage config")
    , OverrideOnlyExplicitProfile(overrideOnlyExplicitProfile)
{
    AddCommand(std::make_unique<TCommandStorageConfigFetch>());
    AddCommand(std::make_unique<TCommandStorageConfigReplace>());
}

void TCommandStorageConfig::PropagateFlags(const TCommandFlags& flags) {
    TClientCommand::PropagateFlags(flags);

    if (OverrideOnlyExplicitProfile) {
        OnlyExplicitProfile = *OverrideOnlyExplicitProfile;
    }

    for (auto& [_, cmd] : SubCommands) {
        cmd->PropagateFlags(TCommandFlags{.Dangerous = Dangerous, .OnlyExplicitProfile = OnlyExplicitProfile});
    }
}

TCommandStorageConfigFetch::TCommandStorageConfigFetch()
    : TYdbCommand("fetch", {}, "Fetch storage config")
{
}

void TCommandStorageConfigFetch::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("dedicated-storage-section", "Fetch dedicated storage section")
        .StoreTrue(&DedicatedStorageSection);
    config.Opts->AddLongOption("dedicated-cluster-section", "Fetch dedicated cluster section")
        .StoreTrue(&DedicatedClusterSection);
    config.SetFreeArgsNum(0);
}

void TCommandStorageConfigFetch::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandStorageConfigFetch::Run(TConfig& config) {
    auto driver = std::make_unique<NYdb::TDriver>(CreateDriver(config));
    auto client = NYdb::NStorageConfig::TStorageConfigClient(*driver);
    auto result = client.FetchStorageConfig(DedicatedStorageSection, DedicatedClusterSection).GetValueSync();
    NStatusHelpers::ThrowOnError(result);

    const auto& clusterConfig = result.GetConfig();
    const auto& storageConfig = result.GetStorageConfig();

    if (!clusterConfig.empty()) {
        if (!storageConfig.empty() || DedicatedStorageSection) {
            Cout << "cluster config: " << Endl;
        }
        Cout << WrapYaml(TString(clusterConfig));
    }

    if (!storageConfig.empty()) {
        if (!clusterConfig.empty() || DedicatedClusterSection) {
            Cout << "storage config:" << Endl;
        }
        Cout << WrapYaml(TString(storageConfig));
    }

    if (clusterConfig.empty() && storageConfig.empty()) {
        Cerr << "No config returned." << Endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

TCommandStorageConfigReplace::TCommandStorageConfigReplace()
    : TYdbCommand("replace", {}, "Replace storage config")
{
}

void TCommandStorageConfigReplace::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption('f', "filename", "Filename of the file containing whole configuration in single-config mode")
        .RequiredArgument("[config.yaml]").StoreResult(&Filename);
    config.Opts->AddLongOption("dedicated-cluster-yaml", "Path to dedicated cluster section of configuration")
        .RequiredArgument("[cluster.yaml]").StoreResult(&ClusterYamlPath);
    config.Opts->AddLongOption("dedicated-storage-yaml", "Path to dedicated storage section of configuration")
        .RequiredArgument("[storage.yaml]").StoreResult(&StorageYamlPath);
    config.Opts->AddLongOption("enable-dedicated-storage-section", "Turn on dedicated storage section in stored configuration")
        .StoreTrue(&EnableDedicatedStorageSection);
    config.Opts->AddLongOption("disable-dedicated-storage-section", "Turn off dedicated storage section in stored configuration")
        .StoreTrue(&DisableDedicatedStorageSection);
    config.SetFreeArgsNum(0);
}

void TCommandStorageConfigReplace::Parse(TConfig& config) {
    TClientCommand::Parse(config);

    if (EnableDedicatedStorageSection && DisableDedicatedStorageSection) {
        ythrow yexception() << "Can't provide both --enable-dedicated-storage-section and --disable-dedicated-storage-section";
    } else if (Filename && (ClusterYamlPath || StorageYamlPath)) {
        ythrow yexception() << "Can't provide -f (--filename) with either --cluster-yaml or --storage-yaml";
    }

    if (ClusterYamlPath) {
        ClusterYaml.emplace(ClusterYamlPath == "-" ? Cin.ReadAll() : TFileInput(ClusterYamlPath).ReadAll());
        DedicatedConfigMode = true;
    }
    if (StorageYamlPath) {
        StorageYaml.emplace(StorageYamlPath == "-" ? Cin.ReadAll() : TFileInput(StorageYamlPath).ReadAll());
        DedicatedConfigMode = true;
    }
    if (Filename && !DedicatedConfigMode) {
        ClusterYaml.emplace(Filename == "-" ? Cin.ReadAll() : TFileInput(Filename).ReadAll());
    }

    if (EnableDedicatedStorageSection) {
        SwitchDedicatedStorageSection.emplace(true);
    } else if (DisableDedicatedStorageSection) {
        SwitchDedicatedStorageSection.emplace(false);
    }
}

int TCommandStorageConfigReplace::Run(TConfig& config) {
    std::unique_ptr<NYdb::TDriver> driver = std::make_unique<NYdb::TDriver>(CreateDriver(config));
    auto client = NYdb::NStorageConfig::TStorageConfigClient(*driver);
    auto status = client.ReplaceStorageConfig(ClusterYaml, StorageYaml, SwitchDedicatedStorageSection, DedicatedConfigMode).GetValueSync();
    NStatusHelpers::ThrowOnError(status);

    if (!status.GetIssues()) {
        Cout << status << Endl;
    }

    return EXIT_SUCCESS;
}

}
