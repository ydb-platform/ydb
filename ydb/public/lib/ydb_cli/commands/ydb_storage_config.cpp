#include "ydb_storage_config.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/config/config.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_distributed_storage.h>
#include <ydb/library/yaml_config/public/yaml_config.h>

#include <openssl/sha.h>

#include <util/folder/path.h>
#include <util/string/hex.h>

using namespace NKikimr;

namespace NYdb::NConsoleClient::NStorageConfig {

namespace {

class TSetPoolRedundancyCommand : public TYdbCommand {
public:
    TSetPoolRedundancyCommand()
        : TYdbCommand("set-redundancy", {"set-redandancy"}, "Set the target redundancy for a mirror-3-dc storage pool")
    {}

    void Config(TConfig& config) override {
        TYdbCommand::Config(config);
        config.AllowEmptyDatabase = true;
        config.SetFreeArgsNum(0);
        config.Opts->AddLongOption("storage-pool-name", "Name of the storage pool")
            .Required().RequiredArgument("NAME").StoreResult(&Name);
        config.Opts->AddLongOption("storage-pool-generation", "Expected sum of group generations")
            .RequiredArgument("N").Handler([this](const TString& value) { Generation = FromString<ui32>(value); });
        config.Opts->AddLongOption("allow-any-storage-pool-generation", "Apply regardless of the pool generation").StoreTrue(&AllowAny);
        config.Opts->MutuallyExclusive("storage-pool-generation", "allow-any-storage-pool-generation");
        config.Opts->AddLongOption("asynchronous-redundancy", "Target layout after repair: Redundancy1plus1 (INV1) or Redundancy2plus2 (INV2)")
            .RequiredArgument("LAYOUT").DefaultValue("Redundancy1plus1").StoreResult(&Asynchronous);
        config.Opts->AddLongOption("synchronous-redundancy", "Write layout (currently fixed at Redundancy2plus2)")
            .RequiredArgument("LAYOUT").DefaultValue("Redundancy2plus2").StoreResult(&Synchronous);
    }

    void Parse(TConfig& config) override {
        TYdbCommand::Parse(config);
        if (Name.empty()) {
            throw TMisuseException() << "--storage-pool-name must not be empty";
        }
        if (!AllowAny && !Generation) {
            throw TMisuseException() << "Specify --storage-pool-generation or --allow-any-storage-pool-generation";
        }
        if (Synchronous != "Redundancy2plus2") {
            throw TMisuseException() << "--synchronous-redundancy supports only Redundancy2plus2";
        }
        if (Asynchronous != "Redundancy1plus1" && Asynchronous != "Redundancy2plus2") {
            throw TMisuseException() << "--asynchronous-redundancy must be Redundancy1plus1 or Redundancy2plus2";
        }
    }

    int Run(TConfig& config) override {
        using ETarget = NDistributedStorage::EPoolRedundancy;
        const auto target = Asynchronous == "Redundancy2plus2" ? ETarget::TwoPlusTwo : ETarget::OnePlusOne;
        auto driver = CreateDriver(config);
        NDistributedStorage::TDistributedStorageClient client(driver);
        NStatusHelpers::ThrowOnError(client.SetPoolRedundancy(Name, target, Generation).GetValueSync());
        return EXIT_SUCCESS;
    }

private:
    TString Name;
    TString Asynchronous;
    TString Synchronous;
    std::optional<ui32> Generation;
    bool AllowAny = false;
};

enum class EDataCenterCommand { TakeDown, Rejoin };

class TDataCenterCommand : public TYdbCommand {
public:
    explicit TDataCenterCommand(EDataCenterCommand command)
        : TYdbCommand(command == EDataCenterCommand::TakeDown ? "takedown" : "rejoin", {},
            command == EDataCenterCommand::TakeDown ? "Exclude a data center from storage" : "Return a data center to storage")
        , Command(command)
    {}

    void Config(TConfig& config) override {
        TYdbCommand::Config(config);
        config.AllowEmptyDatabase = true;
        config.SetFreeArgsNum(0);
        config.Opts->AddLongOption("datacenter-name", "Name of the data center")
            .Required().RequiredArgument("NAME").StoreResult(&DataCenterName);
        if (Command == EDataCenterCommand::TakeDown) {
            config.Opts->AddLongOption("dry-run", "Validate the data center policy change without applying it")
                .StoreTrue(&DryRun);
        }
    }

    void Parse(TConfig& config) override {
        TYdbCommand::Parse(config);
        if (DataCenterName.empty()) {
            throw TMisuseException() << "--datacenter-name must not be empty";
        }
    }

    int Run(TConfig& config) override {
        auto driver = CreateDriver(config);
        NDistributedStorage::TDistributedStorageClient client(driver);
        auto result = Command == EDataCenterCommand::TakeDown
            ? client.TakeDownDataCenter(DataCenterName, DryRun)
            : client.RejoinDataCenter(DataCenterName);
        NStatusHelpers::ThrowOnError(result.GetValueSync());
        return EXIT_SUCCESS;
    }

private:
    EDataCenterCommand Command;
    TString DataCenterName;
    bool DryRun = false;
};

} // namespace

TString WrapYaml(const TString& yaml) {
    auto doc = NFyaml::TDocument::Parse(yaml);

    TStringStream out;
    out << (doc.HasExplicitDocumentStart() ? "" : "---\n")
        << doc << (yaml[yaml.size() - 1] != '\n' ? "\n" : "");

    return out.Str();
}

TCommandStorageConfig::TCommandStorageConfig(std::optional<bool> overrideOnlyExplicitProfile)
    : TClientCommandTree("storage", {}, "Distributed storage administration")
    , OverrideOnlyExplicitProfile(overrideOnlyExplicitProfile)
{
    AddCommand(std::make_unique<TCommandStorageConfigFetch>());
    AddCommand(std::make_unique<TCommandStorageConfigReplace>());
    auto pool = std::make_unique<TClientCommandTree>("pool", std::initializer_list<TString>{}, "Manage storage pools");
    pool->AddCommand(std::make_unique<TSetPoolRedundancyCommand>());
    AddDangerousCommand(std::move(pool));
    auto dataCenter = std::make_unique<TClientCommandTree>("datacenter", std::initializer_list<TString>{}, "Manage data center availability");
    dataCenter->AddCommand(std::make_unique<TDataCenterCommand>(EDataCenterCommand::TakeDown));
    dataCenter->AddCommand(std::make_unique<TDataCenterCommand>(EDataCenterCommand::Rejoin));
    AddDangerousCommand(std::move(dataCenter));
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
    auto driver = CreateDriver(config);
    auto client = NYdb::NConfig::TConfigClient(driver);

    NYdb::NConfig::TFetchAllConfigsSettings settings;

    auto result = client.FetchAllConfigs(settings).GetValueSync();
    NStatusHelpers::ThrowOnError(result);

    TString clusterConfig;
    TString storageConfig;

    for (const auto& entry : result.GetConfigs()) {
        std::visit([&](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, NYdb::NConfig::TMainConfigIdentity>) {
                if (DedicatedClusterSection || !DedicatedStorageSection) {
                    clusterConfig = entry.Config;
                }
            } else if constexpr (std::is_same_v<T, NYdb::NConfig::TStorageConfigIdentity>) {
                if (DedicatedStorageSection || !DedicatedClusterSection) {
                    storageConfig = entry.Config;
                }
            }
        }, entry.Identity);
    }

    if (!clusterConfig.empty()) {
        if (!storageConfig.empty() || DedicatedStorageSection) {
            Cerr << "cluster config: " << Endl;
        }
        Cout << WrapYaml(TString(clusterConfig));
    }

    if (!storageConfig.empty()) {
        if (!clusterConfig.empty() || DedicatedClusterSection) {
            Cerr << "storage config:" << Endl;
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
    config.Opts->AddLongOption('f', "filename", "Path to the file containing whole configuration in single-config mode")
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
    auto driver = CreateDriver(config);
    auto client = NYdb::NConfig::TConfigClient(driver);

    auto status = [&]() {
        if (SwitchDedicatedStorageSection && !*SwitchDedicatedStorageSection) {
            return client.ReplaceConfigDisableDedicatedStorageSection(ClusterYaml.value()).GetValueSync();
        } else if (SwitchDedicatedStorageSection && *SwitchDedicatedStorageSection) {
            return client.ReplaceConfigEnableDedicatedStorageSection(ClusterYaml.value(), StorageYaml.value()).GetValueSync();
        } else if (DedicatedConfigMode) {
            return client.ReplaceConfig(ClusterYaml.value(), StorageYaml.value()).GetValueSync();
        } else {
            return client.ReplaceConfig(ClusterYaml.value()).GetValueSync();
        }
    }();

    NStatusHelpers::ThrowOnError(status);

    if (!status.GetIssues()) {
        Cout << status << Endl;
    }

    return EXIT_SUCCESS;
}

}
