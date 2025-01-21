#include "ydb_storage_config.h"

#include <ydb/public/sdk/cpp/client/ydb_bsconfig/ydb_storage_config.h>
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
    config.SetFreeArgsNum(0);
}

void TCommandStorageConfigFetch::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandStorageConfigFetch::Run(TConfig& config) {
    auto driver = std::make_unique<NYdb::TDriver>(CreateDriver(config));
    auto client = NYdb::NStorageConfig::TStorageConfigClient(*driver);
    auto result = client.FetchStorageConfig().GetValueSync();
    ThrowOnError(result);
    auto cfg = result.GetConfig();

    if (!cfg) {
        Cerr << "YAML config is absent on this cluster." << Endl;
        return EXIT_FAILURE;
    }

    Cout << WrapYaml(cfg);

    return EXIT_SUCCESS;
}

TCommandStorageConfigReplace::TCommandStorageConfigReplace()
    : TYdbCommand("replace", {}, "Replace storage config")
{
}

void TCommandStorageConfigReplace::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption('f', "filename", "Filename of the file containing configuration")
        .Required().RequiredArgument("[config.yaml]").StoreResult(&Filename);
    config.SetFreeArgsNum(0);
}

void TCommandStorageConfigReplace::Parse(TConfig& config) {
    TClientCommand::Parse(config);

    if (Filename == "") {
        ythrow yexception() << "Must specify non-empty -f (--filename)";
    }

   const auto configStr = Filename == "-" ? Cin.ReadAll() : TFileInput(Filename).ReadAll();

   Cout << "Config: " << configStr << Endl;

   StorageConfig = configStr;
}

int TCommandStorageConfigReplace::Run(TConfig& config) {
    std::unique_ptr<NYdb::TDriver> driver = std::make_unique<NYdb::TDriver>(CreateDriver(config));
    auto client = NYdb::NStorageConfig::TStorageConfigClient(*driver);
    auto exec = [&]() {
        return client.ReplaceStorageConfig(StorageConfig).GetValueSync();
    };
    auto status = exec();
    ThrowOnError(status);

    if (!status.GetIssues()) {
        Cout << status << Endl;
    }

    return EXIT_SUCCESS;
}

}
