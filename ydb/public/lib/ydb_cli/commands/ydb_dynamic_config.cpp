#include "ydb_dynamic_config.h"

#include <ydb/public/sdk/cpp/client/draft/ydb_dynamic_config.h>
#include <ydb/library/yaml_config/public/yaml_config.h>

#include <openssl/sha.h>

#include <util/folder/path.h>
#include <util/string/hex.h>

using namespace NKikimr;

namespace NYdb::NConsoleClient::NDynamicConfig {

TString WrapYaml(const TString& yaml) {
    auto doc = NFyaml::TDocument::Parse(yaml);

    TStringStream out;
    out << (doc.HasExplicitDocumentStart() ? "" : "---\n")
        << doc << (yaml[yaml.size() - 1] != '\n' ? "\n" : "");

    return out.Str();
}

TCommandConfig::TCommandConfig()
    : TClientCommandTree("config", {}, "Dynamic config")
{
    AddCommand(std::make_unique<TCommandConfigFetch>());
    AddCommand(std::make_unique<TCommandConfigReplace>());
    AddCommand(std::make_unique<TCommandConfigResolve>());
}

TCommandConfigFetch::TCommandConfigFetch()
    : TYdbCommand("fetch", {"get", "dump"}, "Fetch main dynamic-config")
{
}

void TCommandConfigFetch::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("all", "Fetch both main and volatile config")
        .NoArgument().SetFlag(&All);
    config.Opts->AddLongOption("output-directory", "Directory to save config(s)")
        .RequiredArgument("[directory]").StoreResult(&OutDir);
    config.Opts->AddLongOption("strip-metadata", "Strip metadata from config")
        .NoArgument().SetFlag(&StripMetadata);
    config.SetFreeArgsNum(0);

    config.Opts->MutuallyExclusive("all", "strip-metadata");
    config.Opts->MutuallyExclusive("output-directory", "strip-metadata");
}

void TCommandConfigFetch::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandConfigFetch::Run(TConfig& config) {
    auto driver = std::make_unique<NYdb::TDriver>(CreateDriver(config));
    auto client = NYdb::NDynamicConfig::TDynamicConfigClient(*driver);
    auto result = client.GetConfig().GetValueSync();
    ThrowOnError(result);

    auto cfg = result.GetConfig();

    ui64 version = 0;

    if (cfg) {
        auto metadata = NYamlConfig::GetMetadata(cfg);
        version = metadata.Version.value();

        if (StripMetadata) {
            cfg = NYamlConfig::StripMetadata(cfg);
        }
    } else {
        Cerr << "YAML config is absent on this cluster." << Endl;
        return EXIT_FAILURE;
    }

    if (!OutDir) {
        Cout << WrapYaml(cfg);
    } else {
        TFsPath dir(OutDir);
        dir.MkDirs();
        auto filepath = (dir / "dynconfig.yaml");
        TFileOutput out(filepath);
        out << cfg;
    }

    if (All) {
        for (auto [id, cfg] : result.GetVolatileConfigs()) {
            if (StripMetadata) {
                cfg = NYamlConfig::StripMetadata(cfg);
            }

            if (!OutDir) {
                Cout << WrapYaml(cfg);
            } else {
                auto filename = TString("volatile_") + ToString(version) + "_" + ToString(id) + ".yaml";
                auto filepath = (TFsPath(OutDir) / filename);
                TFileOutput out(filepath);
                out << cfg;
            }
        }
    }

    return EXIT_SUCCESS;
}

TCommandConfigReplace::TCommandConfigReplace()
    : TYdbCommand("replace", {}, "Replace dynamic config")
    , IgnoreCheck(false)
{
}

void TCommandConfigReplace::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption('f', "filename", "Filename of the file containing configuration")
        .Required().RequiredArgument("[config.yaml]").StoreResult(&Filename);
    config.Opts->AddLongOption("ignore-local-validation", "Ignore local config applicability checks")
        .NoArgument().SetFlag(&IgnoreCheck);
    config.Opts->AddLongOption("dry-run", "Check config applicability")
        .NoArgument().SetFlag(&DryRun);
    config.Opts->AddLongOption("allow-unknown-fields", "Allow fields not present in config")
        .NoArgument().SetFlag(&AllowUnknownFields);
    config.Opts->AddLongOption("force", "Ignore metadata on config replacement")
        .NoArgument().SetFlag(&Force);
    config.SetFreeArgsNum(0);
}

void TCommandConfigReplace::Parse(TConfig& config) {
    TClientCommand::Parse(config);

    if (Filename == "") {
        ythrow yexception() << "Must specify non-empty -f (--filename)";
    }

   const auto configStr = Filename == "-" ? Cin.ReadAll() : TFileInput(Filename).ReadAll();

    DynamicConfig = configStr;

    if (!IgnoreCheck) {
        NYamlConfig::GetMetadata(configStr);
        auto tree = NFyaml::TDocument::Parse(configStr);
        const auto resolved = NYamlConfig::ResolveAll(tree);
        Y_UNUSED(resolved); // we can't check it better without ydbd
    }
}

int TCommandConfigReplace::Run(TConfig& config) {
    std::unique_ptr<NYdb::TDriver> driver = std::make_unique<NYdb::TDriver>(CreateDriver(config));
    auto client = NYdb::NDynamicConfig::TDynamicConfigClient(*driver);
    auto exec = [&]() {
        if (Force) {
            return client.SetConfig(DynamicConfig, DryRun, AllowUnknownFields).GetValueSync();
        }

        return client.ReplaceConfig(DynamicConfig, DryRun, AllowUnknownFields).GetValueSync();
    };
    auto status = exec();
    ThrowOnError(status);

    if (!status.GetIssues()) {
        Cout << status << Endl;
    }

    return EXIT_SUCCESS;
}

TCommandConfigResolve::TCommandConfigResolve()
    : TYdbCommand("resolve", {}, "Resolve config")
{
}

void TCommandConfigResolve::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("all", "Resolve for all combinations")
        .NoArgument().SetFlag(&All);
    config.Opts->AddLongOption("label", "Labels for this node")
        .Optional().RequiredArgument("[LABEL=VALUE]")
        .KVHandler([this](TString key, TString val) {
            Labels[key] = val;
        });
    config.Opts->AddLongOption('f', "filename", "Filename of the file containing configuration to resolve")
        .Optional().RequiredArgument("[config.yaml]").StoreResult(&Filename);
    config.Opts->AddLongOption("directory", "Directory with config(s)")
        .Optional().RequiredArgument("[directory]").StoreResult(&Dir);
    config.Opts->AddLongOption("output-directory", "Directory to save config(s)")
        .Optional().RequiredArgument("[directory]").StoreResult(&OutDir);
    config.Opts->AddLongOption("from-cluster", "Fetch current config from cluster instead of the local file")
        .NoArgument().SetFlag(&FromCluster);
    config.Opts->AddLongOption("remote-resolve", "Use resolver on cluster instead of built-in resolver")
        .NoArgument().SetFlag(&RemoteResolve);
    config.Opts->AddLongOption("node-id", "Take labels from node with the specified id")
        .Optional().RequiredArgument("[node]").StoreResult(&NodeId);
    config.Opts->AddLongOption("skip-volatile", "Ignore volatile configs")
        .NoArgument().SetFlag(&SkipVolatile);
    config.SetFreeArgsNum(0);
}

void TCommandConfigResolve::Parse(TConfig& config) {
    TClientCommand::Parse(config);

    if ((ui32(!Filename.empty()) + ui32(!Dir.empty()) + ui32(FromCluster)) != 1) {
        ythrow yexception() << "Must specify one of -f (--filename), --directory and --from-cluster";
    }

    if (All && (!Labels.empty() || NodeId)) {
        ythrow yexception() << "Should specify either --all or --label and --node-id";
    }
}

TString CalcHash(const TString& ser) {
    SHA_CTX ctx;
    SHA1_Init(&ctx);
    SHA1_Update(&ctx, ser.data(), ser.size());
    unsigned char sha1[SHA_DIGEST_LENGTH];
    SHA1_Final(sha1, &ctx);

    TString hex = HexEncode(TString(reinterpret_cast<const char*>(sha1), SHA_DIGEST_LENGTH));
    hex.to_lower();
    return hex;
}

TString LabelSetHash(const TSet<NYamlConfig::TNamedLabel>& labels) {
    TStringStream labelsStream;
    labelsStream << "[";
    for (const auto& label : labels) {
        labelsStream << "(" << label.Name << ":" << (label.Inv ? "-" : label.Value.Quote()) << "),";
    }
    labelsStream << "]";
    TString ser = labelsStream.Str();
    return CalcHash(ser);
}

TString ConfigHash(const NFyaml::TNodeRef& config) {
    TStringStream configStream;
    configStream << config;
    TString ser = configStream.Str();
    return "0_" + CalcHash(ser);
}

int TCommandConfigResolve::Run(TConfig& config) {
    auto driver = std::make_unique<NYdb::TDriver>(CreateDriver(config));
    auto client = NYdb::NDynamicConfig::TDynamicConfigClient(*driver);

    if (NodeId) {
        auto result = client.GetNodeLabels(NodeId).GetValueSync();
        ThrowOnError(result);

        // TODO: maybe we should merge labels instead
        Labels = result.GetLabels();
    }

    TString configStr;
    TMap<ui64, TString> volatileConfigStrs;

    if (!Filename.empty()) {
        configStr = TFileInput(Filename).ReadAll();
        // TODO: support multiple files (e.g. volatile and non-volatile)
    }

    if (!Dir.empty()) {
        auto dir = TFsPath(Dir);
        configStr = TFileInput(dir / "dynconfig.yaml").ReadAll();
        TVector<TFsPath> entries;
        dir.List(entries);
        for (auto& entry : entries) {
            if (entry.IsFile() && entry.GetName().StartsWith("volatile_") && entry.GetName().EndsWith(".yaml")) {
                auto volatileConfigStr = TFileInput(entry).ReadAll();
                auto metadata = NYamlConfig::GetVolatileMetadata(volatileConfigStr);
                volatileConfigStrs[metadata.Id.value()] = volatileConfigStr;
            }
        }
        // TODO: use kind to distinguish between config types
        // TODO: throw error on version mismatch
    }

    if (FromCluster) {
        auto result = client.GetConfig().GetValueSync();
        ThrowOnError(result);

        configStr = result.GetConfig();
        volatileConfigStrs = result.GetVolatileConfigs();
    }

    auto tree = NFyaml::TDocument::Parse(configStr);

    if (!SkipVolatile) {
        for (auto& [_, cfgStr]: volatileConfigStrs) {
            auto volatileCfg = NFyaml::TDocument::Parse(cfgStr);
            auto selectors = volatileCfg.Root().Map().at("selector_config");
            NYamlConfig::AppendVolatileConfigs(tree, selectors);
        }
    }

    TVector<NFyaml::TDocument> configs;

    if (!RemoteResolve) {
        if (All) {
            auto resolved = NYamlConfig::ResolveAll(tree);
            for (auto& [labelSets, config] : resolved.Configs) {
                auto doc = NFyaml::TDocument::Parse("---\nlabel_sets: []\nconfig: {}\n");

                auto node = config.second.Copy(doc);
                doc.Root().Map().at("config").Insert(node.Ref());
                auto labelSetsSeq = doc.Root().Map().at("label_sets").Sequence();

                for (auto& labelSet : labelSets) {
                    auto map = doc.Buildf("{}");
                    for (size_t i = 0; i < labelSet.size(); ++i) {
                        auto& label = labelSet[i];
                        NFyaml::TNodeRef node;
                        switch (label.Type) {
                        case NYamlConfig::TLabel::EType::Common:
                            node = doc.Buildf("%s: {type: COMMON, value: %s}", resolved.Labels[i].c_str(), label.Value.c_str());
                            break;
                        case NYamlConfig::TLabel::EType::Negative:
                            node = doc.Buildf("%s: {type: NOT_SET}", resolved.Labels[i].c_str());
                            break;
                        case NYamlConfig::TLabel::EType::Empty:
                            node = doc.Buildf("%s: {type: EMPTY}", resolved.Labels[i].c_str());
                            break;
                        default:
                            Y_ABORT("unknown label type");
                        }
                        map.Insert(node);
                    }
                    labelSetsSeq.Append(map);
                }

                configs.push_back(std::move(doc));
            }
        } else {
            auto doc = NFyaml::TDocument::Parse("---\nlabel_sets: []\nconfig: {}\n");

            auto labelSetsSeq = doc.Root().Map().at("label_sets").Sequence();
            auto map = doc.Buildf("{}");
            TSet<NYamlConfig::TNamedLabel> namedLabels;
            for (auto& [name, value] : Labels) {
                namedLabels.insert(NYamlConfig::TNamedLabel{name, value});
                auto node = doc.Buildf("%s: {type: COMMON, value: %s}", name.c_str(), value.c_str());
                map.Insert(node);
            }
            labelSetsSeq.Append(map);
            auto resolved = NYamlConfig::Resolve(tree, namedLabels);

            auto node = resolved.second.Copy(doc);
            doc.Root().Map().at("config").Insert(node.Ref());

            configs.push_back(std::move(doc));
        }
    } else {
        if (All) {
            auto result = client.VerboseResolveConfig(configStr, volatileConfigStrs).GetValueSync();
            ThrowOnError(result);

            TVector<TString> labels(result.GetLabels().begin(), result.GetLabels().end());

            for (const auto& [labelSets, configStr] : result.GetConfigs()) {
                auto doc = NFyaml::TDocument::Parse("---\nlabel_sets: []\nconfig: {}\n");
                auto config = NFyaml::TDocument::Parse(configStr);

                auto node = config.Root().Copy(doc);
                doc.Root().Map().at("config").Insert(node.Ref());
                auto labelSetsSeq = doc.Root().Map().at("label_sets").Sequence();

                for (auto& labelSet : labelSets) {
                    auto map = doc.Buildf("{}");
                    for (size_t i = 0; i < labelSet.size(); ++i) {
                        auto& label = labelSet[i];
                        NFyaml::TNodeRef node;
                        switch (label.Type) {
                        case NYdb::NDynamicConfig::TVerboseResolveConfigResult::TLabel::EType::Common:
                            node = doc.Buildf("%s: {type: COMMON, value: %s}", labels[i].c_str(), label.Value.c_str());
                            break;
                        case NYdb::NDynamicConfig::TVerboseResolveConfigResult::TLabel::EType::Negative:
                            node = doc.Buildf("%s: {type: NOT_SET}", labels[i].c_str());
                            break;
                        case NYdb::NDynamicConfig::TVerboseResolveConfigResult::TLabel::EType::Empty:
                            node = doc.Buildf("%s: {type: EMPTY}", labels[i].c_str());
                            break;
                        default:
                            Y_ABORT("unknown label type");
                        }
                        map.Insert(node);
                    }
                    labelSetsSeq.Append(map);
                }

                configs.push_back(std::move(doc));
            }
        } else {
            const auto result = client.ResolveConfig(configStr, volatileConfigStrs, Labels).GetValueSync();
            ThrowOnError(result);

            auto doc = NFyaml::TDocument::Parse("---\nlabel_sets: []\nconfig: {}\n");

            auto labelSetsSeq = doc.Root().Map().at("label_sets").Sequence();
            auto map = doc.Buildf("{}");
            TSet<NYamlConfig::TNamedLabel> namedLabels;
            for (auto& [name, value] : Labels) {
                namedLabels.insert(NYamlConfig::TNamedLabel{name, value});
                auto node = doc.Buildf("%s: {type: COMMON, value: %s}", name.c_str(), value.c_str());
                map.Insert(node);
            }
            labelSetsSeq.Append(map);

            auto config = NFyaml::TDocument::Parse(result.GetConfig());
            auto node = config.Root().Copy(doc);
            doc.Root().Map().at("config").Insert(node.Ref());

            configs.push_back(std::move(doc));
        }
    }

    if (!OutDir) {
        for (auto& doc : configs) {
            Cout << doc;
        }
    } else {
        TFsPath dir(OutDir);
        dir.MkDirs();

        for (auto& doc : configs) {
            auto node = doc.Root().Map().at("config");
            auto filename = TString("config_") + ConfigHash(node) + ".yaml";
            auto filepath = dir / filename;
            TFileOutput out(filepath);
            out << doc;
        }
    }

    return EXIT_SUCCESS;
}

TCommandVolatileConfig::TCommandVolatileConfig()
    : TClientCommandTree("volatile-config", {}, "Volatile dynamic configs")
{
    AddCommand(std::make_unique<TCommandConfigVolatileAdd>());
    AddCommand(std::make_unique<TCommandConfigVolatileDrop>());
    AddCommand(std::make_unique<TCommandConfigVolatileFetch>());
}

TCommandConfigVolatileAdd::TCommandConfigVolatileAdd()
    : TYdbCommand("add", {}, "Add volatile dynamic config")
{
}

void TCommandConfigVolatileAdd::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption('f', "filename", "filename to set")
        .Required().RequiredArgument("[config.yaml]").StoreResult(&Filename);
    config.Opts->AddLongOption("ignore-local-validation", "Ignore local config applicability checks")
        .NoArgument().SetFlag(&IgnoreCheck);
    config.Opts->AddLongOption("dry-run", "Check config applicability")
        .NoArgument().SetFlag(&DryRun);
    config.SetFreeArgsNum(0);

}

void TCommandConfigVolatileAdd::Parse(TConfig& config) {
    TClientCommand::Parse(config);

    if (Filename.empty()) {
        ythrow yexception() << "Must non-empty specify -f (--filename)";
    }
}

int TCommandConfigVolatileAdd::Run(TConfig& config) {
    auto driver = std::make_unique<NYdb::TDriver>(CreateDriver(config));
    auto client = NYdb::NDynamicConfig::TDynamicConfigClient(*driver);

    const auto configStr = Filename == "-" ? Cin.ReadAll() : TFileInput(Filename).ReadAll();

    if (!IgnoreCheck) {
        auto result = client.GetConfig().GetValueSync();
        ThrowOnError(result);

        if (result.GetConfig().empty()) {
            ythrow yexception() << "Config on server is empty";
        }

        NYamlConfig::GetVolatileMetadata(configStr);

        auto tree = NFyaml::TDocument::Parse(result.GetConfig());

        auto volatileCfg = NFyaml::TDocument::Parse(configStr);
        auto selectors = volatileCfg.Root().Map().at("selector_config");
        NYamlConfig::AppendVolatileConfigs(tree, selectors);

        auto resolved = NYamlConfig::ResolveAll(tree);

        Y_UNUSED(resolved); // we can't check it better without ydbd
    }

    auto status = client.AddVolatileConfig(configStr).GetValueSync();
    ThrowOnError(status);

    Cout << status << Endl;

    return EXIT_SUCCESS;
}

TCommandConfigVolatileDrop::TCommandConfigVolatileDrop()
    : TYdbCommand("drop", {}, "Remove volatile dynamic configs")
{
}

void TCommandConfigVolatileDrop::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("id", "Id of config to remove")
        .Optional().RequiredArgument("[ui64]")
        .InsertTo(&Ids);
    config.Opts->AddLongOption("all", "Remove all volatile configs")
        .NoArgument().SetFlag(&All);
    config.Opts->AddLongOption('f', "filename", "Filename of the file containing configuration to remove")
        .RequiredArgument("[String]").DefaultValue("").StoreResult(&Filename);
    config.Opts->AddLongOption("cluster", "Cluster name")
        .RequiredArgument("[String]").DefaultValue("").StoreResult(&Cluster);
    config.Opts->AddLongOption("version", "Config version")
        .RequiredArgument("[ui64]").StoreResult(&Version);
    config.Opts->AddLongOption("force", "Ignore version and cluster check")
        .NoArgument().SetFlag(&Force);
    config.Opts->AddLongOption("directory", "Directory with volatile configs")
        .Optional().RequiredArgument("[directory]").StoreResult(&Dir);
}

void TCommandConfigVolatileDrop::Parse(TConfig& config) {
    TClientCommand::Parse(config);

    if ((ui32(!Ids.empty()) + ui32(All) + ui32(!Filename.empty()) + ui32(!Dir.empty())) != 1) {
        ythrow yexception() << "Must specify one of --id, --all, -f (--filename) and --directory";
    }

    if ((ui32(!Filename.empty() || !Dir.empty()) + ui32(!Ids.empty() || All)) != 1) {
        ythrow yexception() << "Must specify either --directory or -f (--filename) and --id or --all";
    }

    if (!Force && !(!Cluster.empty() || !Version) && Filename.empty() && Dir.empty()) {
        ythrow yexception() << "Must specify either --force or --cluster and --version or --filename";
    }
}

int TCommandConfigVolatileDrop::Run(TConfig& config) {
    auto driver = std::make_unique<NYdb::TDriver>(CreateDriver(config));
    auto client = NYdb::NDynamicConfig::TDynamicConfigClient(*driver);

    if (!Dir.empty()) {
        auto dir = TFsPath(Dir);
        TVector<TFsPath> entries;
        dir.List(entries);
        for (auto& entry : entries) {
            if (entry.IsFile() && entry.GetName().StartsWith("volatile_") && entry.GetName().EndsWith(".yaml")) {
                auto volatileConfigStr = TFileInput(entry).ReadAll();
                if (NYamlConfig::IsVolatileConfig(volatileConfigStr)) {
                    auto metadata = NYamlConfig::GetVolatileMetadata(volatileConfigStr);
                    Ids.insert(metadata.Id.value());
                    Cluster = metadata.Cluster.value();
                    Version = metadata.Version.value();
                }
                // TODO: throw error on multiple Cluster/Versions
            }
        }
    }

    if (!Filename.empty()) {
        auto volatileConfigStr = TFileInput(Filename).ReadAll();
        if (NYamlConfig::IsVolatileConfig(volatileConfigStr)) {
            auto metadata = NYamlConfig::GetVolatileMetadata(volatileConfigStr);
            Ids.insert(metadata.Id.value());
            Cluster = metadata.Cluster.value();
            Version = metadata.Version.value();
        } else {
            ythrow yexception() << "File " << Filename << " is not volatile config";
        }
    }

    auto status = [&]() {
        if (All && Force) {
            return client.ForceRemoveAllVolatileConfigs().GetValueSync();
        }

        if (All) {
            return client.RemoveAllVolatileConfigs(Cluster, Version).GetValueSync();
        }

        if (Force) {
            return client.ForceRemoveVolatileConfig(TVector<ui64>(Ids.begin(), Ids.end())).GetValueSync();
        }

        return client.RemoveVolatileConfig(Cluster, Version, TVector<ui64>(Ids.begin(), Ids.end())).GetValueSync();
    }();

    ThrowOnError(status);

    Cout << status << Endl;

    return EXIT_SUCCESS;
}

TCommandConfigVolatileFetch::TCommandConfigVolatileFetch()
    : TYdbCommand("fetch", {"get", "dump"}, "Fetch volatile config")
{
}

void TCommandConfigVolatileFetch::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("id", "Volatile config id")
        .Optional().RequiredArgument("[ui64]").InsertTo(&Ids);
    config.Opts->AddLongOption("all", "Fetch all volatile configs")
        .NoArgument().SetFlag(&All);
    config.Opts->AddLongOption("output-directory", "Directory to save config(s)")
        .RequiredArgument("[directory]").StoreResult(&OutDir);
    config.Opts->AddLongOption("strip-metadata", "Strip metadata from config(s)")
        .NoArgument().SetFlag(&StripMetadata);
    config.SetFreeArgsNum(0);

    config.Opts->MutuallyExclusive("output-directory", "strip-metadata");
}

void TCommandConfigVolatileFetch::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandConfigVolatileFetch::Run(TConfig& config) {
    auto driver = std::make_unique<NYdb::TDriver>(CreateDriver(config));
    auto client = NYdb::NDynamicConfig::TDynamicConfigClient(*driver);
    auto result = client.GetConfig().GetValueSync();
    ThrowOnError(result);

    if (OutDir) {
        TFsPath dir(OutDir);
        dir.MkDirs();
    }

    ui64 version = 0;

    for (auto [id, cfg] : result.GetVolatileConfigs()) {
        if (All || Ids.contains(id)) {
            version = NYamlConfig::GetVolatileMetadata(cfg).Version.value();

            if (StripMetadata) {
                cfg = NYamlConfig::StripMetadata(cfg);
            }

            if (!OutDir) {
                Cout << WrapYaml(cfg);
            } else {
                auto filename = TString("volatile_") + ToString(version) + "_" + ToString(id) + ".yaml";
                auto filepath = (TFsPath(OutDir) / filename);
                TFileOutput out(filepath);
                out << cfg;
            }
        }
    }

    return EXIT_SUCCESS;
}

} // namespace NYdb::NConsoleClient::NDynamicConfig
