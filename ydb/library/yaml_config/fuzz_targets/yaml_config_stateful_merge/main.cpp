#include <ydb/library/yaml_config/yaml_config.h>
#include <ydb/library/yaml_config/public/yaml_config.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/str.h>

namespace {

using namespace NKikimr;

constexpr TStringBuf MainConfig = R"(metadata:
  kind: MainConfig
  version: 1
  cluster: fuzz
config:
  log_config:
    default: 1
allowed_labels:
  tenant:
    type: string
  flavour:
    type: enum
    values:
      ? small
      ? big
selector_config: []
)";

constexpr TStringBuf VolatileA = R"(
- description: volatile-a
  selector:
    tenant: /Root/A
  config:
    log_config:
      default: 2
)";

constexpr TStringBuf VolatileB = R"(
- description: volatile-b
  selector:
    flavour: big
  config:
    auth_config:
      enforce_user_token_requirement: true
)";

constexpr TStringBuf DatabaseConfig = R"(metadata:
  kind: DatabaseConfig
  version: 1
  database: /Root
config:
  feature_flags:
    enable_topic_service_tx: true
)";

TString Emit(const NFyaml::TDocument& doc) {
    TStringStream out;
    out << doc;
    return out.Str();
}

TString SafeChunk(FuzzedDataProvider& fdp, size_t maxLen) {
    TString value = fdp.ConsumeBytesAsString(fdp.ConsumeIntegralInRange<size_t>(0, maxLen));
    for (char& c : value) {
        if (c == '\0') {
            c = '_';
        }
    }
    return value;
}

NYamlConfig::TMainMetadata MainMetadata(FuzzedDataProvider& fdp, ui64 minVersion) {
    return {
        .Version = minVersion + fdp.ConsumeIntegralInRange<ui64>(0, 1024),
        .Cluster = "cluster-" + ToString(fdp.ConsumeIntegralInRange<ui32>(0, 7)),
    };
}

void CheckUpgradeMonotonic(const TString& config) {
    try {
        const ui64 version = NYamlConfig::GetVersion(config);
        const TString upgraded = NYamlConfig::UpgradeMainConfigVersion(config);
        const ui64 upgradedVersion = NYamlConfig::GetVersion(upgraded);
        Y_ABORT_UNLESS(upgradedVersion == version + 1);
    } catch (...) {
    }
}

void ResolveAndCheck(const TString& main, const TMap<ui64, TString>& volatileConfigs, const std::optional<TString>& databaseConfig, FuzzedDataProvider& fdp) {
    TMap<TString, TString> labels;
    if (fdp.ConsumeBool()) {
        labels["tenant"] = fdp.ConsumeBool() ? "/Root/A" : SafeChunk(fdp, 24);
    }
    if (fdp.ConsumeBool()) {
        labels["flavour"] = fdp.ConsumeBool() ? "small" : "big";
    }

    NKikimrConfig::TAppConfig appConfig;
    TString resolvedYaml;
    TString resolvedJson;
    try {
        NYamlConfig::ResolveAndParseYamlConfig(main, volatileConfigs, labels, appConfig, databaseConfig, &resolvedYaml, &resolvedJson);
        NKikimrConfig::TAppConfig appConfigAgain;
        TString resolvedYamlAgain;
        TString resolvedJsonAgain;
        NYamlConfig::ResolveAndParseYamlConfig(main, volatileConfigs, labels, appConfigAgain, databaseConfig, &resolvedYamlAgain, &resolvedJsonAgain);
        Y_ABORT_UNLESS(resolvedYaml == resolvedYamlAgain);
        Y_ABORT_UNLESS(resolvedJson == resolvedJsonAgain);
    } catch (...) {
    }
}

void FuzzYamlState(FuzzedDataProvider& fdp) {
    TString main(MainConfig);
    TMap<ui64, TString> volatileConfigs;
    std::optional<TString> databaseConfig;
    ui64 lastVersion = 1;

    const ui32 steps = fdp.ConsumeIntegralInRange<ui32>(1, 48);
    for (ui32 step = 0; step < steps && fdp.remaining_bytes(); ++step) {
        switch (fdp.ConsumeIntegralInRange<ui8>(0, 10)) {
            case 0: {
                auto metadata = MainMetadata(fdp, lastVersion);
                if (metadata.Version) {
                    lastVersion = *metadata.Version;
                }
                try {
                    main = NYamlConfig::ReplaceMetadata(main, metadata);
                    Y_ABORT_UNLESS(!metadata.Version || NYamlConfig::GetVersion(main) == *metadata.Version);
                } catch (...) {
                }
                break;
            }
            case 1:
                CheckUpgradeMonotonic(main);
                break;
            case 2: {
                const ui64 id = fdp.ConsumeIntegralInRange<ui64>(1, 8);
                volatileConfigs[id] = fdp.ConsumeBool() ? TString(VolatileA) : TString(VolatileB);
                break;
            }
            case 3:
                if (!volatileConfigs.empty()) {
                    volatileConfigs.erase(fdp.ConsumeIntegralInRange<ui64>(1, 8));
                }
                break;
            case 4:
                databaseConfig = TString(DatabaseConfig);
                break;
            case 5:
                databaseConfig.reset();
                break;
            case 6: {
                try {
                    auto doc = NFyaml::TDocument::Parse(main);
                    auto vol = NFyaml::TDocument::Parse(fdp.ConsumeBool() ? TString(VolatileA) : TString(VolatileB));
                    NYamlConfig::AppendVolatileConfigs(doc, vol);
                    (void)NYamlConfig::ResolveAll(doc);
                } catch (...) {
                }
                break;
            }
            case 7: {
                try {
                    auto doc = NYamlConfig::FuseConfigs("log_config:\n  default: 7\n", main);
                    (void)Emit(doc);
                } catch (...) {
                }
                break;
            }
            case 8: {
                const TString raw = fdp.ConsumeBytesAsString(fdp.ConsumeIntegralInRange<size_t>(0, 512));
                try {
                    auto doc = NFyaml::TDocument::Parse(raw);
                    if (doc.Root().Type() == NFyaml::ENodeType::Mapping) {
                        (void)NYamlConfig::GetGenericMetadata(raw);
                        (void)NYamlConfig::StripMetadata(raw);
                    }
                } catch (...) {
                }
                break;
            }
            default:
                ResolveAndCheck(main, volatileConfigs, databaseConfig, fdp);
                break;
        }
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 64 * 1024) {
        return 0;
    }

    try {
        FuzzedDataProvider fdp(data, size);
        FuzzYamlState(fdp);
    } catch (...) {
    }

    return 0;
}
