#include <ydb/library/yaml_config/public/yaml_config.h>
#include <ydb/library/yaml_config/yaml_config.h>
#include <ydb/library/yaml_config/yaml_config_parser.h>

#include <library/cpp/json/json_reader.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>
#include <util/stream/str.h>
#include <util/string/builder.h>

#include <algorithm>

namespace {

TString ConsumeChunk(FuzzedDataProvider& fdp, size_t maxLen = 16 * 1024) {
    return fdp.ConsumeBytesAsString(fdp.ConsumeIntegralInRange<size_t>(0, maxLen));
}

bool ShouldRunResolveAll(const TMap<TString, NKikimr::NYamlConfig::TLabelType>& labels) {
    size_t combinations = 1;
    for (const auto& [_, label] : labels) {
        const size_t count = std::max<size_t>(1, label.Values.size());
        if (count > 4 || combinations > 64 / count) {
            return false;
        }
        combinations *= count;
    }
    return combinations <= 64;
}

TSet<NKikimr::NYamlConfig::TNamedLabel> ConsumeLabels(FuzzedDataProvider& fdp) {
    TSet<NKikimr::NYamlConfig::TNamedLabel> labels;
    const size_t count = fdp.ConsumeIntegralInRange<size_t>(0, 4);
    for (size_t i = 0; i < count; ++i) {
        NKikimr::NYamlConfig::TNamedLabel label;
        label.Name = TStringBuilder() << "label_" << i << "_" << ConsumeChunk(fdp, 16);
        label.Value = ConsumeChunk(fdp, 32);
        labels.insert(label);
    }
    return labels;
}

void MaybeValidateResolvedConfig(const NKikimr::NFyaml::TNodeRef& node) {
    try {
        TStringStream jsonStream;
        jsonStream << NKikimr::NFyaml::TJsonEmitter(node);

        NJson::TJsonValue json;
        if (!NJson::ReadJsonTree(jsonStream.Str(), &json)) {
            return;
        }

        NKikimrConfig::TAppConfig appConfig;
        NKikimr::NYaml::Parse(
            json,
            NKikimr::NYaml::GetJsonToProtoConfig(true),
            appConfig,
            true,
            nullptr,
            true);

        auto swissKnife = NKikimr::NYamlConfig::CreateDefaultConfigSwissKnife();
        std::vector<TString> messages;
        (void)swissKnife->ValidateConfig(appConfig, messages);
    } catch (...) {
    }
}

void ExerciseModelDocument(
    const TString& configText,
    const TSet<NKikimr::NYamlConfig::TNamedLabel>& labels,
    const TString& volatileConfigText,
    const TString& databaseConfigText)
{
    auto doc = NKikimr::NFyaml::TDocument::Parse(configText);

    auto parsedModel = NKikimr::NYamlConfig::ParseConfig(doc);
    auto collectedLabels = NKikimr::NYamlConfig::CollectLabels(doc);
    (void)parsedModel.Config;
    (void)parsedModel.Selectors.size();
    (void)parsedModel.AllowedLabels.size();
    (void)parsedModel.IncompatibilityRules.GetRuleCount();

    auto resolved = NKikimr::NYamlConfig::Resolve(doc, labels);
    TStringStream resolvedYaml;
    resolvedYaml << resolved.second;
    MaybeValidateResolvedConfig(resolved.second);

    if (ShouldRunResolveAll(collectedLabels)) {
        auto docForAll = doc.Clone();
        auto allConfigs = NKikimr::NYamlConfig::ResolveAll(docForAll);
        (void)NKikimr::NYamlConfig::Hash(allConfigs);
    }

    auto docForUnique = doc.Clone();
    size_t uniqueDocs = 0;
    NKikimr::NYamlConfig::ResolveUniqueDocs(docForUnique, [&uniqueDocs](NKikimr::NYamlConfig::TDocumentConfig&& cfg) {
        ++uniqueDocs;
        TStringStream out;
        out << cfg.second;
        (void)out.Str();
    });
    (void)uniqueDocs;

    if (!volatileConfigText.empty()) {
        try {
            auto docForVolatile = doc.Clone();
            auto volatileDoc = NKikimr::NFyaml::TDocument::Parse(volatileConfigText);
            NKikimr::NYamlConfig::ValidateVolatileConfig(volatileDoc);
            NKikimr::NYamlConfig::AppendVolatileConfigs(docForVolatile, volatileDoc);
            (void)NKikimr::NYamlConfig::Resolve(docForVolatile, labels);
        } catch (...) {
        }
    }

    if (!databaseConfigText.empty()) {
        try {
            auto docForDatabase = doc.Clone();
            auto databaseDoc = NKikimr::NFyaml::TDocument::Parse(databaseConfigText);
            NKikimr::NYamlConfig::AppendDatabaseConfig(docForDatabase, databaseDoc);
            (void)NKikimr::NYamlConfig::Resolve(docForDatabase, labels);
        } catch (...) {
        }
    }
}

void FuzzYamlConfigModel(FuzzedDataProvider& fdp) {
    const TString mainConfig = ConsumeChunk(fdp);
    const TString volatileConfig = ConsumeChunk(fdp, 8 * 1024);
    const TString databaseConfig = ConsumeChunk(fdp, 8 * 1024);
    const TString baseConfig = ConsumeChunk(fdp, 8 * 1024);
    const TString consoleConfig = ConsumeChunk(fdp, 8 * 1024);
    const auto labels = ConsumeLabels(fdp);

    try {
        ExerciseModelDocument(mainConfig, labels, volatileConfig, databaseConfig);
    } catch (...) {
    }

    try {
        auto fused = NKikimr::NYamlConfig::FuseConfigs(baseConfig, consoleConfig);
        TStringStream out;
        out << fused;
        (void)out.Str();
    } catch (...) {
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 128 * 1024) {
        return 0;
    }

    try {
        FuzzedDataProvider fdp(data, size);
        FuzzYamlConfigModel(fdp);
    } catch (...) {
    }

    return 0;
}
