#include <ydb/library/yaml_config/public/yaml_config.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>

namespace {

TString ConsumeChunk(FuzzedDataProvider& fdp, size_t maxLen = 64) {
    return fdp.ConsumeBytesAsString(fdp.ConsumeIntegralInRange<size_t>(0, maxLen));
}

TString MakeParseableConfig(const TString& input) {
    try {
        auto doc = NKikimr::NFyaml::TDocument::Parse(input);
        if (doc.Root().Type() == NKikimr::NFyaml::ENodeType::Mapping) {
            return input;
        }
    } catch (...) {
    }

    return "{}";
}

void FuzzMetadataOps(FuzzedDataProvider& fdp) {
    const TString input = fdp.ConsumeRemainingBytesAsString();
    const TString parseableConfig = MakeParseableConfig(input);

    try {
        (void)NKikimr::NYamlConfig::GetGenericMetadata(input);
        (void)NKikimr::NYamlConfig::GetMainMetadata(input);
        (void)NKikimr::NYamlConfig::GetDatabaseMetadata(input);
        (void)NKikimr::NYamlConfig::GetStorageMetadata(input);
        (void)NKikimr::NYamlConfig::GetVolatileMetadata(input);
        (void)NKikimr::NYamlConfig::IsVolatileConfig(input);
        (void)NKikimr::NYamlConfig::IsMainConfig(input);
        (void)NKikimr::NYamlConfig::IsStorageConfig(input);
        (void)NKikimr::NYamlConfig::IsDatabaseConfig(input);
        (void)NKikimr::NYamlConfig::IsStaticConfig(input);
        (void)NKikimr::NYamlConfig::StripMetadata(input);
    } catch (...) {
    }

    NKikimr::NYamlConfig::TMainMetadata mainMetadata;
    mainMetadata.Version = fdp.ConsumeIntegralInRange<ui64>(0, 1'000'000);
    mainMetadata.Cluster = ConsumeChunk(fdp, 32);

    NKikimr::NYamlConfig::TDatabaseMetadata databaseMetadata;
    databaseMetadata.Version = fdp.ConsumeIntegralInRange<ui64>(0, 1'000'000);
    databaseMetadata.Database = ConsumeChunk(fdp, 32);

    NKikimr::NYamlConfig::TStorageMetadata storageMetadata;
    storageMetadata.Version = fdp.ConsumeIntegralInRange<ui64>(0, 1'000'000);
    storageMetadata.Cluster = ConsumeChunk(fdp, 32);

    NKikimr::NYamlConfig::TVolatileMetadata volatileMetadata;
    volatileMetadata.Version = fdp.ConsumeIntegralInRange<ui64>(0, 1'000'000);
    volatileMetadata.Cluster = ConsumeChunk(fdp, 32);
    volatileMetadata.Id = fdp.ConsumeIntegralInRange<ui64>(0, 1'000'000);

    try {
        const TString replaced = NKikimr::NYamlConfig::ReplaceMetadata(parseableConfig, mainMetadata);
        (void)NKikimr::NYamlConfig::GetVersion(replaced);
        (void)NKikimr::NYamlConfig::GetMainMetadata(replaced);
        (void)NKikimr::NYamlConfig::IsMainConfig(replaced);
        (void)NKikimr::NYamlConfig::UpgradeMainConfigVersion(replaced);
    } catch (...) {
    }

    try {
        const TString replaced = NKikimr::NYamlConfig::ReplaceMetadata(parseableConfig, databaseMetadata);
        (void)NKikimr::NYamlConfig::GetDatabaseMetadata(replaced);
        (void)NKikimr::NYamlConfig::IsDatabaseConfig(replaced);
    } catch (...) {
    }

    try {
        const TString replaced = NKikimr::NYamlConfig::ReplaceMetadata(parseableConfig, storageMetadata);
        (void)NKikimr::NYamlConfig::GetStorageMetadata(replaced);
        (void)NKikimr::NYamlConfig::IsStorageConfig(replaced);
        (void)NKikimr::NYamlConfig::UpgradeStorageConfigVersion(replaced);
    } catch (...) {
    }

    try {
        const TString replaced = NKikimr::NYamlConfig::ReplaceMetadata(parseableConfig, volatileMetadata);
        (void)NKikimr::NYamlConfig::GetVolatileMetadata(replaced);
        (void)NKikimr::NYamlConfig::IsVolatileConfig(replaced);
    } catch (...) {
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 64 * 1024) {
        return 0;
    }

    try {
        FuzzedDataProvider fdp(data, size);
        FuzzMetadataOps(fdp);
    } catch (...) {
    }

    return 0;
}
