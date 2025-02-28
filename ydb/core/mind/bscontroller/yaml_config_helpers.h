#pragma once

#include <ydb/library/yaml_config/yaml_config.h>
#include <ydb/library/yaml_config/yaml_config_compress.h>
#include <ydb/library/yaml_config/yaml_config_helpers.h>

namespace NKikimr {

namespace NBsController {

using TYamlConfig = std::tuple<TString, ui64, TString>; // tuple of yaml, configVersion, yamlReturnedByFetch; this tuple must not change

inline TString CompressYamlConfig(const TYamlConfig& configYaml) {
    TStringStream s;
    {
        TZstdCompress zstd(&s);
        Save(&zstd, configYaml);
    }
    return s.Str();
}

inline TString CompressSingleConfig(const TYamlConfig& configYaml) {
    TString singleConfig = std::get<0>(configYaml);
    TStringStream s;
    {
        TZstdCompress zstd(&s);
        Save(&zstd, singleConfig);
    }
    return s.Str();
}

inline TString CompressStorageYamlConfig(const TString& storageConfigYaml) {
    return NYamlConfig::CompressYamlString(storageConfigYaml);
}

inline TYamlConfig DecompressYamlConfig(const TString& buffer) {
    TStringInput s(buffer);
    TZstdDecompress zstd(&s);
    TYamlConfig res;
    Load(&zstd, res);
    return res;
}

inline TString DecompressStorageYamlConfig(const TString& buffer) {
    return NYamlConfig::DecompressYamlString(buffer);
}

inline TString DecompressSingleConfig(const TString& buffer) {
    return NYamlConfig::DecompressYamlString(buffer);
}

inline ui64 GetVersion(const TYamlConfig& config) {
    return std::get<1>(config);
}

inline ui64 GetSingleConfigHash(const TYamlConfig& config) {
    return NYaml::GetConfigHash(std::get<0>(config));
}

} // namespace NBsController

} // namespace NKikimr
