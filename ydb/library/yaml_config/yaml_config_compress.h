#pragma once

#include <util/generic/string.h>
#include <library/cpp/streams/zstd/zstd.h>
#include <ydb/library/yaml_config/yaml_config.h>

namespace NKikimr::NYamlConfig {

inline TString CompressYamlString(const TString& yamlStr) {
    TStringStream s;
    {
        TZstdCompress zstd(&s);
        Save(&zstd, yamlStr);
    }
    return s.Str();
}

inline TString CompressYamlConfig(const TYamlConfig& configYaml) {
    TStringStream s;
    {
        TZstdCompress zstd(&s);
        Save(&zstd, configYaml);
    }
    return s.Str();
}

inline TString CompressStorageYamlConfig(const TString& storageConfigYaml) {
    return CompressYamlString(storageConfigYaml);
}

inline TString DecompressYamlString(const TString& buffer) {
    TStringInput s(buffer);
    TZstdDecompress zstd(&s);
    TString res;
    Load(&zstd, res);
    return res;
}

inline TYamlConfig DecompressYamlConfig(const TString& buffer) {
    TStringInput s(buffer);
    TZstdDecompress zstd(&s);
    TYamlConfig res;
    Load(&zstd, res);
    return res;
}

inline TString DecompressStorageYamlConfig(const TString& buffer) {
    return DecompressYamlString(buffer);
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

inline TString DecompressSingleConfig(const TString& buffer) {
    return DecompressYamlString(buffer);
}

} // NKikimr::NYamlConfig
