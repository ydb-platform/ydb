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

inline TString DecompressYamlString(const TString& buffer) {
    TStringInput s(buffer);
    TZstdDecompress zstd(&s);
    TString res;
    Load(&zstd, res);
    return res;
}

} // NKikimr::NYamlConfig
