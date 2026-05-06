#include "yaml_config_helpers.h"

#include <ydb/library/yaml_config/yaml_config.h>
#include <ydb/library/yaml_config/yaml_config_parser.h>
#include <ydb/library/yaml_json/yaml_to_json.h>
#include <ydb/public/lib/ydb_cli/common/common.h>

#include <contrib/libs/yaml-cpp/include/yaml-cpp/exceptions.h>

#include <util/generic/ptr.h>
#include <util/string/builder.h>

namespace NKikimr::NConfig {

namespace {

using NYdb::NConsoleClient::TInitializationException;

TStringBuf EffectiveYamlSource(TStringBuf source) {
    return source ? source : "YAML config";
}

TString StripSourceLocationPrefix(TStringBuf message) {
    TStringBuf file;
    TStringBuf tail;
    if (!message.TrySplit(':', file, tail)) {
        return TString(message);
    }

    TStringBuf line;
    TStringBuf rest;
    if (!tail.TrySplit(':', line, rest) || line.empty()) {
        return TString(message);
    }

    for (char c : line) {
        if (c < '0' || c > '9') {
            return TString(message);
        }
    }

    if (!rest.empty() && rest[0] == ' ') {
        rest = rest.SubStr(1);
    }
    return TString(rest);
}

[[noreturn]] void ThrowYamlSyntaxError(TStringBuf source, const YAML::Exception& e) {
    TStringBuilder msg;
    msg << "Failed to parse " << EffectiveYamlSource(source);
    if (!e.mark.is_null()) {
        msg << " at line " << e.mark.line + 1 << ", column " << e.mark.column + 1;
    }
    msg << ": " << e.msg;
    throw TInitializationException("YDB-CFG01") << msg;
}

[[noreturn]] void ThrowUnknownYamlFieldsError(TStringBuf source, const NYamlConfig::TBasicUnknownFieldsCollector& collector) {
    const auto& unknownKeys = collector.GetUnknownKeys();
    TStringBuilder msg;
    if (unknownKeys.size() == 1) {
        const auto& [path, info] = *unknownKeys.begin();
        msg << "Unknown field " << info.first.Quote() << " in " << EffectiveYamlSource(source);
        if (path) {
            msg << " at path " << path.Quote();
        }
    } else {
        msg << "Unknown fields in " << EffectiveYamlSource(source) << ": ";
        size_t shown = 0;
        for (const auto& [path, _] : unknownKeys) {
            if (shown) {
                msg << ", ";
            }
            msg << path.Quote();
            ++shown;
            if (shown == 3 && unknownKeys.size() > shown) {
                msg << ", ...";
                break;
            }
        }
    }
    throw TInitializationException("YDB-CFG02") << msg;
}

[[noreturn]] void ThrowJsonToProtoError(TStringBuf source, const NYamlConfig::TBasicUnknownFieldsCollector& collector, const yexception& e) {
    if (!collector.GetUnknownKeys().empty()) {
        ThrowUnknownYamlFieldsError(source, collector);
    }

    TStringBuilder msg;
    msg << "Invalid value in " << EffectiveYamlSource(source);
    if (const TString currentPath = collector.GetCurrentPath(); currentPath) {
        msg << " at path " << currentPath.Quote();
    }
    msg << ": " << StripSourceLocationPrefix(e.AsStrBuf());
    throw TInitializationException("YDB-CFG03") << msg;
}

[[noreturn]] void ThrowInvalidConfigurationError(TStringBuf source, const yexception& e) {
    TStringBuilder msg;
    msg << "Invalid configuration in " << EffectiveYamlSource(source) << ": " << StripSourceLocationPrefix(e.AsStrBuf());
    throw TInitializationException("YDB-CFG04") << msg;
}

} // anonymous namespace

NJson::TJsonValue LoadYamlAsJsonOrThrow(const TString& config, TStringBuf source) {
    try {
        return NKikimr::NYaml::Yaml2Json(YAML::Load(config), true);
    } catch (const YAML::Exception& e) {
        ThrowYamlSyntaxError(source, e);
    } catch (const yexception& e) {
        TStringBuilder msg;
        msg << "Failed to parse " << EffectiveYamlSource(source) << ": " << StripSourceLocationPrefix(e.AsStrBuf());
        throw TInitializationException("YDB-CFG05") << msg;
    }
}

void ParseJsonConfigOrThrow(const NJson::TJsonValue& json, TStringBuf source, NKikimrConfig::TAppConfig& config) {
    const bool hasMetadataConfig = json.Has("metadata") && json.Has("config") && json["config"].IsMap();
    TSimpleSharedPtr<NYamlConfig::TBasicUnknownFieldsCollector> collector =
        new NYamlConfig::TBasicUnknownFieldsCollector(hasMetadataConfig ? "config" : "");
    NYaml::EParsePhase phase = NYaml::EParsePhase::JsonToProto;

    try {
        NKikimr::NYaml::Parse(json, NKikimr::NYaml::GetJsonToProtoConfig(true, collector), config, true, &phase);
    } catch (const yexception& e) {
        if (phase == NYaml::EParsePhase::JsonToProto) {
            ThrowJsonToProtoError(source, *collector, e);
        } else {
            ThrowInvalidConfigurationError(source, e);
        }
    }

    if (!collector->GetUnknownKeys().empty()) {
        ThrowUnknownYamlFieldsError(source, *collector);
    }
}

} // namespace NKikimr::NConfig
