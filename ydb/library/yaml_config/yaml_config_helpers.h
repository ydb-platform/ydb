#pragma once

#include "yaml_config_parser.h"

#include <util/generic/string.h>
#include <ydb/core/base/domain.h>

#include <library/cpp/json/writer/json.h>

#include <ydb/library/yaml_config/protos/config.pb.h>
#include <ydb/library/fyamlcpp/fyamlcpp.h>
#include <library/cpp/protobuf/json/util.h>
#include <library/cpp/protobuf/json/json2proto.h>

#include <functional>
#include <memory>
#include <span>

namespace NKikimr::NYaml {

NJson::TJsonValue* Traverse(NJson::TJsonValue& json, const TStringBuf& path, TString* lastName = nullptr);

const NJson::TJsonValue* Traverse(const NJson::TJsonValue& json, const TStringBuf& path, TString* lastName = nullptr);

void Iterate(
    const NJson::TJsonValue& json,
    const std::span<TString>& pathPieces,
    std::function<void(const std::vector<ui32>&, const NJson::TJsonValue&)> onElem,
    std::vector<ui32>& offsets,
    size_t offsetId = 0);

void Iterate(const NJson::TJsonValue& json, const TStringBuf& path, std::function<void(const std::vector<ui32>&, const NJson::TJsonValue&)> onElem);

void IterateMut(
    NJson::TJsonValue& json,
    const std::span<TString>& pathPieces,
    std::function<void(const std::vector<ui32>&, NJson::TJsonValue&)> onElem,
    std::vector<ui32>& offsets,
    size_t offsetId = 0);

void IterateMut(NJson::TJsonValue& json, const TStringBuf& path, std::function<void(const std::vector<ui32>&, NJson::TJsonValue&)> onElem);

ui64 GetConfigHash(const TString& config);

// Default Nkikimr::NConfig::TOpaqueConfigParser implementation template.
// Parses YAML string into template-parameter specified proto message.
// Silently ignores unknown fields if allowUnknownFields=true.
//
// @param opaqueYamlConfig - YAML document or YAML subfield (with no "---" YAML document prefix)
// @param allowUnknownFields - silently ignore unknown fields (for specified Proto)
//
// @return
//  nullptr - on empty imput YAML
//  parsed Proto message - on successfull parsing
//
// @throw
//  std::exception - on underlying parser errors / unknown fields present (without allowUnknownFields)
//
// Usage (within end-node service):
//
//  NKikimr::NConfig::TConfigsDispatcherInitInfo initInfo;
//  auto parser = std::bind(NKikimr::NYaml::DefaultOpaqueConfigParser<UserNamespace::TPrivateDatabaseConfigProto>, std::placeholders::_1, true);
//  initInfo.OpaqueConfigParsers[NKikimrConsole::TConfigItem::PrivateConfigItem] = std::move(parser);
//  auto* dispatcher = NKikimr::NConsole::CreateConfigsDispatcher(initInfo);
//
template<typename Proto>
std::shared_ptr<const ::google::protobuf::Message> DefaultOpaqueConfigParser(const TString& opaqueYamlConfig, bool allowUnknownFields)
{
    const auto& subYaml = (opaqueYamlConfig.StartsWith("---") ? opaqueYamlConfig.substr(3) : opaqueYamlConfig);
    if ( subYaml.find_first_not_of(" \t\n\r") == TString::npos )
    {
        return nullptr;
    }

    auto msg = std::make_shared<Proto>();
    auto doc = NFyaml::TDocument::Parse(opaqueYamlConfig);

    TStringStream jsonStream;
    jsonStream << NFyaml::TJsonEmitter(doc.Root());

    NJson::TJsonValue json;
    Y_ENSURE(NJson::ReadJsonTree(jsonStream.Str(), &json));

    auto config = GetJsonToProtoConfig(allowUnknownFields);
    NProtobufJson::Json2Proto(json, *msg, config);

    return msg;
}

}
