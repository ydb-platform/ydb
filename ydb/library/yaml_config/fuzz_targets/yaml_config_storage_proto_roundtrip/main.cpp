#include <ydb/library/yaml_config/yaml_config_parser.h>
#include <ydb/library/yaml_json/yaml_to_json.h>

#include <library/cpp/protobuf/json/json2proto.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>
#include <contrib/libs/yaml-cpp/include/yaml-cpp/yaml.h>

#include <util/generic/string.h>

namespace {

bool LoadStorageJson(const TString& yamlText, NJson::TJsonValue& json) {
    YAML::Node yaml = YAML::Load(yamlText.c_str());
    json = NKikimr::NYaml::Yaml2Json(yaml, true);
    if (json.IsMap() && json.Has("config") && json["config"].IsMap()) {
        json = json["config"];
    }
    return json.IsMap();
}

void NormalizeStorageJson(NJson::TJsonValue& json) {
    if (!json.IsMap()) {
        return;
    }

    if (json.Has("host_configs") && !json.Has("host_config")) {
        json["host_config"] = json["host_configs"];
        json.EraseValue("host_configs");
    }
    if (json.Has("hosts") && !json.Has("host")) {
        json["host"] = json["hosts"];
        json.EraseValue("hosts");
    }
}

void FuzzStorageProtoRoundTrip(const TString& input) {
    TString storageYaml = input;

    try {
        YAML::Node root = YAML::Load(input.c_str());
        if (root.IsMap() && root["config"]) {
            storageYaml = YAML::Dump(root["config"]);
        }
    } catch (...) {
    }

    try {
        auto initRequest = NKikimr::NYaml::BuildInitDistributedStorageCommand(storageYaml);
        (void)initRequest.CommandSize();
        auto replaceRequest = NKikimr::NYaml::BuildReplaceDistributedStorageCommand(storageYaml);
        (void)replaceRequest.replace().size();
    } catch (...) {
    }

    try {
        NJson::TJsonValue json;
        if (!LoadStorageJson(storageYaml, json)) {
            return;
        }

        NormalizeStorageJson(json);

        NKikimrConfig::StorageConfig storageConfig;
        NProtobufJson::MergeJson2Proto(json, storageConfig, NKikimr::NYaml::GetJsonToProtoConfig());

        const TString yamlRoundTripped = NKikimr::NYaml::ParseProtoToYaml(storageConfig);

        NJson::TJsonValue reparsedJson;
        if (LoadStorageJson(yamlRoundTripped, reparsedJson)) {
            NormalizeStorageJson(reparsedJson);
            NKikimrConfig::StorageConfig reparsedStorageConfig;
            NProtobufJson::MergeJson2Proto(reparsedJson, reparsedStorageConfig, NKikimr::NYaml::GetJsonToProtoConfig());
            (void)NKikimr::NYaml::ParseProtoToYaml(reparsedStorageConfig);
        }
    } catch (...) {
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 64 * 1024) {
        return 0;
    }

    try {
        FuzzStorageProtoRoundTrip(TString(reinterpret_cast<const char*>(data), size));
    } catch (...) {
    }

    return 0;
}
