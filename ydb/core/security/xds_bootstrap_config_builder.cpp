#include "xds_bootstrap_config_builder.h"

#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/json_reader.h>

namespace NKikimr {

TXdsBootstrapConfigBuilder::TXdsBootstrapConfigBuilder(const NKikimrProto::TXdsBootstrap& config, const TString& dataCenterId, const TString& nodeId)
    : Config(config)
    , DataCenterId(dataCenterId)
    , NodeId(nodeId)
{}

TString TXdsBootstrapConfigBuilder::Build() const {
    NJson::TJsonValue xdsBootstrapConfigJson;
    NProtobufJson::Proto2Json(Config, xdsBootstrapConfigJson, {.FieldNameMode = NProtobufJson::TProto2JsonConfig::FldNameMode::FieldNameSnakeCaseDense});
    BuildFieldNode(&xdsBootstrapConfigJson);
    BuildFieldXdsServers(&xdsBootstrapConfigJson);
    return NJson::WriteJson(xdsBootstrapConfigJson);
}

void TXdsBootstrapConfigBuilder::BuildFieldNode(NJson::TJsonValue* json) const {
    NJson::TJsonValue& nodeJson = (*json)["node"];
    if (Config.GetNode().HasMeta()) {
        nodeJson.EraseValue("meta");
        NJson::TJsonValue metadataJson;
        NJson::TJsonReaderConfig jsonConfig;
        if (NJson::ReadJsonTree(Config.GetNode().GetMeta(), &jsonConfig, &metadataJson)) {
            nodeJson["metadata"] = metadataJson;
        }
    }
    if (!Config.GetNode().HasId()) {
        nodeJson["id"] = NodeId;
    }
    if (!Config.GetNode().GetLocality().HasZone()) {
        nodeJson["locality"]["zone"] = DataCenterId;
    }
}

void TXdsBootstrapConfigBuilder::BuildFieldXdsServers(NJson::TJsonValue* json) const {
    NJson::TJsonValue& xdsServersJson = *json;
    NJson::TJsonValue::TArray xdsServers;
    xdsServersJson["xds_servers"].GetArray(&xdsServers);
    xdsServersJson.EraseValue("xds_servers");
    for (auto& xdsServerJson : xdsServers) {
        NJson::TJsonValue::TArray channelCreds;
        xdsServerJson["channel_creds"].GetArray(&channelCreds);
        xdsServerJson.EraseValue("channel_creds");
        for (auto& channelCredJson : channelCreds) {
            NJson::TJsonValue typeConfigJson;
            NJson::TJsonReaderConfig jsonConfig;
            if (NJson::ReadJsonTree(channelCredJson["config"].GetString(), &jsonConfig, &typeConfigJson)) {
                channelCredJson["config"] = typeConfigJson;
            }
            xdsServerJson["channel_creds"].AppendValue(channelCredJson);
        }
        xdsServersJson["xds_servers"].AppendValue(xdsServerJson);
    }
}

} // NKikimr
