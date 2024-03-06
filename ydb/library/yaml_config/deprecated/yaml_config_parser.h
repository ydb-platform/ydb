#pragma once

#include <string>
#include <google/protobuf/message.h>

#include <util/generic/algorithm.h>
#include <library/cpp/yaml/as/tstring.h>

#include <contrib/libs/yaml-cpp/include/yaml-cpp/yaml.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/config.pb.h>


namespace NKikimr::NYaml::NDeprecated {
    NJson::TJsonValue Yaml2Json(const YAML::Node& yaml, bool isRoot);

    NKikimrBlobStorage::TConfigRequest BuildInitDistributedStorageCommand(const TString& data);

    void TransformConfig(NJson::TJsonValue& config, bool relaxed = false);

    void Parse(const TString& data, NKikimrConfig::TAppConfig& config, bool needsTransforming = true);
}
