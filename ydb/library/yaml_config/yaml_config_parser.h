#pragma once

#include <string>
#include <optional>

#include <google/protobuf/message.h>

#include <util/generic/algorithm.h>
#include <library/cpp/yaml/as/tstring.h>

#include <contrib/libs/yaml-cpp/include/yaml-cpp/yaml.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <ydb/core/protos/config.pb.h>


namespace NKikimr::NYaml {
    struct TTransformContext {
        bool DisableBuiltinSecurity;
        bool ExplicitEmptyDefaultGroups;
        bool ExplicitEmptyDefaultAccess;
    };

    NJson::TJsonValue Yaml2Json(const YAML::Node& yaml, bool isRoot);

    NKikimrBlobStorage::TConfigRequest BuildInitDistributedStorageCommand(const TString& data);

    void ExtractExtraFields(NJson::TJsonValue& json, TTransformContext& ctx);

    void TransformJsonConfig(NJson::TJsonValue& config, bool relaxed = false);
    void TransformProtoConfig(const TTransformContext& ctx, NKikimrConfig::TAppConfig& config, bool relaxed = false);

    NKikimrConfig::TAppConfig Parse(const TString& data);
}
