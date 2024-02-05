#pragma once

#include <string>
#include <optional>
#include <map>

#include <google/protobuf/message.h>

#include <util/generic/algorithm.h>
#include <library/cpp/yaml/as/tstring.h>

#include <contrib/libs/yaml-cpp/include/yaml-cpp/yaml.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/yaml_config/new/protos/message.pb.h>


namespace NKikimr::NYaml {
    struct TCombinedDiskInfoKey {
        ui32 Group = 0;
        ui32 Ring = 0;
        ui32 FailDomain = 0;
        ui32 VDiskLocation = 0;

        auto operator<=>(const TCombinedDiskInfoKey&) const = default;
    };

    struct TTransformContext {
        bool DisableBuiltinSecurity;
        bool ExplicitEmptyDefaultGroups;
        bool ExplicitEmptyDefaultAccess;
        std::map<TCombinedDiskInfoKey, NKikimrConfig::TCombinedDiskInfo> CombinedDiskInfo;
    };

    NJson::TJsonValue Yaml2Json(const YAML::Node& yaml, bool isRoot);

    NKikimrBlobStorage::TConfigRequest BuildInitDistributedStorageCommand(const TString& data);

    void ExtractExtraFields(NJson::TJsonValue& json, TTransformContext& ctx);

    void TransformJsonConfig(NJson::TJsonValue& config, bool relaxed = false);
    void TransformProtoConfig(const TTransformContext& ctx, NKikimrConfig::TAppConfig& config, bool relaxed = false);

    NKikimrConfig::TAppConfig Parse(const TString& data);
}
