#pragma once

#include <ydb/core/protos/config.pb.h>
#include <ydb/library/yaml_config/protos/config.pb.h>

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/yaml/as/tstring.h>

#include <google/protobuf/message.h>

#include <util/generic/string.h>

#include <map>

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
        std::map<ui32, TString> GroupErasureSpecies;
    };

    NProtobufJson::TJson2ProtoConfig GetJsonToProtoConfig(
        bool allowUnknownFields = false,
        TSimpleSharedPtr<NProtobufJson::IUnknownFieldsCollector> unknownFieldsCollector = nullptr);

    NKikimrBlobStorage::TConfigRequest BuildInitDistributedStorageCommand(const TString& data);

    void ExtractExtraFields(NJson::TJsonValue& json, TTransformContext& ctx);
    void ClearEphemeralFields(NJson::TJsonValue& json);
    void ClearNonEphemeralFields(NJson::TJsonValue& json);

    void TransformProtoConfig(TTransformContext& ctx, NKikimrConfig::TAppConfig& config, NKikimrConfig::TEphemeralInputFields& ephemeralConfig, bool relaxed = false);

    void Parse(const NJson::TJsonValue& json, NProtobufJson::TJson2ProtoConfig convertConfig, NKikimrConfig::TAppConfig& config, bool transform, bool relaxed = false);
    NKikimrConfig::TAppConfig Parse(const TString& data, bool transform = true);

} // namespace NKikimr::NYaml
