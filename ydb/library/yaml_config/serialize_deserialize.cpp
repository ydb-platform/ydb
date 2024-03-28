#include <ydb/library/yaml_config/yaml_config_parser.h>

#include <ydb/core/protos/bootstrap.pb.h>
#include <ydb/core/protos/blobstorage_base3.pb.h>

#include <google/protobuf/descriptor.h>

#include <util/generic/string.h>
#include <util/string/cast.h>

Y_DECLARE_OUT_SPEC(, NKikimr::NYaml::TCombinedDiskInfoKey, stream, value) {
    stream << "{" << value.Group << "," << value.Ring << "," << value.FailDomain << "," << value.VDiskLocation << "}";
}

template <>
bool TryFromStringImpl<::NKikimrConfig::TBootstrap_ETabletType, char>(const char* value, size_t size, NKikimrConfig::TBootstrap_ETabletType& res) {
    const google::protobuf::EnumDescriptor *descriptor = ::NKikimrConfig::TBootstrap_ETabletType_descriptor();
    const auto* valueDescriptor = descriptor->FindValueByName(TString(value, size));
    if (valueDescriptor) {
        res = static_cast<NKikimrConfig::TBootstrap_ETabletType>(valueDescriptor->number());
        return true;
    }
    return false;
}

template <>
bool TryFromStringImpl<::NKikimrBlobStorage::EPDiskType, char>(const char* value, size_t size, NKikimrBlobStorage::EPDiskType& res) {
    const google::protobuf::EnumDescriptor *descriptor = ::NKikimrBlobStorage::EPDiskType_descriptor();
    const auto* valueDescriptor = descriptor->FindValueByName(TString(value, size));
    if (valueDescriptor) {
        res = static_cast<NKikimrBlobStorage::EPDiskType>(valueDescriptor->number());
        return true;
    }
    return false;
}
