#pragma once

#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/scheme/protos/type_info.pb.h>

namespace NKikimr::NScheme {

struct TProtoColumnType {
    ui32 TypeId = 0;
    std::optional<NKikimrProto::TTypeInfo> TypeInfo;
};

TProtoColumnType ProtoColumnTypeFromTypeInfoMod(const TTypeInfo typeInfo, const ::TString& typeMod);

struct TTypeInfoMod {
    TTypeInfo TypeInfo;
    ::TString TypeMod;
};

TTypeInfoMod TypeInfoModFromProtoColumnType(ui32 typeId, const NKikimrProto::TTypeInfo* typeInfo);

} // namespace NKikimr::NScheme
