#pragma once

#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/scheme/protos/type_info.pb.h>
#include <ydb/core/scheme_types/scheme_decimal_type.h>
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/type_desc.h>

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

NKikimrProto::TTypeInfo DefaultDecimalProto();

void ProtoFromTypeInfo(const NScheme::TTypeInfo& typeInfo, const TProtoStringType& typeMod, ::NKikimrProto::TTypeInfo& typeInfoProto);

NScheme::TTypeInfo TypeInfoFromProto(NScheme::TTypeId typeId, const ::NKikimrProto::TTypeInfo& typeInfoProto);

void ProtoFromDecimalType(const NScheme::TDecimalType& decimal, ::Ydb::DecimalType& decimalProto);

} // namespace NKikimr::NScheme
