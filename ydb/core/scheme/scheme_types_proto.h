#pragma once

#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/protos/type_info.pb.h>

namespace NKikimr::NScheme {

struct TProtoColumnType {
    ui32 TypeId = 0;
    std::optional<NKikimrProto::TTypeInfo> TypeInfo;
};

TProtoColumnType ProtoColumnTypeFromTypeInfo(const TTypeInfo typeInfo);

TTypeInfo TypeInfoFromProtoColumnType(ui32 typeId, const NKikimrProto::TTypeInfo* typeInfo);

} // namespace NKikimr::NScheme
