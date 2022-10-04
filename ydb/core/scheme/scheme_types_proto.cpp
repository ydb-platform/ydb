#include "scheme_types_proto.h"
#include "scheme_tablecell.h"

namespace NKikimr::NScheme {

TProtoColumnType ProtoColumnTypeFromTypeInfo(const TTypeInfo typeInfo) {
    TProtoColumnType columnType;
    columnType.TypeId = (ui32)typeInfo.GetTypeId();
    if (typeInfo.GetTypeId() == NTypeIds::Pg) {
        Y_VERIFY(typeInfo.GetTypeDesc(), "no pg type descriptor");
        columnType.TypeInfo = NKikimrProto::TTypeInfo();
        columnType.TypeInfo->SetPgTypeId(NPg::PgTypeIdFromTypeDesc(typeInfo.GetTypeDesc()));
    }
    return columnType;
}

TTypeInfo TypeInfoFromProtoColumnType(ui32 typeId, const NKikimrProto::TTypeInfo* typeInfo) {
    auto type = (TTypeId)typeId;
    if (type == NTypeIds::Pg) {
        Y_VERIFY(typeInfo);
        return TTypeInfo(type, NPg::TypeDescFromPgTypeId(typeInfo->GetPgTypeId()));
    }
    return TTypeInfo(type);
}

} // namespace NKikimr::NScheme
