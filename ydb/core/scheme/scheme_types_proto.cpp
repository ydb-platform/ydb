#include "scheme_types_proto.h"
#include "scheme_tablecell.h"

namespace NKikimr::NScheme {

TProtoColumnType ProtoColumnTypeFromTypeInfoMod(const TTypeInfo typeInfo, const ::TString& typeMod) {
    TProtoColumnType columnType;
    columnType.TypeId = (ui32)typeInfo.GetTypeId();
    if (typeInfo.GetTypeId() == NTypeIds::Pg) {
        Y_ABORT_UNLESS(typeInfo.GetPgTypeDesc(), "no pg type descriptor");
        columnType.TypeInfo = NKikimrProto::TTypeInfo();
        columnType.TypeInfo->SetPgTypeId(NPg::PgTypeIdFromTypeDesc(typeInfo.GetPgTypeDesc()));
        if (typeMod) {
            columnType.TypeInfo->SetPgTypeMod(typeMod);
        }
    }
    return columnType;
}

TTypeInfoMod TypeInfoModFromProtoColumnType(ui32 typeId, const NKikimrProto::TTypeInfo* typeInfo) {
    auto type = (TTypeId)typeId;
    if (type == NTypeIds::Pg) {
        Y_ABORT_UNLESS(typeInfo, "no type info for pg type");
        TTypeInfoMod res;
        res.TypeInfo = TTypeInfo(type, NPg::TypeDescFromPgTypeId(typeInfo->GetPgTypeId()));
        if (typeInfo->HasPgTypeMod()) {
            res.TypeMod = typeInfo->GetPgTypeMod();
        }
        return res;
    }
    return {TTypeInfo(type), ""};
}

} // namespace NKikimr::NScheme
