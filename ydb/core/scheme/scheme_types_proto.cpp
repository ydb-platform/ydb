#include "scheme_types_proto.h"
#include "scheme_tablecell.h"
#include <ydb/core/scheme_types/scheme_decimal_type.h>

namespace NKikimr::NScheme {

TProtoColumnType ProtoColumnTypeFromTypeInfoMod(const TTypeInfo typeInfo, const ::TString& typeMod) {
    TProtoColumnType columnType;
    columnType.TypeId = (ui32)typeInfo.GetTypeId();
    switch (typeInfo.GetTypeId()) {
    case NTypeIds::Pg: {
        Y_ABORT_UNLESS(typeInfo.GetPgTypeDesc(), "no pg type descriptor");
        columnType.TypeInfo = NKikimrProto::TTypeInfo();
        columnType.TypeInfo->SetPgTypeId(NPg::PgTypeIdFromTypeDesc(typeInfo.GetPgTypeDesc()));
        if (typeMod) {
            columnType.TypeInfo->SetPgTypeMod(typeMod);
        }
        break;
    }
    case NTypeIds::Decimal: {
        const TDecimalType decimalType = typeInfo.GetDecimalType();
        // TODO Uncomment after parametrized decimal in KQP
        //Y_ABORT_UNLESS(decimalType.GetPrecision() != 0 && decimalType.GetScale() != 0);
        columnType.TypeInfo = NKikimrProto::TTypeInfo();
        columnType.TypeInfo->SetDecimalPrecision(decimalType.GetPrecision());
        columnType.TypeInfo->SetDecimalScale(decimalType.GetScale());
        break;
    }
    }
    return columnType;
}

TTypeInfoMod TypeInfoModFromProtoColumnType(ui32 typeId, const NKikimrProto::TTypeInfo* typeInfo) {
    auto type = (TTypeId)typeId;
    switch (type) {
    case NTypeIds::Pg: {
        Y_ABORT_UNLESS(typeInfo, "no type info for pg type");
        TTypeInfoMod res = {
            .TypeInfo = {NPg::TypeDescFromPgTypeId(typeInfo->GetPgTypeId())},
            .TypeMod = (typeInfo->HasPgTypeMod() ? typeInfo->GetPgTypeMod() : TProtoStringType{})
        };
        return res;
    }
    case NTypeIds::Decimal: {
        Y_ABORT_UNLESS(typeInfo, "no type info for decimal type");
        TTypeInfoMod res = {
            .TypeInfo = {{typeInfo->GetDecimalPrecision(), typeInfo->GetDecimalScale()}},
            .TypeMod = {}
        };
        return res;
    }
    default: {
        return {TTypeInfo(type), {}};
    }
    }
}

} // namespace NKikimr::NScheme
