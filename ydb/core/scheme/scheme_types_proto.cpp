#include "scheme_types_proto.h"
#include <ydb/library/yql/parser/pg_wrapper/interface/type_desc.h>

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
        columnType.TypeInfo = NKikimrProto::TTypeInfo();
        ProtoFromTypeInfo(typeInfo, {}, *columnType.TypeInfo);
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

NKikimrProto::TTypeInfo DefaultDecimalProto() {
    NKikimrProto::TTypeInfo typeInfoProto;
    typeInfoProto.SetDecimalPrecision(NScheme::DECIMAL_PRECISION);
    typeInfoProto.SetDecimalScale(NScheme::DECIMAL_SCALE);
    return typeInfoProto;
}


void ProtoFromTypeInfo(const NScheme::TTypeInfo& typeInfo, const TProtoStringType& typeMod, ::NKikimrProto::TTypeInfo& typeInfoProto) {
    Y_ABORT_UNLESS(NTypeIds::IsParametrizedType(typeInfo.GetTypeId()), "Unexpected typeId %u", typeInfo.GetTypeId());

    switch (typeInfo.GetTypeId()) {
    case NScheme::NTypeIds::Pg: {
        typeInfoProto.SetPgTypeId(NPg::PgTypeIdFromTypeDesc(typeInfo.GetPgTypeDesc()));
        typeInfoProto.SetPgTypeMod(typeMod);
        break;;
    }
    case NScheme::NTypeIds::Decimal: {
        const NScheme::TDecimalType& decimal = typeInfo.GetDecimalType();
        typeInfoProto.SetDecimalPrecision(decimal.GetPrecision());
        typeInfoProto.SetDecimalScale(decimal.GetScale());
        break;
    }
    default:
        Y_ABORT_UNLESS(false, "Unexpected typeId %u", typeInfo.GetTypeId());
    }  
}

NScheme::TTypeInfo TypeInfoFromProto(NScheme::TTypeId typeId, const ::NKikimrProto::TTypeInfo& typeInfoProto) {
    switch (typeId) {
    case NScheme::NTypeIds::Pg: {
        Y_ABORT_UNLESS(typeInfoProto.HasPgTypeId());
        return NScheme::TTypeInfo(NPg::TypeDescFromPgTypeId(typeInfoProto.GetPgTypeId()));
    }
    case NScheme::NTypeIds::Decimal: {
        Y_ABORT_UNLESS(typeInfoProto.HasDecimalPrecision());
        Y_ABORT_UNLESS(typeInfoProto.HasDecimalScale());
        NScheme::TDecimalType decimal(typeInfoProto.GetDecimalPrecision(), typeInfoProto.GetDecimalScale());
        return NScheme::TTypeInfo(decimal);
    }
    default:
        return NScheme::TTypeInfo(typeId);
    }
}

void ProtoFromDecimalType(const NScheme::TDecimalType& decimal, ::Ydb::DecimalType& decimalProto) {
    decimalProto.set_precision(decimal.GetPrecision());
    decimalProto.set_scale(decimal.GetScale());
}

NScheme::TDecimalType DecimalTypeFromProto(const ::Ydb::DecimalType& decimalProto) {
    return NScheme::TDecimalType(decimalProto.precision(), decimalProto.scale());
}

} // namespace NKikimr::NScheme
