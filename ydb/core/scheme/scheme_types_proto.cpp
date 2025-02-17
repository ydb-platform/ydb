#include "scheme_types_proto.h"

#include <util/string/printf.h>
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
        ui32 precision, scale;
        if (!typeInfo) {
            precision = DECIMAL_PRECISION;
            scale = DECIMAL_SCALE;
        } else {
            precision = typeInfo->GetDecimalPrecision();
            scale = typeInfo->GetDecimalScale();
        }
        TTypeInfoMod res = {{TDecimalType(precision, scale)}, {}};
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
        break;
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
        if (!typeInfoProto.HasDecimalPrecision() || !typeInfoProto.HasDecimalScale()) {
            return NScheme::TTypeInfo(NScheme::TDecimalType::Default());
        }
        NScheme::TDecimalType decimal(typeInfoProto.GetDecimalPrecision(), typeInfoProto.GetDecimalScale());
        return NScheme::TTypeInfo(decimal);
    }
    default:
        return NScheme::TTypeInfo(typeId);
    }
}

bool TypeInfoFromProto(const ::Ydb::Type& typeProto, TTypeInfoMod& typeInfoMod, TString& error) {
    if (typeProto.has_type_id()) {
        typeInfoMod = {NScheme::TTypeInfo(typeProto.type_id()), {}};
        return true;
    } else if (typeProto.has_decimal_type()) {
        ui32 precision = typeProto.decimal_type().precision();
        ui32 scale = typeProto.decimal_type().scale();
        if (!NScheme::TDecimalType::Validate(precision, scale, error)) {
            return false;
        }
        typeInfoMod = {NScheme::TTypeInfo(TDecimalType(precision, scale)), {}};
        return true;
    } else if (typeProto.has_pg_type()) {
        const auto& typeName = typeProto.pg_type().type_name();
        auto pgDesc = NPg::TypeDescFromPgTypeName(typeName);
        if (!pgDesc) {
            error = Sprintf("Unknown pg type %s", typeName.c_str());
            return false;
        }

        typeInfoMod = {NScheme::TTypeInfo(pgDesc), typeProto.pg_type().type_modifier()};
        return true;
    } else {
        error = Sprintf("Unexpected type, got proto: %s", typeProto.ShortDebugString().c_str());
        return false;
    }
}

void ProtoFromTypeInfo(const NScheme::TTypeInfo& typeInfo, ::Ydb::Type& typeProto, bool notNull) {
    switch (typeInfo.GetTypeId()) {
    case NScheme::NTypeIds::Pg: {
        ProtoFromPgType(typeInfo.GetPgTypeDesc(), *typeProto.mutable_pg_type());
        break;
    }
    case NScheme::NTypeIds::Decimal: {
        Ydb::Type& item = notNull ? typeProto : *typeProto.mutable_optional_type()->mutable_item();
        ProtoFromDecimalType(typeInfo.GetDecimalType(), *item.mutable_decimal_type());
        break;
    }
    default: {
        Ydb::Type& item = notNull ? typeProto : *typeProto.mutable_optional_type()->mutable_item();
        item.set_type_id((Ydb::Type::PrimitiveTypeId)typeInfo.GetTypeId());
        break;
    }
    }    
}

void ProtoFromPgType(const NKikimr::NPg::ITypeDesc* pgDesc, ::Ydb::PgType& pgProto) {
    pgProto.set_type_name(NPg::PgTypeNameFromTypeDesc(pgDesc));
    pgProto.set_oid(NPg::PgTypeIdFromTypeDesc(pgDesc));                    
}

void ProtoFromDecimalType(const NScheme::TDecimalType& decimal, ::Ydb::DecimalType& decimalProto) {
    decimalProto.set_precision(decimal.GetPrecision());
    decimalProto.set_scale(decimal.GetScale());
}

} // namespace NKikimr::NScheme
