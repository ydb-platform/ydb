#include "scheme_types_proto.h"

#include <util/generic/yexception.h>
#include <util/string/builder.h>


namespace NKikimr::NScheme {

TProtoColumnType ProtoColumnTypeFromTypeInfoMod(const TTypeInfo typeInfo, const ::TString&) {
    TProtoColumnType columnType;
    columnType.TypeId = (ui32)typeInfo.GetTypeId();
    switch (typeInfo.GetTypeId()) {
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


void ProtoFromTypeInfo(const NScheme::TTypeInfo& typeInfo, const TProtoStringType&, ::NKikimrProto::TTypeInfo& typeInfoProto) {
    Y_ENSURE(NTypeIds::IsParametrizedType(typeInfo.GetTypeId()), "Unexpected typeId " << typeInfo.GetTypeId());

    switch (typeInfo.GetTypeId()) {
    case NScheme::NTypeIds::Decimal: {
        const NScheme::TDecimalType& decimal = typeInfo.GetDecimalType();
        typeInfoProto.SetDecimalPrecision(decimal.GetPrecision());
        typeInfoProto.SetDecimalScale(decimal.GetScale());
        break;
    }
    default:
        Y_ENSURE(false, "Unexpected typeId " << typeInfo.GetTypeId());
    }  
}

NScheme::TTypeInfo TypeInfoFromProto(NScheme::TTypeId typeId, const ::NKikimrProto::TTypeInfo& typeInfoProto) {
    switch (typeId) {
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
    } else {
        error = TStringBuilder() << "Unexpected type, got proto: " << typeProto.ShortDebugString();
        return false;
    }
}

void ProtoFromTypeInfo(const NScheme::TTypeInfo& typeInfo, ::Ydb::Type& typeProto, bool notNull) {
    switch (typeInfo.GetTypeId()) {
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

void ProtoFromDecimalType(const NScheme::TDecimalType& decimal, ::Ydb::DecimalType& decimalProto) {
    decimalProto.set_precision(decimal.GetPrecision());
    decimalProto.set_scale(decimal.GetScale());
}

} // namespace NKikimr::NScheme
