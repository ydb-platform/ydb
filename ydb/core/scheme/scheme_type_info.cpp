#include "scheme_type_info.h"

#include <util/string/printf.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/type_desc.h>

namespace NKikimr::NScheme {

::TString TypeName(const TTypeInfo typeInfo, const ::TString& typeMod) {
    switch (typeInfo.GetTypeId()) {
    case NScheme::NTypeIds::Pg:
        return NPg::PgTypeNameFromTypeDesc(typeInfo.GetPgTypeDesc(), typeMod);
    case NScheme::NTypeIds::Decimal: {
        const TDecimalType& decimal = typeInfo.GetDecimalType();
        return Sprintf("Decimal(%u,%u)", decimal.GetPrecision(), decimal.GetScale());
    }
    default:
        return TypeName(typeInfo.GetTypeId());
    }
}

bool GetTypeInfo(const NScheme::IType* type, const NKikimrProto::TTypeInfo& typeInfoProto, const TStringBuf& typeName, const TStringBuf& columnName, NScheme::TTypeInfo &typeInfo, ::TString& errorStr) {
    if (type) {
        // Only allow YQL types
        if (!NScheme::NTypeIds::IsYqlType(type->GetTypeId())) {
            errorStr = Sprintf("Type '%s' specified for column '%s' is no longer supported", typeName.data(), columnName.data());
            return false;
        }
        typeInfo = NScheme::TTypeInfo(type->GetTypeId());
        return true;
    } else if (const auto decimalType = NScheme::TDecimalType::ParseTypeName(typeName)) {
        if (typeInfoProto.HasDecimalPrecision() && typeInfoProto.HasDecimalScale()) {
            typeInfo = NScheme::TypeInfoFromProto(NScheme::NTypeIds::Decimal, typeInfoProto);
        } else {
            typeInfo = *decimalType;
        }
        return true;
    } else if (const auto pgTypeDesc = NPg::TypeDescFromPgTypeName(typeName)) {
        if (typeInfoProto.HasPgTypeId()) {
            typeInfo = NScheme::TypeInfoFromProto(NScheme::NTypeIds::Pg, typeInfoProto);
        } else {
            typeInfo = pgTypeDesc;
        }
        return true;
    } else {
        errorStr = Sprintf("Type '%s' specified for column '%s' is not supported by storage", typeName.data(), columnName.data());
        return false;
    }
}

} // namespace NKikimr::NScheme
