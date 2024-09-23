#include "scheme_type_info.h"

#include <util/string/printf.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/type_desc.h>

namespace NKikimr::NScheme {

::TString TypeName(const TTypeInfo typeInfo, const ::TString& typeMod) {
    switch (typeInfo.GetTypeId()) {
    case NScheme::NTypeIds::Pg:
        return NPg::PgTypeNameFromTypeDesc(typeInfo.GetPgTypeDesc(), typeMod);
    case NScheme::NTypeIds::Decimal: {
        const TDecimalType decimal = typeInfo.GetDecimalType();
        if (decimal.GetPrecision() == DECIMAL_PRECISION && decimal.GetScale() == DECIMAL_SCALE)
            return "Decimal";
        return Sprintf("Decimal(%u,%u)", decimal.GetPrecision(), decimal.GetScale());
    }
    default:
        return TypeName(typeInfo.GetTypeId());
    }
}

} // namespace NKikimr::NScheme
