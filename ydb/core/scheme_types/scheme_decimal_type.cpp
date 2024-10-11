#include "scheme_decimal_type.h"

#include <ydb/library/yql/public/decimal/yql_decimal.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/system/rwlock.h>

#include <regex>

namespace NKikimr::NScheme {

TDecimalType::TDecimalType(ui32 precision, ui32 scale)
    : Precision(precision)
    , Scale(scale)
{
    TString error;
    Y_ABORT_UNLESS(Validate(precision, scale, error), "%s", error.c_str());
}

bool TDecimalType::operator==(const TDecimalType& other) const {
    return Precision == other.Precision && Scale == other.Scale;
} 

TString TDecimalType::CellValueToString(const std::pair<ui64, i64>& cellValue) const {
    return NYql::NDecimal::ToString(NYql::NDecimal::FromHalfs(cellValue.first, cellValue.second),
        Precision, Scale);
}

void TDecimalType::CellValueToStream(const std::pair<ui64, i64>& cellValue, IOutputStream& out) const {
    out << NYql::NDecimal::ToString(NYql::NDecimal::FromHalfs(cellValue.first, cellValue.second),
        Precision, Scale);
}    

const std::optional<TDecimalType> TDecimalType::ParseTypeName(const TStringBuf& typeName) {
    if (strcasecmp(typeName.data(), "decimal") == 0) {
        return TDecimalType::Default();
    } else {
        static const std::regex regex("decimal\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)", std::regex_constants::icase);
        std::smatch match;
        if (std::regex_search(typeName.data(), match, regex)) {
            ui32 precision = FromString<ui32>(match[1].str());
            ui32 scale = FromString<ui32>(match[2].str());

            if (precision > DECIMAL_MAX_PRECISION)
                return {};
            if (scale > precision)
                return {};

            return TDecimalType(precision, scale);
        } else {
            return {};
        }
    }
}

bool TDecimalType::Validate(ui32 precision, ui32 scale, TString& error) {
    if (precision == 0) {
        error = Sprintf("Decimal precision should not be zero");
        return false;
    }
    if (precision > NKikimr::NScheme::DECIMAL_MAX_PRECISION) {
        error = Sprintf("Decimal precision %u should be less than %u", precision, NKikimr::NScheme::DECIMAL_MAX_PRECISION);
        return false;
    }
    if (scale > precision) {
        error = Sprintf("Decimal precision %u should be more than scale %u", precision, scale);
        return false;
    }
    return true;
}

} // namespace NKikimr::NScheme

