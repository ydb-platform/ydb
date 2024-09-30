#include "scheme_decimal_type.h"

#include <ydb/library/yql/public/decimal/yql_decimal.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/system/rwlock.h>

#include <regex>

namespace NKikimr::NScheme {

TString TDecimalType::CellValueToString(const std::pair<ui64, i64>& cellValue) const {
    return NYql::NDecimal::ToString(NYql::NDecimal::FromHalfs(cellValue.first, cellValue.second),
        Precision, Scale);
}

void TDecimalType::CellValueToStream(const std::pair<ui64, i64>& cellValue, IOutputStream& out) const {
    out << NYql::NDecimal::ToString(NYql::NDecimal::FromHalfs(cellValue.first, cellValue.second),
        Precision, Scale);
}    

const std::optional<TDecimalType> TDecimalType::ParseTypeName(const TStringBuf& typeName) {
    ui32 precision = 0;
    ui32 scale = 0;
    if (strcasecmp(typeName.data(), "decimal") == 0) {
        precision = DECIMAL_PRECISION;
        scale = DECIMAL_SCALE;
    } else {
        static const std::regex regex("decimal\\((\\d+),(\\d+)\\)", std::regex_constants::icase);
        std::smatch match;
        if (std::regex_search(typeName.data(), match, regex)) {
            precision = FromString<ui32>(match[1].str());
            scale = FromString<ui32>(match[2].str());
        }
    }
    if (precision == 0 || scale == 0) {
        return {};
    }
    return TDecimalType(precision, scale);
}

} // namespace NKikimr::NScheme

