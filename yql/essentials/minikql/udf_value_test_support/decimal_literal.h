#pragma once

#include <yql/essentials/public/decimal/yql_decimal.h>

#include <util/system/types.h>

namespace NYql::NUdf::NTest {

template <ui8 Precision, ui8 Scale>
struct TDecimalLiteral {
    NYql::NDecimal::TInt128 Value{};
};

} // namespace NYql::NUdf::NTest
