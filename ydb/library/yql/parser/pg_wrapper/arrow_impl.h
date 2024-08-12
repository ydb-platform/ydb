#pragma once

#include <arrow/array.h>
#include <arrow/array/builder_binary.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/arrow.h>

extern "C" {
#include "utils/numeric.h"
}

namespace NYql {

Numeric Uint64ToPgNumeric(ui64 value);
Numeric DecimalToPgNumeric(const NUdf::TUnboxedValuePod& value, ui8 precision, ui8 scale);
Numeric DyNumberToPgNumeric(const NUdf::TUnboxedValuePod& value);
Numeric PgFloatToNumeric(double item, ui64 scale, int digits);
Numeric PgDecimal128ToNumeric(arrow::Decimal128 val, int32_t precision, int32_t scale, Numeric high_bits_mul);
TColumnConverter BuildPgColumnConverter(const std::shared_ptr<arrow::DataType>& originalType, NKikimr::NMiniKQL::TPgType* targetType);

template<typename T>
std::shared_ptr<arrow::Array> PgConvertNumeric(const std::shared_ptr<arrow::Array>& value) {
    TArenaMemoryContext arena;
    const auto& data = value->data();
    size_t length = data->length;
    arrow::BinaryBuilder builder;
    auto input = data->GetValues<T>(1);
    for (size_t i = 0; i < length; ++i) {
        if (value->IsNull(i)) {
            ARROW_OK(builder.AppendNull());
            continue;
        }
        T item = input[i];
        Numeric v;
        if constexpr(std::is_same_v<T, double>) {
            v = PgFloatToNumeric(item, 1000000000000LL, 12);
        } else if constexpr(std::is_same_v<T, float>) {
            v = PgFloatToNumeric(item, 1000000LL, 6);
        } else {
            v = int64_to_numeric(item);
        }
        auto datum = NumericGetDatum(v);
        auto ptr = (char*)datum;
        auto len = GetFullVarSize((const text*)datum);
        NUdf::ZeroMemoryContext(ptr);
        ARROW_OK(builder.Append(ptr - sizeof(void*), len + sizeof(void*)));
    }

    std::shared_ptr<arrow::BinaryArray> ret;
    ARROW_OK(builder.Finish(&ret));
    return ret;
}


std::shared_ptr<arrow::Array> PgDecimal128ConvertNumeric(const std::shared_ptr<arrow::Array>& value, int32_t precision, int32_t scale);

}

