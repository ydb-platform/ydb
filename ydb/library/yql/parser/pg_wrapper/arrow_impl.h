#pragma once

#include <arrow/array.h>
#include <arrow/array/builder_binary.h>

extern "C" {
#include "utils/numeric.h"
}

namespace NYql {

Numeric PgFloatToNumeric(double item, ui64 scale, int digits);

template<typename T>
std::shared_ptr<arrow::Array> PgConvertNumeric(const std::shared_ptr<arrow::Array>& value) {
    TArenaMemoryContext arena;
    const auto& data = value->data();
    size_t length = data->length;
    arrow::BinaryBuilder builder;
    auto input = data->GetValues<T>(1);
    for (size_t i = 0; i < length; ++i) {
        if (value->IsNull(i)) {
            builder.AppendNull();
            continue;
        }
        T item = input[i];
        Numeric v;
        if constexpr(std::is_same_v<T,double>) {
            v = PgFloatToNumeric(item, 1000000000000LL, 12);
        } else if constexpr(std::is_same_v<T,float>) {
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

}

