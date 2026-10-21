#pragma once

#include <arrow/scalar.h>
#include <util/system/types.h>

namespace NYql::NUdf {

// Arrow's DenseUnionScalar doesn't store type_code in this Arrow version, only the child scalar value.
// TDenseUnionScalar extends it with an explicit Index field so callers can recover the active alternative
// without ambiguity (unlike type-matching, which fails for Variant<T, T>).
class TDenseUnionScalar final: public arrow::DenseUnionScalar {
public:
    TDenseUnionScalar(std::shared_ptr<arrow::Scalar> value,
                      ui32 index,
                      std::shared_ptr<arrow::DataType> type)
        : arrow::DenseUnionScalar(std::move(value), std::move(type))
        , Index(index)
    {
    }

    const ui32 Index;
};

} // namespace NYql::NUdf
