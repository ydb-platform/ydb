#pragma once

#include <arrow/array/data.h>
#include <arrow/type.h>

#include <util/generic/array_ref.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NYql::NUdf {

constexpr size_t DenseUnionMaxAlternativesCount = arrow::UnionType::kMaxTypeCode;

struct TDenseUnionChildUsage {
    ui64 Offset = 0;
    ui64 Length = 0;
};

TVector<TDenseUnionChildUsage> CalculateDenseUnionChildrenUsage(const arrow::ArrayData& data);

void AdjustDenseUnionValueOffsets(
    TArrayRef<i32> valueOffsets,
    TArrayRef<const i8> typeCodes,
    TArrayRef<const TDenseUnionChildUsage> childUsage);

} // namespace NYql::NUdf
