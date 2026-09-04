#pragma once

#include "dense_union_scalar.h"

#include <arrow/array/data.h>
#include <arrow/buffer.h>
#include <arrow/scalar.h>
#include <arrow/type.h>
#include <arrow/util/checked_cast.h>

#include <util/generic/array_ref.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <memory>

namespace NYql::NUdf {

constexpr size_t DenseUnionMaxAlternativesCount = arrow::UnionType::kMaxTypeCode;

std::shared_ptr<arrow::Scalar> CreateOptionalUnionScalar(
    std::shared_ptr<arrow::Scalar> unionScalar,
    std::shared_ptr<arrow::DataType> optionalUnionType);

std::shared_ptr<arrow::ArrayData> CreateOptionalUnionArray(
    i64 length,
    std::shared_ptr<arrow::Buffer> validityBitmap,
    std::shared_ptr<arrow::ArrayData> unionArray,
    i64 nullCount,
    std::shared_ptr<arrow::DataType> optionalUnionType);

template <bool IsOptional>
const TDenseUnionScalar* SplitUnionIntoMaskAndDataScalar(const arrow::Scalar& scalar) {
    if constexpr (IsOptional) {
        if (!scalar.is_valid) {
            return nullptr;
        }
        const auto& structScalar = arrow::internal::checked_cast<const arrow::StructScalar&>(scalar);
        return &arrow::internal::checked_cast<const TDenseUnionScalar&>(*structScalar.value.front());
    } else {
        return &arrow::internal::checked_cast<const TDenseUnionScalar&>(scalar);
    }
}

struct TOptionalUnionArrayParts {
    std::shared_ptr<arrow::Buffer> Mask;
    const arrow::ArrayData* UnionArray = nullptr;
};

template <bool IsOptional>
TOptionalUnionArrayParts SplitUnionIntoMaskAndData(const arrow::ArrayData& array) {
    if constexpr (IsOptional) {
        return {.Mask = array.buffers[0], .UnionArray = array.child_data[0].get()};
    } else {
        return {.Mask = nullptr, .UnionArray = &array};
    }
}

struct TDenseUnionChildUsage {
    ui64 Offset = 0;
    ui64 Length = 0;
};

TVector<TDenseUnionChildUsage> CalculateDenseUnionChildrenUsage(const arrow::ArrayData& data);

void AdjustDenseUnionValueOffsets(
    TArrayRef<const i32> src,
    TArrayRef<i32> dst,
    TArrayRef<const i8> typeCodes,
    TArrayRef<const TDenseUnionChildUsage> childUsage);

void AdjustDenseUnionValueOffsetsInplace(
    TArrayRef<i32> valueOffsets,
    TArrayRef<const i8> typeCodes,
    TArrayRef<const TDenseUnionChildUsage> childUsage);

} // namespace NYql::NUdf
