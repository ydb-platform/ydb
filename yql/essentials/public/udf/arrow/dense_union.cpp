#include "dense_union.h"

#include <util/generic/algorithm.h>
#include <util/system/types.h>

#include <algorithm>
#include <limits>

namespace NYql::NUdf {

TVector<TDenseUnionChildUsage> CalculateDenseUnionChildrenUsage(const arrow::ArrayData& data) {
    const size_t numChildren = data.child_data.size();
    TVector<TDenseUnionChildUsage> result(numChildren);
    TVector<std::pair<ui64, ui64>> minMaxOffsets(numChildren);

    if (data.length == 0) {
        return result;
    }

    for (size_t childIndex = 0; childIndex < numChildren; ++childIndex) {
        minMaxOffsets[childIndex] = {std::numeric_limits<ui64>::max(), 0};
    }

    const i8* typeCodes = data.GetValues<i8>(1);
    const i32* valueOffsets = data.GetValues<i32>(2);

    for (i64 rowIndex = 0; rowIndex < data.length; ++rowIndex) {
        const size_t typeCode = static_cast<size_t>(static_cast<ui8>(typeCodes[rowIndex]));
        const ui64 valueOffset = static_cast<ui64>(valueOffsets[rowIndex]);
        minMaxOffsets[typeCode].first = std::min(minMaxOffsets[typeCode].first, valueOffset);
        minMaxOffsets[typeCode].second = std::max(minMaxOffsets[typeCode].second, valueOffset);
    }

    for (size_t childIndex = 0; childIndex < numChildren; ++childIndex) {
        if (minMaxOffsets[childIndex].first <= minMaxOffsets[childIndex].second) {
            result[childIndex].Offset = minMaxOffsets[childIndex].first;
            result[childIndex].Length = minMaxOffsets[childIndex].second - minMaxOffsets[childIndex].first + 1;
        } else {
            result[childIndex] = {};
        }
    }

    return result;
}

void AdjustDenseUnionValueOffsets(
    TArrayRef<i32> valueOffsets,
    TArrayRef<const i8> typeCodes,
    TArrayRef<const TDenseUnionChildUsage> childUsage) {
    const bool needsAdjust = AnyOf(childUsage, [](const TDenseUnionChildUsage& usage) {
        return usage.Offset > 0;
    });
    if (!needsAdjust) {
        return;
    }

    for (size_t rowIndex = 0; rowIndex < valueOffsets.size(); ++rowIndex) {
        const size_t typeCode = static_cast<size_t>(static_cast<ui8>(typeCodes[rowIndex]));
        valueOffsets[rowIndex] -= static_cast<i32>(childUsage[typeCode].Offset);
    }
}

} // namespace NYql::NUdf
