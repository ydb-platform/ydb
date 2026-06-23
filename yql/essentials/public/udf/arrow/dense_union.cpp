#include "dense_union.h"

#include <util/generic/algorithm.h>
#include <util/system/types.h>

namespace NYql::NUdf {

namespace {

struct TForwardScanResult {
    TVector<ui64> MinOffsets;
    TVector<ui64> MaxOffsets;
    TVector<bool> ChildHasOccurrence;
    size_t ForwardScanEnd = 0;
};

TForwardScanResult ForwardScanOffsetsUntilAllMinsFound(
    TArrayRef<const i8> typeCodes,
    TArrayRef<const i32> valueOffsets,
    size_t numberOfTypeIds,
    size_t numberOfNonEmptyChildren) {
    TForwardScanResult result{
        .MinOffsets = TVector<ui64>(numberOfTypeIds, Max<ui64>()),
        .MaxOffsets = TVector<ui64>(numberOfTypeIds, Min<ui64>()),
        .ChildHasOccurrence = TVector<bool>(numberOfTypeIds, false),
    };
    size_t childrenFoundFromLeft = 0;
    size_t idx = 0;
    for (; idx < typeCodes.size() && childrenFoundFromLeft < numberOfNonEmptyChildren; ++idx) {
        const size_t typeCode = static_cast<size_t>(static_cast<ui8>(typeCodes[idx]));
        const ui64 offset = static_cast<ui64>(valueOffsets[idx]);
        childrenFoundFromLeft += static_cast<size_t>(!result.ChildHasOccurrence[typeCode]);
        result.ChildHasOccurrence[typeCode] = true;
        result.MinOffsets[typeCode] = Min(result.MinOffsets[typeCode], offset);
        result.MaxOffsets[typeCode] = Max(result.MaxOffsets[typeCode], offset);
    }
    result.ForwardScanEnd = idx;
    return result;
}

void BackwardScanCompleteMaxOffsets(
    TArrayRef<const i8> typeCodes,
    TArrayRef<const i32> valueOffsets,
    size_t forwardScanEnd,
    size_t numberOfNonEmptyChildren,
    TVector<ui64>& maxOffsets) {
    TVector<bool> childFoundFromRight(maxOffsets.size(), false);
    size_t childrenFoundFromRight = 0;
    for (size_t idx = typeCodes.size(); idx > forwardScanEnd && childrenFoundFromRight < numberOfNonEmptyChildren; --idx) {
        const size_t typeCode = static_cast<size_t>(static_cast<ui8>(typeCodes[idx - 1]));
        const ui64 offset = static_cast<ui64>(valueOffsets[idx - 1]);
        childrenFoundFromRight += static_cast<size_t>(!childFoundFromRight[typeCode]);
        childFoundFromRight[typeCode] = true;
        maxOffsets[typeCode] = Max(maxOffsets[typeCode], offset);
    }
}

} // namespace

TVector<TDenseUnionChildUsage> CalculateDenseUnionChildrenUsage(const arrow::ArrayData& data) {
    const size_t numberOfTypeIds = data.child_data.size();
    const size_t numberOfNonEmptyChildren = CountIf(data.child_data, [](const std::shared_ptr<arrow::ArrayData>& child) {
        Y_ENSURE(child, "Child data is null");
        return child->length > 0;
    });
    TVector<TDenseUnionChildUsage> result(numberOfTypeIds);
    const size_t length = static_cast<size_t>(data.length);
    const TArrayRef<const i8> typeCodes{data.GetValues<i8>(1), length};
    const TArrayRef<const i32> valueOffsets{data.GetValues<i32>(2), length};

    auto minResult = ForwardScanOffsetsUntilAllMinsFound(typeCodes, valueOffsets, numberOfTypeIds, numberOfNonEmptyChildren);
    BackwardScanCompleteMaxOffsets(typeCodes, valueOffsets, minResult.ForwardScanEnd, numberOfNonEmptyChildren, minResult.MaxOffsets);

    for (size_t childIndex = 0; childIndex < numberOfTypeIds; ++childIndex) {
        if (minResult.ChildHasOccurrence[childIndex]) {
            result[childIndex] = {
                .Offset = minResult.MinOffsets[childIndex],
                .Length = minResult.MaxOffsets[childIndex] - minResult.MinOffsets[childIndex] + 1,
            };
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
