#include "yql_yt_binary_yson_comparator.h"

namespace NYql::NFmr {

int CompareKeyRows(const TFmrTableKeysBoundary& lhs, const TFmrTableKeysBoundary& rhs) {
    Y_ENSURE(lhs.SortOrders.size() == rhs.SortOrders.size(), "SortOrders mismatch in TFmrTableKeysBoundary comparison");
    return CompareKeyRowsAcrossYsonBlocks(lhs.Row, lhs.Markup, rhs.Row, rhs.Markup, lhs.SortOrders);
}

int TBinaryYsonComparator::CompareYsonValues(TColumnOffsetRange lhs, TColumnOffsetRange rhs) const {
    Y_ENSURE(lhs.IsValid(), "Invalid column offset range");
    Y_ENSURE(rhs.IsValid(), "Invalid column offset range");
    Y_ENSURE(lhs.EndOffset <= BlobData_.size(), "Offset out of bounds");
    Y_ENSURE(rhs.EndOffset <= BlobData_.size(), "Offset out of bounds");

    const TStringBuf lhsVal(BlobData_.data() + lhs.StartOffset, lhs.EndOffset - lhs.StartOffset);
    const TStringBuf rhsVal(BlobData_.data() + rhs.StartOffset, rhs.EndOffset - rhs.StartOffset);
    return CompareYsonValuesImpl(lhsVal, rhsVal);
}

int TBinaryYsonComparator::CompareRows(
    const TRowIndexMarkup& lhsRow,
    const TRowIndexMarkup& rhsRow
) const {
    Y_ENSURE(lhsRow.size() == rhsRow.size(), "Row sizes mismatch");
    Y_ENSURE(lhsRow.size() - 1 == SortOrders_.size(), "Row size doesn't match sort orders. Expected "
                                                      "last column to be the row boundary.");

    // The last column is the row boundary, so we don't need to compare it
    for (ui64 colIdx = 0; colIdx < lhsRow.size() - 1; ++colIdx) {
        const auto& lhsRange = lhsRow[colIdx];
        const auto& rhsRange = rhsRow[colIdx];

        bool lhsIsNull = !lhsRange.IsValid();
        bool rhsIsNull = !rhsRange.IsValid();

        if (lhsIsNull && rhsIsNull) {
            continue;
        }
        if (lhsIsNull) {
            return -1;
        }
        if (rhsIsNull) {
            return 1;
        }

        int result = CompareYsonValues(lhsRange, rhsRange);

        if (SortOrders_[colIdx] == ESortOrder::Descending) {
            result = -result;
        }

        if (result != 0) {
            return result;
        }
    }

    return 0;
}

} // namespace NYql::NFmr
