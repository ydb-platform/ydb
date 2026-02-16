
#pragma once

#include <util/generic/yexception.h>
#include <yt/yql/providers/yt/fmr/utils/comparator/yql_yt_binary_yson_compare_impl.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_parser_fragment_list_index.h>

namespace NYql::NFmr {

struct TFmrTableKeysBoundary;
int CompareKeyRows(const TFmrTableKeysBoundary& lhs, const TFmrTableKeysBoundary& rhs);

struct TFmrTableKeysBoundary {
    TString Row;
    TRowIndexMarkup Markup;
    std::vector<ESortOrder> SortOrders;

    TFmrTableKeysBoundary() = default;

    TFmrTableKeysBoundary(TStringBuf row, const std::vector<TString>& keyColumns, std::vector<ESortOrder> sortOrders)
        : Row(row)
        , SortOrders(std::move(sortOrders))
    {
        TParserFragmentListIndex parser(Row, keyColumns);
        parser.Parse();
        const auto& rows = parser.GetRows();
        if (!rows.empty()) {
            Markup = rows[0];
        }
    }

    int CompareTo(const TFmrTableKeysBoundary& other) const {
        return CompareKeyRows(*this, other);
    }

    bool operator<(const TFmrTableKeysBoundary& other) const { return CompareTo(other) < 0; }
    bool operator>(const TFmrTableKeysBoundary& other) const { return CompareTo(other) > 0; }
    bool operator==(const TFmrTableKeysBoundary& other) const { return CompareTo(other) == 0; }
    bool operator<=(const TFmrTableKeysBoundary& other) const { return CompareTo(other) <= 0; }
    bool operator>=(const TFmrTableKeysBoundary& other) const { return CompareTo(other) >= 0; }
};

Y_FORCE_INLINE int CompareKeys(const TFmrTableKeysBoundary& lhs, const TFmrTableKeysBoundary& rhs) {
    return CompareKeyRows(lhs, rhs);
}

struct TFmrTableKeysRange {
    TMaybe<TFmrTableKeysBoundary> FirstKeysBound;
    TMaybe<TFmrTableKeysBoundary> LastKeysBound;

    bool IsFirstBoundInclusive = true;
    bool IsLastBoundInclusive = true;
    bool IsEmpty = false;

    bool IsFirstKeySet() const { return FirstKeysBound.Defined(); }
    bool IsLastKeySet() const { return LastKeysBound.Defined(); }

    void SetFirstKeysBound(TFmrTableKeysBoundary bound, bool inclusive = true) {
        FirstKeysBound = std::move(bound);
        IsFirstBoundInclusive = inclusive;
    }

    void SetLastKeysBound(TFmrTableKeysBoundary bound, bool inclusive = false) {
        LastKeysBound = std::move(bound);
        IsLastBoundInclusive = inclusive;
    }

    TFmrTableKeysRange GetIntersection(const TFmrTableKeysRange& other) const {
        if (IsEmpty || other.IsEmpty) {
            return TFmrTableKeysRange{.IsEmpty = true};
        }

        TMaybe<TFmrTableKeysBoundary> newFirst;
        TMaybe<TFmrTableKeysBoundary> newLast;
        bool isFirstInclusive = true;
        bool isLastInclusive = true;

        if (!IsFirstKeySet()) {
            newFirst = other.FirstKeysBound;
            isFirstInclusive = other.IsFirstBoundInclusive;
        } else if (!other.IsFirstKeySet()) {
            newFirst = FirstKeysBound;
            isFirstInclusive = IsFirstBoundInclusive;
        } else {
            if (*FirstKeysBound > *other.FirstKeysBound) {
                newFirst = FirstKeysBound;
                isFirstInclusive = IsFirstBoundInclusive;
            } else if (*FirstKeysBound < *other.FirstKeysBound) {
                newFirst = other.FirstKeysBound;
                isFirstInclusive = other.IsFirstBoundInclusive;
            } else {
                newFirst = FirstKeysBound;
                isFirstInclusive = IsFirstBoundInclusive && other.IsFirstBoundInclusive;
            }
        }

        if (!IsLastKeySet()) {
            newLast = other.LastKeysBound;
            isLastInclusive = other.IsLastBoundInclusive;
        } else if (!other.IsLastKeySet()) {
            newLast = LastKeysBound;
            isLastInclusive = IsLastBoundInclusive;
        } else {
            if (*LastKeysBound < *other.LastKeysBound) {
                newLast = LastKeysBound;
                isLastInclusive = IsLastBoundInclusive;
            } else if (*LastKeysBound > *other.LastKeysBound) {
                newLast = other.LastKeysBound;
                isLastInclusive = other.IsLastBoundInclusive;
            } else {
                newLast = LastKeysBound;
                isLastInclusive = IsLastBoundInclusive && other.IsLastBoundInclusive;
            }
        }

        if (newFirst.Defined() && newLast.Defined()) {
            if (*newFirst > *newLast) {
                return TFmrTableKeysRange{.IsEmpty = true};
            }
            if (*newFirst == *newLast && !(isFirstInclusive && isLastInclusive)) {
                return TFmrTableKeysRange{.IsEmpty = true};
            }
        }

        TFmrTableKeysRange out{.IsEmpty = false};
        if (newFirst.Defined()) {
            out.SetFirstKeysBound(*newFirst, isFirstInclusive);
        }
        if (newLast.Defined()) {
            out.SetLastKeysBound(*newLast, isLastInclusive);
        }
        return out;
    }
};


class TBinaryYsonComparator {
public:
    TBinaryYsonComparator(
        TStringBuf blobData,
        std::vector<ESortOrder> sortOrders
    )
        : BlobData_(blobData)
        , SortOrders_(std::move(sortOrders))
    {}

    int CompareYsonValues(
        TColumnOffsetRange lhsOffsetRange,
        TColumnOffsetRange rhsOffsetRange
    ) const;

    int CompareRows(
        const TRowIndexMarkup& lhsRow,
        const TRowIndexMarkup& rhsRow
    ) const;

private:
    TStringBuf BlobData_;
    std::vector<ESortOrder> SortOrders_;
};

} // namespace NYql::NFmr
