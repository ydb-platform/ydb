#pragma once
#include <ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/datum.h>
#include <util/system/types.h>
#include <deque>

namespace NKikimr::NArrow {

enum class ECompareType {
    LESS = 1,
    LESS_OR_EQUAL,
    GREATER,
    GREATER_OR_EQUAL,
};

class TColumnFilter {
private:
    bool DefaultFilterValue = true;
    bool CurrentValue = true;
    ui32 Count = 0;
    std::vector<ui32> Filter;
    mutable std::optional<std::vector<bool>> FilterPlain;
    TColumnFilter(const bool defaultFilterValue)
        : DefaultFilterValue(defaultFilterValue)
    {

    }

    bool GetStartValue(const bool reverse = false) const {
        if (Filter.empty()) {
            return DefaultFilterValue;
        }
        if (reverse) {
            return CurrentValue;
        } else {
            if (Filter.size() % 2 == 0) {
                return !CurrentValue;
            } else {
                return CurrentValue;
            }
        }
    }

    static ui32 CrossSize(const ui32 s1, const ui32 f1, const ui32 s2, const ui32 f2);
    class TMergerImpl;
    void Add(const bool value, const ui32 count = 1);
    void Reset(const ui32 count);
public:

    ui64 GetDataSize() const {
        return Filter.capacity() * sizeof(ui32) + Count * sizeof(bool);
    }

    class TIterator {
    private:
        i64 InternalPosition = 0;
        i64 CurrentRemainVolume = 0;
        const std::vector<ui32>* FilterPointer = nullptr;
        i32 Position = 0;
        bool CurrentValue;
        const i32 FinishPosition;
        const i32 DeltaPosition;
    public:
        TString DebugString() const;

        TIterator(const bool reverse, const std::vector<ui32>& filter, const bool startValue)
            : FilterPointer(&filter)
            , CurrentValue(startValue)
            , FinishPosition(reverse ? -1 : FilterPointer->size())
            , DeltaPosition(reverse ? -1 : 1)
        {
            if (!FilterPointer->size()) {
                Position = FinishPosition;
            } else {
                if (reverse) {
                    Position = FilterPointer->size() - 1;
                }
                CurrentRemainVolume = (*FilterPointer)[Position];
            }
        }

        TIterator(const bool reverse, const ui32 size, const bool startValue)
            : CurrentValue(startValue)
            , FinishPosition(reverse ? -1 : 1)
            , DeltaPosition(reverse ? -1 : 1) {
            if (!size) {
                Position = FinishPosition;
            } else {
                if (reverse) {
                    Position = 0;
                }
                CurrentRemainVolume = size;
            }
        }

        bool GetCurrentAcceptance() const {
            Y_VERIFY(CurrentRemainVolume);
            return CurrentValue;
        }

        bool IsBatchForSkip(const ui32 size) const {
            Y_VERIFY(CurrentRemainVolume);
            return !CurrentValue && CurrentRemainVolume >= size;
        }

        bool Next(const ui32 size);
    };

    TIterator GetIterator(const bool reverse, const ui32 expectedSize) const;

    bool empty() const {
        return Filter.empty();
    }

    TColumnFilter(std::vector<bool>&& values) {
        const ui32 count = values.size();
        Reset(count, values);
        FilterPlain = std::move(values);
    }

    template <class TGetter>
    void Reset(const ui32 count, const TGetter& getter) {
        Reset(count);
        if (!count) {
            return;
        }
        bool currentValue = getter[0];
        ui32 sameValueCount = 0;
        for (ui32 i = 0; i < count; ++i) {
            if (getter[i] != currentValue) {
                Add(currentValue, sameValueCount);
                sameValueCount = 0;
                currentValue = !currentValue;
            }
            ++sameValueCount;
        }
        Add(currentValue, sameValueCount);
    }

    ui32 Size() const {
        return Count;
    }

    const std::vector<bool>& BuildSimpleFilter(const ui32 expectedSize) const;

    std::shared_ptr<arrow::BooleanArray> BuildArrowFilter(const ui32 expectedSize) const;

    bool IsTotalAllowFilter() const;

    bool IsTotalDenyFilter() const;

    static TColumnFilter BuildStopFilter() {
        return TColumnFilter(false);
    }

    static TColumnFilter BuildAllowFilter() {
        return TColumnFilter(true);
    }

    TColumnFilter And(const TColumnFilter& extFilter) const Y_WARN_UNUSED_RESULT;
    TColumnFilter Or(const TColumnFilter& extFilter) const Y_WARN_UNUSED_RESULT;

    // It makes a filter using composite predicate
    static TColumnFilter MakePredicateFilter(const arrow::Datum& datum, const arrow::Datum& border,
        ECompareType compareType);

    bool Apply(std::shared_ptr<arrow::RecordBatch>& batch);

    // Combines filters by 'and' operator (extFilter count is true positions count in self, thought extFitler patch exactly that positions)
    TColumnFilter CombineSequentialAnd(const TColumnFilter& extFilter) const Y_WARN_UNUSED_RESULT;
};

}
