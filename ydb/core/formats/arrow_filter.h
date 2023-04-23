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
    std::deque<ui32> Filter;
    TColumnFilter(const bool defaultFilterValue)
        : DefaultFilterValue(defaultFilterValue)
    {

    }

    bool GetStartValue() const {
        if (Filter.empty()) {
            return DefaultFilterValue;
        }
        bool value = CurrentValue;
        if (Filter.size() % 2 == 0) {
            value = !value;
        }
        return value;
    }

public:

    TColumnFilter(std::vector<bool>&& values) {
        const ui32 count = values.size();
        Reset(count, std::move(values));
    }

    template <class TGetter>
    void Reset(const ui32 count, TGetter&& getter) {
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

    ui32 GetInactiveHeadSize() const;

    ui32 GetInactiveTailSize() const;

    void CutInactiveTail();

    void CutInactiveHead();

    std::vector<bool> BuildFilter() const;

    TColumnFilter() = default;

    std::shared_ptr<arrow::BooleanArray> MakeFilter() const;

    bool IsTotalAllowFilter() const;

    bool IsTotalDenyFilter() const;

    static TColumnFilter BuildStopFilter() {
        return TColumnFilter(false);
    }

    static TColumnFilter BuildAllowFilter() {
        return TColumnFilter(true);
    }

    void Reset(const ui32 count);

    void Add(const bool value, const ui32 count = 1);

    void And(const TColumnFilter& extFilter);

    // It makes a filter using composite predicate
    static TColumnFilter MakePredicateFilter(const arrow::Datum& datum, const arrow::Datum& border,
        ECompareType compareType);

    bool Apply(std::shared_ptr<arrow::RecordBatch>& batch);

    void CombineSequential(const TColumnFilter& extFilter);
};

}
