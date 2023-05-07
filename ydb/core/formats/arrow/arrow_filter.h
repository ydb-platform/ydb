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

    template <class TIterator>
    class TIteratorImpl {
    private:
        ui32 InternalPosition = 0;
        ui32 CurrentRemainVolume = 0;
        TIterator It;
        TIterator ItEnd;
        bool CurrentValue;
    public:
        TIteratorImpl(TIterator itBegin, TIterator itEnd, const bool startValue)
            : It(itBegin)
            , ItEnd(itEnd)
            , CurrentValue(startValue) {
            if (It != ItEnd) {
                CurrentRemainVolume = *It;
            }
        }

        bool GetCurrentAcceptance() const {
            Y_VERIFY_DEBUG(CurrentRemainVolume);
            return CurrentValue;
        }

        bool IsBatchForSkip(const ui32 size) const {
            Y_VERIFY_DEBUG(CurrentRemainVolume);
            return !CurrentValue && CurrentRemainVolume >= size;
        }

        bool Next(const ui32 size) {
            if (CurrentRemainVolume > size) {
                InternalPosition += size;
                CurrentRemainVolume -= size;
                return true;
            }
            ui32 sizeRemain = size;
            while (It != ItEnd) {
                if (*It - InternalPosition > sizeRemain) {
                    InternalPosition = sizeRemain;
                    CurrentRemainVolume = *It - InternalPosition - sizeRemain;
                    return true;
                } else {
                    sizeRemain -= *It - InternalPosition;
                    InternalPosition = 0;
                    CurrentValue = !CurrentValue;
                    ++It;
                }
            }
            CurrentRemainVolume = 0;
            return false;
        }
    };

    static ui32 CrossSize(const ui32 s1, const ui32 f1, const ui32 s2, const ui32 f2);
public:

    using TIterator = TIteratorImpl<std::deque<ui32>::const_iterator>;
    using TReverseIterator = TIteratorImpl<std::deque<ui32>::const_reverse_iterator>;

    template <bool ForReverse>
    class TIteratorSelector {

    };

    template <>
    class TIteratorSelector<true> {
    public:
        using TIterator = TReverseIterator;
    };

    template <>
    class TIteratorSelector<false> {
    public:
        using TIterator = TIterator;
    };

    TIterator GetIterator() const {
        return TIterator(Filter.cbegin(), Filter.cend(), GetStartValue());
    }

    TReverseIterator GetReverseIterator() const {
        return TReverseIterator(Filter.crbegin(), Filter.crend(), CurrentValue);
    }

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

    std::vector<bool> BuildSimpleFilter() const;

    TColumnFilter() = default;

    std::shared_ptr<arrow::BooleanArray> BuildArrowFilter() const;

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

    template <class TCalcer>
    void Merge(const TColumnFilter& extFilter, const TCalcer actor) {
        if (Filter.empty() && extFilter.Filter.empty()) {
            DefaultFilterValue = (extFilter.DefaultFilterValue && DefaultFilterValue);
        } else if (Filter.empty()) {
            if (DefaultFilterValue) {
                Filter = extFilter.Filter;
                Count = extFilter.Count;
                CurrentValue = extFilter.CurrentValue;
            }
        } else if (extFilter.Filter.empty()) {
            if (!extFilter.DefaultFilterValue) {
                DefaultFilterValue = false;
                Filter.clear();
                Count = 0;
            }
        } else {
            Y_VERIFY(extFilter.Count == Count);
            auto itSelf = Filter.begin();
            auto itExt = extFilter.Filter.cbegin();

            std::deque<ui32> result;
            ui32 selfPos = 0;
            ui32 extPos = 0;
            bool curSelf = GetStartValue();
            bool curExt = extFilter.GetStartValue();
            bool curCurrent = false;
            ui32 count = 0;

            while (itSelf != Filter.end() && itExt != extFilter.Filter.cend()) {
                const ui32 delta = CrossSize(extPos, extPos + *itExt, selfPos, selfPos + *itSelf);
                if (delta) {
                    if (!count || curCurrent != actor(curSelf, curExt)) {
                        result.emplace_back(delta);
                        curCurrent = actor(curSelf, curExt);
                    } else {
                        result.back() += delta;
                    }
                    count += delta;
                }
                if (selfPos + *itSelf < extPos + *itExt) {
                    selfPos += *itSelf;
                    curSelf = !curSelf;
                    ++itSelf;
                } else if (selfPos + *itSelf > extPos + *itExt) {
                    extPos += *itExt;
                    curExt = !curExt;
                    ++itExt;
                } else {
                    curExt = !curExt;
                    curSelf = !curSelf;
                    ++itSelf;
                    ++itExt;
                }
            }
            Y_VERIFY(itSelf == Filter.end() && itExt == extFilter.Filter.cend());
            std::swap(result, Filter);
            std::swap(curCurrent, CurrentValue);
            std::swap(count, Count);
        }
    }


    void And(const TColumnFilter& extFilter) {
        return Merge(extFilter, [](const bool selfBool, const bool extBool) {
            return selfBool && extBool;
            });
    }
    void Or(const TColumnFilter& extFilter) {
        return Merge(extFilter, [](const bool selfBool, const bool extBool) {
            return selfBool || extBool;
            });
    }

    // It makes a filter using composite predicate
    static TColumnFilter MakePredicateFilter(const arrow::Datum& datum, const arrow::Datum& border,
        ECompareType compareType);

    bool Apply(std::shared_ptr<arrow::RecordBatch>& batch);

    void CombineSequential(const TColumnFilter& extFilter);
};

}
