#pragma once
#include <ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/datum.h>
#include <util/system/types.h>

#include <deque>

namespace NKikimr::NArrow::NAccessor {
class IChunkedArray;
}

namespace NKikimr::NArrow {

class TGeneralContainer;

enum class ECompareType {
    LESS = 1,
    LESS_OR_EQUAL,
    GREATER,
    GREATER_OR_EQUAL,
};

class TColumnFilter {
private:
    bool DefaultFilterValue = true;
    bool LastValue = true;
    ui32 RecordsCount = 0;
    YDB_READONLY_DEF(std::vector<ui32>, Filter);
    mutable std::optional<std::vector<bool>> FilterPlain;
    mutable std::optional<ui32> FilteredCount;
    TColumnFilter(const bool defaultFilterValue)
        : DefaultFilterValue(defaultFilterValue) {
    }

    static ui32 CrossSize(const ui32 s1, const ui32 f1, const ui32 s2, const ui32 f2);
    class TMergerImpl;
    void Reset(const ui32 count);
    void ResetCaches() const {
        FilterPlain.reset();
        FilteredCount.reset();
    }

public:
    class TSlicesIterator {
    private:
        const TColumnFilter& Owner;
        const std::optional<ui32> StartIndex;
        const std::optional<ui32> Count;
        ui32 CurrentStartIndex = 0;
        bool CurrentIsFiltered = false;
        std::vector<ui32>::const_iterator CurrentIterator;

    public:
        TSlicesIterator(const TColumnFilter& owner, const std::optional<ui32> start, const std::optional<ui32> count);

        bool IsFiltered() const {
            return CurrentIsFiltered;
        }

        ui32 GetRecordsCount() const {
            if (StartIndex) {
                return *Count;
            } else {
                return Owner.GetRecordsCountVerified();
            }
        }

        ui32 GetStartIndex() const {
            if (!StartIndex) {
                return CurrentStartIndex;
            } else {
                return std::max<ui32>(CurrentStartIndex, *StartIndex);
            }
        }

        ui32 GetSliceSize() const;

        void Start();

        bool IsValid() const {
            return CurrentIterator != Owner.GetFilter().end() && (!StartIndex || CurrentStartIndex < *StartIndex + *Count);
        }

        bool Next();
    };

    TColumnFilter Cut(const ui32 filteredRecordsCount, const ui32 limit, const bool reverse) const;

    TSlicesIterator BuildSlicesIterator(const std::optional<ui32> startIndex, const std::optional<ui32> count) const {
        return TSlicesIterator(*this, startIndex, count);
    }

    std::optional<ui32> GetRecordsCount() const;

    ui32 GetRecordsCountVerified() const;

    bool GetStartValue(const bool reverse = false) const {
        if (Filter.empty()) {
            return DefaultFilterValue;
        }
        if (reverse) {
            return LastValue;
        } else {
            if (Filter.size() % 2 == 0) {
                return !LastValue;
            } else {
                return LastValue;
            }
        }
    }

    void Append(const TColumnFilter& filter);
    void Add(const bool value, const ui32 count = 1);
    std::optional<ui32> GetFilteredCount() const;
    ui32 GetFilteredCountVerified() const;
    const std::vector<bool>& BuildSimpleFilter() const;
    std::shared_ptr<arrow::BooleanArray> BuildArrowFilter(
        const ui32 expectedSize, const std::optional<ui32> startPos = {}, const std::optional<ui32> count = {}) const;

    ui64 GetDataSize() const {
        return Filter.capacity() * sizeof(ui32) + RecordsCount * sizeof(bool);
    }

    static TColumnFilter BuildConstFilter(const bool startValue, const std::initializer_list<ui32> list) {
        TColumnFilter result = BuildAllowFilter();
        bool value = startValue;
        for (auto&& i : list) {
            result.Add(value, i);
            value = !value;
        }
        return result;
    }

    static ui64 GetPredictedMemorySize(const ui32 recordsCount) {
        return 2 /* capacity */ * recordsCount * (sizeof(ui32) + sizeof(bool));
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
            , DeltaPosition(reverse ? -1 : 1) {
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
            Y_ABORT_UNLESS(CurrentRemainVolume);
            return CurrentValue;
        }

        bool IsBatchForSkip(const ui32 size) const {
            Y_ABORT_UNLESS(CurrentRemainVolume);
            return !CurrentValue && CurrentRemainVolume >= size;
        }

        bool Next(const ui32 size);
    };

    TString DebugString() const;

    TIterator GetIterator(const bool reverse, const ui32 expectedSize) const;

    bool CheckSlice(const ui32 offset, const ui32 count) const;

    TColumnFilter Slice(const ui32 offset, const ui32 count) const;

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
        ui32 sameValueCount = 1;
        for (ui32 i = 1; i < count; ++i) {
            if (getter[i] != currentValue) {
                Add(currentValue, sameValueCount);
                sameValueCount = 0;
                currentValue = !currentValue;
            }
            ++sameValueCount;
        }
        Add(currentValue, sameValueCount);
    }

    template <class TGetterLambda>
    struct TAdapterLambda {
    private:
        TGetterLambda Getter;

    public:
        TAdapterLambda(const TGetterLambda& getter)
            : Getter(getter) {
        }

        bool operator[](const ui32 index) const {
            return Getter(index);
        }
    };

    template <class TGetterLambda>
    void ResetWithLambda(const ui32 count, const TGetterLambda getter) {
        return Reset(count, TAdapterLambda<TGetterLambda>(getter));
    }

    bool IsTotalAllowFilter() const;
    bool IsTotalDenyFilter() const;
    bool IsEmpty() const {
        return Filter.empty();
    }

    static TColumnFilter BuildAllowFilter() {
        return TColumnFilter(true);
    }

    static TColumnFilter BuildDenyFilter() {
        return TColumnFilter(false);
    }

    TColumnFilter And(const TColumnFilter& extFilter) const Y_WARN_UNUSED_RESULT;
    TColumnFilter Or(const TColumnFilter& extFilter) const Y_WARN_UNUSED_RESULT;

    // It makes a filter using composite predicate
    static TColumnFilter MakePredicateFilter(const arrow::Datum& datum, const arrow::Datum& border, ECompareType compareType);

    class TApplyContext {
    private:
        YDB_READONLY_DEF(std::optional<ui32>, StartPos);
        YDB_READONLY_DEF(std::optional<ui32>, Count);
        YDB_ACCESSOR(bool, TrySlices, false);

    public:
        TApplyContext() = default;
        bool HasSlice() const {
            return !!StartPos && !!Count;
        }

        TApplyContext(const ui32 start, const ui32 count)
            : StartPos(start)
            , Count(count) {
        }

        TApplyContext& Slice(const ui32 start, const ui32 count);
    };

    [[nodiscard]] bool Apply(std::shared_ptr<TGeneralContainer>& batch, const TApplyContext& context = Default<TApplyContext>()) const;
    [[nodiscard]] bool Apply(std::shared_ptr<arrow::Table>& batch, const TApplyContext& context = Default<TApplyContext>()) const;
    [[nodiscard]] bool Apply(std::shared_ptr<arrow::RecordBatch>& batch, const TApplyContext& context = Default<TApplyContext>()) const;
    void Apply(const ui32 expectedRecordsCount, std::vector<arrow::Datum*>& datums) const;
    [[nodiscard]] std::shared_ptr<NAccessor::IChunkedArray> Apply(
        const std::shared_ptr<NAccessor::IChunkedArray>& source, const TApplyContext& context = Default<TApplyContext>()) const;

    // Combines filters by 'and' operator (extFilter count is true positions count in self, thought extFitler patch exactly that positions)
    TColumnFilter CombineSequentialAnd(const TColumnFilter& extFilter) const Y_WARN_UNUSED_RESULT;
};

}   // namespace NKikimr::NArrow
