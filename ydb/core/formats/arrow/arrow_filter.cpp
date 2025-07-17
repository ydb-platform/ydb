#include "arrow_filter.h"

#include "common/adapter.h"
#include "common/container.h"
#include "switch/switch_type.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/chunked_array.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api_vector.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NArrow {

#define Y_VERIFY_OK(status) Y_ABORT_UNLESS(status.ok(), "%s", status.ToString().c_str())

TColumnFilter::TSlicesIterator::TSlicesIterator(const TColumnFilter& owner, const std::optional<ui32> start, const std::optional<ui32> count)
    : Owner(owner)
    , StartIndex(start)
    , Count(count) {
    AFL_VERIFY(!!StartIndex == !!Count);
    AFL_VERIFY(Owner.GetFilter().size());
    if (StartIndex) {
        AFL_VERIFY(*StartIndex + *Count <= owner.GetRecordsCountVerified())("start", *StartIndex)("count", *count)("size", owner.GetRecordsCount());
    }
}

TColumnFilter::TApplyContext& TColumnFilter::TApplyContext::Slice(const ui32 start, const ui32 count) {
    AFL_VERIFY(!StartPos && !Count);
    StartPos = start;
    Count = count;
    return *this;
}

ui32 TColumnFilter::TSlicesIterator::GetSliceSize() const {
    AFL_VERIFY(IsValid());
    if (!StartIndex) {
        return *CurrentIterator;
    } else {
        const ui32 startIndex = GetStartIndex();
        const ui32 finishIndex = std::min<ui32>(CurrentStartIndex + *CurrentIterator, *StartIndex + *Count);
        AFL_VERIFY(startIndex < finishIndex)("start", startIndex)("finish", finishIndex);
        return finishIndex - startIndex;
    }
}

void TColumnFilter::TSlicesIterator::Start() {
    CurrentStartIndex = 0;
    CurrentIsFiltered = Owner.GetStartValue();
    CurrentIterator = Owner.GetFilter().begin();
    if (StartIndex) {
        while (IsValid() && CurrentStartIndex + *CurrentIterator < *StartIndex) {
            AFL_VERIFY(Next());
        }
        AFL_VERIFY(IsValid());
    }
}

bool TColumnFilter::TSlicesIterator::Next() {
    AFL_VERIFY(IsValid());
    CurrentIsFiltered = !CurrentIsFiltered;
    CurrentStartIndex += *CurrentIterator;
    ++CurrentIterator;
    return IsValid();
}

bool TColumnFilter::TIterator::Next(const ui32 size) {
    Y_ABORT_UNLESS(size);
    if (CurrentRemainVolume > size) {
        InternalPosition += size;
        CurrentRemainVolume -= size;
        return true;
    }
    if (!FilterPointer && CurrentRemainVolume == size) {
        InternalPosition += size;
        CurrentRemainVolume -= size;
        return false;
    }
    Y_ABORT_UNLESS(FilterPointer);
    ui32 sizeRemain = size;
    while (Position != FinishPosition) {
        const ui32 currentVolume = (*FilterPointer)[Position];
        if (currentVolume > sizeRemain + InternalPosition) {
            InternalPosition += sizeRemain;
            CurrentRemainVolume = currentVolume - InternalPosition;
            return true;
        } else {
            sizeRemain -= currentVolume - InternalPosition;
            InternalPosition = 0;
            CurrentValue = !CurrentValue;
            Position += DeltaPosition;
        }
    }
    Y_ABORT_UNLESS(Position == FinishPosition);
    CurrentRemainVolume = 0;
    return false;
}

TString TColumnFilter::TIterator::DebugString() const {
    TStringBuilder sb;
    if (FilterPointer) {
        sb << "filter_pointer=";
        ui32 idx = 0;
        ui64 sum = 0;
        for (auto&& i : *FilterPointer) {
            sb << i << ",";
            sum += i;
            if (++idx > 100) {
                break;
            }
        }
        sb << ";sum=" << sum << ";";
    }
    sb << "internal_position=" << InternalPosition << ";";
    sb << "position=" << Position << ";";
    sb << "current=" << CurrentValue << ";";
    sb << "finish=" << FinishPosition << ";";
    sb << "delta=" << DeltaPosition << ";";
    sb << "remain=" << CurrentRemainVolume << ";";
    return sb;
}

std::shared_ptr<arrow::BooleanArray> TColumnFilter::BuildArrowFilter(
    const ui32 expectedSize, const std::optional<ui32> startPos, const std::optional<ui32> count) const {
    AFL_VERIFY(!!startPos == !!count);
    auto& simpleFilter = BuildSimpleFilter();
    arrow::BooleanBuilder builder;
    auto res = builder.Reserve(count.value_or(expectedSize));
    if (startPos) {
        AFL_VERIFY(*startPos + *count <= simpleFilter.size());
        Y_VERIFY_OK(builder.AppendValues(simpleFilter.begin() + *startPos, simpleFilter.begin() + *startPos + *count));
    } else {
        Y_VERIFY_OK(builder.AppendValues(simpleFilter));
    }
    std::shared_ptr<arrow::BooleanArray> out;
    TStatusValidator::Validate(builder.Finish(&out));
    return out;
}

bool TColumnFilter::IsTotalAllowFilter() const {
    if (DefaultFilterValue && Filter.empty()) {
        return true;
    }
    if (Filter.size() == 1 && LastValue) {
        return true;
    }
    return false;
}

bool TColumnFilter::IsTotalDenyFilter() const {
    if (!DefaultFilterValue && Filter.empty()) {
        return true;
    }
    if (Filter.size() == 1 && !LastValue) {
        return true;
    }
    return false;
}

void TColumnFilter::Reset(const ui32 count) {
    RecordsCount = 0;
    FilterPlain.reset();
    Filter.clear();
    Filter.reserve(count / 4);
}

void TColumnFilter::Add(const bool value, const ui32 count) {
    if (!count) {
        return;
    }
    if (Y_UNLIKELY(LastValue != value || !RecordsCount)) {
        Filter.emplace_back(count);
        LastValue = value;
    } else {
        Filter.back() += count;
    }
    RecordsCount += count;
}

ui32 TColumnFilter::CrossSize(const ui32 s1, const ui32 f1, const ui32 s2, const ui32 f2) {
    const ui32 f = std::min(f1, f2);
    const ui32 s = std::max(s1, s2);
    Y_ABORT_UNLESS(f >= s);
    return f - s;
}

template <class TData>
void ApplyImpl(const TColumnFilter& filter, std::shared_ptr<TData>& batch, const TColumnFilter::TApplyContext& context) {
    if (!batch || !batch->num_rows()) {
        return;
    }
    if (!filter.IsEmpty()) {
        if (context.HasSlice()) {
            AFL_VERIFY(filter.GetRecordsCountVerified() >= *context.GetStartPos() + *context.GetCount())(
                                                                                    "filter_size", filter.GetRecordsCountVerified())(
                                                                                    "start", context.GetStartPos())("count", context.GetCount());
            AFL_VERIFY(*context.GetCount() == (size_t)batch->num_rows())("count", context.GetCount())("batch_size", batch->num_rows());
        } else {
            AFL_VERIFY(filter.GetRecordsCountVerified() == (size_t)batch->num_rows())("filter_size", filter.GetRecordsCountVerified())(
                                                             "batch_size", batch->num_rows());
        }
    }
    if (filter.IsTotalDenyFilter()) {
        batch = NAdapter::TDataBuilderPolicy<TData>::GetEmptySame(batch);
        return;
    }
    if (filter.IsTotalAllowFilter()) {
        return;
    }
    if (context.GetTrySlices() && filter.GetFilter().size() * 10 < filter.GetRecordsCountVerified() &&
        filter.GetRecordsCountVerified() < filter.GetFilteredCountVerified() * 50) {
        batch =
            NAdapter::TDataBuilderPolicy<TData>::ApplySlicesFilter(batch, filter.BuildSlicesIterator(context.GetStartPos(), context.GetCount()));
    } else if (context.HasSlice()) {
        batch = NAdapter::TDataBuilderPolicy<TData>::ApplyArrowFilter(batch, filter.Slice(*context.GetStartPos(), *context.GetCount()));
    } else {
        batch = NAdapter::TDataBuilderPolicy<TData>::ApplyArrowFilter(batch, filter);
    }
}

void TColumnFilter::Apply(std::shared_ptr<TGeneralContainer>& batch, const TApplyContext& context) const {
    return ApplyImpl(*this, batch, context);
}

void TColumnFilter::Apply(std::shared_ptr<arrow::Table>& batch, const TApplyContext& context) const {
    return ApplyImpl(*this, batch, context);
}

void TColumnFilter::Apply(std::shared_ptr<arrow::RecordBatch>& batch, const TApplyContext& context) const {
    return ApplyImpl(*this, batch, context);
}

void TColumnFilter::Apply(const ui32 expectedRecordsCount, std::vector<arrow::Datum*>& datums) const {
    if (IsTotalAllowFilter()) {
        return;
    }
    if (IsTotalDenyFilter()) {
        for (auto&& d : datums) {
            AFL_VERIFY(d);
            switch (d->kind()) {
                case arrow::Datum::ARRAY:
                    *d = d->array()->Slice(0, 0);
                    break;
                case arrow::Datum::CHUNKED_ARRAY:
                    *d = d->chunked_array()->Slice(0, 0);
                    break;
                case arrow::Datum::RECORD_BATCH:
                    *d = d->record_batch()->Slice(0, 0);
                    break;
                case arrow::Datum::TABLE:
                    *d = d->table()->Slice(0, 0);
                    break;
                default:
                    AFL_VERIFY(false);
            }
        }
    } else {
        auto filter = BuildArrowFilter(expectedRecordsCount);
        for (auto&& d : datums) {
            AFL_VERIFY(d);
            *d = TStatusValidator::GetValid(arrow::compute::Filter(*d, filter));
        }
    }
}

std::shared_ptr<NAccessor::IChunkedArray> TColumnFilter::Apply(
    const std::shared_ptr<NAccessor::IChunkedArray>& source, const TApplyContext& context /*= Default<TApplyContext>()*/) const {
    if (context.HasSlice()) {
        auto sliceArray = source->ISlice(*context.GetStartPos(), *context.GetCount());
        return sliceArray->ApplyFilter(*this, sliceArray);
    } else {
        return source->ApplyFilter(*this, source);
    }
}

const std::vector<bool>& TColumnFilter::BuildSimpleFilter() const {
    if (!FilterPlain) {
        Y_ABORT_UNLESS(RecordsCount);
        std::vector<bool> result;
        result.resize(RecordsCount, true);
        bool currentValue = GetStartValue();
        ui32 currentPosition = 0;
        for (auto&& i : Filter) {
            if (!currentValue) {
                memset(&result[currentPosition], 0, sizeof(bool) * i);
            }
            currentPosition += i;
            currentValue = !currentValue;
        }
        FilterPlain = std::move(result);
    }
    return *FilterPlain;
}

class TMergePolicyAnd {
private:
public:
    static bool HasValue(const bool /*a*/, const bool /*b*/) {
        return true;
    }
    static bool Calc(const bool a, const bool b) {
        return a && b;
    }
    static TColumnFilter MergeWithSimple(const TColumnFilter& filter, const bool simpleValue) {
        if (simpleValue) {
            return filter;
        } else {
            return TColumnFilter::BuildDenyFilter();
        }
    }
    static TColumnFilter MergeWithSimple(const bool simpleValue, const TColumnFilter& filter) {
        return MergeWithSimple(filter, simpleValue);
    }
};

class TMergePolicyOr {
private:
public:
    static bool HasValue(const bool /*a*/, const bool /*b*/) {
        return true;
    }
    static bool Calc(const bool a, const bool b) {
        return a || b;
    }
    static TColumnFilter MergeWithSimple(const TColumnFilter& filter, const bool simpleValue) {
        if (simpleValue) {
            return TColumnFilter::BuildAllowFilter();
        } else {
            return filter;
        }
    }
    static TColumnFilter MergeWithSimple(const bool simpleValue, const TColumnFilter& filter) {
        return MergeWithSimple(filter, simpleValue);
    }
};

class TMergePolicyApplyFilter {
private:
public:
    static bool HasValue(const bool /*a*/, const bool b) {
        return b;
    }
    static bool Calc(const bool a, const bool /*b*/) {
        return a;
    }
    static TColumnFilter MergeWithSimple(const TColumnFilter& filter, const bool simpleValue) {
        if (simpleValue) {
            return filter;
        } else {
            return TColumnFilter::BuildDenyFilter();
        }
    }
    static TColumnFilter MergeWithSimple(const bool simpleValue, const TColumnFilter& filter) {
        const auto count = filter.GetRecordsCount();
        if (simpleValue) {
            TColumnFilter result = TColumnFilter::BuildAllowFilter();
            if (count) {
                result.Add(true, *count);
            }
            return result;
        } else {
            TColumnFilter result = TColumnFilter::BuildDenyFilter();
            if (count) {
                result.Add(false, *count);
            }
            return result;
        }
    }
};

class TColumnFilter::TMergerImpl {
private:
    const TColumnFilter& Filter1;
    const TColumnFilter& Filter2;

public:
    TMergerImpl(const TColumnFilter& filter1, const TColumnFilter& filter2)
        : Filter1(filter1)
        , Filter2(filter2) {
    }

    template <class TMergePolicy>
    TColumnFilter Merge() const {
        if (Filter1.empty() && Filter2.empty()) {
            return TColumnFilter(TMergePolicy::Calc(Filter1.DefaultFilterValue, Filter2.DefaultFilterValue));
        } else if (Filter1.empty()) {
            return TMergePolicy::MergeWithSimple(Filter1.DefaultFilterValue, Filter2);
        } else if (Filter2.empty()) {
            return TMergePolicy::MergeWithSimple(Filter1, Filter2.DefaultFilterValue);
        } else {
            Y_ABORT_UNLESS(Filter1.RecordsCount == Filter2.RecordsCount);
            auto it1 = Filter1.Filter.cbegin();
            auto it2 = Filter2.Filter.cbegin();

            std::vector<ui32> resultFilter;
            resultFilter.reserve(Filter1.Filter.size() + Filter2.Filter.size());
            ui32 pos1 = 0;
            ui32 pos2 = 0;
            bool curValue1 = Filter1.GetStartValue();
            bool curValue2 = Filter2.GetStartValue();
            bool curCurrent = false;
            ui32 count = 0;

            while (it1 != Filter1.Filter.cend() && it2 != Filter2.Filter.cend()) {
                const ui32 delta = TColumnFilter::CrossSize(pos2, pos2 + *it2, pos1, pos1 + *it1);
                if (delta && TMergePolicy::HasValue(curValue1, curValue2)) {
                    if (!count || curCurrent != TMergePolicy::Calc(curValue1, curValue2)) {
                        resultFilter.emplace_back(delta);
                        curCurrent = TMergePolicy::Calc(curValue1, curValue2);
                    } else {
                        resultFilter.back() += delta;
                    }
                    count += delta;
                }
                if (pos1 + *it1 < pos2 + *it2) {
                    pos1 += *it1;
                    curValue1 = !curValue1;
                    ++it1;
                } else if (pos1 + *it1 > pos2 + *it2) {
                    pos2 += *it2;
                    curValue2 = !curValue2;
                    ++it2;
                } else {
                    curValue2 = !curValue2;
                    curValue1 = !curValue1;
                    pos1 += *it1;
                    pos2 += *it2;
                    ++it1;
                    ++it2;
                }
            }
            Y_ABORT_UNLESS(it1 == Filter1.Filter.end() && it2 == Filter2.Filter.cend());
            TColumnFilter result = TColumnFilter::BuildAllowFilter();
            std::swap(resultFilter, result.Filter);
            std::swap(curCurrent, result.LastValue);
            std::swap(count, result.RecordsCount);
            return result;
        }
    }
};

TColumnFilter TColumnFilter::And(const TColumnFilter& extFilter) const {
    ResetCaches();
    return TMergerImpl(*this, extFilter).Merge<TMergePolicyAnd>();
}

TColumnFilter TColumnFilter::Or(const TColumnFilter& extFilter) const {
    ResetCaches();
    return TMergerImpl(*this, extFilter).Merge<TMergePolicyOr>();
}

TColumnFilter TColumnFilter::ApplyFilterFrom(const TColumnFilter& extFilter) const {
    ResetCaches();
    return TMergerImpl(*this, extFilter).Merge<TMergePolicyApplyFilter>();
}

TColumnFilter TColumnFilter::CombineSequentialAnd(const TColumnFilter& extFilter) const {
    ResetCaches();
    if (Filter.empty()) {
        return TMergePolicyAnd::MergeWithSimple(DefaultFilterValue, extFilter);
    } else if (extFilter.Filter.empty()) {
        return TMergePolicyAnd::MergeWithSimple(*this, extFilter.DefaultFilterValue);
    } else {
        auto itSelf = Filter.begin();
        auto itExt = extFilter.Filter.cbegin();

        std::vector<ui32> resultFilter;
        resultFilter.reserve(Filter.size() + extFilter.Filter.size());
        ui32 selfPos = 0;
        ui32 extPos = 0;
        bool curSelf = GetStartValue();
        bool curExt = extFilter.GetStartValue();
        bool curCurrent = false;
        ui32 count = 0;

        while (itSelf != Filter.end()) {
            Y_ABORT_UNLESS(!curSelf || itExt != extFilter.Filter.cend());
            const ui32 delta = curSelf ? CrossSize(extPos, extPos + *itExt, selfPos, selfPos + *itSelf) : *itSelf;
            if (delta) {
                if (!count || curCurrent != (curSelf && curExt)) {
                    resultFilter.emplace_back(delta);
                    curCurrent = curSelf && curExt;
                } else {
                    resultFilter.back() += delta;
                }
                count += delta;
            }
            if (!curSelf) {
                extPos += *itSelf;
                selfPos += *itSelf;
                curSelf = !curSelf;
                ++itSelf;
            } else if (selfPos + *itSelf < extPos + *itExt) {
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
                selfPos += *itSelf;
                extPos += *itExt;
                ++itSelf;
                ++itExt;
            }
        }
        AFL_VERIFY(itSelf == Filter.end() && itExt == extFilter.Filter.cend());
        TColumnFilter result = TColumnFilter::BuildAllowFilter();
        std::swap(resultFilter, result.Filter);
        std::swap(curCurrent, result.LastValue);
        std::swap(count, result.RecordsCount);
        return result;
    }
}

TColumnFilter::TIterator TColumnFilter::GetBegin(const bool reverse, const ui32 expectedSize) const {
    return GetIterator(reverse, expectedSize, 0);
}

TColumnFilter::TIterator TColumnFilter::GetIterator(const bool reverse, const ui32 expectedSize, const ui64 startOffset) const {
    AFL_VERIFY(expectedSize >= startOffset);
    if (IsTotalAllowFilter()) {
        return TIterator(reverse, expectedSize, true, startOffset);
    } else if (IsTotalDenyFilter()) {
        return TIterator(reverse, expectedSize, false, startOffset);
    } else {
        AFL_VERIFY(expectedSize == GetRecordsCountVerified())("expected", expectedSize)("count", GetRecordsCountVerified())("reverse", reverse);
        return TIterator(reverse, Filter, GetStartValue(reverse), startOffset);
    }
}

std::optional<ui32> TColumnFilter::GetFilteredCount() const {
    if (!FilteredCount) {
        if (IsTotalAllowFilter()) {
            if (!RecordsCount) {
                return {};
            } else {
                FilteredCount = RecordsCount;
            }
        } else if (IsTotalDenyFilter()) {
            FilteredCount = 0;
        } else {
            FilteredCount = 0;
            bool current = GetStartValue();
            for (auto&& i : Filter) {
                if (current) {
                    *FilteredCount += i;
                }
                current = !current;
            }
        }
    }
    return *FilteredCount;
}

void TColumnFilter::Append(const TColumnFilter& filter) {
    bool currentVal = filter.GetStartValue();
    for (auto&& i : filter.Filter) {
        Add(currentVal, i);
        currentVal = !currentVal;
    }
}

std::optional<ui32> TColumnFilter::GetRecordsCount() const {
    if (Filter.size()) {
        AFL_VERIFY(RecordsCount);
        return RecordsCount;
    } else {
        return std::nullopt;
    }
}

ui32 TColumnFilter::GetRecordsCountVerified() const {
    AFL_VERIFY(Filter.size());
    AFL_VERIFY(RecordsCount);
    return RecordsCount;
}

ui32 TColumnFilter::GetFilteredCountVerified() const {
    const std::optional<ui32> result = GetFilteredCount();
    AFL_VERIFY(!!result);
    return *result;
}

TColumnFilter TColumnFilter::Slice(const ui32 offset, const ui32 count) const {
    AFL_VERIFY(count);
    if (IsTotalAllowFilter()) {
        return TColumnFilter::BuildAllowFilter();
    }
    if (IsTotalDenyFilter()) {
        return TColumnFilter::BuildDenyFilter();
    }
    AFL_VERIFY(offset + count <= GetRecordsCountVerified())("offset", offset)("count", count)("records_count", GetRecordsCountVerified());
    std::vector<ui32> chunks;
    ui32 index = 0;
    bool currentValue = GetStartValue();

    const auto buildResult = [](std::vector<ui32>&& chunks, const bool currentValue, const ui32 count) {
        TColumnFilter result = TColumnFilter::BuildAllowFilter();
        result.LastValue = !currentValue;
        result.Filter = std::move(chunks);
        result.RecordsCount = count;
        return result;
    };
    ui32 countFilter = 0;
    for (auto&& i : Filter) {
        const ui32 nextIndex = index + i;
        if (index >= offset + count) {
            AFL_VERIFY(countFilter == count);
            return buildResult(std::move(chunks), currentValue, count);
        } else if (nextIndex > offset) {
            chunks.emplace_back(std::min(nextIndex, offset + count) - std::max(index, offset));
            countFilter += chunks.back();
        }
        currentValue = !currentValue;
        index = nextIndex;
    }
    AFL_VERIFY(countFilter == count);
    AFL_VERIFY(offset + count <= index)("index", index)("offset", offset)("count", count);
    return buildResult(std::move(chunks), currentValue, count);
}

bool TColumnFilter::CheckSlice(const ui32 offset, const ui32 count) const {
    if (IsTotalAllowFilter()) {
        return true;
    }
    if (IsTotalDenyFilter()) {
        return false;
    }
    ui32 index = 0;
    bool currentValue = GetStartValue();
    for (auto&& i : Filter) {
        const ui32 nextIndex = index + i;
        if (index >= offset + count) {
            return false;
        } else if (nextIndex > offset) {
            if (currentValue) {
                return true;
            }
        }
        currentValue = !currentValue;
        index = nextIndex;
    }
    AFL_VERIFY(offset + count <= index)("index", index)("offset", offset)("count", count);
    return false;
}

TString TColumnFilter::DebugString() const {
    TStringBuilder sb;
    sb << "{" << GetStartValue() << "}";
    sb << "[";
    for (ui32 i = 0; i < Filter.size(); ++i) {
        sb << Filter[i];
        if (i + 1 < Filter.size()) {
            sb << ",";
        }
    }
    sb << "]";
    return sb;
}

TColumnFilter TColumnFilter::Cut(const ui32 totalRecordsCount, const ui32 limit, const bool reverse) const {
    if (IsTotalDenyFilter()) {
        return TColumnFilter::BuildDenyFilter();
    }
    TColumnFilter result = TColumnFilter::BuildAllowFilter();
    if (IsTotalAllowFilter()) {
        if (totalRecordsCount <= limit) {
            return result;
        }
        if (reverse) {
            result.Add(false, totalRecordsCount - limit);
            result.Add(true, limit);
        } else {
            result.Add(true, limit);
            result.Add(false, totalRecordsCount - limit);
        }
    } else {
        AFL_VERIFY(GetRecordsCountVerified() == totalRecordsCount)("total", GetRecordsCountVerified())("ext", totalRecordsCount);
        ui32 cutCount = 0;
        bool currentValue = reverse ? LastValue : GetStartValue();
        const auto scan = [&](auto begin, auto end) {
            for (auto it = begin; it != end; ++it) {
                AFL_VERIFY(cutCount <= limit);
                if (currentValue) {
                    if (cutCount < limit) {
                        if (limit <= cutCount + *it) {
                            result.Add(true, limit - cutCount);
                            result.Add(false, cutCount + *it - limit);
                            cutCount = limit;
                        } else {
                            result.Add(true, *it);
                            cutCount += *it;
                        }
                    } else {
                        result.Add(false, *it);
                    }
                }
                if (!currentValue) {
                    result.Add(false, *it);
                }
                currentValue = !currentValue;
            }
        };
        if (reverse) {
            scan(Filter.rbegin(), Filter.rend());
        } else {
            scan(Filter.begin(), Filter.end());
        }
        if (reverse) {
            std::reverse(result.Filter.begin(), result.Filter.end());
            result.LastValue = result.GetStartValue();
        }
    }
    return result;
}

}   // namespace NKikimr::NArrow
