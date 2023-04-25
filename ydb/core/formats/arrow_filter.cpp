#include "arrow_filter.h"
#include "switch_type.h"
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/chunked_array.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api_vector.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <ydb/core/util/yverify_stream.h>

namespace NKikimr::NArrow {

#define Y_VERIFY_OK(status) Y_VERIFY(status.ok(), "%s", status.ToString().c_str())

namespace {
enum class ECompareResult: i8 {
    LESS = -1,
    BORDER = 0,
    GREATER = 1
};

template <typename TArray>
inline auto GetValue(const std::shared_ptr<TArray>& array, int pos) {
    return array->GetView(pos);
}

template <typename T>
inline void UpdateCompare(const T& value, const T& border, ECompareResult& res) {
    if (res == ECompareResult::BORDER) {
        if constexpr (std::is_same_v<T, arrow::util::string_view>) {
            size_t minSize = (value.size() < border.size()) ? value.size() : border.size();
            int cmp = memcmp(value.data(), border.data(), minSize);
            if (cmp < 0) {
                res = ECompareResult::LESS;
            } else if (cmp > 0) {
                res = ECompareResult::GREATER;
            } else {
                UpdateCompare(value.size(), border.size(), res);
            }
        } else {
            if (value < border) {
                res = ECompareResult::LESS;
            } else if (value > border) {
                res = ECompareResult::GREATER;
            }
        }
    }
}

template <typename TArray, typename T>
bool CompareImpl(const std::shared_ptr<arrow::Array>& column, const T& border,
    std::vector<NArrow::ECompareResult>& rowsCmp) {
    bool hasBorder = false;
    ECompareResult* res = &rowsCmp[0];
    auto array = std::static_pointer_cast<TArray>(column);

    for (int i = 0; i < array->length(); ++i, ++res) {
        UpdateCompare(GetValue(array, i), border, *res);
        hasBorder = hasBorder || (*res == ECompareResult::BORDER);
    }
    return !hasBorder;
}

template <typename TArray, typename T>
bool CompareImpl(const std::shared_ptr<arrow::ChunkedArray>& column, const T& border,
    std::vector<NArrow::ECompareResult>& rowsCmp) {
    bool hasBorder = false;
    ECompareResult* res = &rowsCmp[0];

    for (auto& chunk : column->chunks()) {
        auto array = std::static_pointer_cast<TArray>(chunk);

        for (int i = 0; i < chunk->length(); ++i, ++res) {
            UpdateCompare(GetValue(array, i), border, *res);
            hasBorder = hasBorder || (*res == ECompareResult::BORDER);
        }
    }
    return !hasBorder;
}

/// @return true in case we have no borders in compare: no need for future keys, allow early exit
template <typename TArray>
bool Compare(const arrow::Datum& column, const std::shared_ptr<arrow::Array>& borderArray,
    std::vector<NArrow::ECompareResult>& rowsCmp) {
    auto border = GetValue(std::static_pointer_cast<TArray>(borderArray), 0);

    switch (column.kind()) {
        case arrow::Datum::ARRAY:
            return CompareImpl<TArray>(column.make_array(), border, rowsCmp);
        case arrow::Datum::CHUNKED_ARRAY:
            return CompareImpl<TArray>(column.chunked_array(), border, rowsCmp);
        default:
            break;
    }
    Y_VERIFY(false);
    return false;
}

bool SwitchCompare(const arrow::Datum& column, const std::shared_ptr<arrow::Array>& border,
    std::vector<NArrow::ECompareResult>& rowsCmp) {
    Y_VERIFY(border->length() == 1);

    // first time it's empty
    if (rowsCmp.empty()) {
        rowsCmp.resize(column.length(), ECompareResult::BORDER);
    }

    return SwitchArrayType(column, [&](const auto& type) -> bool {
        using TWrap = std::decay_t<decltype(type)>;
        using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;
        return Compare<TArray>(column, border, rowsCmp);
        });
}

template <typename T>
void CompositeCompare(std::shared_ptr<T> some, std::shared_ptr<arrow::RecordBatch> borderBatch,
    std::vector<NArrow::ECompareResult>& rowsCmp) {
    auto key = borderBatch->schema()->fields();
    Y_VERIFY(key.size());

    for (size_t i = 0; i < key.size(); ++i) {
        auto& field = key[i];
        auto typeId = field->type()->id();
        auto column = some->GetColumnByName(field->name());
        std::shared_ptr<arrow::Array> border = borderBatch->GetColumnByName(field->name());
        Y_VERIFY(column);
        Y_VERIFY(border);
        Y_VERIFY(some->schema()->GetFieldByName(field->name())->type()->id() == typeId);

        if (SwitchCompare(column, border, rowsCmp)) {
            break; // early exit in case we have all rows compared: no borders, can omit key tail
        }
    }
}
}

std::shared_ptr<arrow::BooleanArray> TColumnFilter::MakeFilter() const {
    arrow::BooleanBuilder builder;
    auto res = builder.Reserve(Count);
    Y_VERIFY_OK(res);
    bool currentFilter = GetStartValue();
    for (auto&& i : Filter) {
        Y_VERIFY_OK(builder.AppendValues(i, currentFilter));
        currentFilter = !currentFilter;
    }

    std::shared_ptr<arrow::BooleanArray> out;
    res = builder.Finish(&out);
    Y_VERIFY_OK(res);
    return out;
}

bool TColumnFilter::IsTotalAllowFilter() const {
    if (DefaultFilterValue && Filter.empty()) {
        return true;
    }
    if (Filter.size() == 1 && CurrentValue) {
        return true;
    }
    return false;
}

bool TColumnFilter::IsTotalDenyFilter() const {
    if (!DefaultFilterValue && Filter.empty()) {
        return true;
    }
    if (Filter.size() == 1 && !CurrentValue) {
        return true;
    }
    return false;
}

void TColumnFilter::Reset(const ui32 /*count*/) {
    Count = 0;
    Filter.clear();
}

void TColumnFilter::Add(const bool value, const ui32 count) {
    if (!count) {
        return;
    }
    if (Y_UNLIKELY(CurrentValue != value || !Count)) {
        Filter.emplace_back(count);
        CurrentValue = value;
    } else {
        Filter.back() += count;
    }
    Count += count;
}

ui32 CrossSize(const ui32 s1, const ui32 f1, const ui32 s2, const ui32 f2) {
    const ui32 f = std::min(f1, f2);
    const ui32 s = std::max(s1, s2);
    Y_VERIFY(f >= s);
    return f - s;
}

void TColumnFilter::And(const TColumnFilter& extFilter) {
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
                if (!count || curCurrent != (curSelf && curExt)) {
                    result.emplace_back(delta);
                    curCurrent = (curSelf && curExt);
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

NKikimr::NArrow::TColumnFilter TColumnFilter::MakePredicateFilter(const arrow::Datum& datum, const arrow::Datum& border, ECompareType compareType) {
    std::vector<ECompareResult> cmps;

    switch (datum.kind()) {
        case arrow::Datum::ARRAY:
            Y_VERIFY(border.kind() == arrow::Datum::ARRAY);
            SwitchCompare(datum, border.make_array(), cmps);
            break;
        case arrow::Datum::CHUNKED_ARRAY:
            Y_VERIFY(border.kind() == arrow::Datum::ARRAY);
            SwitchCompare(datum, border.make_array(), cmps);
            break;
        case arrow::Datum::RECORD_BATCH:
            Y_VERIFY(border.kind() == arrow::Datum::RECORD_BATCH);
            CompositeCompare(datum.record_batch(), border.record_batch(), cmps);
            break;
        case arrow::Datum::TABLE:
            Y_VERIFY(border.kind() == arrow::Datum::RECORD_BATCH);
            CompositeCompare(datum.table(), border.record_batch(), cmps);
            break;
        default:
            Y_VERIFY(false);
            break;
    }

    NArrow::TColumnFilter result;
    result.Reset(cmps.size());

    switch (compareType) {
        case ECompareType::LESS:
            for (size_t i = 0; i < cmps.size(); ++i) {
                result.Add(cmps[i] < ECompareResult::BORDER);
            }
            break;
        case ECompareType::LESS_OR_EQUAL:
            for (size_t i = 0; i < cmps.size(); ++i) {
                result.Add(cmps[i] <= ECompareResult::BORDER);
            }
            break;
        case ECompareType::GREATER:
            for (size_t i = 0; i < cmps.size(); ++i) {
                result.Add(cmps[i] > ECompareResult::BORDER);
            }
            break;
        case ECompareType::GREATER_OR_EQUAL:
            for (size_t i = 0; i < cmps.size(); ++i) {
                result.Add(cmps[i] >= ECompareResult::BORDER);
            }
            break;
    }

    return result;
}

bool TColumnFilter::Apply(std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch || !batch->num_rows()) {
        return false;
    }
    Y_VERIFY_S(Filter.empty() || Count == (size_t)batch->num_rows(), Count << " != " << batch->num_rows());
    if (IsTotalDenyFilter()) {
        batch = batch->Slice(0, 0);
        return false;
    }
    if (IsTotalAllowFilter()) {
        return true;
    }
    auto res = arrow::compute::Filter(batch, MakeFilter());
    Y_VERIFY_S(res.ok(), res.status().message());
    Y_VERIFY((*res).kind() == arrow::Datum::RECORD_BATCH);
    batch = (*res).record_batch();
    return batch->num_rows();
}

void TColumnFilter::CombineSequential(const TColumnFilter& extFilter) {
    if (Filter.empty()) {
        DefaultFilterValue = DefaultFilterValue && extFilter.DefaultFilterValue;
        Filter = extFilter.Filter;
        Count = extFilter.Count;
    } else if (extFilter.Filter.empty()) {
        if (!extFilter.DefaultFilterValue) {
            DefaultFilterValue = DefaultFilterValue && extFilter.DefaultFilterValue;
            Filter.clear();
            Count = 0;
        }
    } else {
        auto itSelf = Filter.begin();
        auto itExt = extFilter.Filter.cbegin();

        std::deque<ui32> result;
        ui32 selfPos = 0;
        ui32 extPos = 0;
        bool curSelf = GetStartValue();
        bool curExt = extFilter.GetStartValue();
        bool curCurrent = false;
        ui32 count = 0;

        while (itSelf != Filter.end()) {
            Y_VERIFY(!curSelf || itExt != extFilter.Filter.cend());
            const ui32 delta = curSelf ? CrossSize(extPos, extPos + *itExt, selfPos, selfPos + *itSelf) : *itSelf;
            if (delta) {
                if (!count || curCurrent != (curSelf && curExt)) {
                    result.emplace_back(delta);
                    curCurrent = curSelf && curExt;
                } else {
                    result.back() += delta;
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

void TColumnFilter::CutInactiveTail() {
    if (Filter.empty()) {
        return;
    }
    if (!CurrentValue) {
        Count -= Filter.back();
        Filter.pop_back();
        CurrentValue = !CurrentValue;
        if (Filter.empty()) {
            CurrentValue = DefaultFilterValue;
        }
    }
}

void TColumnFilter::CutInactiveHead() {
    if (Filter.empty()) {
        return;
    }
    if (!GetStartValue()) {
        Count -= Filter.front();
        Filter.pop_front();
        if (Filter.empty()) {
            CurrentValue = DefaultFilterValue;
        }
    }
}

ui32 TColumnFilter::GetInactiveTailSize() const {
    if (Filter.empty()) {
        return 0;
    }
    if (CurrentValue) {
        return 0;
    } else {
        return Filter.back();
    }
}

ui32 TColumnFilter::GetInactiveHeadSize() const {
    if (Filter.empty()) {
        return 0;
    }
    if (GetStartValue()) {
        return 0;
    } else {
        return Filter[0];
    }
}

std::vector<bool> TColumnFilter::BuildFilter() const {
    std::vector<bool> result;
    result.reserve(Count);
    bool currentValue = GetStartValue();
    for (auto&& i : Filter) {
        for (ui32 idx = 0; idx < i; ++idx) {
            result.emplace_back(currentValue);
        }
        currentValue = !currentValue;
    }
    return result;
}

}
