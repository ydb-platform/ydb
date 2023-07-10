#include "arrow_filter.h"
#include "switch_type.h"
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/chunked_array.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api_vector.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

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

std::shared_ptr<arrow::BooleanArray> TColumnFilter::BuildArrowFilter(const ui32 expectedSize) const {
    arrow::BooleanBuilder builder;
    auto res = builder.Reserve(expectedSize);
    Y_VERIFY_OK(builder.AppendValues(BuildSimpleFilter(expectedSize)));
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

void TColumnFilter::Reset(const ui32 count) {
    Count = 0;
    FilterPlain.reset();
    Filter.clear();
    Filter.reserve(count / 4);
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

ui32 TColumnFilter::CrossSize(const ui32 s1, const ui32 f1, const ui32 s2, const ui32 f2) {
    const ui32 f = std::min(f1, f2);
    const ui32 s = std::max(s1, s2);
    Y_VERIFY(f >= s);
    return f - s;
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

    std::vector<bool> bits;
    bits.reserve(cmps.size());

    switch (compareType) {
        case ECompareType::LESS:
            for (size_t i = 0; i < cmps.size(); ++i) {
                bits.emplace_back(cmps[i] < ECompareResult::BORDER);
            }
            break;
        case ECompareType::LESS_OR_EQUAL:
            for (size_t i = 0; i < cmps.size(); ++i) {
                bits.emplace_back(cmps[i] <= ECompareResult::BORDER);
            }
            break;
        case ECompareType::GREATER:
            for (size_t i = 0; i < cmps.size(); ++i) {
                bits.emplace_back(cmps[i] > ECompareResult::BORDER);
            }
            break;
        case ECompareType::GREATER_OR_EQUAL:
            for (size_t i = 0; i < cmps.size(); ++i) {
                bits.emplace_back(cmps[i] >= ECompareResult::BORDER);
            }
            break;
    }

    return NArrow::TColumnFilter(std::move(bits));
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
    auto res = arrow::compute::Filter(batch, BuildArrowFilter(batch->num_rows()));
    Y_VERIFY_S(res.ok(), res.status().message());
    Y_VERIFY((*res).kind() == arrow::Datum::RECORD_BATCH);
    batch = (*res).record_batch();
    return batch->num_rows();
}

const std::vector<bool>& TColumnFilter::BuildSimpleFilter(const ui32 expectedSize) const {
    if (!FilterPlain) {
        Y_VERIFY(expectedSize == Count || !Count);
        std::vector<bool> result;
        if (Count) {
            result.resize(Count, true);
            bool currentValue = GetStartValue();
            ui32 currentPosition = 0;
            for (auto&& i : Filter) {
                if (!currentValue) {
                    memset(&result[currentPosition], 0, sizeof(bool) * i);
                }
                currentPosition += i;
                currentValue = !currentValue;
            }
        } else {
            result.resize(expectedSize, DefaultFilterValue);
        }
        FilterPlain = std::move(result);
    }
    Y_VERIFY(FilterPlain->size() == expectedSize);
    return *FilterPlain;
}

class TMergePolicyAnd {
private:
public:
    static bool Calc(const bool a, const bool b) {
        return a && b;
    }
    static TColumnFilter MergeWithSimple(const TColumnFilter& filter, const bool simpleValue) {
        if (simpleValue) {
            return filter;
        } else {
            return TColumnFilter::BuildStopFilter();
        }
    }
};

class TMergePolicyOr {
private:
public:
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
};

class TColumnFilter::TMergerImpl {
private:
    const TColumnFilter& Filter1;
    const TColumnFilter& Filter2;
public:
    TMergerImpl(const TColumnFilter& filter1, const TColumnFilter& filter2)
        : Filter1(filter1)
        , Filter2(filter2)
    {

    }

    template <class TMergePolicy>
    TColumnFilter Merge() const {
        if (Filter1.empty() && Filter2.empty()) {
            return TColumnFilter(TMergePolicy::Calc(Filter1.DefaultFilterValue, Filter2.DefaultFilterValue));
        } else if (Filter1.empty()) {
            return TMergePolicy::MergeWithSimple(Filter2, Filter1.DefaultFilterValue);
        } else if (Filter2.empty()) {
            return TMergePolicy::MergeWithSimple(Filter1, Filter2.DefaultFilterValue);
        } else {
            Y_VERIFY(Filter1.Count == Filter2.Count);
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

            while (it1 != Filter1.Filter.end() && it2 != Filter2.Filter.cend()) {
                const ui32 delta = TColumnFilter::CrossSize(pos2, pos2 + *it2, pos1, pos1 + *it1);
                if (delta) {
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
                    ++it1;
                    ++it2;
                }
            }
            Y_VERIFY(it1 == Filter1.Filter.end() && it2 == Filter2.Filter.cend());
            TColumnFilter result = TColumnFilter::BuildAllowFilter();
            std::swap(resultFilter, result.Filter);
            std::swap(curCurrent, result.CurrentValue);
            std::swap(count, result.Count);
            return result;
        }
    }

};

TColumnFilter TColumnFilter::And(const TColumnFilter& extFilter) const {
    FilterPlain.reset();
    return TMergerImpl(*this, extFilter).Merge<TMergePolicyAnd>();
}

TColumnFilter TColumnFilter::Or(const TColumnFilter& extFilter) const {
    FilterPlain.reset();
    return TMergerImpl(*this, extFilter).Merge<TMergePolicyOr>();
}

TColumnFilter TColumnFilter::CombineSequentialAnd(const TColumnFilter& extFilter) const {
    if (Filter.empty()) {
        return TMergePolicyAnd::MergeWithSimple(extFilter, DefaultFilterValue);
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
            Y_VERIFY(!curSelf || itExt != extFilter.Filter.cend());
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
                ++itSelf;
                ++itExt;
            }
        }
        Y_VERIFY(itSelf == Filter.end() && itExt == extFilter.Filter.cend());
        TColumnFilter result = TColumnFilter::BuildAllowFilter();
        std::swap(resultFilter, result.Filter);
        std::swap(curCurrent, result.CurrentValue);
        std::swap(count, result.Count);
        return result;
    }
}

}
