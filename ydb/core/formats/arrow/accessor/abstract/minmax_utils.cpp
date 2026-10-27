#include "minmax_utils.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/scalar/serialization.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/c/bridge.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/chunked_array.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api_aggregate.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/array/util.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/c/bridge.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/api_aggregate.h>

namespace NKikimr::NArrow::NAccessor {

TMinMax TMinMax::Compute(const std::shared_ptr<arrow::Array>& arr, const NScheme::TTypeInfo& logicalType) {
    if (arr->type_id() == arrow::Type::FIXED_SIZE_BINARY) {
        const auto& values = static_cast<const arrow::FixedSizeBinaryArray&>(*arr);
        i64 min = -1;
        i64 max = -1;
        for (i64 i = 0; i < values.length(); ++i) {
            if (values.IsNull(i)) {
                continue;
            }

            if (min == -1) {
                min = max = i;
                continue;
            }

            if (TComparator::CompareValueNotNull(values.GetView(i), values.GetView(min), logicalType) < 0) {
                min = i;
            }

            if (TComparator::CompareValueNotNull(values.GetView(i), values.GetView(max), logicalType) > 0) {
                max = i;
            }
        }

        auto result = MakeNull(arr->type());
        if (min != -1) {
            result.Min() = arr->GetScalar(min).ValueOrDie();
            result.Max() = arr->GetScalar(max).ValueOrDie();
            result.ComparisonOrder = TComparator::GetComparisonOrder(logicalType);
        }

        return result;
    }

    struct ArrowArray c_array;
    struct ArrowSchema c_schema;
    namespace arrow5 = arrow;

    arrow5::Status export_status = arrow5::ExportArray(*arr, &c_array, &c_schema);
    AFL_VERIFY(export_status.ok())("error", export_status.ToString());

    auto import_result = arrow20::ImportArray(&c_array, &c_schema).ValueOrDie();

    auto scalar20 = arrow20::compute::MinMax(import_result).ValueOrDie().scalar();

    auto new_array = arrow20::MakeArrayFromScalar(*scalar20, 1).ValueOrDie();

    arrow20::Status export_status20 = arrow20::ExportArray(*new_array, &c_array, &c_schema);
    AFL_VERIFY(export_status20.ok())("error", export_status20.ToString());

    auto import_result5 = arrow5::ImportArray(&c_array, &c_schema).ValueOrDie();
    auto res= *dynamic_cast<arrow5::StructScalar*>(import_result5->GetScalar(0).ValueOrDie().get());
    AFL_VERIFY(res.type->field(0)->name() == "min");
    AFL_VERIFY(res.type->field(1)->name() == "max");
    return res;
}

std::shared_ptr<arrow::DataType> TMinMax::ElementType() const {
    AFL_VERIFY(Min()->type->Equals(Max()->type))("details", "inconsistent TMinMax state");
    return Min()->type;
}

TMinMax TMinMax::Compute(const std::shared_ptr<arrow::ChunkedArray>& arr, const NScheme::TTypeInfo& logicalType) {
    auto res = TMinMax::MakeNull(arr->type());
    for (const auto& chunk : arr->chunks()) {
        res.UniteWith(Compute(chunk, logicalType), logicalType);
    }

    return res;
}

using TSizeType = ui32;
constexpr char NullTMinMaxFlag = '0';
constexpr char DefaultOrderFlag = '1';
constexpr char DecimalNumericOrderFlag = '2';

TMinMax TMinMax::FromBinaryString(std::string_view data, const std::shared_ptr<arrow::DataType>& fieldType) {
    AFL_VERIFY(!data.empty())("details", "empty binary minmax payload");
    if (data[0] == NullTMinMaxFlag) {
        AFL_VERIFY(data.size() == 1);
        return MakeNull(fieldType);
    }

    AFL_VERIFY(data[0] == DefaultOrderFlag || data[0] == DecimalNumericOrderFlag);
    std::vector<std::shared_ptr<arrow::Scalar>> fields;
    ui64 offset = 1;
    auto readNext = [&] {
        TSizeType size = 0;
        AFL_VERIFY(offset <= data.size() && sizeof(TSizeType) <= data.size() - offset )("details",
                                                               Sprintf("out of bounds read, data.size(): %i, current_offset: %i, read size: %i",
                                                                   data.size(), offset, sizeof(TSizeType)));
        memcpy(&size, data.data() + offset, sizeof(size));
        offset += sizeof(TSizeType);
        AFL_VERIFY(offset <= data.size() && size <= data.size() - offset)(
                                                  "details", Sprintf("out of bounds read, data.size(): %i, current_offset: %i, read size: %i",
                                                                 data.size(), offset, size));
        auto res = NArrow::NScalar::TSerializer::DeserializeFromStringWithPayload(
            TStringBuf{ data.data() + offset, data.data() + offset + size }, fieldType)
                       .DetachResult();
        offset += size;
        return res;
    };
    fields.push_back(readNext());
    fields.push_back(readNext());
    AFL_VERIFY(offset == data.size());
    TMinMax result = *arrow::StructScalar::Make(fields, { "min", "max" }).ValueOrDie();
    result.ComparisonOrder =
        data[0] == DecimalNumericOrderFlag ? TComparator::EComparisonOrder::DecimalNumeric : TComparator::EComparisonOrder::Default;
    return result;
}

TString TMinMax::ToBinaryString() const {
    if (IsNull()) {
        return std::string{ NullTMinMaxFlag };
    }
    TString res;
    // Preserve the existing format for the default order, including old Decimal indexes.
    res.push_back(ComparisonOrder == TComparator::EComparisonOrder::Default ? DefaultOrderFlag : DecimalNumericOrderFlag);
    TString minSerialized = NArrow::NScalar::TSerializer::SerializePayloadToString(Min()).DetachResult();
    TString maxSerialized = NArrow::NScalar::TSerializer::SerializePayloadToString(Max()).DetachResult();
    auto writeNext = [&](TStringBuf data) {
        TSizeType dataSize = data.size();
        ui64 resSize = res.size();
        res.resize(resSize + sizeof(TSizeType));
        memcpy(res.MutRef().data() + resSize, &dataSize, sizeof(TSizeType));
        res.append(data);
    };
    writeNext(minSerialized);
    writeNext(maxSerialized);
    return res;
}

bool TMinMax::IsNull() const {
    AFL_VERIFY(Min()->is_valid == Max()->is_valid)("details", "inconsistent state");
    return !Min()->is_valid;
}

void TMinMax::UniteWith(const TMinMax& other, const NScheme::TTypeInfo& logicalType) {
    AFL_VERIFY(other.MinMax.type->Equals(MinMax.type));
    AFL_VERIFY(other.ElementType()->Equals(ElementType()));
    
    if (other.IsNull()) {
        return;
    }

    AFL_VERIFY(other.ComparisonOrder == TComparator::GetComparisonOrder(logicalType));
    if (IsNull()) {
        *this = other;
        return;
    }

    AFL_VERIFY(ComparisonOrder == other.ComparisonOrder);
    if (ScalarComparePartial(*Max(), *other.Max(), logicalType) < 0) {
        Max() = other.Max();
    }

    if (ScalarComparePartial(*Min(), *other.Min(), logicalType) > 0) {
        Min() = other.Min();
    }
}

std::partial_ordering TMinMax::CompareWithMin(const std::shared_ptr<arrow::Scalar>& value, const NScheme::TTypeInfo& logicalType) const {
    // Bounds computed in a different order cannot prove that a predicate has no matches.
    if (ComparisonOrder != TComparator::GetComparisonOrder(logicalType)) {
        return std::partial_ordering::unordered;
    }

    return ScalarComparePartial(value, Min(), logicalType);
}

std::partial_ordering TMinMax::CompareWithMax(const std::shared_ptr<arrow::Scalar>& value, const NScheme::TTypeInfo& logicalType) const {
    if (ComparisonOrder != TComparator::GetComparisonOrder(logicalType)) {
        return std::partial_ordering::unordered;
    }

    return ScalarComparePartial(value, Max(), logicalType);
}

}   // namespace NKikimr::NArrow::NAccessor
