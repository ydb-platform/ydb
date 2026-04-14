#include "minmax_utils.h"

#include <ydb/core/formats/arrow/program/functions.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/arrow_kernels/operations.h>
#include <ydb/library/formats/arrow/scalar/serialization.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/c/bridge.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/chunked_array.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api_aggregate.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/array/util.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/c/bridge.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/api_aggregate.h>

namespace NKikimr::NArrow::NAccessor {

TMinMax TMinMax::Compute(std::shared_ptr<arrow::Array> arr) {
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

TMinMax TMinMax::Compute(std::shared_ptr<arrow::ChunkedArray> arr) {
    auto res = TMinMax::MakeNull(arr->type());
    for (const auto& chunk : arr->chunks()) {
        res.UniteWith(Compute(chunk));
    }
    return res;
}

using TSizeType = ui32;
constexpr char NullTMinMaxFlag = '0';

TMinMax TMinMax::FromBinaryString(std::string_view data, const std::shared_ptr<arrow::DataType>& fieldType) {
    AFL_VERIFY(!data.empty())("details", "empty binary minmax payload");
    if (data[0] == NullTMinMaxFlag) {
        AFL_VERIFY(data.size() == 1);
        return MakeNull(fieldType);
    }
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
    return *arrow::StructScalar::Make(fields, { "min", "max" }).ValueOrDie();
}

TString TMinMax::ToBinaryString() const {
    if (IsNull()) {
        return std::string{ NullTMinMaxFlag };
    }
    TString res;
    res.push_back(NullTMinMaxFlag + 1);
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

NJson::TJsonValue TMinMax::ToJson() const {
    NJson::TJsonValue json;
    json.InsertValue("min", Min()->ToString());
    json.InsertValue("max", Max()->ToString());
    return json;
}

bool TMinMax::IsNull() const {
    AFL_VERIFY(Min()->is_valid == Max()->is_valid)("details", "inconsistent state");
    return !Min()->is_valid;
}

void TMinMax::UniteWith(TMinMax other) {
    AFL_VERIFY(other.MinMax.type->Equals(MinMax.type));
    AFL_VERIFY(other.ElementType()->Equals(ElementType()));
    
    if (other.IsNull()) {
        return;
    }

    if (IsNull()) {
        *this = other;
        return;
    }

    if (NArrowCompare::Less(Max(), other.Max())) {
        Max() = other.Max();
    }
    if (NArrowCompare::Greater(Min(), other.Min())) {
        Min() = other.Min();
    }
}

namespace NArrowCompare {

bool cmp(NKikimr::NKernels::EOperation op, const std::shared_ptr<arrow::Scalar>& left, const std::shared_ptr<arrow::Scalar>& right) {
    arrow::Datum res = NKikimr::NArrow::TStatusValidator::GetValid(
        arrow::compute::CallFunction(NKikimr::NArrow::NSSA::TSimpleFunction::GetFunctionName(op), { left, right }));
    return res.scalar_as<arrow::BooleanScalar>().value;
}

bool Less(const std::shared_ptr<arrow::Scalar>& left, const std::shared_ptr<arrow::Scalar>& right) {
    return cmp(NKernels::EOperation::Less, left, right);
}

bool Greater(const std::shared_ptr<arrow::Scalar>& left, const std::shared_ptr<arrow::Scalar>& right) {
    return cmp(NKernels::EOperation::Greater, left, right);
}

bool LessOrEqual(const std::shared_ptr<arrow::Scalar>& left, const std::shared_ptr<arrow::Scalar>& right) {
    return cmp(NKernels::EOperation::LessEqual, left, right);
}

bool GreaterOrEqual(const std::shared_ptr<arrow::Scalar>& left, const std::shared_ptr<arrow::Scalar>& right) {
    return cmp(NKernels::EOperation::GreaterEqual, left, right);
}

}   // namespace NArrowCompare

}   // namespace NKikimr::NArrow::NAccessor
