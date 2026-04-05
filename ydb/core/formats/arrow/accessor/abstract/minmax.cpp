#include "minmax_with_arrow_next.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/scalar/serialization.h>

#include <arrow/array/array_base.h>
#include <arrow/c/bridge.h>   //order matters
#include <arrow/chunked_array.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/array/util.h>   // Для MakeArrayFromScalar
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/c/bridge.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/api_aggregate.h>

namespace NKikimr::NArrow::NAccessor {

NKikimr::NArrow::NAccessor::TMinMax ComputeMinMaxWithArrowNext(std::shared_ptr<arrow::Array> arr) {
    struct ArrowArray c_array;
    struct ArrowSchema c_schema;
    namespace arrow5 = arrow;

    arrow5::Status export_status = arrow5::ExportArray(*arr, &c_array, &c_schema);
    AFL_VERIFY(export_status.ok());

    auto import_result = arrow20::ImportArray(&c_array, &c_schema).ValueOrDie();

    auto scalar20 = arrow20::compute::MinMax(import_result).ValueOrDie().scalar();

    auto new_array = arrow20::MakeArrayFromScalar(*scalar20, 1).ValueOrDie();

    auto export_status20 = arrow20::ExportArray(*new_array, &c_array, &c_schema);
    AFL_VERIFY(export_status.ok());

    auto import_result5 = arrow5::ImportArray(&c_array, &c_schema).ValueOrDie();

    // --- ШАГ 3: Извлекаем скаляр из массива (старая версия) ---
    return *dynamic_cast<arrow5::StructScalar*>(import_result5->GetScalar(0).ValueOrDie().get());
}
NKikimr::NArrow::NAccessor::TMinMax ComputeMinMaxWithArrowNext(std::shared_ptr<arrow::ChunkedArray> arr) {
    auto res = TMinMax::MakeNull(arr->type());
    for (auto& chunk : arr->chunks()) {
        res.UniteWith(ComputeMinMaxWithArrowNext(chunk));
    }
    return res;
}
using TSizeType = ui32;
constexpr char NullTMinMaxFlag = '0';

TMinMax TMinMax::FromBinaryString(std::string_view data, const std::shared_ptr<arrow::DataType>& fieldType) {
    if (data[0] == NullTMinMaxFlag) {
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

std::string TMinMax::ToBinaryString() const {
    if (IsNull()) {
        return std::string{ NullTMinMaxFlag };
    }
    std::string res;
    res.push_back(NullTMinMaxFlag + 1);
    TString minSerialized = NArrow::NScalar::TSerializer::SerializePayloadToString(Min()).DetachResult();
    TString maxSerialized = NArrow::NScalar::TSerializer::SerializePayloadToString(Max()).DetachResult();
    auto writeNext = [&](TStringBuf data) {
        TSizeType dataSize = data.size();
        ui64 resSize = res.size();
        res.resize(resSize + sizeof(TSizeType));
        memcpy(res.data() + resSize, &dataSize, sizeof(TSizeType));
        res.append(data);
    };
    writeNext(minSerialized);
    writeNext(maxSerialized);
    return res;
}

NJson::TJsonValue TMinMax::Json() const {
    NJson::TJsonValue json;
    json.InsertValue("min", Min()->ToString());
    json.InsertValue("max", Max()->ToString());
    return json;
}
}   // namespace NKikimr::NArrow::NAccessor
