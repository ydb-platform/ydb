#include "types.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>

#include <yql/essentials/types/binary_json/read.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

namespace {

TStringBuf ExtractStringScalar(const NBinaryJson::TBinaryJson& blob) {
    auto reader = NBinaryJson::TBinaryJsonReader::Make(blob);
    return reader->GetRootCursor().GetElement(0).GetString();
}

double ExtractDoubleScalar(const NBinaryJson::TBinaryJson& blob) {
    auto reader = NBinaryJson::TBinaryJsonReader::Make(blob);
    return reader->GetRootCursor().GetElement(0).GetNumber();
}

bool ExtractBoolScalar(const NBinaryJson::TBinaryJson& blob) {
    auto reader = NBinaryJson::TBinaryJsonReader::Make(blob);
    return reader->GetRootCursor().GetElement(0).GetType() == NBinaryJson::EEntryType::BoolTrue;
}

EValueType ValueTypeForItem(const NBinaryJson::TBinaryJson& blob) {
    auto reader = NBinaryJson::TBinaryJsonReader::Make(blob);
    auto rootCursor = reader->GetRootCursor();
    if (rootCursor.GetType() != NBinaryJson::EContainerType::TopLevelScalar) {
        return EValueType::BinaryJson;
    }
    switch (rootCursor.GetElement(0).GetType()) {
        case NBinaryJson::EEntryType::String:
            return EValueType::String;
        case NBinaryJson::EEntryType::Number:
            return EValueType::Double;
        case NBinaryJson::EEntryType::BoolFalse:
        case NBinaryJson::EEntryType::BoolTrue:
            return EValueType::Bool;
        case NBinaryJson::EEntryType::Container:
        case NBinaryJson::EEntryType::Null:
            return EValueType::BinaryJson;
    }
}

}   // namespace

std::shared_ptr<arrow::DataType> GetArrowTypeForValueType(const EValueType valueType) {
    switch (valueType) {
        case EValueType::BinaryJson:
        case EValueType::String:
            return arrow::binary();
        case EValueType::Double:
            return arrow::float64();
        case EValueType::Bool:
            return arrow::boolean();
    }
}

bool CanBeDictionaryEncoded(EValueType valueType) {
    return valueType == EValueType::BinaryJson || valueType == EValueType::String;
}

TJsonValueView ArrayElementToJsonValueView(const arrow::Array& array, const i64 index, const EValueType valueType) {
    switch (valueType) {
        case EValueType::String: {
            AFL_VERIFY(array.type_id() == arrow::Type::BINARY)("actual", (ui32)array.type_id());
            const auto view = static_cast<const arrow::BinaryArray&>(array).GetView(index);
            return TJsonValueView::OfString(TStringBuf(view.data(), view.size()));
        }
        case EValueType::Double:
            AFL_VERIFY(array.type_id() == arrow::Type::DOUBLE)("actual", (ui32)array.type_id());
            return TJsonValueView::OfNumber(static_cast<const arrow::DoubleArray&>(array).Value(index));
        case EValueType::Bool:
            AFL_VERIFY(array.type_id() == arrow::Type::BOOL)("actual", (ui32)array.type_id());
            return TJsonValueView::OfBool(static_cast<const arrow::BooleanArray&>(array).Value(index));
        case EValueType::BinaryJson: {
            AFL_VERIFY(array.type_id() == arrow::Type::BINARY)("actual", (ui32)array.type_id());
            const auto view = static_cast<const arrow::BinaryArray&>(array).GetView(index);
            return TJsonValueView::OfBinaryJson(TStringBuf(view.data(), view.size()));
        }
    }
}

NBinaryJson::TBinaryJson ArrayElementToBinaryJson(const arrow::Array& array, const i64 index, const EValueType valueType) {
    return ArrayElementToJsonValueView(array, index, valueType).ToBinaryJson();
}

ui32 ArrayElementSize(const arrow::Array& array, const i64 index, const EValueType valueType) {
    switch (valueType) {
        case EValueType::BinaryJson:
        case EValueType::String:
            return static_cast<const arrow::BinaryArray&>(array).GetView(index).size();
        case EValueType::Double:
            return sizeof(double);
        case EValueType::Bool:
            // actually only 1 bit in arrow representation, not 1 byte, but let's not overcomplicate things
            return 1;
    }
}

std::unique_ptr<arrow::ArrayBuilder> MakeBuilderForValueType(const EValueType valueType, const ui32 reserveItems, const ui32 reserveData) {
    // Only variable-length (BinaryJson/String) storage makes use of a data-size reservation.
    const bool variableLength = valueType == EValueType::BinaryJson || valueType == EValueType::String;
    return NArrow::MakeBuilder(GetArrowTypeForValueType(valueType), reserveItems, variableLength ? reserveData : 0);
}

void AppendValueFromBinaryJson(arrow::ArrayBuilder& builder, const NBinaryJson::TBinaryJson& blob, const EValueType valueType) {
    switch (valueType) {
        case EValueType::BinaryJson:
            AFL_VERIFY(NArrow::Append<arrow::BinaryType>(builder, arrow::util::string_view(blob.data(), blob.size())));
            return;
        case EValueType::String: {
            const auto scalar = ExtractStringScalar(blob);
            AFL_VERIFY(NArrow::Append<arrow::BinaryType>(builder, arrow::util::string_view(scalar.data(), scalar.size())));
            return;
        }
        case EValueType::Double:
            AFL_VERIFY(NArrow::Append<arrow::DoubleType>(builder, ExtractDoubleScalar(blob)));
            return;
        case EValueType::Bool:
            AFL_VERIFY(NArrow::Append<arrow::BooleanType>(builder, ExtractBoolScalar(blob)));
            return;
    }
}

EValueType MergeValueTypes(const std::optional<EValueType>& acc, const EValueType next) {
    if (!acc) {
        return next;
    }
    return (*acc == next) ? *acc : EValueType::BinaryJson;
}

EValueType DetectValueTypeForArray(const std::deque<NBinaryJson::TBinaryJson>& values) {
    std::optional<EValueType> common;
    for (const auto& v : values) {
        common = MergeValueTypes(common, ValueTypeForItem(v));
        if (*common == EValueType::BinaryJson) {
            break;
        }
    }
    return common.value_or(EValueType::BinaryJson);
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
