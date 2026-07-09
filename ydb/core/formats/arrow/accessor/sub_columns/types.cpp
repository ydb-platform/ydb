#include "types.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <ydb/library/actors/core/log.h>

#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

namespace {

NBinaryJson::TBinaryJson ToBinaryJson(const NJson::TJsonValue& json) {
    auto result = NBinaryJson::SerializeToBinaryJson(NJson::WriteJson(&json, false));
    AFL_VERIFY(std::holds_alternative<NBinaryJson::TBinaryJson>(result));
    return std::get<NBinaryJson::TBinaryJson>(std::move(result));
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
        default:
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
        AFL_VERIFY(false)("unhandled value_type", (ui32)valueType);
            return arrow::binary();
    }
}

bool DictionaryApplicableForValueType(const EValueType valueType) {
    switch (valueType) {
        case EValueType::BinaryJson:
        case EValueType::String:
            return true;
        // Integral types trade fixed-size position in array for fixed-size index.
        // May still be good for compression, but requires further experiments
        // and probably a separate threshold from strings
        default:
            return false;
    }
}

// Element type to represent result of merging arrays with arg types
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

NJson::TJsonValue ArrayElementToJsonValue(const arrow::Array& array, const i64 index, const EValueType valueType) {
    switch (valueType) {
        case EValueType::String: {
            const auto view = static_cast<const arrow::BinaryArray&>(array).GetView(index);
            return NJson::TJsonValue(TStringBuf(view.data(), view.size()));
        }
        case EValueType::Double:
            return NJson::TJsonValue(static_cast<const arrow::DoubleArray&>(array).Value(index));
        case EValueType::Bool:
            return NJson::TJsonValue(static_cast<const arrow::BooleanArray&>(array).Value(index));
        case EValueType::BinaryJson: {
            const auto view = static_cast<const arrow::BinaryArray&>(array).GetView(index);
            const auto text = NBinaryJson::SerializeToJson(TStringBuf(view.data(), view.size()));
            NJson::TJsonValue result;
            AFL_VERIFY(NJson::ReadJsonTree(text, &result));
            return result;
        }
        default:
            AFL_VERIFY(false)("unhandled value_type", (ui32)valueType);
            return NJson::TJsonValue();
    }
}

NBinaryJson::TBinaryJson ArrayElementToBinaryJson(const arrow::Array& array, const i64 index, const EValueType valueType) {
    switch (valueType) {
        case EValueType::BinaryJson: {
            const auto view = static_cast<const arrow::BinaryArray&>(array).GetView(index);
            return NBinaryJson::TBinaryJson(view.data(), view.size());
        }
        case EValueType::String:
        case EValueType::Double:
        case EValueType::Bool:
            return ToBinaryJson(ArrayElementToJsonValue(array, index, valueType));
        default:
            AFL_VERIFY(false)("unhandled value_type", (ui32)valueType);
            return NBinaryJson::TBinaryJson();
    }
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
