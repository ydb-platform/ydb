#include "native_scalars.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

std::optional<EValueType> ClassifyBinaryJsonScalar(const TStringBuf blob) {
    if (blob.empty()) {
        return std::nullopt;
    }
    auto reader = NBinaryJson::TBinaryJsonReader::Make(blob);
    auto rootCursor = reader->GetRootCursor();
    if (rootCursor.GetType() != NBinaryJson::EContainerType::TopLevelScalar) {
        return std::nullopt;
    }
    switch (rootCursor.GetElement(0).GetType()) {
        case NBinaryJson::EEntryType::String:
            return EValueType::String;
        // Number -> Double and BoolTrue/BoolFalse -> Bool are enabled in a later phase.
        default:
            return std::nullopt;
    }
}

EValueType DetectNativeValueType(const std::deque<NBinaryJson::TBinaryJson>& values) {
    std::optional<EValueType> common;
    for (const auto& v : values) {
        const auto valueType = ClassifyBinaryJsonScalar(TStringBuf(v.data(), v.size()));
        if (!valueType) {
            return EValueType::BinaryJson;
        }
        if (!common) {
            common = *valueType;
        } else if (*common != *valueType) {
            return EValueType::BinaryJson;
        }
    }
    return common.value_or(EValueType::BinaryJson);
}

TStringBuf ExtractNativeScalar(const TStringBuf blob, const EValueType valueType) {
    switch (valueType) {
        case EValueType::String: {
            auto reader = NBinaryJson::TBinaryJsonReader::Make(blob);
            return reader->GetRootCursor().GetElement(0).GetString();
        }
        case EValueType::BinaryJson:
        case EValueType::Double:
        case EValueType::Bool:
            AFL_VERIFY(false)("value_type", (ui32)valueType);
            return {};
    }
}

NBinaryJson::TBinaryJson NativeScalarToBinaryJson(const TStringBuf rawValue, const EValueType valueType) {
    switch (valueType) {
        case EValueType::String: {
            const NJson::TJsonValue json(rawValue);
            auto result = NBinaryJson::SerializeToBinaryJson(NJson::WriteJson(&json, false));
            return std::get<NBinaryJson::TBinaryJson>(std::move(result));
        }
        case EValueType::BinaryJson:
        case EValueType::Double:
        case EValueType::Bool:
            AFL_VERIFY(false)("value_type", (ui32)valueType);
            return {};
    }
}

NJson::TJsonValue NativeScalarToJsonValue(const TStringBuf rawValue, const EValueType valueType) {
    switch (valueType) {
        case EValueType::String:
            return NJson::TJsonValue(rawValue);
        case EValueType::BinaryJson: {
            const auto text = NBinaryJson::SerializeToJson(rawValue);
            NJson::TJsonValue result;
            AFL_VERIFY(NJson::ReadJsonTree(text, &result));
            return result;
        }
        case EValueType::Double:
        case EValueType::Bool:
            AFL_VERIFY(false)("value_type", (ui32)valueType);
            return {};
    }
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
