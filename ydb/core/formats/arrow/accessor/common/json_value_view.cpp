#include "json_value_view.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <ydb/library/actors/core/log.h>

#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>

#include <util/generic/ylimits.h>
#include <util/string/cast.h>

#include <cmath>
#include <limits>

namespace NKikimr::NArrow::NAccessor {

namespace {

NBinaryJson::TBinaryJson JsonValueToBinaryJson(const NJson::TJsonValue& json) {
    auto result = NBinaryJson::SerializeToBinaryJson(NJson::WriteJson(&json, false));
    AFL_VERIFY(std::holds_alternative<NBinaryJson::TBinaryJson>(result));
    return std::get<NBinaryJson::TBinaryJson>(std::move(result));
}

}   // namespace

std::optional<TString> TJsonValueView::JsonNumberToString(double jsonNumber) {
    if (std::isnan(jsonNumber)) {
        return std::nullopt;
    }

    double integerPart;
    double fractionPart = std::modf(jsonNumber, &integerPart);
    if (!(fractionPart == 0.0)) {
        return ::ToString(jsonNumber);
    }

    static constexpr double minD = static_cast<double>(std::numeric_limits<i64>::min());
    static constexpr double maxD = MaxFloor<i64>();

    if (minD <= jsonNumber && jsonNumber <= maxD) {
        return ::ToString(static_cast<i64>(jsonNumber));
    }

    return ::ToString(jsonNumber);
}

TJsonValueView TJsonValueView::OfBinaryJson(const TStringBuf& blob) {
    TJsonValueView result(EKind::BinaryJson);
    result.Bytes = blob;
    return result;
}

TJsonValueView TJsonValueView::OfString(const TStringBuf& value) {
    TJsonValueView result(EKind::String);
    result.Bytes = value;
    return result;
}

TJsonValueView TJsonValueView::OfNumber(double value) {
    TJsonValueView result(EKind::Number);
    result.Number = value;
    return result;
}

TJsonValueView TJsonValueView::OfBool(bool value) {
    TJsonValueView result(EKind::Bool);
    result.Bool = value;
    return result;
}

std::optional<TStringBuf> TJsonValueView::ProjectNumber(double value) const {
    auto number = JsonNumberToString(value);
    if (!number) {
        return std::nullopt;
    }
    ScalarHolder = std::move(*number);
    return TStringBuf(ScalarHolder);
}

TStringBuf TJsonValueView::ProjectBool(bool value) {
    return value ? TStringBuf("true") : TStringBuf("false");
}

std::optional<TStringBuf> TJsonValueView::GetScalarOptional() const {
    switch (Kind) {
        case EKind::BinaryJson:
            return ScalarFromBinaryJson();
        case EKind::String:
            return Bytes;
        case EKind::Number:
            return ProjectNumber(Number);
        case EKind::Bool:
            return ProjectBool(Bool);
    }
}

std::optional<TStringBuf> TJsonValueView::GetBinaryJsonBlobOptional() const {
    if (Kind != EKind::BinaryJson || Bytes.empty()) {
        return std::nullopt;
    }
    return Bytes;
}

NJson::TJsonValue TJsonValueView::ToJsonValue() const {
    switch (Kind) {
        case EKind::BinaryJson: {
            const auto text = NBinaryJson::SerializeToJson(Bytes);
            NJson::TJsonValue result;
            AFL_VERIFY(NJson::ReadJsonTree(text, &result));
            return result;
        }
        case EKind::String:
            return NJson::TJsonValue(Bytes);
        case EKind::Number:
            return NJson::TJsonValue(Number);
        case EKind::Bool:
            return NJson::TJsonValue(Bool);
    }
}

NBinaryJson::TBinaryJson TJsonValueView::ToBinaryJson() const {
    switch (Kind) {
        case EKind::BinaryJson:
            return NBinaryJson::TBinaryJson(Bytes.data(), Bytes.size());
        case EKind::String:
        case EKind::Number:
        case EKind::Bool:
            return JsonValueToBinaryJson(ToJsonValue());
    }
}

std::optional<TStringBuf> TJsonValueView::ScalarFromBinaryJson() const {
    if (Bytes.empty()) {
        return std::nullopt;
    }

    auto reader = NBinaryJson::TBinaryJsonReader::Make(Bytes);
    auto rootCursor = reader->GetRootCursor();
    if (rootCursor.GetType() != NBinaryJson::EContainerType::TopLevelScalar) {
        return std::nullopt;
    }

    auto rootElement = rootCursor.GetElement(0);
    switch (rootElement.GetType()) {
        case NBinaryJson::EEntryType::String:
            return rootElement.GetString();
        case NBinaryJson::EEntryType::Number:
            return ProjectNumber(rootElement.GetNumber());
        case NBinaryJson::EEntryType::BoolTrue:
            return ProjectBool(true);
        case NBinaryJson::EEntryType::BoolFalse:
            return ProjectBool(false);
        case NBinaryJson::EEntryType::Null:
        case NBinaryJson::EEntryType::Container:
            return std::nullopt;
    }
}

} // namespace NKikimr::NArrow::NAccessor
