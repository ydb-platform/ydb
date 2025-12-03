#include "binary_json_value_view.h"

#include <ydb/library/actors/core/log.h>
#include <yql/essentials/types/binary_json/read.h>
#include <util/generic/ylimits.h>

#include <limits>
#include <cmath>

namespace NKikimr::NArrow::NAccessor {

namespace {

std::optional<std::string> JsonNumberToString(double val) {
    if (std::isnan(val)) {
        return std::nullopt;
    }

    double integerPart;
    double fractionPart = std::modf(val, &integerPart);
    if (!(fractionPart == 0.0)) {
        return std::to_string(val);
    }

    static constexpr double minD = static_cast<double>(std::numeric_limits<i64>::min());
    static constexpr double maxD = MaxFloor<i64>();

    if (minD <= val && val <= maxD) {
        return std::to_string(static_cast<i64>(val));
    }

    return std::to_string(val);
}

} // namespace

TBinaryJsonValueView::TBinaryJsonValueView(const TStringBuf& rawValue)
    : RawValue(rawValue) {
    if (!RawValue.empty()) {
        AFL_VERIFY(NBinaryJson::IsValidBinaryJson(RawValue));
    }
}

std::optional<TStringBuf> TBinaryJsonValueView::GetScalarOptional() const {
    if (RawValue.empty()) {
        return std::nullopt;
    }

    if (ScalarView.has_value()) {
        return ScalarView.value();
    }

    auto reader = NBinaryJson::TBinaryJsonReader::Make(RawValue);
    auto rootCursor = reader->GetRootCursor();
    if (rootCursor.GetType() != NBinaryJson::EContainerType::TopLevelScalar) {
        return std::nullopt;
    }

    auto rootElement = rootCursor.GetElement(0);

    switch (rootElement.GetType()) {
        case NBinaryJson::EEntryType::String:
            ScalarView = rootElement.GetString();
            break;
        case NBinaryJson::EEntryType::Number: {
            auto jsonNumber = JsonNumberToString(rootElement.GetNumber());
            if (jsonNumber.has_value()) {
                ScalarHolder = jsonNumber.value();
                ScalarView = ScalarHolder;
            } else {
                return std::nullopt;
            }
            break;
        }
        case NBinaryJson::EEntryType::BoolFalse: {
            static const TString falseString = "false";
            ScalarView = falseString;
            break;
        }
        case NBinaryJson::EEntryType::BoolTrue: {
            static const TString trueString = "true";
            ScalarView = trueString;
            break;
        }
        case NBinaryJson::EEntryType::Null: {
            return std::nullopt;
        }
        default:
            return std::nullopt;
    }

    return ScalarView.value();
}

} // namespace NKikimr::NArrow::NAccessor
