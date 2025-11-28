#include "binary_json_value_view.h"

#include <ydb/library/actors/core/log.h>
#include <yql/essentials/types/binary_json/read.h>

#include <limits>
#include <cmath>

namespace NKikimr::NArrow::NAccessor {

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
            const double val = rootElement.GetNumber();
            double integer;
            if (val < std::numeric_limits<i64>::min() || val > std::numeric_limits<i64>::max() || modf(val, &integer)) {
                ScalarHolder = ToString(val);
            } else {
                ScalarHolder = ToString((i64)integer);
            }
            ScalarView = ScalarHolder;
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
