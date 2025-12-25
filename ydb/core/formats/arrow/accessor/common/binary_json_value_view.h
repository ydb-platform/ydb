#pragma once

#include <util/generic/string.h>

#include <optional>

namespace NKikimr::NArrow::NAccessor {

class TBinaryJsonValueView {
public:
    TBinaryJsonValueView(const TStringBuf& rawValue);

    std::optional<TStringBuf> GetScalarOptional() const;

    static std::optional<TString> JsonNumberToString(double jsonNumber);

private:
    TStringBuf RawValue;
    mutable TString ScalarHolder;
    mutable std::optional<TStringBuf> ScalarView;
};

} // namespace NKikimr::NArrow::NAccessor
