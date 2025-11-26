#pragma once

#include <library/cpp/json/writer/json_value.h>
#include <util/generic/string.h>

#include <optional>

namespace NKikimr::NArrow::NAccessor {

class TBinaryJsonValueView {
public:
    TBinaryJsonValueView(const TStringBuf& rawValue);

    std::optional<TStringBuf> GetScalarOptional() const;

private:
    TStringBuf RawValue;
    mutable TString ScalarHolder;
    mutable TStringBuf ScalarView;
};

} // namespace NKikimr::NArrow::NAccessor
