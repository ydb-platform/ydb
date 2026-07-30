#pragma once

#include <util/generic/string.h>

namespace NYql::NUdf::NTest {

struct TUtf8 {
    TString Value;

    TUtf8() = default;

    explicit TUtf8(TStringBuf value)
        : Value(value)
    {
    }

    explicit TUtf8(const char* value)
        : Value(value)
    {
    }

    explicit TUtf8(TString value)
        : Value(std::move(value))
    {
    }
};

} // namespace NYql::NUdf::NTest
