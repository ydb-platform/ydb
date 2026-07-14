
#pragma once

#include <util/generic/string.h>

namespace NYql::NUdf::NTest {

struct TTestDyNumber {
    TString Value;

    TTestDyNumber()
        : Value("0")
    {
    }

    explicit TTestDyNumber(TStringBuf value)
        : Value(TString{value})
    {
    }

    explicit TTestDyNumber(const char* value)
        : Value(value)
    {
    }
};

} // namespace NYql::NUdf::NTest
