#pragma once

#include <util/generic/string.h>
#include <util/str_stl.h>

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

template <>
struct TEqualTo<NYql::NUdf::NTest::TUtf8> {
    bool operator()(const NYql::NUdf::NTest::TUtf8& lhs, const NYql::NUdf::NTest::TUtf8& rhs) const {
        return lhs.Value == rhs.Value;
    }
};

template <>
struct THash<NYql::NUdf::NTest::TUtf8> {
    size_t operator()(const NYql::NUdf::NTest::TUtf8& value) const {
        return THash<TString>{}(value.Value);
    }
};
