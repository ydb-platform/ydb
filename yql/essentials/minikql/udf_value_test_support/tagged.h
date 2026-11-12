#pragma once

#include <util/generic/strbuf.h>

#include <utility>

namespace NYql::NUdf::NTest {

enum class ETag {
    A,
    B,
    C
};

template <typename T, ETag tag>
class TTagged {
public:
    TTagged()
        : Value_()
    {};

    explicit TTagged(T value)
        : Value_(std::move(value))
    {
    }

    const T& Value() const {
        return Value_;
    }

    TStringBuf Tag() const {
        switch (tag) {
            case ETag::A:
                return "A";
            case ETag::B:
                return "B";
            case ETag::C:
                return "C";
        };
    }

private:
    T Value_;
};

} // namespace NYql::NUdf::NTest
