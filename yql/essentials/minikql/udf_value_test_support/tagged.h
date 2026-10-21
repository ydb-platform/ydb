#pragma once

#include <util/generic/strbuf.h>

#include <utility>

namespace NYql::NUdf::NTest {

enum class TTag {
    A,
    B,
    C
};

template <typename T, TTag tag>
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
            case TTag::A:
                return "A";
            case TTag::B:
                return "B";
            case TTag::C:
                return "C";
        };
    }

private:
    T Value_;
};

} // namespace NYql::NUdf::NTest
