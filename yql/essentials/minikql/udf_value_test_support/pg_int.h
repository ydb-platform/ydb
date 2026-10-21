#pragma once

#include <util/generic/maybe.h>
#include <util/system/types.h>

namespace NYql::NUdf::NTest {

class TPgInt {
public:
    TPgInt() = default;

    explicit TPgInt(i32 value)
        : Value_(value)
    {
    }

    TMaybe<i32> Value() const {
        return Value_;
    }

private:
    TMaybe<i32> Value_{};
};

} // namespace NYql::NUdf::NTest
