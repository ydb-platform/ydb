#pragma once

#include <util/system/types.h>

namespace NYql {

struct TPositionHandle {
    friend struct TExprContext;
    bool operator==(TPositionHandle const& other) const = default;
    bool operator!=(TPositionHandle const& other) const = default;
private:
    ui32 Handle_ = 0; // 0 is guaranteed to represent default-constructed TPosition
};

}
