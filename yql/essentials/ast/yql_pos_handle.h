#pragma once

#include <util/system/types.h>

namespace NYql {

struct TPositionHandle {
    friend struct TExprContext;
private:
    ui32 Handle = 0; // 0 is guaranteed to represent default-constructed TPosition
};

}
