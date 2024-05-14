#pragma once

#include "util/system/types.h"

enum EFormat : ui8 {
    FloatVector = 1,        // 4-byte per element
    ByteVector = 2,         // 1-byte per element
    BitVector = 10          // 1-bit per element
};

static constexpr size_t HeaderLen = sizeof(ui8);
