#pragma once

#include "defs.h"

namespace NKikimr::NUtil {

inline void ShrinkToFit(TString& input) {
    // capacity / size > 1.5
    if (input.capacity() * 2 > input.size() * 3) {
        input = input.copy();
    }
}

}
