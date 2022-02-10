#pragma once

#include <library/cpp/malloc/api/malloc.h>
#include <util/string/type.h>

inline bool BallocEnabled() {
    return !::NMalloc::MallocInfo().CheckParam("disable", true);
}

inline void ThreadDisableBalloc() {
    ::NMalloc::MallocInfo().SetParam("disable", "true");
}

inline void ThreadEnableBalloc() {
    ::NMalloc::MallocInfo().SetParam("disable", "false");
}
