#pragma once
#include <util/generic/array_ref.h>
#include <string_view>

namespace NTi {

TArrayRef<const std::string_view> GetTimezones();

inline bool IsValidTimezoneIndex(size_t idx) {
    // this function is equivalent to:
    // const auto zones = GetTimezones();
    // return idx < zones.size() && !zones[idx].empty();
    #include "is_valid_gen.h"
}

}
