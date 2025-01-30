#pragma once

#include <tuple>

namespace NYql {
    // returns version, patch level, sublevel, e.g. (4, 4, 114) for `uname -r` == "4.4.114-50"
    std::tuple<int, int, int> DetectLinuxKernelVersion3();

    // returns version, patch level
    std::pair<int, int> DetectLinuxKernelVersion2();

    bool IsLinuxKernelBelow4_3();
}
