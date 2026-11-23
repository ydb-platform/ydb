#pragma once

#include <util/system/types.h>
#include <array>
#include <type_traits>

namespace NVectorDistance::NInt4::NTesting {
    std::array<float, 8> LoadAvx2(const ui8* ptr, std::false_type);
    std::array<float, 8> LoadAvx2(const ui8* ptr, std::true_type);
}
