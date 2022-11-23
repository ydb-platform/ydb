#pragma once

#include <cstdint>
#include <string>

namespace CH
{

using Int8 = int8_t;
using Int16 = int16_t;
using Int32 = int32_t;
using Int64 = int64_t;

#ifndef __cpp_char8_t
using char8_t = unsigned char;
#endif

/// This is needed for more strict aliasing. https://godbolt.org/z/xpJBSb https://stackoverflow.com/a/57453713
#if !defined(PVS_STUDIO) /// But PVS-Studio does not treat it correctly.
using UInt8 = char8_t;
#else
using UInt8 = uint8_t;
#endif

using UInt16 = uint16_t;
using UInt32 = uint32_t;
using UInt64 = uint64_t;

using String = std::string;

}
