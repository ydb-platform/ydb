#pragma once

#include <cstdint>
#include <string>

/// Using char8_t more strict aliasing (https://stackoverflow.com/a/57453713)
using UInt8 = char8_t;

/// Same for using signed _BitInt(8) (there isn't a signed char8_t, which would be more convenient)
/// See https://godbolt.org/z/fafnWEnnf
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wbit-int-extension"
using Int8 = signed _BitInt(8);
#pragma clang diagnostic pop

namespace std
{
template <>
struct hash<Int8> /// NOLINT (cert-dcl58-cpp)
{
    size_t operator()(const Int8 x) const { return std::hash<int8_t>()(int8_t{x}); }
};
}

using size_t = std::size_t;

using UInt16 = uint16_t;
using UInt32 = uint32_t;
using UInt64 = uint64_t;

using Int16 = int16_t;
using Int32 = int32_t;
using Int64 = int64_t;

using Float32 = float;
using Float64 = double;

using String = std::string;

namespace DB
{

using UInt8 = ::UInt8;
using UInt16 = ::UInt16;
using UInt32 = ::UInt32;
using UInt64 = ::UInt64;

using Int8 = ::Int8;
using Int16 = ::Int16;
using Int32 = ::Int32;
using Int64 = ::Int64;

using Float32 = float;
using Float64 = double;

using String = std::string;

}
