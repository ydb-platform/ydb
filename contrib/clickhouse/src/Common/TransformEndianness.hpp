#pragma once

#include <base/Decimal_fwd.h>
#include <base/extended_types.h>
#include <base/strong_typedef.h>

#include <city.h>

#include <magic_enum.hpp>

#include <utility>

namespace DB
{

template <typename T>
inline constexpr T byteswap(T n) noexcept
{

    if constexpr (sizeof(T) == 1) {
        return n;
    } else if constexpr (sizeof(T) == 2) {
        return std::bit_cast<T>(__builtin_bswap16(std::bit_cast<uint16_t>(n)));
    } else if constexpr (sizeof(T) == 4) {
        return std::bit_cast<T>(__builtin_bswap32(std::bit_cast<uint32_t>(n)));
    } else if constexpr (sizeof(T) == 8) {
        return std::bit_cast<T>(__builtin_bswap64(std::bit_cast<uint64_t>(n)));
    } else {
        static_assert(sizeof(T) == 16);
        return std::bit_cast<T>(__builtin_bswap128(std::bit_cast<__uint128_t>(n)));
    }
}

template <std::endian ToEndian, std::endian FromEndian = std::endian::native, typename T>
requires std::is_integral_v<T>
inline void transformEndianness(T & value)
{
    if constexpr (ToEndian != FromEndian)
        value = DB::byteswap(value);
}

template <std::endian ToEndian, std::endian FromEndian = std::endian::native, typename T>
requires is_big_int_v<T>
inline void transformEndianness(T & x)
{
    if constexpr (ToEndian != FromEndian)
    {
        auto & items = x.items;
        std::transform(std::begin(items), std::end(items), std::begin(items), [](auto & item) { return DB::byteswap(item); });
        std::reverse(std::begin(items), std::end(items));
    }
}

template <std::endian ToEndian, std::endian FromEndian = std::endian::native, typename T>
requires is_decimal<T>
inline void transformEndianness(T & x)
{
    transformEndianness<ToEndian, FromEndian>(x.value);
}

template <std::endian ToEndian, std::endian FromEndian = std::endian::native, typename T>
requires std::is_floating_point_v<T>
inline void transformEndianness(T & value)
{
    if constexpr (ToEndian != FromEndian)
    {
        auto * start = reinterpret_cast<std::byte *>(&value);
        std::reverse(start, start + sizeof(T));
    }
}

template <std::endian ToEndian, std::endian FromEndian = std::endian::native, typename T>
requires std::is_enum_v<T> || magic_enum::is_scoped_enum_v<T>
inline void transformEndianness(T & x)
{
    using UnderlyingType = std::underlying_type_t<T>;
    transformEndianness<ToEndian, FromEndian>(reinterpret_cast<UnderlyingType &>(x));
}

template <std::endian ToEndian, std::endian FromEndian = std::endian::native, typename A, typename B>
inline void transformEndianness(std::pair<A, B> & pair)
{
    transformEndianness<ToEndian, FromEndian>(pair.first);
    transformEndianness<ToEndian, FromEndian>(pair.second);
}

template <std::endian ToEndian, std::endian FromEndian = std::endian::native, typename T, typename Tag>
inline void transformEndianness(StrongTypedef<T, Tag> & x)
{
    transformEndianness<ToEndian, FromEndian>(x.toUnderType());
}

template <std::endian ToEndian, std::endian FromEndian = std::endian::native>
inline void transformEndianness(CityHash_v1_0_2::uint128 & x)
{
    transformEndianness<ToEndian, FromEndian>(x.low64);
    transformEndianness<ToEndian, FromEndian>(x.high64);
}
}
