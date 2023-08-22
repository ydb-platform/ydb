#pragma once

#include <util/generic/string.h>

#include <string>
#include <string_view>

// Base32 encoding based on RFC 4648 alphabet (incompatible with Crockford and Geohash alphabet)
// https://en.wikipedia.org/wiki/Base32#RFC_4648_Base32_alphabet

///
/// @return Size of the buffer required to decode Base32 encoded data of size `len`.
///
constexpr size_t Base32DecodeBufSize(size_t len) noexcept {
    return (len * 5 + 7) / 8;
}

///
/// @brief Decodes only valid Base32 string, behaviour for invalid data is unspecified.
///
/// @param src a base32 encoded string.
/// @param dst an pointer to allocated memory for writing result.
///
/// @return Count of written bytes.
///
size_t Base32Decode(std::string_view src, char* dst);

///
/// @param src a base32 encoded string.
/// @param dst a decoded string.
///
inline void Base32Decode(std::string_view src, std::string& dst)
{
    ::ResizeUninitialized(dst, Base32DecodeBufSize(src.size()));
    dst.resize(Base32Decode(src, dst.data()));
}

///
/// @param s a base32 encoded string.
///
/// @returns a decoded string.
///
inline std::string Base32Decode(std::string_view s)
{
    std::string ret;
    Base32Decode(s, ret);
    return ret;
}

///
/// @brief Decodes Base32 string with strict verification of invalid symbols,
///        also tries to decode Base32 string with padding inside.
///
/// @throws Throws exceptions on inputs which contain invalid symbols or incorrect padding.
///
/// @param src a base32 encoded string.
/// @param dst an pointer to allocated memory for writing result.
///
/// @return Count of written bytes.
///
size_t Base32StrictDecode(std::string_view src, char* dst);

///
/// @param src a base32 encoded string.
/// @param dst a decoded string.
///
inline void Base32StrictDecode(std::string_view src, std::string& dst)
{
    ::ResizeUninitialized(dst, Base32DecodeBufSize(src.size()));
    dst.resize(Base32StrictDecode(src, dst.data()));
}

///
/// @param s a base32 encoded string.
///
/// @returns a decoded string.
///
inline std::string Base32StrictDecode(std::string_view s)
{
    std::string ret;
    Base32StrictDecode(s, ret);
    return ret;
}

///
/// @return Size of the buffer required to encode Base32 decoded data of size `len`.
///
constexpr size_t Base32EncodeBufSize(size_t len) noexcept {
    return ((len * 8 + 4) / 5 + 7) / 8 * 8;
}

///
/// @param src a base32 decoded string.
/// @param dst an pointer to allocated memory for writing result.
///
/// @return Count of written bytes.
///
size_t Base32Encode(std::string_view src, char* dst);

///
/// @param src a base32 decoded string.
/// @param dst a encoded string.
///
inline void Base32Encode(std::string_view src, std::string& dst)
{
    ::ResizeUninitialized(dst, Base32EncodeBufSize(src.size()));
    dst.resize(Base32Encode(src, dst.data()));
}

///
/// @param s a base32 decoded string.
///
/// @returns a encoded string.
///
inline std::string Base32Encode(std::string_view s)
{
    std::string ret;
    Base32Encode(s, ret);
    return ret;
}
