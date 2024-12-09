#pragma once

#include <library/cpp/int128/int128.h>

#include <util/system/defaults.h>
#include <util/generic/string.h>

#include <array>

namespace NYT::NDecimal {

////////////////////////////////////////////////////////////////////////////////

class TDecimal
{
public:
    //! Both types defined below represent signed values. They are only intended
    //! to be used for manipulating binary data, since they don't actually expose
    //! any arithmetic operations. You should avoid dealing with individual parts.
    struct TValue128
    {
        ui64 Low;
        i64 High;
    };
    static_assert(sizeof(TValue128) == 2 * sizeof(ui64));

    //! Lower-endian representation of 256-bit decimal value.
    struct TValue256
    {
        std::array<ui32, 8> Parts;
    };
    static_assert(sizeof(TValue256) == 4 * sizeof(ui64));

public:
    //! Maximum precision supported by YT.
    static constexpr int MaxPrecision = 76;
    static constexpr int MaxBinarySize = 32;

    // NB. Sometimes we print values that exceed MaxPrecision (e.g. in error messages)
    // MaxTextSize is chosen so we can print ANY i256 number as decimal.
    static constexpr int MaxTextSize =
        77 // length of 2^63 in decimal form
        + 1 // possible decimal point
        + 1; // possible minus sign

    static void ValidatePrecisionAndScale(int precision, int scale);

    static void ValidateBinaryValue(TStringBuf binaryValue, int precision, int scale);

    static TString BinaryToText(TStringBuf binaryDecimal, int precision, int scale);
    static TString TextToBinary(TStringBuf textDecimal, int precision, int scale);

    // More efficient versions of conversion functions without allocations.
    // `buffer` must be at least of size MaxTextSize / MaxBinarySize.
    // Returned value is substring of buffer.
    static TStringBuf BinaryToText(TStringBuf binaryDecimal, int precision, int scale, char* buffer, size_t bufferLength);
    static TStringBuf TextToBinary(TStringBuf textDecimal, int precision, int scale, char* buffer, size_t bufferLength);

    static int GetValueBinarySize(int precision);

    static TStringBuf WriteBinary32(int precision, i32 value, char* buffer, size_t bufferLength);
    static TStringBuf WriteBinary64(int precision, i64 value, char* buffer, size_t bufferLength);
    static TStringBuf WriteBinary128(int precision, TValue128 value, char* buffer, size_t bufferLength);
    static TStringBuf WriteBinary256(int precision, TValue256 value, char* buffer, size_t bufferLength);

    // Writes either 32-bit, 64-bit or 128-bit binary value depending on precision, provided a TValue128.
    static TStringBuf WriteBinary128Variadic(int precision, TValue128 value, char* buffer, size_t bufferLength);
    // Writes either 32-bit, 64-bit, 128-bit or 256-bit binary value depending on precision, provided a TValue256.
    static TStringBuf WriteBinary256Variadic(int precision, TValue256 value, char* buffer, size_t bufferLength);

    static i32 ParseBinary32(int precision, TStringBuf buffer);
    static i64 ParseBinary64(int precision, TStringBuf buffer);
    static TValue128 ParseBinary128(int precision, TStringBuf buffer);
    static TValue256 ParseBinary256(int precision, TStringBuf buffer);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDecimal
