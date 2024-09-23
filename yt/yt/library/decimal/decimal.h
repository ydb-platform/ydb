#pragma once

#include <library/cpp/int128/int128.h>

#include <util/system/defaults.h>
#include <util/generic/string.h>

namespace NYT::NDecimal {

////////////////////////////////////////////////////////////////////////////////

class TDecimal
{
public:
    struct TValue128
    {
        ui64 Low;
        i64 High;
    };
    static_assert(sizeof(TValue128) == 2 * sizeof(ui64));

public:
    // Maximum precision supported by YT
    static constexpr int MaxPrecision = 35;
    static constexpr int MaxBinarySize = 16;

    // NB. Sometimes we print values that exceed MaxPrecision (e.g. in error messages)
    // MaxTextSize is chosen so we can print ANY i128 number as decimal.
    static constexpr int MaxTextSize =
        std::numeric_limits<ui128>::digits + 1 // max number of digits in ui128 number
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

    // Writes either 32-bit, 64-bit or 128-bit binary value depending on precision, provided a TValue128.
    static TStringBuf WriteBinaryVariadic(int precision, TValue128 value, char* buffer, size_t bufferLength);

    static i32 ParseBinary32(int precision, TStringBuf buffer);
    static i64 ParseBinary64(int precision, TStringBuf buffer);
    static TValue128 ParseBinary128(int precision, TStringBuf buffer);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDecimal
