#pragma once

#include <library/cpp/int128/int128.h>

#include <array>
#include <iterator>
#include <limits>

namespace NInt128ToStringBenchmark {
    constexpr ui64 DecimalBase1e19 = 10'000'000'000'000'000'000ULL;

    using TFormatter = IOutputStream& (*)(IOutputStream&, ui64, ui64);

    inline IOutputStream& OutDecimalLegacy(IOutputStream& out, const ui64 high, const ui64 low) {
        // This is the implementation that preceded the base-10^9 formatter.
        int digits[39] = {0};
        int i;
        int j;
        for (i = 63; i > -1; --i) {
            if ((high >> i) & 1) {
                ++digits[0];
            }
            for (j = 0; j < 39; ++j) {
                digits[j] *= 2;
            }
            for (j = 0; j < 38; ++j) {
                digits[j + 1] += digits[j] / 10;
                digits[j] %= 10;
            }
        }
        for (i = 63; i > -1; --i) {
            if ((low >> i) & 1) {
                ++digits[0];
            }
            if (i > 0) {
                for (j = 0; j < 39; ++j) {
                    digits[j] *= 2;
                }
            }
            for (j = 0; j < 38; ++j) {
                digits[j + 1] += digits[j] / 10;
                digits[j] %= 10;
            }
        }
        for (i = 38; i > 0; --i) {
            if (digits[i] > 0) {
                break;
            }
        }
        for (; i > -1; --i) {
            out << static_cast<char>('0' + digits[i]);
        }

        return out;
    }

    inline IOutputStream& OutDecimalBase1e9(IOutputStream& out, const ui64 high, const ui64 low) {
        constexpr ui64 BinaryWordBase = ui64{1} << 32;
        constexpr ui64 DecimalBase = 1'000'000'000;

        const ui32 binaryWords[] = {
            static_cast<ui32>(high >> 32),
            static_cast<ui32>(high),
            static_cast<ui32>(low >> 32),
            static_cast<ui32>(low),
        };

        // 10^9 is the largest power of ten whose product with 2^32 fits in ui64.
        // Five base-10^9 words are enough to hold any 128-bit unsigned integer.
        ui32 decimalWords[5] = {0};
        size_t decimalWordCount = 1;
        for (const ui32 binaryWord : binaryWords) {
            ui64 carry = binaryWord;
            for (size_t i = 0; i < decimalWordCount; ++i) {
                const ui64 current = decimalWords[i] * BinaryWordBase + carry;
                decimalWords[i] = current % DecimalBase;
                carry = current / DecimalBase;
            }
            while (carry != 0) {
                decimalWords[decimalWordCount++] = carry % DecimalBase;
                carry /= DecimalBase;
            }
        }

        char buffer[39];
        char* position = std::end(buffer);

        // All but the most significant decimal word must occupy exactly 9 digits.
        for (size_t i = 0; i + 1 < decimalWordCount; ++i) {
            ui32 word = decimalWords[i];
            for (size_t digit = 0; digit < 9; ++digit) {
                *--position = static_cast<char>('0' + word % 10);
                word /= 10;
            }
        }

        ui32 word = decimalWords[decimalWordCount - 1];
        do {
            *--position = static_cast<char>('0' + word % 10);
            word /= 10;
        } while (word != 0);

        out.Write(position, std::end(buffer) - position);
        return out;
    }

    inline TString FormatUnsigned(const ui128 value, const TFormatter formatter) {
        TStringBuilder result;
        formatter(result.Out, GetHigh(value), GetLow(value));
        return result;
    }

    inline TString FormatSigned(const i128 value, const TFormatter formatter) {
        TStringBuilder result;
        ui64 high = GetHigh(value);
        ui64 low = GetLow(value);
        if ((high >> 63) != 0) {
            result.Out << '-';
            low = ~low + 1;
            high = ~high + (low == 0);
        }
        formatter(result.Out, high, low);
        return result;
    }

    inline TString LegacyToString(const ui128 value) {
        return FormatUnsigned(value, OutDecimalLegacy);
    }

    inline TString LegacyToString(const i128 value) {
        return FormatSigned(value, OutDecimalLegacy);
    }

    inline TString Base1e9ToString(const ui128 value) {
        return FormatUnsigned(value, OutDecimalBase1e9);
    }

    inline TString Base1e9ToString(const i128 value) {
        return FormatSigned(value, OutDecimalBase1e9);
    }

    inline TString Base1e19ToString(const ui128 value) {
        return ToString(value);
    }

    inline TString Base1e19ToString(const i128 value) {
        return ToString(value);
    }

    inline TString DivisionBy10ToString(ui128 value) {
        char buffer[39];
        char* current = std::end(buffer);

        if (value == 0) {
            *--current = static_cast<char>('0');
        }
        while (value != 0) {
            const ui128 remainder = value % ui128{10};
            *--current = static_cast<char>('0' + GetLow(remainder));
            value = value / ui128{10};
        }

        return TString(current, std::end(buffer));
    }

    inline TString DivisionBy10ToString(i128 value) {
        char buffer[40];
        char* current = std::end(buffer);
        const bool negative = value < 0;

        if (value == 0) {
            *--current = static_cast<char>('0');
        }
        while (value != 0) {
            const i128 remainder = value % i128{10};
            const i128 magnitude = remainder < 0 ? -remainder : remainder;
            *--current = static_cast<char>('0' + GetLow(magnitude));
            value = value / i128{10};
        }
        if (negative) {
            *--current = '-';
        }

        return TString(current, std::end(buffer));
    }

    inline const std::array<ui128, 18>& GetUnsignedToStringTestValues() {
        const ui128 decimalBase = DecimalBase1e19;
        const ui128 decimalBaseSquared = decimalBase * decimalBase;
        static const std::array<ui128, 18> values = {
            ui128{0},
            ui128{1},
            ui128{9},
            ui128{10},
            decimalBase - 1,
            decimalBase,
            decimalBase + 1,
            (ui128{1} << 32) - 1,
            ui128{1} << 32,
            (ui128{1} << 64) - 1,
            ui128{1} << 64,
            (ui128{1} << 96) - 1,
            ui128{1} << 96,
            decimalBaseSquared - 1,
            decimalBaseSquared,
            decimalBaseSquared + 1,
            std::numeric_limits<ui128>::max() - 1,
            std::numeric_limits<ui128>::max(),
        };
        return values;
    }

    inline const std::array<i128, 16>& GetSignedToStringTestValues() {
        const i128 decimalBase = DecimalBase1e19;
        const i128 decimalBaseSquared = decimalBase * decimalBase;
        static const std::array<i128, 16> values = {
            -i128{1},
            -i128{42},
            -((i128{1} << 32) - 1),
            -(i128{1} << 32),
            -((i128{1} << 64) - 1),
            -(i128{1} << 64),
            -((i128{1} << 96) - 1),
            -(i128{1} << 96),
            -decimalBase,
            -decimalBaseSquared,
            -std::numeric_limits<i128>::max(),
            std::numeric_limits<i128>::min(),
            i128{0},
            i128{1},
            decimalBase,
            std::numeric_limits<i128>::max(),
        };
        return values;
    }

    inline const std::array<ui128, 8>& GetUnsignedToStringFastPathValues() {
        static const std::array<ui128, 8> values = {
            ui128{0},
            ui128{1},
            ui128{9},
            ui128{10},
            (ui128{1} << 32) - 1,
            ui128{1} << 32,
            ui128{DecimalBase1e19},
            ui128{std::numeric_limits<ui64>::max()},
        };
        return values;
    }

    inline const std::array<i128, 12>& GetSignedToStringFastPathValues() {
        static const std::array<i128, 12> values = {
            i128{0},
            i128{1},
            -i128{1},
            i128{9},
            -i128{9},
            i128{10},
            -i128{10},
            i128{std::numeric_limits<i64>::max()},
            i128{std::numeric_limits<i64>::min()},
            -i128{std::numeric_limits<i64>::max()},
            i128{DecimalBase1e19},
            -i128{DecimalBase1e19},
        };
        return values;
    }
} // namespace NInt128ToStringBenchmark
