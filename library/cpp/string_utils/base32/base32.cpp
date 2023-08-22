#include "base32.h"

#include <util/generic/yexception.h>

#include <algorithm>
#include <array>
#include <limits>

namespace {

    // RFC 4648 Base32 alphabet
    //
    //      A        9    J       18    S       27    3
    // 1    B       10    K       19    T       28    4
    // 2    C       11    L       20    U       29    5
    // 3    D       12    M       21    V       30    6
    // 4    E       13    N       22    W       31    7
    // 5    F       14    O       23    X
    // 6    G       15    P       24    Y
    // 7    H       16    Q       25    Z
    // 8    I       17    R       26    2       pad    =

    constexpr std::string_view BASE32_TABLE = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                              "234567";

    constexpr uint8_t BAD = 0xff;

    // clang-format off
    constexpr std::array<uint8_t, 256> BASE32_DECODE_TABLE = {{
        BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,
        BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,
        BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,
        BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,
        BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,
        0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, BAD,  BAD,  BAD,  BAD,
        BAD,  BAD,  BAD,  BAD,  BAD,  0x0,  0x1,  0x2,  0x3,  0x4,
        0x5,  0x6,  0x7,  0x8,  0x9,  0xa,  0xb,  0xc,  0xd,  0xe,
        0xf,  0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
        0x19, BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,
        BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,
        BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,
        BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,
        BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,
        BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,
        BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,
        BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,
        BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,
        BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,
        BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,
        BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,
        BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,
        BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,
        BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,
        BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,  BAD,
        BAD,  BAD,  BAD,  BAD,  BAD,  BAD,
    }};
    // clang-format on

    char encodeBits(unsigned char sz) {
        static_assert(static_cast<size_t>(std::numeric_limits<decltype(sz)>::max()) < BASE32_DECODE_TABLE.size());
        return BASE32_TABLE[sz];
    }

    uint8_t decodeChar(unsigned char ch, bool isStrict) {
        if (uint8_t val = BASE32_DECODE_TABLE[ch]; val != BAD) {
            return val;
        }

        if (isStrict) {
            ythrow yexception() << "Error during decode symbol from Base32: character is not in Base32 set";
        }
        return 0;
    }

    void shiftBitsFrom(unsigned char& dst, const unsigned char& src, size_t i, size_t n) {
        unsigned char m = ((src << i) & 0xFF) >> (8 - n);
        dst = dst << n;
        dst |= m;
    }

    size_t Base32DecodeImpl(std::string_view src, char* dst, bool isStrict) {
        if (src.empty()) {
            return 0;
        }

        size_t dstSize = 0;
        size_t bitIndex = 0;
        unsigned char byte = 0;

        for (auto c = src.cbegin(); c != src.cend(); ++c) {
            if (*c == '=') {
                Y_ENSURE(
                    !isStrict || std::all_of(c, src.cend(), [](char pad) { return pad == '='; }),
                    "Unexpected character after padding");
                break;
            }

            uint8_t octet = decodeChar(*c, isStrict) << 3;
            byte = byte | (octet >> bitIndex);

            size_t bitsWritten = std::min<size_t>(5, 8 - bitIndex);
            if (bitsWritten < 5 || (bitIndex + bitsWritten) == 8) {
                dst[dstSize++] = byte;
                byte = (octet << bitsWritten);
            }
            bitIndex = (bitIndex + 5) % 8;
        }

        // For example, correct encoding of \x00 is
        // AA====== (\b0000'0000\b00xx'xxxx), not a
        // AAA=     (\b0000'0000\b0000'000x)
        size_t lastOctetBitsCount = (dstSize * 8) % 5;
        size_t expectedBitIndex = (lastOctetBitsCount == 0) ? 0 : 5 - lastOctetBitsCount;
        Y_ENSURE(!isStrict || (byte == 0 && bitIndex == expectedBitIndex), "Invalid Base32 string format");
        return dstSize;
    }

} // namespace

size_t Base32Encode(std::string_view src, char* dst) {
    if (src.size() == 0) {
        return 0;
    }

    size_t dstSize = 0;
    size_t curInd = 0;
    unsigned char c = src[curInd];
    unsigned char bitInd = 0;
    unsigned char n = 0;

    unsigned char ind = 0;
    while (curInd < src.size()) {
        ind = 0;

        unsigned char bitProcessed = 0;
        while (bitProcessed < 5) {
            n = std::min<unsigned char>(8 - bitInd, 5 - bitProcessed);

            shiftBitsFrom(ind, c, bitInd, n);

            bitProcessed += n;

            if (bitInd + n >= 8) {
                ++curInd;
                if (curInd == src.size()) {
                    c = 0;
                } else {
                    c = src[curInd];
                }
            }
            bitInd = (bitInd + n) % 8;
        }
        dst[dstSize++] = encodeBits(ind);
    }

    const size_t paddingSize = ((8 - (dstSize & 7)) & 7);
    for (size_t i = 0; i < paddingSize; ++i) {
        dst[dstSize++] = '=';
    }

    return dstSize;
}

size_t Base32Decode(std::string_view src, char* dst) {
    return Base32DecodeImpl(src, dst, /*isStrict*/ false);
}

size_t Base32StrictDecode(std::string_view src, char* dst) {
    return Base32DecodeImpl(src, dst, /*isStrict*/ true);
}
