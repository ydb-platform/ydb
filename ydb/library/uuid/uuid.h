#pragma once
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <cctype>
#include <cstring>
#include <utility>

class IOutputStream;

namespace NKikimr {
namespace NUuid {

static constexpr ui32 UUID_LEN = 16;

TString UuidBytesToString(const TString& in);
void UuidBytesToString(const TString& in, IOutputStream& out);
void UuidHalfsToString(ui64 low, ui64 hi, IOutputStream& out);
void UuidToString(ui16 dw[8], IOutputStream& out);
void UuidHalfsToByteString(ui64 low, ui64 hi, IOutputStream& out);

inline bool GetDigit(char c, ui32& digit) {
    digit = 0;
    if ('0' <= c && c <= '9') {
        digit = c - '0';
    }
    else if ('a' <= c && c <= 'f') {
        digit = c - 'a' + 10;
    }
    else if ('A' <= c && c <= 'F') {
        digit = c - 'A' + 10;
    }
    else {
        return false; // non-hex character
    }
    return true;
}

template<typename T>
inline bool IsValidUuid(const T& buf) {
    if (buf.Size() != 36) {
        return false;
    }

    for (size_t i = 0; i < buf.Size(); ++i) {
        const char c = buf.Data()[i];

        if (c == '-') {
            if (i != 8 && i != 13 && i != 18 && i != 23) {
                return false;
            }
        } else if (!std::isxdigit(c)) {
            return false;
        }
    }

    return true;
}

template<typename T>
bool ParseUuidToArray(const T& buf, ui16* dw, bool shortForm) {
    if (buf.Size() != (shortForm ? 32 : 36)) {
        return false;
    }

    size_t partId = 0;
    ui64 partValue = 0;
    size_t digitCount = 0;

    for (size_t i = 0; i < buf.Size(); ++i) {
        const char c = buf.Data()[i];

        if (!shortForm && (i == 8 || i == 13 || i == 18 || i == 23)) {
            if (c == '-') {
                continue;
            } else {
                return false;
            }
        }

        ui32 digit = 0;
        if (!GetDigit(c, digit)) {
            return false;
        }

        partValue = partValue * 16 + digit;

        if (++digitCount == 4) {
            dw[partId++] = partValue;
            digitCount = 0;
        }
    }

    std::swap(dw[0], dw[1]);
    for (ui32 i = 4; i < 8; ++i) {
        dw[i] = ((dw[i] >> 8) & 0xff) | ((dw[i] & 0xff) << 8);
    }

    return true;
}

inline void UuidHalfsToBytes(char *dst, size_t dstSize, ui64 hi, ui64 low) {
    union {
        char bytes[UUID_LEN];
        ui64 half[2];
    } buf;
    Y_ABORT_UNLESS(UUID_LEN == dstSize);
    buf.half[0] = low;
    buf.half[1] = hi;
    memcpy(dst, buf.bytes, sizeof(buf));
}

inline void UuidBytesToHalfs(const char *str, size_t sz, ui64 &high, ui64 &low) {
    union {
        char bytes[UUID_LEN];
        ui64 half[2];
    } buf;
    Y_ABORT_UNLESS(UUID_LEN == sz);
    memcpy(buf.bytes, str, sizeof(buf));
    low = buf.half[0];
    high = buf.half[1];
}

}
}
