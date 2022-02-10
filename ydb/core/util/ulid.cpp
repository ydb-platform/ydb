#include "ulid.h"

#include <util/random/random.h>
#include <util/stream/output.h>

namespace NKikimr {

namespace {

    constexpr TStringBuf ULID_TO_ALPHABET("0123456789abcdefghjkmnpqrstvwxyz");

    const ui8 ULID_FROM_ALPHABET[256] = {
        // 0-32
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        // 32-64
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 255, 255, 255, 255, 255, 255,
        // 64-96
        255, 10, 11, 12, 13, 14, 15, 16, 17, 255, 18, 19, 255, 20, 21, 255,
        22, 23, 24, 25, 26, 255, 27, 28, 29, 30, 31, 255, 255, 255, 255, 255,
        // 9625528
        255, 10, 11, 12, 13, 14, 15, 16, 17, 255, 18, 19, 255, 20, 21, 255,
        22, 23, 24, 25, 26, 255, 27, 28, 29, 30, 31, 255, 255, 255, 255, 255,
        // 128-256
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    };

    char ULIDToChar(ui8 b) {
        return ULID_TO_ALPHABET[b];
    }

    ui8 ULIDFromChar(char ch) {
        return ULID_FROM_ALPHABET[(unsigned char)ch];
    }

} // namespace

TString TULID::ToString() const {
    TString res = TString::Uninitialized(26);
    char* p = res.Detach();

#define ULID_BITS_HIGH_VALUE(idx, count) ( ui8(Data[idx] >> (8 - count)) )
#define ULID_BITS_LOW_VALUE(idx, count) ( ui8(Data[idx] & ((1 << count) - 1)) )
#define ULID_BITS_MID_VALUE(idx, skip, count) ( ui8((Data[idx] >> (8 - count - skip)) & ((1 << count) - 1)) )
#define ULID_BITS_CROSS_VALUE(idx, count1, count2) ( (ULID_BITS_LOW_VALUE(idx, count1) << count2) | ULID_BITS_HIGH_VALUE(idx + 1, count2) )

#define ULID_BITS_HIGH(idx, count) do { *p++ = ULIDToChar(ULID_BITS_HIGH_VALUE(idx, count)); } while (0)
#define ULID_BITS_LOW(idx, count) do { *p++ = ULIDToChar(ULID_BITS_LOW_VALUE(idx, count)); } while (0)
#define ULID_BITS_MID(idx, skip, count) do { *p++ = ULIDToChar(ULID_BITS_MID_VALUE(idx, skip, count)); } while (0)
#define ULID_BITS_CROSS(idx, count1, count2) do { *p++ = ULIDToChar(ULID_BITS_CROSS_VALUE(idx, count1, count2)); } while (0)

    // 10 char timestamp
    ULID_BITS_HIGH(0, 3); // 3 bits from idx 0
    ULID_BITS_LOW(0, 5); // 5 bits from idx 0
    ULID_BITS_HIGH(1, 5); // 5 bits from idx 1
    ULID_BITS_CROSS(1, 3, 2); // 3 bits from idx 1, 2 bits from idx 2
    ULID_BITS_MID(2, 2, 5); // 5 bits from idx 2
    ULID_BITS_CROSS(2, 1, 4); // 1 bit from idx 2, 4 bits from idx 3
    ULID_BITS_CROSS(3, 4, 1); // 4 bits from idx 3, 1 bit from idx 4
    ULID_BITS_MID(4, 1, 5); // 5 bits from idx 4
    ULID_BITS_CROSS(4, 2, 3); // 2 bits from idx 4, 3 bits from idx 5
    ULID_BITS_LOW(5, 5); // 5 bits from idx 5

    // 5 bytes to 8 char random
    ULID_BITS_HIGH(6, 5);
    ULID_BITS_CROSS(6, 3, 2);
    ULID_BITS_MID(7, 2, 5);
    ULID_BITS_CROSS(7, 1, 4);
    ULID_BITS_CROSS(8, 4, 1);
    ULID_BITS_MID(9, 1, 5);
    ULID_BITS_CROSS(9, 2, 3);
    ULID_BITS_LOW(10, 5);

    // 5 bytes to 8 char random
    ULID_BITS_HIGH(11, 5);
    ULID_BITS_CROSS(11, 3, 2);
    ULID_BITS_MID(12, 2, 5);
    ULID_BITS_CROSS(12, 1, 4);
    ULID_BITS_CROSS(13, 4, 1);
    ULID_BITS_MID(14, 1, 5);
    ULID_BITS_CROSS(14, 2, 3);
    ULID_BITS_LOW(15, 5);

#undef ULID_BITS_HIGH_VALUE
#undef ULID_BITS_LOW_VALUE
#undef ULID_BITS_MID_VALUE
#undef ULID_BITS_CROSS_VALUE
#undef ULID_BITS_HIGH
#undef ULID_BITS_LOW
#undef ULID_BITS_MID
#undef ULID_BITS_CROSS

    return res;
}

bool TULID::ParseString(TStringBuf buf) noexcept {
    if (buf.size() != 26) {
        return false;
    }
    const char* p = buf.data();

    memset(Data, 0, 16);

#define ULID_BITS_HIGH(idx, count) do { \
        ui8 v = ULIDFromChar(*p++); \
        if (!(v <= ((1 << count) - 1))) { \
            return false; \
        } \
        Data[idx] |= v << (8 - count); \
    } while(0)
#define ULID_BITS_LOW(idx, count) do { \
        ui8 v = ULIDFromChar(*p++); \
        if (!(v <= ((1 << count) - 1))) { \
            return false; \
        } \
        Data[idx] |= v; \
    } while(0)
#define ULID_BITS_MID(idx, skip, count) do { \
        ui8 v = ULIDFromChar(*p++); \
        if (!(v <= ((1 << count) - 1))) { \
            return false; \
        } \
        Data[idx] |= v << (8 - skip - count); \
    } while(0)
#define ULID_BITS_CROSS(idx, count1, count2) do { \
        ui8 v = ULIDFromChar(*p++); \
        if (!(v <= ((1 << (count1 + count2)) - 1))) { \
            return false; \
        } \
        Data[idx] |= v >> count2; \
        Data[idx+1] |= (v & ((1 << count2) - 1)) << (8 - count2); \
    } while(0)

    // 10 char timestamp
    ULID_BITS_HIGH(0, 3); // 3 bits from idx 0
    ULID_BITS_LOW(0, 5); // 5 bits from idx 0
    ULID_BITS_HIGH(1, 5); // 5 bits from idx 1
    ULID_BITS_CROSS(1, 3, 2); // 3 bits from idx 1, 2 bits from idx 2
    ULID_BITS_MID(2, 2, 5); // 5 bits from idx 2
    ULID_BITS_CROSS(2, 1, 4); // 1 bit from idx 2, 4 bits from idx 3
    ULID_BITS_CROSS(3, 4, 1); // 4 bits from idx 3, 1 bit from idx 4
    ULID_BITS_MID(4, 1, 5); // 5 bits from idx 4
    ULID_BITS_CROSS(4, 2, 3); // 2 bits from idx 4, 3 bits from idx 5
    ULID_BITS_LOW(5, 5); // 5 bits from idx 5

    // 5 bytes to 8 char random
    ULID_BITS_HIGH(6, 5);
    ULID_BITS_CROSS(6, 3, 2);
    ULID_BITS_MID(7, 2, 5);
    ULID_BITS_CROSS(7, 1, 4);
    ULID_BITS_CROSS(8, 4, 1);
    ULID_BITS_MID(9, 1, 5);
    ULID_BITS_CROSS(9, 2, 3);
    ULID_BITS_LOW(10, 5);

    // 5 bytes to 8 char random
    ULID_BITS_HIGH(11, 5);
    ULID_BITS_CROSS(11, 3, 2);
    ULID_BITS_MID(12, 2, 5);
    ULID_BITS_CROSS(12, 1, 4);
    ULID_BITS_CROSS(13, 4, 1);
    ULID_BITS_MID(14, 1, 5);
    ULID_BITS_CROSS(14, 2, 3);
    ULID_BITS_LOW(15, 5);

#undef ULID_BITS_HIGH
#undef ULID_BITS_LOW
#undef ULID_BITS_MID
#undef ULID_BITS_CROSS

    return true;
}

TULID TULIDGenerator::Last() const {
    TULID res;
    res.SetTimeMs(LastTimeMs);
    res.SetRandomHi(LastRandomHi);
    res.SetRandomLo(LastRandomLo);
    return res;
}

TULID TULIDGenerator::Next(TInstant now, EULIDMode mode) {
    ui64 ms = now.MilliSeconds();
    if (LastTimeMs != ms || mode == EULIDMode::Random) {
        LastTimeMs = ms;
        LastRandomLo = ::RandomNumber<ui64>();
        LastRandomHi = ::RandomNumber<ui16>() & 0x7FFF;
    } else {
        ui64 increment = ::RandomNumber<ui32>() + 1ull;
        ui64 prevRandomLo = LastRandomLo;
        LastRandomLo += increment;
        if (LastRandomLo < prevRandomLo) {
            // overflow
            ++LastRandomHi;
        }
    }
    return Last();
}

TULID TULIDGenerator::Next(EULIDMode mode) {
    return Next(TInstant::Now(), mode);
}

} // namespace NKikimr

Y_DECLARE_OUT_SPEC(, NKikimr::TULID, out, id) {
    out << id.ToString();
}
