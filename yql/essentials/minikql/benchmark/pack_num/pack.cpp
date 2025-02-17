#include "pack.h"

////////////////////////////////////////////////////////////////////////////////
//
// Pack
//

int Pack(ui8* buf, ui32 value) {
    ui8* p = buf;
    for (;;) {
        ui8 v = (ui8)value & 0x7f;
        value >>= 7;
        if (!value) {
            *p++ = v;
            break;
        }
        *p++ = (v | 0x80);
    }
    return (int)(p - buf);
}

ui32 Unpack(const ui8*& data) {
    ui32 value = 0;
    const ui8* p = data;
    for (int off = 0;; ++p, off += 7) {
        value |= (*p & 0x7f) << off;
        if ((*p & 0x80) == 0) {
            ++p;
            break;
        }
    }
    data = p;
    return value;
}

////////////////////////////////////////////////////////////////////////////////
//
// PackU32
//

int PackU32(ui32 value, void* buf) {
    ui8* p = (ui8*)buf;       //   654 3210  <-- bit of 'value' (7 bits total)
    if (value < (1u << 7)) {  // [0--- ----]
        p[0] = (ui8)value;    //  ^       ^
        return 1;             // MSB     LSB
    }
    value -= (1u << 7);       //    DC BA98   7654 3210 <-- bit of 'value' (14 == 0xD bits total)
    if (value < (1u << 14)) { // [10-- ----] [---- ----]
        p[1] = (ui8)value; value >>= 8;
        p[0] = 0x80 | (ui8)value;
        return 2;
    }
    value -= (1u << 14);      // And so on...
    if (value < (1u << 21)) { // [110- ----] [---- ----] [---- ----]
        p[2] = (ui8)value; value >>= 8;
        p[1] = (ui8)value; value >>= 8;
        p[0] = 0xc0 | (ui8)value;
        return 3;
    }
    value -= (1u << 21);
    if (value < (1u << 28)) { // [1110 ----] [---- ----] [---- ----] [---- ----]
        p[3] = (ui8)value; value >>= 8;
        p[2] = (ui8)value; value >>= 8;
        p[1] = (ui8)value; value >>= 8;
        p[0] = 0xe0 | (ui8)value;
        return 4;
    }                         // ...but for largest numbers, whole first byte is reserved.
    value -= (1u << 28);      // [1111 0000] [---- ----] [---- ----] [---- ----] [---- ----]
    p[4] = (ui8)value; value >>= 8;
    p[3] = (ui8)value; value >>= 8;
    p[2] = (ui8)value; value >>= 8;
    p[1] = (ui8)value;
    p[0] = 0xf0;
    return 5;
}

int UnpackU32(ui32* value, const void* buf) {
    const ui8* p = (const ui8*)buf;
    ui8 b = p[0];
    if (!(b & 0x80)) {
        *value = b;
        return 1;
    }
    if (!(b & 0x40)) {
        *value = 0x80 + (((b & 0x3f) << 8) | p[1]);
        return 2;
    }
    if (!(b & 0x20)) {
        *value = 0x4080 + (((b & 0x1f) << 16) | (p[1] << 8) | p[2]);
        return 3;
    }
    if (!(b & 0x10)) {
        *value = 0x204080 + (((b & 0x0f) << 24) | (p[1] << 16) | (p[2] << 8) | p[3]);
        return 4;
    }
    *value = 0x10204080 + ((p[1] << 24) | (p[2] << 16) | (p[3] << 8) | p[4]);
    return 5;
}

int SkipU32(const void* buf) {
    static const i8 skip[16] = { 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 4, 5 };
    int i = ((const ui8*)buf)[0] >> 4;
    return skip[i];
}

////////////////////////////////////////////////////////////////////////////////
//
// PackU64
//

int PackU64(ui64 value, void* buf) { // Packing scheme is analogous to PackU32()
    ui8* p = (ui8*)buf;
    if (value < (1u << 7)) {
        p[0] = (ui8)value;
        return 1;
    }
    value -= (1u << 7);
    if (value < (1u << 14)) {
        p[1] = (ui8)value; value >>= 8;
        p[0] = 0x80 | (ui8)value;
        return 2;
    }
    value -= (1u << 14);
    if (value < (1u << 21)) {
        p[2] = (ui8)value; value >>= 8;
        p[1] = (ui8)value; value >>= 8;
        p[0] = 0xc0 | (ui8)value;
        return 3;
    }
    value -= (1u << 21);
    if (value < (1u << 28)) {
        p[3] = (ui8)value; value >>= 8;
        p[2] = (ui8)value; value >>= 8;
        p[1] = (ui8)value; value >>= 8;
        p[0] = 0xe0 | (ui8)value;
        return 4;
    }
    value -= (1u << 28);
    if (value < (ui64(1) << 35)) {
        p[4] = (ui8)value; value >>= 8;
        p[3] = (ui8)value; value >>= 8;
        p[2] = (ui8)value; value >>= 8;
        p[1] = (ui8)value; value >>= 8;
        p[0] = 0xf0 | (ui8)value;
        return 5;
    }
    value -= ui64(1) << 35;
    if (value < (ui64(1) << 42)) {
        p[5] = (ui8)value; value >>= 8;
        p[4] = (ui8)value; value >>= 8;
        p[3] = (ui8)value; value >>= 8;
        p[2] = (ui8)value; value >>= 8;
        p[1] = (ui8)value; value >>= 8;
        p[0] = 0xf8 | (ui8)value;
        return 6;
    }
    value -= ui64(1) << 42;
    if (value < (ui64(1) << 49)) {  // [1111 110-] [---- ----] ... (+ 5 bytes)
        p[6] = (ui8)value; value >>= 8;
        p[5] = (ui8)value; value >>= 8;
        p[4] = (ui8)value; value >>= 8;
        p[3] = (ui8)value; value >>= 8;
        p[2] = (ui8)value; value >>= 8;
        p[1] = (ui8)value; value >>= 8;
        p[0] = 0xfc | (ui8)value;
        return 7;
    }
    value -= ui64(1) << 49;
    if (value < (ui64(1) << 56)) {  // [1111 1110] [---- ----] ... (+ 6 bytes)
        p[7] = (ui8)value; value >>= 8;
        p[6] = (ui8)value; value >>= 8;
        p[5] = (ui8)value; value >>= 8;
        p[4] = (ui8)value; value >>= 8;
        p[3] = (ui8)value; value >>= 8;
        p[2] = (ui8)value; value >>= 8;
        p[1] = (ui8)value; value >>= 8;
        p[0] = 0xfe | (ui8)value;
        return 8;
    }
    value -= ui64(1) << 56;         // [1111 1111] [---- ----] ... (+ 7 bytes)
    p[8] = (ui8)value; value >>= 8; //              ^
    p[7] = (ui8)value; value >>= 8; //         not a zero; contains actual bit
    p[6] = (ui8)value; value >>= 8;
    p[5] = (ui8)value; value >>= 8;
    p[4] = (ui8)value; value >>= 8;
    p[3] = (ui8)value; value >>= 8;
    p[2] = (ui8)value; value >>= 8;
    p[1] = (ui8)value; value >>= 8;
    p[0] = 0xff;
    return 9;
}

int UnpackU64(ui64* value, const void* buf) {
    const ui8* p = (const ui8*)buf;
    ui8 b = p[0];
    if (!(b & 0x80)) {
        *value = b;
        return 1;
    }
    if (!(b & 0x40)) {
        *value = 0x80 + (((b & 0x3f) << 8) | p[1]);
        return 2;
    }
    if (!(b & 0x20)) {
        *value = 0x4080 + (((b & 0x1f) << 16) | (p[1] << 8) | p[2]);
        return 3;
    }
    if (!(b & 0x10)) {
        *value = 0x204080 + (((b & 0x0f) << 24) | (p[1] << 16) | (p[2] << 8) | p[3]);
        return 4;
    }
    if (!(b & 0x08)) {
        *value = 0x10204080 + (
            (ui64(b & 0x07) << 32) |
            (ui32(p[1])     << 24) |
            (     p[2]      << 16) |
            (     p[3]      << 8)  |
                  p[4]
        );
        return 5;
    }
    if (!(b & 0x04)) {
        *value = 0x0810204080ull + (
            (ui64(b & 0x03) << 40) |
            (ui64(p[1])     << 32) |
            (ui32(p[2])     << 24) |
            (     p[3]      << 16) |
            (     p[4]      << 8)  |
                  p[5]
        );
        return 6;
    }
    if (!(b & 0x02)) {
        *value = 0x040810204080ull + (
            (ui64(b & 0x01) << 48) |
            (ui64(p[1])     << 40) |
            (ui64(p[2])     << 32) |
            (ui32(p[3])     << 24) |
            (     p[4]      << 16) |
            (     p[5]      << 8)  |
                  p[6]
        );
        return 7;
    }
    if (!(b & 0x01)) {
        *value = 0x02040810204080ull + (
            (ui64(p[1]) << 48) |
            (ui64(p[2]) << 40) |
            (ui64(p[3]) << 32) |
            (ui32(p[4]) << 24) |
            (     p[5]  << 16) |
            (     p[6]  << 8)  |
                  p[7]
        );
        return 8;
    }
    *value = 0x0102040810204080ull + (
        (ui64(p[1]) << 56) |
        (ui64(p[2]) << 48) |
        (ui64(p[3]) << 40) |
        (ui64(p[4]) << 32) |
        (ui32(p[5]) << 24) |
        (     p[6]  << 16) |
        (     p[7]  << 8)  |
              p[8]
    );
    return 9;
}

#define REPEAT_16(x)  x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x
#define REPEAT_32(x)  REPEAT_16(x), REPEAT_16(x)
#define REPEAT_64(x)  REPEAT_32(x), REPEAT_32(x)
#define REPEAT_128(x) REPEAT_64(x), REPEAT_64(x)

int SkipU64(const void* buf) {
    static const i8 skip[256] = {
        REPEAT_128(1), REPEAT_64(2), REPEAT_32(3), REPEAT_16(4),
        5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 7, 7, 8, 9
    };
    return skip[*(const ui8*)buf];
}

#undef REPEAT_16
#undef REPEAT_32
#undef REPEAT_64
#undef REPEAT_128
