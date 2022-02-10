#pragma once

#include <util/system/defaults.h>

#define CRC16INIT 0
#define CRC32INIT 0
#define CRC64INIT ULL(0xFFFFFFFFFFFFFFFF)

// CCITT CRC-16
inline ui16 crc16(const char* buf, size_t buflen, ui32 crcinit = CRC16INIT) {
    ui32 crc = 0xFFFF & ((crcinit >> 8) ^ (crcinit << 8));
    const char* end = buf + buflen;
    extern const ui32* crctab16;

    while (buf < end) {
        crc = (crc >> 8) ^ crctab16[(crc ^ *buf) & 0xFF];
        ++buf;
    }
    return (ui16)(0xFFFF & ((crc >> 8) ^ (crc << 8)));
}

struct IdTR {
    Y_FORCE_INLINE static ui8 T(ui8 a) {
        return a;
    }
};

// CCITT CRC-32
template <class TR>
inline ui32 crc32(const char* buf, size_t buflen, ui32 crcinit = CRC32INIT) {
    ui32 crc = crcinit ^ 0xFFFFFFFF;
    const char* end = buf + buflen;
    extern const ui32* crctab32;

    while (buf < end) {
        crc = (crc >> 8) ^ crctab32[(crc ^ TR::T((ui8)*buf)) & 0xFF];
        ++buf;
    }

    return crc ^ 0xFFFFFFFF;
}

inline ui32 crc32(const char* buf, size_t buflen, ui32 crcinit = CRC32INIT) {
    return crc32<IdTR>(buf, buflen, crcinit);
}

inline ui32 crc32(const void* buf, size_t buflen, ui32 crcinit = CRC32INIT) {
    return crc32((const char*)buf, buflen, crcinit);
}

// Copyright (C) Sewell Development Corporation, 1994 - 1998.
inline ui64 crc64(const void* buf, size_t buflen, ui64 crcinit = CRC64INIT) {
    const unsigned char* ptr = (const unsigned char*)buf;
    extern const ui64* crctab64;

    while (buflen--) {
        crcinit = crctab64[((crcinit >> 56) ^ *ptr++)] ^ (crcinit << 8);
    }
    return crcinit;
}

namespace NCrcPrivate {
    template <unsigned N>
    struct TCrcHelper;

#define DEF_CRC_FUNC(t)                                                          \
    template <>                                                                  \
    struct TCrcHelper<t> {                                                       \
        static const ui##t Init = CRC##t##INIT;                                  \
        static inline ui##t Crc(const void* buf, size_t buflen, ui##t crcinit) { \
            return crc##t((const char*)buf, buflen, crcinit);                    \
        }                                                                        \
    };

    DEF_CRC_FUNC(16)
    DEF_CRC_FUNC(32)
    DEF_CRC_FUNC(64)

#undef DEF_CRC_FUNC
}

template <class T>
static inline T Crc(const void* buf, size_t len, T init) {
    return (T)NCrcPrivate::TCrcHelper<8 * sizeof(T)>::Crc(buf, len, init);
}

template <class T>
static inline T Crc(const void* buf, size_t len) {
    return Crc<T>(buf, len, (T)NCrcPrivate::TCrcHelper<8 * sizeof(T)>::Init);
}
