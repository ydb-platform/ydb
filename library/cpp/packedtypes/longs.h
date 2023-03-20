#pragma once

#include <util/system/defaults.h> // _BIDSCLASS _EXPCLASS
#include <util/system/yassert.h>
#include <util/system/unaligned_mem.h>

#define PUT_8(x, buf, shift) WriteUnaligned<ui8>((buf)++, (x) >> (shift))
#define GET_8_OR(x, buf, type, shift) (x) |= (type) * (buf)++ << (shift)

#if defined(_big_endian_)
#define LO_SHIFT 1
#define HI_SHIFT 0
#elif defined(_little_endian_)
#define LO_SHIFT 0
#define HI_SHIFT 1
#endif

#if !defined(_must_align2_)
#define PUT_16(x, buf, shift) WriteUnaligned<ui16>(buf, (x) >> (shift)), (buf) += 2
#define GET_16_OR(x, buf, type, shift) (x) |= (type)ReadUnaligned<ui16>(buf) << (shift), (buf) += 2
#else
#define PUT_16(x, buf, shift) PUT_8(x, buf, shift + 8 * LO_SHIFT), PUT_8(x, buf, shift + 8 * HI_SHIFT)
#define GET_16_OR(x, buf, type, shift) GET_8_OR(x, buf, type, shift + 8 * LO_SHIFT), GET_8_OR(x, buf, type, shift + 8 * HI_SHIFT)
#endif

#if !defined(_must_align4_)
#define PUT_32(x, buf, shift) WriteUnaligned<ui32>(buf, (x) >> (shift)), (buf) += 4
#define GET_32_OR(x, buf, type, shift) (x) |= (type)ReadUnaligned<ui32>(buf) << (shift), (buf) += 4
#else
#define PUT_32(x, buf, shift) PUT_16(x, buf, shift + 16 * LO_SHIFT), PUT_16(x, buf, shift + 16 * HI_SHIFT)
#define GET_32_OR(x, buf, type, shift) GET_16_OR(x, buf, type, shift + 16 * LO_SHIFT), GET_16_OR(x, buf, type, shift + 16 * HI_SHIFT)
#endif

#if !defined(_must_align8_)
#define PUT_64(x, buf, shift) WriteUnaligned<ui64>(buf, (x) >> (shift)), (buf) += 8
#define GET_64_OR(x, buf, type, shift) (x) |= (type)ReadUnaligned<ui64>(buf) << (shift), (buf) += 8
#else
#define PUT_64(x, buf, shift) PUT_32(x, buf, shift + 32 * LO_SHIFT), PUT_32(x, buf, shift + 32 * HI_SHIFT)
#define GET_64_OR(x, buf, type, shift) GET_32_OR(x, buf, type, shift + 32 * LO_SHIFT), GET_32_OR(x, buf, type, shift + 32 * HI_SHIFT)
#endif

struct mem_traits {
    static ui8 get_8(const char*& mem) {
        ui8 x = 0;
        GET_8_OR(x, mem, ui8, 0);
        return x;
    }
    static ui16 get_16(const char*& mem) {
        ui16 x = 0;
        GET_16_OR(x, mem, ui16, 0);
        return x;
    }
    static ui32 get_32(const char*& mem) {
        ui32 x = 0;
        GET_32_OR(x, mem, ui32, 0);
        return x;
    }
    static void put_8(ui8 x, char*& mem) {
        PUT_8(x, mem, 0);
    }
    static void put_16(ui16 x, char*& mem) {
        PUT_16(x, mem, 0);
    }
    static void put_32(ui32 x, char*& mem) {
        PUT_32(x, mem, 0);
    }
    static int is_good(char*&) {
        return 1;
    }
};

/*
|____|____|____|____|____|____|____|____|____|____|____|____|____|____|8***|****
|____|____|____|____|____|____|____|____|____|____|____|____|i4**|****|****|****
|____|____|____|____|____|____|____|____|____|____|ii2*|****|****|****|****|****
|____|____|____|____|____|____|____|____|iii1|****|****|****|****|****|****|****
|____|____|____|____|____|____|iiii|8***|****|****|****|****|****|****|****|****
|____|____|____|____|iiii|i4**|****|****|****|****|****|****|****|****|****|****
|____|____|iiii|ii2*|****|****|****|****|****|****|****|****|****|****|****|****
|iiii|iii1|****|****|****|****|****|****|****|****|****|****|****|****|****|****
*/

#define PACK1LIM 0x80u
#define PACK2LIM 0x4000u
#define PACK3LIM 0x200000u
#define PACK4LIM 0x10000000u
#define PACK5LIM 0x800000000ull
#define PACK6LIM 0x40000000000ull
#define PACK7LIM 0x2000000000000ull
#define PACK8LIM 0x100000000000000ull

#define MY_14(x) ((ui16)(x) < PACK1LIM ? 1 : 2)
#define MY_28(x) ((ui32)(x) < PACK2LIM ? MY_14(x) : ((ui32)(x) < PACK3LIM ? 3 : 4))

#define MY_32(x) ((ui32)(x) < PACK4LIM ? MY_28(x) : 5)
#define MY_64(x) ((ui64)(x) < PACK4LIM ? MY_28(x) : ((ui64)(x) < PACK6LIM ? ((ui64)(x) < PACK5LIM ? 5 : 6) : ((ui64)(x) < PACK7LIM ? 7 : ((ui64)(x) < PACK8LIM ? 8 : 9))))

#if !defined(MACRO_BEGIN)
#define MACRO_BEGIN do {
#define MACRO_END \
    }             \
    while (0)
#endif

#define PACK_14(x, buf, how, ret)                  \
    MACRO_BEGIN                                    \
    if ((ui16)(x) < PACK1LIM) {                    \
        how::put_8((ui8)(x), (buf));               \
        (ret) = 1;                                 \
    } else {                                       \
        how::put_8((ui8)(0x80 | (x) >> 8), (buf)); \
        how::put_8((ui8)(x), (buf));               \
        (ret) = 2;                                 \
    }                                              \
    MACRO_END

#define PACK_28(x, buf, how, ret)                     \
    MACRO_BEGIN                                       \
    if ((ui32)(x) < PACK2LIM) {                       \
        PACK_14(x, buf, how, ret);                    \
    } else {                                          \
        if ((ui32)(x) < PACK3LIM) {                   \
            how::put_8((ui8)(0xC0 | (x) >> 16), buf); \
            (ret) = 3;                                \
        } else {                                      \
            how::put_8((ui8)(0xE0 | (x) >> 24), buf); \
            how::put_8((ui8)((x) >> 16), buf);        \
            (ret) = 4;                                \
        }                                             \
        how::put_16((ui16)(x), (buf));                \
    }                                                 \
    MACRO_END

#define PACK_32(x, buf, how, ret)     \
    MACRO_BEGIN                       \
    if ((ui32)(x) < PACK4LIM) {       \
        PACK_28(x, buf, how, ret);    \
    } else {                          \
        how::put_8((ui8)(0xF0), buf); \
        how::put_32((ui32)(x), buf);  \
        (ret) = 5;                    \
    }                                 \
    MACRO_END

#define PACK_64(x, buf, how, ret)                             \
    MACRO_BEGIN                                               \
    if ((ui64)(x) < PACK4LIM) {                               \
        PACK_28((ui32)(x), buf, how, ret);                    \
    } else {                                                  \
        if ((ui64)(x) < PACK6LIM) {                           \
            if ((ui64)(x) < PACK5LIM) {                       \
                how::put_8((ui8)(0xF0 | (x) >> 32), buf);     \
                (ret) = 5;                                    \
            } else {                                          \
                how::put_8((ui8)(0xF8 | (x) >> 40), buf);     \
                how::put_8((ui8)((x) >> 32), buf);            \
                (ret) = 6;                                    \
            }                                                 \
        } else {                                              \
            if ((ui64)(x) < PACK7LIM) {                       \
                how::put_8((ui8)(0xFC | (x) >> 48), buf);     \
                (ret) = 7;                                    \
            } else {                                          \
                if ((ui64)(x) < PACK8LIM) {                   \
                    how::put_8((ui8)(0xFE | (x) >> 56), buf); \
                    how::put_8((ui8)((x) >> 48), buf);        \
                    (ret) = 8;                                \
                } else {                                      \
                    how::put_8((ui8)(0xFF), buf);             \
                    how::put_16((ui16)((x) >> 48), buf);      \
                    (ret) = 9;                                \
                }                                             \
            }                                                 \
            how::put_16((ui16)((x) >> 32), buf);              \
        }                                                     \
        how::put_32((ui32)(x), buf);                          \
    }                                                         \
    MACRO_END

#define DO_UNPACK_14(firstByte, x, buf, how, ret) \
    MACRO_BEGIN                                   \
    if (firstByte < 0x80) {                       \
        (x) = (firstByte);                        \
        (ret) = 1;                                \
    } else {                                      \
        (x) = (firstByte & 0x7F) << 8;            \
        (x) |= how::get_8(buf);                   \
        (ret) = 2;                                \
    }                                             \
    MACRO_END

#define UNPACK_14(x, buf, how, ret)            \
    MACRO_BEGIN                                \
    ui8 firstByte = how::get_8(buf);           \
    DO_UNPACK_14(firstByte, x, buf, how, ret); \
    MACRO_END

#define DO_UNPACK_28(firstByte, x, buf, how, ret)  \
    MACRO_BEGIN                                    \
    if (firstByte < 0xC0) {                        \
        DO_UNPACK_14(firstByte, x, buf, how, ret); \
    } else {                                       \
        if (firstByte < 0xE0) {                    \
            (x) = (firstByte & 0x3F) << 16;        \
            (ret) = 3;                             \
        } else {                                   \
            (x) = (firstByte & 0x1F) << 24;        \
            (x) |= how::get_8(buf) << 16;          \
            (ret) = 4;                             \
        }                                          \
        (x) |= how::get_16(buf);                   \
    }                                              \
    MACRO_END

#define UNPACK_28(x, buf, how, ret)            \
    MACRO_BEGIN                                \
    ui8 firstByte = how::get_8(buf);           \
    DO_UNPACK_28(firstByte, x, buf, how, ret); \
    MACRO_END

#define DO_UNPACK_32(firstByte, x, buf, how, ret)  \
    MACRO_BEGIN                                    \
    if (firstByte < 0xF0) {                        \
        DO_UNPACK_28(firstByte, x, buf, how, ret); \
    } else {                                       \
        (x) = how::get_32(buf);                    \
        (ret) = 5;                                 \
    }                                              \
    MACRO_END

#define UNPACK_32(x, buf, how, ret)            \
    MACRO_BEGIN                                \
    ui8 firstByte = how::get_8(buf);           \
    DO_UNPACK_32(firstByte, x, buf, how, ret); \
    MACRO_END

#define DO_UNPACK_64(firstByte, x, buf, how, ret)         \
    MACRO_BEGIN                                           \
    if (firstByte < 0xF0) {                               \
        DO_UNPACK_28(firstByte, x, buf, how, ret);        \
    } else {                                              \
        if (firstByte < 0xFC) {                           \
            if (firstByte < 0xF8) {                       \
                (x) = (ui64)(firstByte & 0x0F) << 32;     \
                (ret) = 5;                                \
            } else {                                      \
                (x) = (ui64)(firstByte & 0x07) << 40;     \
                (x) |= (ui64)how::get_8(buf) << 32;       \
                (ret) = 6;                                \
            }                                             \
        } else {                                          \
            if (firstByte < 0xFE) {                       \
                (x) = (ui64)(firstByte & 0x03) << 48;     \
                (ret) = 7;                                \
            } else {                                      \
                if (firstByte < 0xFF) {                   \
                    (x) = (ui64)(firstByte & 0x01) << 56; \
                    (x) |= (ui64)how::get_8(buf) << 48;   \
                    (ret) = 8;                            \
                } else {                                  \
                    (x) = (ui64)how::get_16(buf) << 48;   \
                    (ret) = 9;                            \
                }                                         \
            }                                             \
            (x) |= (ui64)how::get_16(buf) << 32;          \
        }                                                 \
        (x) |= how::get_32(buf);                          \
    }                                                     \
    MACRO_END

#define UNPACK_64(x, buf, how, ret)            \
    MACRO_BEGIN                                \
    ui8 firstByte = how::get_8(buf);           \
    DO_UNPACK_64(firstByte, x, buf, how, ret); \
    MACRO_END

inline int in_long(i64& longVal, const char* ptrBuf) {
    int ret = 0;
    UNPACK_64(longVal, ptrBuf, mem_traits, ret);
    return ret;
}

inline int out_long(const i64& longVal, char* ptrBuf) {
    int ret = 0;
    PACK_64(longVal, ptrBuf, mem_traits, ret); /*7*/
    return ret;
}

inline int len_long(const i64& longVal) {
    return MY_64(longVal);
}

inline int in_long(i32& longVal, const char* ptrBuf) {
    int ret = 0;
    UNPACK_32(longVal, ptrBuf, mem_traits, ret);
    return ret;
}

inline int out_long(const i32& longVal, char* ptrBuf) {
    int ret = 0;
    PACK_32(longVal, ptrBuf, mem_traits, ret);
    return ret;
}

inline int len_long(const i32& longVal) {
    return MY_32(longVal);
}

template <typename T, typename C>
inline const C* Unpack32(T& x, const C* src) {
    int pkLen = 0;
    const char* c = reinterpret_cast<const char*>(src);
    Y_UNUSED(pkLen);
    UNPACK_32(x, c, mem_traits, pkLen);
    Y_ASSERT(pkLen);
    return reinterpret_cast<const C*>(c);
}

template <typename T, typename C>
inline const C* Unpack64(T& x, const C* src) {
    int pkLen = 0;
    const char* c = reinterpret_cast<const char*>(src);
    Y_UNUSED(pkLen);
    UNPACK_64(x, c, mem_traits, pkLen);
    Y_ASSERT(pkLen);
    return reinterpret_cast<const C*>(c);
}

template <typename T, typename C>
inline C* Pack32(const T& x, C* dest) {
    int pkLen = 0;
    Y_UNUSED(pkLen);
    char* c = reinterpret_cast<char*>(dest);
    PACK_32(x, c, mem_traits, pkLen);
    Y_ASSERT(pkLen);
    return reinterpret_cast<C*>(c);
}

template <typename T, typename C>
inline C* Pack64(const T& x, C* dest) {
    int pkLen = 0;
    Y_UNUSED(pkLen);
    char* c = reinterpret_cast<char*>(dest);
    PACK_64(x, c, mem_traits, pkLen);
    Y_ASSERT(pkLen);
    return reinterpret_cast<C*>(c);
}
