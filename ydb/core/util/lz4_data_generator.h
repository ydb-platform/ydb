#pragma once

#include <util/generic/string.h>
#include <util/random/fast.h>

#include <contrib/libs/lz4/lz4.h>

namespace NKikimr {

template <class ResultContainer = TString>
inline ResultContainer GenDataForLZ4(const ui64 size, const ui64 seed = 0) {
    ResultContainer data = ResultContainer::Uninitialized(size);
    const ui32 long_step = Max<ui32>(2027, size / 20);
    const ui32 short_step = Min<ui32>(53, long_step / 400);
    char *buffer = [&]() -> char * {
        if constexpr(std::is_same<ResultContainer, TString>::value) {
            return data.Detach();
        } else {
            return data.mutable_data();
        }
    }();
    for (ui32 i = 0; i < data.size(); ++i) {
        const ui32 j = i + seed;
        buffer[i] = 0xff & (j % short_step + j / long_step);
    }
    return data;
}

template <class ResultContainer = TString>
inline ResultContainer FastGenDataForLZ4(size_t size, ui64 seed = 0) {
    ResultContainer data = ResultContainer::Uninitialized(size);
    char *ptr = [&]() -> char * {
        if constexpr(std::is_same<ResultContainer, TString>::value) {
            return data.Detach();
        } else {
            return data.mutable_data();
        }
    }();
    TReallyFastRng32 rng(seed);

    constexpr size_t minRunLen = 32;
    constexpr size_t maxRunLen = 64;
    const size_t runLen = minRunLen + sizeof(ui32) * (rng() % ((maxRunLen - minRunLen) / sizeof(ui32) + 1));

#define UNROLL(LEN) \
    do {                                                        \
        ui64 x0, x1, x2, x3, x4 = 0, x5 = 0, x6 = 0, x7 = 0;    \
        x0 = rng() | (ui64)rng() << 32;                         \
        x1 = rng() | (ui64)rng() << 32;                         \
        x2 = rng() | (ui64)rng() << 32;                         \
        x3 = rng() | (ui64)rng() << 32;                         \
        if constexpr (LEN >= 36) {                              \
            x4 = rng() | (ui64)rng() << 32;                     \
        }                                                       \
        if constexpr (LEN >= 44) {                              \
            x5 = rng() | (ui64)rng() << 32;                     \
        }                                                       \
        if constexpr (LEN >= 52) {                              \
            x6 = rng() | (ui64)rng() << 32;                     \
        }                                                       \
        if constexpr (LEN >= 60) {                              \
            x7 = rng() | (ui64)rng() << 32;                     \
        }                                                       \
        while (size >= LEN) {                                   \
            *reinterpret_cast<ui64*>(ptr) = x0;                 \
            *reinterpret_cast<ui64*>(ptr + 8) = x1;             \
            *reinterpret_cast<ui64*>(ptr + 16) = x2;            \
            *reinterpret_cast<ui64*>(ptr + 24) = x3;            \
            if constexpr (LEN == 36) {                          \
                *reinterpret_cast<ui32*>(ptr + 32) = x4;        \
            } else if constexpr (LEN >= 40) {                   \
                *reinterpret_cast<ui64*>(ptr + 32) = x4;        \
            }                                                   \
            if constexpr (LEN == 44) {                          \
                *reinterpret_cast<ui32*>(ptr + 40) = x5;        \
            } else if constexpr (LEN >= 48) {                   \
                *reinterpret_cast<ui64*>(ptr + 40) = x5;        \
            }                                                   \
            if constexpr (LEN == 52) {                          \
                *reinterpret_cast<ui32*>(ptr + 48) = x6;        \
            } else if constexpr (LEN >= 56) {                   \
                *reinterpret_cast<ui64*>(ptr + 48) = x6;        \
            }                                                   \
            if constexpr (LEN == 60) {                          \
                *reinterpret_cast<ui32*>(ptr + 56) = x7;        \
            } else if constexpr (LEN >= 64) {                   \
                *reinterpret_cast<ui64*>(ptr + 56) = x7;        \
            }                                                   \
            ptr += LEN;                                         \
            size -= LEN;                                        \
        }                                                       \
        for (ui64 x : {x0, x1, x2, x3, x4, x5, x6, x7}) {       \
            if (size >= 8) {                                    \
                *reinterpret_cast<ui64*>(ptr) = x;              \
                ptr += 8;                                       \
                size -= 8;                                      \
            } else {                                            \
                memcpy(ptr, &x, size);                          \
                break;                                          \
            }                                                   \
        }                                                       \
    } while (false);

    switch (runLen) {
        case 32: UNROLL(32); break;
        case 36: UNROLL(36); break;
        case 40: UNROLL(40); break;
        case 44: UNROLL(44); break;
        case 48: UNROLL(48); break;
        case 52: UNROLL(52); break;
        case 56: UNROLL(56); break;
        case 60: UNROLL(60); break;
        case 64: UNROLL(64); break;
        default: Y_ABORT();
    }

    return data;
}

}
