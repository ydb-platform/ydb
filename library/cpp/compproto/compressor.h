#pragma once

#include <util/system/defaults.h>

namespace NCompProto {
    struct TEmpty {
    };

    struct TTable {
        TTable() {
            for (size_t i = 0; i < 64; ++i) {
                CodeBase[i] = 0;
                CodeMask[i] = 0;
                Length[i] = 0;
                PrefLength[i] = 0;
                Id[i] = 0;
            }
        }
        ui32 CodeBase[64];
        ui32 CodeMask[64];
        ui8 Length[64];
        ui8 PrefLength[64];
        ui8 Id[64];
        enum {
            PAGE_BOUND = 4096,
#ifdef WITH_VALGRIND
            SAFE_MODE = 1,
#else
#if defined(__has_feature)
#if __has_feature(address_sanitizer)
            SAFE_MODE = 1,
#else
            SAFE_MODE = 0,
#endif
#else
            SAFE_MODE = 0,
#endif
#endif
        };
        ui32 inline Decompress(const ui8* codes, ui64& offset) const {
            codes += (offset >> 3);
            size_t pageOff = size_t(codes) % PAGE_BOUND;
            size_t readOff = offset & 7;
            if (pageOff > PAGE_BOUND - 8 || SAFE_MODE) {
                size_t off = 8;
                ui64 res = codes[0];
                ++codes;
                ui64 indexCur = ((res + 0x0000) >> readOff) & 63;
                ui64 indexAlt = ((res + 0xff00) >> readOff) & 63;
                if (Id[indexCur] != Id[indexAlt]) {
                    res += (ui64(codes[0]) << off);
                    ++codes;
                    off += 8;
                    indexCur = (res >> readOff) & 63;
                }
                ui64 index = indexCur;
                ui64 length = Length[index];
                while (off < readOff + length) {
                    res += (ui64(codes[0]) << off);
                    ++codes;
                    off += 8;
                }
                offset += length;
                ui64 code = res >> readOff;
                return (((ui32)(code >> PrefLength[index])) & CodeMask[index]) + CodeBase[index];
            }
            ui64 code = ((const ui64*)(codes))[0] >> readOff;
            ui64 index = code & 63;
            offset += Length[index];
            return (((ui32)(code >> PrefLength[index])) & CodeMask[index]) + CodeBase[index];
        }
    };

}
