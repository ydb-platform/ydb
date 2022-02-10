#pragma once

#include <library/cpp/streams/zc_memory_input/zc_memory_input.h>

#include <util/stream/output.h>
#include <util/ysaveload.h>

#include "longs.h"

struct Stream_traits {
    template <typename T>
    static T get(IInputStream& in) {
        T x;
        ::Load(&in, x);
        return x;
    }
    static ui8 get_8(IInputStream& in) {
        return get<ui8>(in);
    }
    static ui16 get_16(IInputStream& in) {
        return get<ui16>(in);
    }
    static ui32 get_32(IInputStream& in) {
        return get<ui32>(in);
    }
    static void put_8(ui8 x, IOutputStream& out) {
        ::Save(&out, x);
    }
    static void put_16(ui16 x, IOutputStream& out) {
        ::Save(&out, x);
    }
    static void put_32(ui32 x, IOutputStream& out) {
        ::Save(&out, x);
    }
    static int is_good(IInputStream& /*in*/) {
        return 1;
    }
    static int is_good(IOutputStream& /*out*/) {
        return 1;
    }
};

struct TZCMemoryInput_traits {
    template <typename T>
    static T get(TZCMemoryInput& in) {
        T x;
        in.ReadPOD(x);
        return x;
    }

    static ui8 Y_FORCE_INLINE get_8(TZCMemoryInput& in) {
        return get<ui8>(in);
    }

    static ui16 Y_FORCE_INLINE get_16(TZCMemoryInput& in) {
        return get<ui16>(in);
    }

    static ui32 Y_FORCE_INLINE get_32(TZCMemoryInput& in) {
        return get<ui32>(in);
    }

    static int Y_FORCE_INLINE is_good(TZCMemoryInput&) {
        return 1;
    }
};

void Y_FORCE_INLINE PackUI32(IOutputStream& out, ui32 v) {
    char buf[sizeof(ui32)];
    char* bufPtr = buf;
    size_t size;
    PACK_28(v, bufPtr, mem_traits, size);
    out.Write(buf, size);
}

template <class TStream>
struct TInputStream2Traits {
    typedef Stream_traits TTraits;
};

template <>
struct TInputStream2Traits<TZCMemoryInput> {
    typedef TZCMemoryInput_traits TTraits;
};

template <class TStream>
void Y_FORCE_INLINE UnPackUI32(TStream& in, ui32& v) {
    size_t size;
    UNPACK_28(v, in, TInputStream2Traits<TStream>::TTraits, size);
    (void)size;
}

template <class TStream>
ui32 Y_FORCE_INLINE UnPackUI32(TStream& in) {
    ui32 res;
    UnPackUI32(in, res);
    return res;
}
