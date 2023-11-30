#pragma once

#include <util/generic/cast.h>

// although target value is float, this thing is only used as unsigned int container
struct pf16ui32 {
    ui16 val;
    pf16ui32()
        : val(0)
    {
    }
    void operator=(ui32 t) {
        val = static_cast<ui16>(BitCast<ui32>(static_cast<float>(t)) >> 15);
    }
    operator ui32() const {
        return (ui32)BitCast<float>((ui32)(val << 15));
    }
};

// unsigned float value
struct pf16float {
    ui16 val;
    pf16float()
        : val(0)
    {
    }
    void operator=(float t) {
        assert(t >= 0.);
        val = static_cast<ui16>(BitCast<ui32>(t) >> 15);
    }
    operator float() const {
        return BitCast<float>((ui32)(val << 15));
    }
};

// signed float value
struct sf16float {
    ui16 val;
    sf16float()
        : val(0)
    {
    }
    void operator=(float t) {
        assert(t >= 0.);
        val = BitCast<ui32>(t) >> 16;
    }
    operator float() const {
        return BitCast<float>((ui32)(val << 16));
    }
};

typedef i32 time_t32; // not really lossy, should be placed somewhere else
