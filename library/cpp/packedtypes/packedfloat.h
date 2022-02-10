#pragma once

#include <util/generic/cast.h>
#include <util/generic/ylimits.h>
#include <util/system/hi_lo.h>

#include <cmath>
#include <cfloat>
#include <limits>
#include <algorithm>
#include <cassert>

namespace NPackedFloat {
    /*
 Exponent    Mantissa zero   Mantissa non-zero   Equation
 0x00        zero            denormal            (-1)^sign * 2^-126 * 0.mantissa
 0x01–0xfe       normalized value                (-1)^sign * 2^(exponent - 127) * 1.mantissa
 0xff        infinity        NaN
 * */

    //fast 16 bit floats by melkov
    template <ui8 SIGNED>
    struct float16 {
    private:
        typedef float16<SIGNED> self;

    public:
        ui16 val;

        explicit float16(ui16 v = 0)
            : val(v)
        {
        }

        self& operator=(float t) {
            assert(SIGNED == 1 || SIGNED == 0 && t >= 0.);
            val = BitCast<ui32>(t) >> (15 + SIGNED);
            return *this;
        }

        operator float() const {
            return BitCast<float>((ui32)val << (15 + SIGNED));
        }

        static self New(float v) {
            self f;
            return f = v;
        }

        static self denorm_min() {
            return self(0x0001);
        }

        static self min() {
            return self(SIGNED ? 0x0080 : 0x0100);
        }

        static self max() {
            return self(SIGNED ? 0x7f7f : 0xfeff);
        }
    };

    //fast 8 bit floats
    template <ui8 SIGNED, ui8 DENORM = 0>
    struct float8 {
    private:
        typedef float8<SIGNED, DENORM> self;
        enum {
            FMinExp = SIGNED ? 0x7c : 0x78,
            FMaxExp = SIGNED ? 0x83 : 0x87,
            MaxExp = SIGNED ? 0x70 : 0xf0,
        };

    public:
        ui8 val;

        explicit float8(ui8 v = 0)
            : val(v)
        {
        }

        self& operator=(float t) {
            assert(SIGNED == 1 || SIGNED == 0 && t >= 0.);
            ui16 hi16 = Hi16(t);

            ui8 sign = SIGNED ? Hi8(hi16) & 0x80 : 0;

            hi16 <<= 1;

            ui8 fexp = Hi8(hi16);
            ui8 exp;
            ui8 frac = (Lo8(hi16) & 0xf0) >> 4;

            if (fexp <= FMinExp) {
                exp = 0;
                frac = DENORM ? ((ui8)(0x10 | frac) >> std::min<int>((FMinExp - fexp + 1), 8)) : 0;
            } else if (fexp > FMaxExp) {
                exp = MaxExp;
                frac = 0x0f;
            } else {
                exp = (fexp - FMinExp) << 4;
            }

            val = sign | exp | frac;
            return *this;
        }

        operator float() const {
            ui32 v = 0;

            v |= SIGNED ? (val & 0x80) << 24 : 0;
            ui8 frac = val & 0x0f;
            ui8 exp = val & MaxExp;

            if (exp) {
                v |= ((exp >> 4) + FMinExp) << 23 | frac << 19;
            } else if (DENORM && val & 0x0f) {
                while (!(frac & 0x10)) {
                    frac <<= 1;
                    ++exp;
                }

                v |= (FMinExp - exp + 1) << 23 | (frac & 0x0f) << 19;
            } else
                v |= 0;

            return BitCast<float>(v);
        }

        static self New(float v) {
            self f;
            return f = v;
        }

        static self denorm_min() {
            return self(0x01);
        }

        static self min() {
            return self(0x10);
        }

        static self max() {
            return self(SIGNED ? 0x7f : 0xff);
        }
    };
}

using f64 = double;
using f32 = float;
static_assert(sizeof(f32) == 4, "expect sizeof(f32) == 4");
static_assert(sizeof(f64) == 8, "expect sizeof(f64) == 8");
using f16 = NPackedFloat::float16<1>;
using uf16 = NPackedFloat::float16<0>;
using f8 = NPackedFloat::float8<1>;
using uf8 = NPackedFloat::float8<0>;
using f8d = NPackedFloat::float8<1, 1>;
using uf8d = NPackedFloat::float8<0, 1>;

// [0,1) value in 1/255s.
using frac8 = ui8;

using frac16 = ui16;

template <class T>
inline constexpr T Float2Frac(float fac) {
    return T(fac * float(Max<T>()));
}

template <class T>
inline constexpr T Float2FracR(float fac) {
    float v = fac * float(Max<T>());
    return T(v + 0.5f);
}

template <class T>
inline constexpr float Frac2Float(T pf) {
    constexpr float multiplier = float(1.0 / Max<T>());
    return pf * multiplier;
}

class TUi82FloatMapping {
private:
    float Mapping[Max<ui8>() + 1] = {};

public:
    constexpr TUi82FloatMapping() noexcept {
        for (ui32 i = 0; i < Y_ARRAY_SIZE(Mapping); ++i) {
            Mapping[i] = static_cast<float>(i) / Max<ui8>();
        }
    }

    inline float operator [] (ui8 index) const {
        return Mapping[index];
    }
};

constexpr TUi82FloatMapping Ui82FloatMapping{};

template <>
inline float Frac2Float(ui8 pf) {
    return Ui82FloatMapping[pf];
}

// Probably you don't want to use it, since sizeof(float) == sizeof(ui32)
template <>
inline float Frac2Float(ui32 pf) = delete;

template <class T>
inline float FracOrFloatToFloat(T t) {
    return Frac2Float(t);
}

template <>
inline float FracOrFloatToFloat<float>(float t) {
    return t;
}
