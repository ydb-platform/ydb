#pragma once

#include <cstdint>
#include <immintrin.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/system/types.h>
#include <util/stream/output.h>
#include <util/generic/string.h>

namespace NSimd {
namespace NFallback {

template <typename T>
struct TSimd8;

template<typename Child>
struct TBase {
    ui64 Value;

    inline TBase()
        : Value{ui64()} {
    }

    inline TBase(const ui64 value)
        : Value(value) {
    }

    explicit inline operator const ui64&() const {
        return this->Value;
    }
    explicit inline operator ui64&() {
        return this->Value;
    }

    inline Child operator|(const Child other) const {
        return this->Value | other.Value;
    }
    inline Child operator&(const Child other) const {
        return this->Value & other.Value;
    }
    inline Child operator^(const Child other) const {
        return this->Value ^ other.Value;
    }
    inline Child BitAndNot(const Child other) const {
        return (~this->Value) & other.Value;
    };
    inline Child& operator|=(const Child other) {
        auto cast = static_cast<Child*>(*this);
        *cast = *cast | other;
        return *cast;
    }
    inline Child& operator&=(const Child other) {
        auto cast = static_cast<Child*>(*this);
        *cast = *cast & other;
        return *cast;
    };
    inline Child& operator^=(const Child other) {
        auto cast = static_cast<Child*>(*this);
        *cast = *cast ^ other;
        return *cast;
    };
};

template <typename T>
struct FallbackTrait {

    T Value;
    
    static const int SIZE = sizeof(T);

    inline FallbackTrait() : Value() {}

    inline FallbackTrait(const FallbackTrait& other) : Value(other.Value) {}

    inline FallbackTrait(const T* ptr) : Value(*ptr) {}

    inline FallbackTrait& operator=(const FallbackTrait& other) {
        if (&other == this) return *this;

        Value = other.Value;
        return *this;
    }

    inline FallbackTrait& operator+=(const FallbackTrait& other) {
        Value += other.Value;
        return *this;
    }

    inline FallbackTrait operator+(const FallbackTrait& other) {
        FallbackTrait ans;

        ans += other;
        
        return ans;
    }

    inline void Store(T* ptr) {
        *ptr = Value;
    }
};

template<typename T, typename Mask=TSimd8<bool>>
struct TBase8: TBase<TSimd8<T>> {

    inline TBase8()
        : TBase<TSimd8<T>>()
    {
    }

    inline TBase8(const ui64 _value)
        : TBase<TSimd8<T>>(_value)
    {
    }

    template<int N>
    inline TSimd8<T> Blend16(const TSimd8<T> other) {
        ui64 dst = 0;
        size_t j = (1 << 16) - 1;
        for (size_t i = 0; i < 4; i += 1, j <<= 16) {
            if (N & (1LL << i)) {
                dst |= other.Value & j;
            } else {
                dst |= this->Value & j;
            }
        }
        return TSimd8<T>(dst);
    }

    inline TSimd8<T> BlendVar(const TSimd8<T> other, const TSimd8<T> mask) {
        ui64 dst = 0;
        size_t j = (1 << 8) - 1;
        for (size_t i = 0; i < 8; i += 1, j <<= 8) {
            if (mask.Value & (1uLL << (i * 8 + 7))) {
                dst |= other.Value & j;
            } else {
                dst |= this->Value & j;
            }
        }
        return TSimd8<T>(dst);
    }

    static inline ui32 CRC32u8(ui32 crc, ui8 data) {
        return ~Crc32cExtend(~crc, (void*) &data, 1);
    }

    static inline ui32 CRC32u16(ui32 crc, ui16 data) {
        return ~Crc32cExtend(~crc, (void*) &data, 2);
    }

    static inline ui32 CRC32u32(ui32 crc, ui32 data) {
        return ~Crc32cExtend(~crc, (void*) &data, 4);
    }

    static inline ui64 CRC32u64(ui64 crc, ui64 data) {
        return ~Crc32cExtend(~crc, (void*) &data, 8);
    }

    friend inline Mask operator==(const TSimd8<T> lhs, const TSimd8<T> rhs) {
        return lhs.Value == rhs.Value;
    }

    static const int SIZE = sizeof(TBase<T>::Value);
};

template<>
struct TSimd8<bool>: TBase8<bool> {

    inline TSimd8<bool>()
        : TBase8()
    {
    }

    inline TSimd8<bool>(const ui64 value)
        : TBase8<bool>(value)
    {
    }

    inline TSimd8<bool>(bool value)
        : TBase8<bool>(Set(value))
    {
    }

    static inline TSimd8<bool> Set(bool value) {
        return ui64(-value);
    }

    inline int ToBitMask() const {
        int result = 0;
        for (size_t j = 0; j < 8; j += 1) {
            if ((1ULL << (j * 8 + 7)) & this->Value) {
                result |= (1 << j);
            }
        }
        return result;
    }

    inline bool Any() const {
        return Value != 0;
    }

    inline bool All() const {
        return this->Value == ui64(-1);
    }

    inline TSimd8<bool> operator~() const {
        return *this ^ true;
    }
};

template<typename T>
struct TBase8Numeric: TBase8<T> {

    inline TBase8Numeric()
        : TBase8<T>()
    {
    }
    inline TBase8Numeric(const ui64 value)
        : TBase8<T>(value)
    {
    }

    static inline TSimd8<T> Set(T value) {
        TSimd8<T> result = TSimd8<T>::Zero();
        auto dst = (ui8*)(&result.Value);
        dst[0] = dst[1] = dst[2] = dst[3] = value;
        dst[4] = dst[5] = dst[6] = dst[7] = value;
        return result;
    }
    static inline TSimd8<T> Zero() {
        return (ui64) 0;
    }
    static inline TSimd8<T> Load(const T values[8]) {
        return TSimd8<T>(*((const ui64*) values));
    }

    static inline TSimd8<T> LoadAligned(const T values[8]) {
        return Load(values);
    }

    static inline TSimd8<T> LoadStream(const T values[8]) {
        return Load(values);
    }

    inline void Store(T dst[8]) const {
        *((ui64*) dst) = this->Value;
    }

    inline void StoreAligned(T dst[8]) const {
        Store(dst);
    }

    inline void StoreStream(T dst[8]) const {
        Store(dst);
    }

    inline TSimd8<T> Shuffle(const TSimd8<T> other) const {
        TSimd8<T> dst(T(0));
        ui64 mask_byte = 255;
        for (size_t i = 0; i < 8; i += 1) {
            size_t j = i * 8;
            ui64 mask = 15;
            if (!((1ULL << (j + 7)) & other.Value)) {
                ui64 index = (other.Value >> j) & mask;
                dst.Value |= ((this->Value & (mask_byte << (index * 8))) >> index * 8) << i * 8;
            }
        }
        return dst;
    }

    inline TSimd8<T> Shuffle128(const TSimd8<T> other) const {
        return Shuffle(other);
    }

    template<typename TOut>
    void Log(IOutputStream& out, TString delimeter = " ", TString end = "\n") {
        const size_t n = sizeof(this->Value) / sizeof(TOut);
        TOut buf[n];
        Store((T*) buf);
        if (n == sizeof(this->Value)) {
            for (size_t i = 0; i < n; i += 1) {
                out << int(buf[i]);
                if (i + 1 < n) {
                    out << delimeter;
                } else {
                    out << end;
                }
            }
        } else {
            for (size_t i = 0; i < n; i += 1) {
                out << buf[i];
                if (i + 1 < n) {
                    out << delimeter;
                } else {
                    out << end;
                }
            }
        }
    }

    inline TSimd8<T> operator+(const TSimd8<T> other) const {
        return this->Value + other.Value;
    }
    inline TSimd8<T> operator-(const TSimd8<T> other) const {
        return this->Value - other.Value;
    }
    inline TSimd8<T>& operator+=(const TSimd8<T> other) {
        *this = *this + other;
        return *static_cast<TSimd8<T>*>(this);
    }
    inline TSimd8<T>& operator-=(const TSimd8<T> other) {
        *this = *this - other;
        return *static_cast<TSimd8<T>*>(this);
    }

    // 0xFFu = 11111111 = 2^8 - 1
    inline TSimd8<T> operator~() const {
        return *this ^ ui8(0xFFu);
    }
};

template<>
struct TSimd8<i8> : TBase8Numeric<i8> {
    inline TSimd8()
        : TBase8Numeric<i8>()
    {
    }
    inline TSimd8(const ui64 value)
        : TBase8Numeric<i8>(value)
    {
    }
    inline TSimd8(i8 value)
        : TSimd8(Set(value))
    {
    }
    inline TSimd8(const i8 values[8])
        : TSimd8(Load(values))
    {
    }
    inline TSimd8(i8 v0,  i8 v1,  i8 v2,  i8 v3,  i8 v4,  i8 v5,  i8 v6,  i8 v7) {
        this->Value = ui8(v7);
        this->Value = (this->Value << 8) | ui8(v6);
        this->Value = (this->Value << 8) | ui8(v5);
        this->Value = (this->Value << 8) | ui8(v4);
        this->Value = (this->Value << 8) | ui8(v3);
        this->Value = (this->Value << 8) | ui8(v2);
        this->Value = (this->Value << 8) | ui8(v1);
        this->Value = (this->Value << 8) | ui8(v0);
    }

    inline TSimd8<i8> MaxValue(const TSimd8<i8> other) const {
        return (*this > other).Any() ? *this : other;
    }
    inline TSimd8<i8> MinValue(const TSimd8<i8> other) const {
        return (*this < other).Any() ? *this : other;
    }

    inline TSimd8<bool> operator>(const TSimd8<i8> other) const {
        return i64(this->Value) > i64(other.Value);
    }
    inline TSimd8<bool> operator<(const TSimd8<i8> other) const {
        return i64(this->Value) < i64(other.Value);
    }
};

template<>
struct TSimd8<ui8>: TBase8Numeric<ui8> {
    inline TSimd8()
        : TBase8Numeric<ui8>()
    {
    }
    inline TSimd8(const ui64 _value)
        : TBase8Numeric<ui8>(_value)
    {
    }
    inline TSimd8(ui8 _value)
        : TSimd8(Set(_value))
    {
    }
    inline TSimd8(const ui8 values[8])
        : TSimd8(Load(values))
    {
    }
    inline TSimd8(ui8 v0,  ui8 v1,  ui8 v2,  ui8 v3,  ui8 v4,  ui8 v5,  ui8 v6,  ui8 v7) {
        this->Value = v7;
        this->Value = (this->Value << 8) | v6;
        this->Value = (this->Value << 8) | v5;
        this->Value = (this->Value << 8) | v4;
        this->Value = (this->Value << 8) | v3;
        this->Value = (this->Value << 8) | v2;
        this->Value = (this->Value << 8) | v1;
        this->Value = (this->Value << 8) | v0;
    }

    inline TSimd8<ui8> MaxValue(const TSimd8<ui8> other) const {
        return this->Value > other.Value ? *this : other;
    }
    inline TSimd8<ui8> MinValue(const TSimd8<ui8> other) const {
        return this->Value < other.Value ? *this : other;
    }

    inline TSimd8<bool> operator<=(const TSimd8<ui8> other) const {
        return other.MaxValue(*this) == other;
    }
    inline TSimd8<bool> operator>=(const TSimd8<ui8> other) const {
        return other.MinValue(*this) == other;
    }

    inline TSimd8<bool> BitsNotSet() const {
        return this->Value == 0;
    }
    inline TSimd8<bool> AnyBitsSet() const {
        return ~this->BitsNotSet();
    }
    inline bool BitsNotSetAnywhere() const {
        return BitsNotSet().Any();
    }
    inline bool AnyBitsSetAnywhere() const {
        return !BitsNotSetAnywhere();
    }
    inline bool BitsNotSetAnywhere(TSimd8<ui8> bits) const {
        return ((*this) & bits).Value == 0;
    }
    inline bool AnyBitsSetAnywhere(TSimd8<ui8> bits) const {
        return !BitsNotSetAnywhere(bits);
    }

    template<int N>
    inline TSimd8<ui8> Shr() const {
        return (this->Value >> N) & ui8(0xFFu >> N);
    }
    template<int N>
    inline TSimd8<ui8> Shl() const {
        return (this->Value << N) & ui8(0xFFu << N);
    }

    template<int N>
    inline int GetBit() const {
        int result = 0;
        for (size_t i = 0; i < 8; i += 1) {
            result |= ((this->Value >> (i * 8 + N)) & 1) << i;
        }
        return result;
    }
};

}
}
