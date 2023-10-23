#pragma once

#include <cstdint>
#include <immintrin.h>


#pragma clang attribute push(__attribute__((target("avx2"))), apply_to=function)

namespace NSimd {
namespace NAVX2 {

template <typename T>
struct TSimd8;

template<typename Child>
struct TBase {
    __m256i Value;

    inline TBase()
        : Value{__m256i()} {
    }

    inline TBase(const __m256i value)
        : Value(value) {
    }

    explicit inline operator const __m256i&() const {
        return this->Value;
    }
    explicit inline operator __m256i&() {
        return this->Value;
    }

    inline Child operator|(const Child other) const {
        return _mm256_or_si256(this->Value, other.Value);
    }
    inline Child operator&(const Child other) const {
        return _mm256_and_si256(this->Value, other.Value);
    }
    inline Child operator^(const Child other) const {
        return _mm256_xor_si256(this->Value, other.Value);
    }
    inline Child BitAndNot(const Child other) const {
        return _mm256_andnot_si256(this->Value, other.Value);
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

template<typename T, typename Mask=TSimd8<bool>>
struct TBase8: TBase<TSimd8<T>> {

    inline TBase8()
        : TBase<TSimd8<T>>()
    {
    }
    
    inline TBase8(const __m256i _value)
        : TBase<TSimd8<T>>(_value)
    {
    }

    friend inline Mask operator==(const TSimd8<T> lhs, const TSimd8<T> rhs) {
        return _mm256_cmpeq_epi8(lhs.Value, rhs.Value);
    }

    static const int SIZE = sizeof(TBase<T>::Value);
};

template<>
struct TSimd8<bool>: TBase8<bool> {

    inline TSimd8<bool>()
        : TBase8()
    {
    }
    
    inline TSimd8<bool>(const __m256i value)
        : TBase8<bool>(value)
    {
    }
    
    inline TSimd8<bool>(bool value)
        : TBase8<bool>(Set(value))
    {
    }

    static inline TSimd8<bool> Set(bool value) {
        return _mm256_set1_epi8(ui8(-(!!value)));
    }

    inline bool Any() const {
        return !_mm256_testz_si256(this->Value, this->Value);
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
    inline TBase8Numeric(const __m256i value)
        : TBase8<T>(value)
    {
    }

    static inline TSimd8<T> Set(T value) {
        return _mm256_set1_epi8(value);
    }
    static inline TSimd8<T> Zero() {
        return _mm256_setzero_si256();
    }
    static inline TSimd8<T> Load(const T values[32]) {
        return _mm256_loadu_si256(reinterpret_cast<const __m256i *>(values));
    }

    static inline TSimd8<T> Repeat16(
        T v0,  T v1,  T v2,  T v3,  T v4,  T v5,  T v6,  T v7,
        T v8,  T v9,  T v10, T v11, T v12, T v13, T v14, T v15
    ) {
        return TSimd8<T>(
            v0, v1, v2, v3, v4, v5, v6, v7,
            v8, v9, v10,v11,v12,v13,v14,v15,
            v0, v1, v2, v3, v4, v5, v6, v7,
            v8, v9, v10,v11,v12,v13,v14,v15
        );
    }

    inline void Store(T dst[32]) const {
        return _mm256_storeu_si256(reinterpret_cast<__m256i *>(dst), this->Value);
    }

    inline TSimd8<T> operator+(const TSimd8<T> other) const {
        return _mm256_add_epi8(this->Value, other.Value);
    }
    inline TSimd8<T> operator-(const TSimd8<T> other) const {
        return _mm256_sub_epi8(this->Value, other.Value);
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
        return *this ^ 0xFFu;
    }
};

template<>
struct TSimd8<i8> : TBase8Numeric<i8> {
    inline TSimd8()
        : TBase8Numeric<i8>()
    {    
    }    
    inline TSimd8(const __m256i value)
        : TBase8Numeric<i8>(value)
    {
    }
    inline TSimd8(i8 value)
        : TSimd8(Set(value))
    {
    }
    inline TSimd8(const i8 values[32])
        : TSimd8(Load(values))
    {
    }
    inline TSimd8(
        i8 v0,  i8 v1,  i8 v2,  i8 v3,  i8 v4,  i8 v5,  i8 v6,  i8 v7,
        i8 v8,  i8 v9,  i8 v10, i8 v11, i8 v12, i8 v13, i8 v14, i8 v15,
        i8 v16, i8 v17, i8 v18, i8 v19, i8 v20, i8 v21, i8 v22, i8 v23,
        i8 v24, i8 v25, i8 v26, i8 v27, i8 v28, i8 v29, i8 v30, i8 v31
    ) : TSimd8(_mm256_setr_epi8(
        v0, v1, v2, v3, v4, v5, v6, v7,
        v8, v9, v10,v11,v12,v13,v14,v15,
        v16,v17,v18,v19,v20,v21,v22,v23,
        v24,v25,v26,v27,v28,v29,v30,v31
    ))
    {
    }
    
    inline static TSimd8<i8> Repeat16(
        i8 v0,  i8 v1,  i8 v2,  i8 v3,  i8 v4,  i8 v5,  i8 v6,  i8 v7,
        i8 v8,  i8 v9,  i8 v10, i8 v11, i8 v12, i8 v13, i8 v14, i8 v15
    ) {
        return TSimd8<i8>(
            v0, v1, v2, v3, v4, v5, v6, v7,
            v8, v9, v10,v11,v12,v13,v14,v15,
            v0, v1, v2, v3, v4, v5, v6, v7,
            v8, v9, v10,v11,v12,v13,v14,v15
        );
    }

    inline TSimd8<i8> MaxValue(const TSimd8<i8> other) const {
        return _mm256_max_epi8(this->Value, other.Value);
    }
    inline TSimd8<i8> MinValue(const TSimd8<i8> other) const {
        return _mm256_min_epi8(this->Value, other.Value);
    }
    inline TSimd8<bool> operator>(const TSimd8<i8> other) const {
        return _mm256_cmpgt_epi8(this->Value, other.Value);
    }
    inline TSimd8<bool> operator<(const TSimd8<i8> other) const {
        return _mm256_cmpgt_epi8(other.Value, this->Value);
    }
};

template<>
struct TSimd8<ui8>: TBase8Numeric<ui8> {
    inline TSimd8()
        : TBase8Numeric<ui8>()
    {
    }
    inline TSimd8(const __m256i _value)
        : TBase8Numeric<ui8>(_value) 
    {
    }
    inline TSimd8(ui8 _value)
        : TSimd8(Set(_value))
    {
    }
    inline TSimd8(const ui8 values[32])
        : TSimd8(Load(values))
    {
    }
    inline TSimd8(
        ui8 v0,  ui8 v1,  ui8 v2,  ui8 v3,  ui8 v4,  ui8 v5,  ui8 v6,  ui8 v7,
        ui8 v8,  ui8 v9,  ui8 v10, ui8 v11, ui8 v12, ui8 v13, ui8 v14, ui8 v15,
        ui8 v16, ui8 v17, ui8 v18, ui8 v19, ui8 v20, ui8 v21, ui8 v22, ui8 v23,
        ui8 v24, ui8 v25, ui8 v26, ui8 v27, ui8 v28, ui8 v29, ui8 v30, ui8 v31
    ) : TSimd8(_mm256_setr_epi8(
        v0, v1, v2, v3, v4, v5, v6, v7,
        v8, v9, v10,v11,v12,v13,v14,v15,
        v16,v17,v18,v19,v20,v21,v22,v23,
        v24,v25,v26,v27,v28,v29,v30,v31
    )) {}

    inline static TSimd8<ui8> Repeat16(
        ui8 v0,  ui8 v1,  ui8 v2,  ui8 v3,  ui8 v4,  ui8 v5,  ui8 v6,  ui8 v7,
        ui8 v8,  ui8 v9,  ui8 v10, ui8 v11, ui8 v12, ui8 v13, ui8 v14, ui8 v15
    ) {
        return TSimd8<ui8>(
            v0, v1, v2, v3, v4, v5, v6, v7,
            v8, v9, v10,v11,v12,v13,v14,v15,
            v0, v1, v2, v3, v4, v5, v6, v7,
            v8, v9, v10,v11,v12,v13,v14,v15
        );
    }
    
    inline TSimd8<ui8> MaxValue(const TSimd8<ui8> other) const {
        return _mm256_max_epu8(this->Value, other.Value);
    }
    inline TSimd8<ui8> MinValue(const TSimd8<ui8> other) const {
        return _mm256_min_epu8(other.Value, this->Value);
    }
    inline TSimd8<bool> operator<=(const TSimd8<ui8> other) const {
        return other.MaxValue(*this) == other;
    }
    inline TSimd8<bool> operator>=(const TSimd8<ui8> other) const {
        return other.MinValue(*this) == other;
    }

    inline TSimd8<bool> BitsNotSet() const {
        return *this == ui8(0);
    }
    inline TSimd8<bool> AnyBitsSet() const {
        return ~this->BitsNotSet();
    }
    inline bool BitsNotSetAnywhere() const {
        return _mm256_testz_si256(this->Value, this->Value);
    }
    inline bool AnyBitsSetAnywhere() const {
        return !BitsNotSetAnywhere();
    }
    inline bool BitsNotSetAnywhere(TSimd8<ui8> bits) const {
        return _mm256_testz_si256(this->Value, bits.Value);
    }
    inline bool AnyBitsSetAnywhere(TSimd8<ui8> bits) const {
        return !BitsNotSetAnywhere(bits);
    }
    
    template<int N>
    inline TSimd8<ui8> Shr() const {
        return TSimd8<ui8>(_mm256_srli_epi16(this->Value, N)) & ui8(0xFFu >> N);
    }
    template<int N>
    inline TSimd8<ui8> Shl() const {
        return TSimd8<ui8>(_mm256_slli_epi16(this->Value, N)) & ui8(0xFFu << N);
    }
    
    template<int N>
    inline int GetBit() const {
        return _mm256_movemask_epi8(_mm256_slli_epi16(this->Value, 7-N));
    }
};

}
}

#pragma clang attribute pop