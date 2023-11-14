#pragma once

#include <cstdint>
#include <immintrin.h>

#include <util/system/types.h>
#include <util/stream/output.h>
#include <util/generic/string.h>

#pragma clang attribute push(__attribute__((target("avx2"))), apply_to=function)
namespace NSimd {
namespace NAVX2 {

template<typename T>
struct TSimd8 {
    __m256i Value;

    static const int SIZE = 32;

    inline TSimd8()
        : Value{__m256i()} {
    }

    inline TSimd8(const __m256i& value)
        : Value(value) {
    }

    inline TSimd8(T value) {
        this->Value = _mm256_set1_epi8(value);
    }

    inline TSimd8(const T values[32]) {
        this->Value = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(values));
    };

    inline TSimd8(
        T v0,  T v1,  T v2,  T v3,  T v4,  T v5,  T v6,  T v7,
        T v8,  T v9,  T v10, T v11, T v12, T v13, T v14, T v15,
        T v16, T v17, T v18, T v19, T v20, T v21, T v22, T v23,
        T v24, T v25, T v26, T v27, T v28, T v29, T v30, T v31
    )  : TSimd8(_mm256_setr_epi8(
        v0, v1, v2, v3, v4, v5, v6, v7,
        v8, v9, v10,v11,v12,v13,v14,v15,
        v16,v17,v18,v19,v20,v21,v22,v23,
        v24,v25,v26,v27,v28,v29,v30,v31
    ))
    {
    }

    explicit inline operator const __m256i&() const {
        return this->Value;
    }
    explicit inline operator __m256i&() {
        return this->Value;
    }

    static inline ui32 CRC32u8(ui32 crc, ui32 data) {
        return _mm_crc32_u8(crc, data);
    }

    static inline ui32 CRC32u16(ui32 crc, ui32 data) {
        return _mm_crc32_u16(crc, data);
    }

    static inline ui32 CRC32u32(ui32 crc, ui32 data) {
        return _mm_crc32_u32(crc, data);
    }

    static inline ui64 CRC32u64(ui64 crc, ui64 data) {
        return _mm_crc32_u64(crc, data);
    }

    inline ui32 CRC32(ui32 crc) {
        crc = _mm_crc32_u64(crc, *((ui64*) &this->Value));
        crc = _mm_crc32_u64(crc, *((ui64*) &this->Value + 1));
        crc = _mm_crc32_u64(crc, *((ui64*) &this->Value + 2));
        crc = _mm_crc32_u64(crc, *((ui64*) &this->Value + 3));
        return crc;
    }

    inline int ToBitMask() const {
        return _mm256_movemask_epi8(this->Value);
    }

    inline bool Any() const {
        return !_mm256_testz_si256(this->Value, this->Value);
    }

    inline bool All() const {
        return this->ToBitMask() == i32(0xFFFFFFFF);
    }

    static inline TSimd8<T> Set(T value) {
        return TSimd8<T>(value);
    }

    static inline TSimd8<T> Zero() {
        return _mm256_setzero_si256();
    }

    static inline TSimd8<T> Load(const T values[32]) {
        return TSimd8<T>(values);
    }

    static inline TSimd8<T> LoadAligned(const T values[32]) {
        return _mm256_load_si256(reinterpret_cast<const __m256i *>(values));
    }

    static inline TSimd8<T> LoadStream(T dst[32]) {
        return _mm256_stream_load_si256(reinterpret_cast<__m256i *>(dst));
    }

    inline void Store(T dst[32]) const {
        return _mm256_storeu_si256(reinterpret_cast<__m256i *>(dst), this->Value);
    }

    inline void StoreAligned(T dst[32]) const {
        return _mm256_store_si256(reinterpret_cast<__m256i *>(dst), this->Value);
    }

    inline void StoreStream(T dst[32]) const {
        return _mm256_stream_si256(reinterpret_cast<__m256i *>(dst), this->Value);
    }

    inline TSimd8<T> Shuffle(const TSimd8<T>& other) const {
        TSimd8<T> mask0(0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70,
                        0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0);
        TSimd8<T> mask1(0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0,
                        0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70);
        TSimd8<T> perm = _mm256_permute4x64_epi64(this->Value, 0x4E);
        TSimd8<T> tmp = Shuffle128(other + mask0) | perm.Shuffle128(other + mask1);
        TSimd8<T> mask = _mm256_cmpgt_epi8(other.Value, _mm256_set1_epi8(-1));
        return tmp & mask;
    }

    inline TSimd8<T> Shuffle128(const TSimd8<T>& other) const {
        return _mm256_shuffle_epi8(this->Value, other.Value);
    }

    template<int N>
    inline TSimd8<T> Blend16(const TSimd8<T>& other) {
        return _mm256_blend_epi16(this->Value, other.Value, N);
    }

    template<int N>
    inline TSimd8<T> Blend32(const TSimd8<T>& other) {
        return _mm256_blend_epi32(this->Value, other.Value, N);
    }

    inline TSimd8<T> BlendVar(const TSimd8<T>& other, const TSimd8<T>& mask) {
        return _mm256_blendv_epi8(this->Value, other.Value, mask.Value);
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

    template<typename TOut>
    void Log(IOutputStream& out, TString delimeter = " ", TString end = "\n") {
        const size_t n = sizeof(this->Value) / sizeof(TOut);
        TOut buf[n];
        this->Store((T*) buf);
        if (n == sizeof(this->Value)) {
            for (size_t i = 0; i < n; i += 1) {
                out << int(buf[i]);
                if (i + 1 < n) {
                    out << delimeter;
                }
            }
        } else {
            for (size_t i = 0; i < n; i += 1) {
                out << buf[i];
                if (i + 1 < n) {
                    out << delimeter;
                }
            }
        }
        out << end;
    }

    inline TSimd8<T> MaxValue(const TSimd8<T>& other) const {
        return _mm256_max_epu8(this->Value, other.Value);
    }
    inline TSimd8<T> MinValue(const TSimd8<T>& other) const {
        return _mm256_min_epu8(this->Value, other.Value);
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
    inline bool BitsNotSetAnywhere(const TSimd8<T>& bits) const {
        return _mm256_testz_si256(this->Value, bits.Value);
    }
    inline bool AnyBitsSetAnywhere(const TSimd8<T>& bits) const {
        return !BitsNotSetAnywhere(bits);
    }

    template<int N>
    inline TSimd8<T> Shr() const {
        return TSimd8<T>(_mm256_srli_epi16(this->Value, N)) & T(0xFFu >> N);
    }
    template<int N>
    inline TSimd8<T> Shl() const {
        return TSimd8<T>(_mm256_slli_epi16(this->Value, N)) & T(0xFFu << N);
    }

    template<int N>
    inline int GetBit() const {
        return _mm256_movemask_epi8(_mm256_slli_epi16(this->Value, 7-N));
    }

    friend inline TSimd8<bool> operator==(const TSimd8<T> &lhs, const TSimd8<T> &rhs) {
        return _mm256_cmpeq_epi8(lhs.Value, rhs.Value);
    }

    inline TSimd8<bool> operator<=(const TSimd8<T> &other) {
        return other.MaxValue(*this) == other;
    }

    inline TSimd8<bool> operator>=(const TSimd8<T> &other) {
        return other.MinValue(*this) == other;
    }

    inline TSimd8<bool> operator>(const TSimd8<T> &other) {
        return _mm256_cmpgt_epi8(*this, other);
    }

    inline TSimd8<bool> operator<(const TSimd8<T> &other) {
        return _mm256_cmpgt_epi8(other, *this);
    }

    inline TSimd8<T> operator|(const TSimd8<T>& other) const {
        return _mm256_or_si256(this->Value, other.Value);
    }
    inline TSimd8<T> operator&(const TSimd8<T>& other) const {
        return _mm256_and_si256(this->Value, other.Value);
    }
    inline TSimd8<T> operator^(const TSimd8<T>& other) const {
        return _mm256_xor_si256(this->Value, other.Value);
    }
    inline TSimd8<T> BitAndNot(const TSimd8<T>& other) const {
        return _mm256_andnot_si256(this->Value, other.Value);
    };
    inline TSimd8<T>& operator|=(const TSimd8<T>& other) {
        *this = *this | other;
        return *this;
    }
    inline TSimd8<T>& operator&=(const TSimd8<T>& other) {
        *this = *this & other;
        return *this;
    };
    inline TSimd8<T>& operator^=(const TSimd8<T>& other) {
        *this = *this ^ other;
        return *this;
    };

    inline TSimd8<T> operator+(const TSimd8<T>& other) const {
        return _mm256_add_epi8(this->Value, other.Value);
    }
    inline TSimd8<T> operator-(const TSimd8<T>& other) const {
        return _mm256_sub_epi8(this->Value, other.Value);
    }
    inline TSimd8<T>& operator+=(const TSimd8<T>& other) {
        *this = *this + other;
        return *static_cast<TSimd8<T>*>(this);
    }
    inline TSimd8<T>& operator-=(const TSimd8<T>& other) {
        *this = *this - other;
        return *static_cast<TSimd8<T>*>(this);
    }

    // 0xFFu = 11111111 = 2^8 - 1
    inline TSimd8<T> operator~() const {
        return *this ^ 0xFFu;
    }
};

template<>
inline TSimd8<bool> TSimd8<bool>::Set(bool value) {
    return _mm256_set1_epi8(ui8(-value));
}

template<>
inline TSimd8<bool>::TSimd8(bool value) {
    this->Value = _mm256_set1_epi8(ui8(-value));
}

template<>
inline TSimd8<i8> TSimd8<i8>::MaxValue(const TSimd8<i8>& other) const {
    return _mm256_max_epi8(this->Value, other.Value);
}

template<>
inline TSimd8<i8> TSimd8<i8>::MinValue(const TSimd8<i8>& other) const {
    return _mm256_min_epi8(this->Value, other.Value);
}

template<>
inline TSimd8<bool> TSimd8<ui8>::operator>(const TSimd8<ui8> &other) {
    return TSimd8<ui8>(_mm256_subs_epu8(this->Value, other.Value)).AnyBitsSet();
}

template<>
inline TSimd8<bool> TSimd8<ui8>::operator<(const TSimd8<ui8> &other) {
    return TSimd8<ui8>(_mm256_subs_epu8(other.Value, this->Value)).AnyBitsSet();
}


}
}

#pragma clang attribute pop