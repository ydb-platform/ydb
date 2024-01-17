#pragma once

#include <cstdint>
#include <immintrin.h>

#include <util/system/types.h>
#include <util/stream/output.h>
#include <util/generic/string.h>

#pragma clang attribute push(__attribute__((target("sse4.2"))), apply_to=function)
namespace NSimd {
namespace NSSE42 {

template<typename T>
struct TSimd8 {
    __m128i Value;

    static const int SIZE = 32;

    inline TSimd8()
        : Value{__m128i()} {
    }

    inline TSimd8(const __m128i& value)
        : Value(value) {
    }

    inline TSimd8(T value) {
        this->Value = _mm_set1_epi8(value);
    }

    inline TSimd8(const T values[16]) {
        this->Value = _mm_loadu_si128(reinterpret_cast<const __m128i *>(values));
    };

    inline TSimd8(
        T v0,  T v1,  T v2,  T v3,  T v4,  T v5,  T v6,  T v7,
        T v8,  T v9,  T v10, T v11, T v12, T v13, T v14, T v15
    )  : TSimd8(_mm_setr_epi8(
        v0, v1, v2, v3, v4, v5, v6, v7,
        v8, v9, v10,v11,v12,v13,v14,v15
    ))
    {
    }

    explicit inline operator const __m128i&() const {
        return this->Value;
    }
    explicit inline operator __m128i&() {
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
        return _mm_movemask_epi8(this->Value);
    }

    inline bool Any() const {
        return !_mm_testz_si128(this->Value, this->Value);
    }

    inline bool All() const {
        return this->ToBitMask() == i32(0xFFFF);
    }

    static inline TSimd8<T> Set(T value) {
        return TSimd8<T>(value);
    }

    static inline TSimd8<T> Zero() {
        return _mm_setzero_si128();
    }

    static inline TSimd8<T> Load(const T values[16]) {
        return TSimd8<T>(values);
    }

    static inline TSimd8<T> Load128(const T values[16]) {
        return Load(values);
    }

    static inline TSimd8<T> LoadAligned(const T values[16]) {
        return _mm_load_si128(reinterpret_cast<const __m128i *>(values));
    }

    static inline TSimd8<T> LoadStream(T dst[16]) {
        return _mm_stream_load_si128(reinterpret_cast<__m128i *>(dst));
    }

    inline void Store(T dst[16]) const {
        return _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), this->Value);
    }

    inline void StoreAligned(T dst[16]) const {
        return _mm_store_si128(reinterpret_cast<__m128i *>(dst), this->Value);
    }

    inline void StoreStream(T dst[16]) const {
        return _mm_stream_si128(reinterpret_cast<__m128i *>(dst), this->Value);
    }

    inline void StoreMasked(void* dst, const TSimd8<T>& mask) const {
        _mm_maskmoveu_si128(this->Value, mask.Value, dst);
    }

    template<bool CanBeNegative = true>
    inline TSimd8<T> Shuffle(const TSimd8<T>& other) const {
        return Shuffle128(other);
    }


    inline TSimd8<T> Shuffle128(const TSimd8<T>& other) const {
        return _mm_shuffle_epi8(this->Value, other.Value);
    }

    template<int N>
    inline TSimd8<T> Blend16(const TSimd8<T>& other) const {
        return _mm_blend_epi16(this->Value, other.Value, N);
    }

    template<int N>
    inline TSimd8<T> Blend32(const TSimd8<T>& other) const {
        return _mm_blend_epi32(this->Value, other.Value, N);
    }

    inline TSimd8<T> BlendVar(const TSimd8<T>& other, const TSimd8<T>& mask) const {
        return _mm_blendv_epi8(this->Value, other.Value, mask.Value);
    }

    template<int N>
    inline TSimd8<T> ByteShift128() const {
        if constexpr (N < 0) {
            return _mm_bsrli_si128(this->Value, -N);
        } else {
            return _mm_bslli_si128(this->Value, N);
        }
    }

    template<int N>
    inline TSimd8<T> ByteShift() const {
        return ByteShift128<N>();
    }

    template<int N>
    inline TSimd8<T> ByteShiftWithCarry(const TSimd8<T>& other) const {
        return Rotate<N>().BlendVar(other.Rotate<N>(), ~TSimd8<T>(T(-1)).ByteShift<N>());
    }

    template<int N>
    inline TSimd8<T> Rotate128() const {
        if constexpr (N % 16 == 0) {
            return *this;
        } else {
            constexpr T A0 = (16 - N) % 16;
            constexpr T A1 = (16 - N + 1) % 16;
            constexpr T A2 = (16 - N + 2) % 16;
            constexpr T A3 = (16 - N + 3) % 16;
            constexpr T A4 = (16 - N + 4) % 16;
            constexpr T A5 = (16 - N + 5) % 16;
            constexpr T A6 = (16 - N + 6) % 16;
            constexpr T A7 = (16 - N + 7) % 16;
            constexpr T A8 = (16 - N + 8) % 16;
            constexpr T A9 = (16 - N + 9) % 16;
            constexpr T A10 = (16 - N + 10) % 16;
            constexpr T A11 = (16 - N + 11) % 16;
            constexpr T A12 = (16 - N + 12) % 16;
            constexpr T A13 = (16 - N + 13) % 16;
            constexpr T A14 = (16 - N + 14) % 16;
            constexpr T A15 = (16 - N + 15) % 16;
            return Shuffle128(TSimd8<T>(A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15));
        }
    }

    template<int N>
    inline TSimd8<T> Rotate() const {
        return Rotate128<N>();
    }

    static inline TSimd8<T> Repeat16(
        T v0,  T v1,  T v2,  T v3,  T v4,  T v5,  T v6,  T v7,
        T v8,  T v9,  T v10, T v11, T v12, T v13, T v14, T v15
    ) {
        return TSimd8<T>(
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
        return _mm_max_epu8(this->Value, other.Value);
    }
    inline TSimd8<T> MinValue(const TSimd8<T>& other) const {
        return _mm_min_epu8(this->Value, other.Value);
    }

    inline TSimd8<bool> BitsNotSet() const {
        return *this == ui8(0);
    }
    inline TSimd8<bool> AnyBitsSet() const {
        return ~this->BitsNotSet();
    }
    inline bool BitsNotSetAnywhere() const {
        return _mm_testz_si128(this->Value, this->Value);
    }
    inline bool AnyBitsSetAnywhere() const {
        return !BitsNotSetAnywhere();
    }
    inline bool BitsNotSetAnywhere(const TSimd8<T>& bits) const {
        return _mm_testz_si128(this->Value, bits.Value);
    }
    inline bool AnyBitsSetAnywhere(const TSimd8<T>& bits) const {
        return !BitsNotSetAnywhere(bits);
    }

    template<int N>
    inline TSimd8<T> Shr() const {
        return TSimd8<T>(_mm_srli_epi16(this->Value, N)) & T(0xFFu >> N);
    }
    template<int N>
    inline TSimd8<T> Shl() const {
        return TSimd8<T>(_mm_slli_epi16(this->Value, N)) & T(0xFFu << N);
    }

    template<int N>
    inline int GetBit() const {
        return _mm_movemask_epi8(_mm_slli_epi16(this->Value, 7-N));
    }

    friend inline TSimd8<bool> operator==(const TSimd8<T> &lhs, const TSimd8<T> &rhs) {
        return _mm_cmpeq_epi8(lhs.Value, rhs.Value);
    }

    inline TSimd8<bool> operator<=(const TSimd8<T> &other) {
        return other.MaxValue(*this) == other;
    }

    inline TSimd8<bool> operator>=(const TSimd8<T> &other) {
        return other.MinValue(*this) == other;
    }

    inline TSimd8<bool> operator>(const TSimd8<T> &other) {
        return _mm_cmpgt_epi8(*this, other);
    }

    inline TSimd8<bool> operator<(const TSimd8<T> &other) {
        return _mm_cmpgt_epi8(other, *this);
    }

    inline TSimd8<T> operator|(const TSimd8<T>& other) const {
        return _mm_or_si128(this->Value, other.Value);
    }
    inline TSimd8<T> operator&(const TSimd8<T>& other) const {
        return _mm_and_si128(this->Value, other.Value);
    }
    inline TSimd8<T> operator^(const TSimd8<T>& other) const {
        return _mm_xor_si128(this->Value, other.Value);
    }
    inline TSimd8<T> BitAndNot(const TSimd8<T>& other) const {
        return _mm_andnot_si128(this->Value, other.Value);
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
        return _mm_add_epi8(this->Value, other.Value);
    }
    inline TSimd8<T> operator-(const TSimd8<T>& other) const {
        return _mm_sub_epi8(this->Value, other.Value);
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
    return _mm_set1_epi8(ui8(-value));
}

template<>
inline TSimd8<bool>::TSimd8(bool value) {
    this->Value = _mm_set1_epi8(ui8(-value));
}

template<>
inline TSimd8<i8> TSimd8<i8>::MaxValue(const TSimd8<i8>& other) const {
    return _mm_max_epi8(this->Value, other.Value);
}

template<>
inline TSimd8<i8> TSimd8<i8>::MinValue(const TSimd8<i8>& other) const {
    return _mm_min_epi8(this->Value, other.Value);
}

template<>
inline TSimd8<bool> TSimd8<ui8>::operator>(const TSimd8<ui8> &other) {
    return TSimd8<ui8>(_mm_subs_epu8(this->Value, other.Value)).AnyBitsSet();
}

template<>
inline TSimd8<bool> TSimd8<ui8>::operator<(const TSimd8<ui8> &other) {
    return TSimd8<ui8>(_mm_subs_epu8(other.Value, this->Value)).AnyBitsSet();
}


}
}

#pragma clang attribute pop