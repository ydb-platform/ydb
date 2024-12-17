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

    Y_FORCE_INLINE TSimd8()
        : Value{__m256i()} {
    }

    Y_FORCE_INLINE TSimd8(const __m256i& value)
        : Value(value) {
    }

    Y_FORCE_INLINE TSimd8(T value) {
        this->Value = _mm256_set1_epi8(value);
    }

    Y_FORCE_INLINE TSimd8(const T values[32]) {
        this->Value = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(values));
    };

    Y_FORCE_INLINE TSimd8(
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

    explicit Y_FORCE_INLINE operator const __m256i&() const {
        return this->Value;
    }
    explicit Y_FORCE_INLINE operator __m256i&() {
        return this->Value;
    }

    static Y_FORCE_INLINE ui32 CRC32u8(ui32 crc, ui32 data) {
        return _mm_crc32_u8(crc, data);
    }

    static Y_FORCE_INLINE ui32 CRC32u16(ui32 crc, ui32 data) {
        return _mm_crc32_u16(crc, data);
    }

    static Y_FORCE_INLINE ui32 CRC32u32(ui32 crc, ui32 data) {
        return _mm_crc32_u32(crc, data);
    }

    static Y_FORCE_INLINE ui64 CRC32u64(ui64 crc, ui64 data) {
        return _mm_crc32_u64(crc, data);
    }

    Y_FORCE_INLINE ui32 CRC32(ui32 crc) {
        crc = _mm_crc32_u64(crc, *((ui64*) &this->Value));
        crc = _mm_crc32_u64(crc, *((ui64*) &this->Value + 1));
        crc = _mm_crc32_u64(crc, *((ui64*) &this->Value + 2));
        crc = _mm_crc32_u64(crc, *((ui64*) &this->Value + 3));
        return crc;
    }
    
    Y_FORCE_INLINE void Add64(const TSimd8<T>& another) {
        Value = _mm256_add_epi64(Value, another.Value);
    }

    Y_FORCE_INLINE int ToBitMask() const {
        return _mm256_movemask_epi8(this->Value);
    }

    Y_FORCE_INLINE bool Any() const {
        return !_mm256_testz_si256(this->Value, this->Value);
    }

    Y_FORCE_INLINE bool All() const {
        return this->ToBitMask() == i32(0xFFFFFFFF);
    }

    static Y_FORCE_INLINE TSimd8<T> Set(T value) {
        return TSimd8<T>(value);
    }

    static Y_FORCE_INLINE TSimd8<T> Zero() {
        return _mm256_setzero_si256();
    }

    static Y_FORCE_INLINE TSimd8<T> Load(const T values[32]) {
        return TSimd8<T>(values);
    }

    static Y_FORCE_INLINE TSimd8<T> LoadAligned(const T values[32]) {
        return _mm256_load_si256(reinterpret_cast<const __m256i *>(values));
    }

    static Y_FORCE_INLINE TSimd8<T> LoadStream(const T values[32]) {
        return _mm256_stream_load_si256(reinterpret_cast<__m256i *>(values));
    }

    Y_FORCE_INLINE void Store(T dst[32]) const {
        return _mm256_storeu_si256(reinterpret_cast<__m256i *>(dst), this->Value);
    }

    Y_FORCE_INLINE void StoreAligned(T dst[32]) const {
        return _mm256_store_si256(reinterpret_cast<__m256i *>(dst), this->Value);
    }

    Y_FORCE_INLINE void StoreStream(T dst[32]) const {
        return _mm256_stream_si256(reinterpret_cast<__m256i *>(dst), this->Value);
    }

    Y_FORCE_INLINE void StoreMasked(void* dst, const TSimd8<T>& mask) const {
        _mm_maskmoveu_si128(_mm256_castsi256_si128(this->Value), _mm256_castsi256_si128(mask.Value), dst);
        _mm_maskmoveu_si128(_mm256_extracti128_si256(this->Value, 1), _mm256_extracti128_si256(mask.Value, 1), (char*) dst);
    }

    template<bool CanBeNegative = true>
    Y_FORCE_INLINE TSimd8<T> Shuffle(const TSimd8<T>& other) const {
        const TSimd8<T> mask0(0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70,
                              0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0);
        const TSimd8<T> mask1(0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0,
                              0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70);
        TSimd8<T> perm = _mm256_permute4x64_epi64(this->Value, 0x4E);
        if constexpr (CanBeNegative) {
            TSimd8<T> tmp = Shuffle128(other + mask0) | perm.Shuffle128(other + mask1);
            TSimd8<T> mask = _mm256_cmpgt_epi8(other.Value, _mm256_set1_epi8(-1));
            return tmp & mask;
        } else {
            return Shuffle128(other + mask0) | perm.Shuffle128(other + mask1);
        }
    }

    Y_FORCE_INLINE TSimd8<T> Shuffle128(const TSimd8<T>& other) const {
        return _mm256_shuffle_epi8(this->Value, other.Value);
    }

    template<int N>
    Y_FORCE_INLINE TSimd8<T> Blend16(const TSimd8<T>& other) const {
        return _mm256_blend_epi16(this->Value, other.Value, N);
    }

    template<int N>
    Y_FORCE_INLINE TSimd8<T> Blend32(const TSimd8<T>& other) const {
        return _mm256_blend_epi32(this->Value, other.Value, N);
    }

    Y_FORCE_INLINE TSimd8<T> BlendVar(const TSimd8<T>& other, const TSimd8<T>& mask) const {
        return _mm256_blendv_epi8(this->Value, other.Value, mask.Value);
    }

    template<int N>
    Y_FORCE_INLINE TSimd8<T> ByteShift128() const {
        if constexpr (N < 0) {
            return _mm256_bsrli_epi128(this->Value, -N);
        } else {
            return _mm256_bslli_epi128(this->Value, N);
        }
    }

    template<int N>
    Y_FORCE_INLINE TSimd8<T> ByteShift() const {
        constexpr T A0 = N > 0 ? -(0 < N) : -(0 >= 32 + N);
        constexpr T A1 = N > 0 ? -(1 < N) : -(1 >= 32 + N);
        constexpr T A2 = N > 0 ? -(2 < N) : -(2 >= 32 + N);
        constexpr T A3 = N > 0 ? -(3 < N) : -(3 >= 32 + N);
        constexpr T A4 = N > 0 ? -(4 < N) : -(4 >= 32 + N);
        constexpr T A5 = N > 0 ? -(5 < N) : -(5 >= 32 + N);
        constexpr T A6 = N > 0 ? -(6 < N) : -(6 >= 32 + N);
        constexpr T A7 = N > 0 ? -(7 < N) : -(7 >= 32 + N);
        constexpr T A8 = N > 0 ? -(8 < N) : -(8 >= 32 + N);
        constexpr T A9 = N > 0 ? -(9 < N) : -(9 >= 32 + N);
        constexpr T A10 = N > 0 ? -(10 < N) : -(10 >= 32 + N);
        constexpr T A11 = N > 0 ? -(11 < N) : -(11 >= 32 + N);
        constexpr T A12 = N > 0 ? -(12 < N) : -(12 >= 32 + N);
        constexpr T A13 = N > 0 ? -(13 < N) : -(13 >= 32 + N);
        constexpr T A14 = N > 0 ? -(14 < N) : -(14 >= 32 + N);
        constexpr T A15 = N > 0 ? -(15 < N) : -(15 >= 32 + N);
        constexpr T A16 = N > 0 ? -(16 < N) : -(16 >= 32 + N);
        constexpr T A17 = N > 0 ? -(17 < N) : -(17 >= 32 + N);
        constexpr T A18 = N > 0 ? -(18 < N) : -(18 >= 32 + N);
        constexpr T A19 = N > 0 ? -(19 < N) : -(19 >= 32 + N);
        constexpr T A20 = N > 0 ? -(20 < N) : -(20 >= 32 + N);
        constexpr T A21 = N > 0 ? -(21 < N) : -(21 >= 32 + N);
        constexpr T A22 = N > 0 ? -(22 < N) : -(22 >= 32 + N);
        constexpr T A23 = N > 0 ? -(23 < N) : -(23 >= 32 + N);
        constexpr T A24 = N > 0 ? -(24 < N) : -(24 >= 32 + N);
        constexpr T A25 = N > 0 ? -(25 < N) : -(25 >= 32 + N);
        constexpr T A26 = N > 0 ? -(26 < N) : -(26 >= 32 + N);
        constexpr T A27 = N > 0 ? -(27 < N) : -(27 >= 32 + N);
        constexpr T A28 = N > 0 ? -(28 < N) : -(28 >= 32 + N);
        constexpr T A29 = N > 0 ? -(29 < N) : -(29 >= 32 + N);
        constexpr T A30 = N > 0 ? -(30 < N) : -(30 >= 32 + N);
        constexpr T A31 = N > 0 ? -(31 < N) : -(31 >= 32 + N);
        return Rotate<N>().BlendVar(TSimd8<T>(T(0)),
                                    TSimd8<T>(A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15,
                                              A16, A17, A18, A19, A20, A21, A22, A23, A24, A25, A26, A27, A28, A29, A30, A31));
    }

    template<int N>
    Y_FORCE_INLINE TSimd8<T> ByteShiftWithCarry(const TSimd8<T>& other) const {
        return Rotate<N>().BlendVar(other.Rotate<N>(), ~TSimd8<T>(T(-1)).ByteShift<N>());
    }

    template<int N>
    Y_FORCE_INLINE TSimd8<T> Rotate128() const {
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
            return Shuffle128(Repeat16(A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15));
        }
    }

    template<int N>
    Y_FORCE_INLINE TSimd8<T> Rotate() const {
        if constexpr (N % 32 == 0) {
            return *this;
        } else {
            constexpr T A0 = (32 - N) % 32;
            constexpr T A1 = (32 - N + 1) % 32;
            constexpr T A2 = (32 - N + 2) % 32;
            constexpr T A3 = (32 - N + 3) % 32;
            constexpr T A4 = (32 - N + 4) % 32;
            constexpr T A5 = (32 - N + 5) % 32;
            constexpr T A6 = (32 - N + 6) % 32;
            constexpr T A7 = (32 - N + 7) % 32;
            constexpr T A8 = (32 - N + 8) % 32;
            constexpr T A9 = (32 - N + 9) % 32;
            constexpr T A10 = (32 - N + 10) % 32;
            constexpr T A11 = (32 - N + 11) % 32;
            constexpr T A12 = (32 - N + 12) % 32;
            constexpr T A13 = (32 - N + 13) % 32;
            constexpr T A14 = (32 - N + 14) % 32;
            constexpr T A15 = (32 - N + 15) % 32;
            constexpr T A16 = (32 - N + 16) % 32;
            constexpr T A17 = (32 - N + 17) % 32;
            constexpr T A18 = (32 - N + 18) % 32;
            constexpr T A19 = (32 - N + 19) % 32;
            constexpr T A20 = (32 - N + 20) % 32;
            constexpr T A21 = (32 - N + 21) % 32;
            constexpr T A22 = (32 - N + 22) % 32;
            constexpr T A23 = (32 - N + 23) % 32;
            constexpr T A24 = (32 - N + 24) % 32;
            constexpr T A25 = (32 - N + 25) % 32;
            constexpr T A26 = (32 - N + 26) % 32;
            constexpr T A27 = (32 - N + 27) % 32;
            constexpr T A28 = (32 - N + 28) % 32;
            constexpr T A29 = (32 - N + 29) % 32;
            constexpr T A30 = (32 - N + 30) % 32;
            constexpr T A31 = (32 - N + 31) % 32;
            return Shuffle(TSimd8<T>(A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15,
                                     A16, A17, A18, A19, A20, A21, A22, A23, A24, A25, A26, A27, A28, A29, A30, A31));
        }
    }

    static Y_FORCE_INLINE TSimd8<T> Repeat16(
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

    Y_FORCE_INLINE TSimd8<T> MaxValue(const TSimd8<T>& other) const {
        return _mm256_max_epu8(this->Value, other.Value);
    }
    Y_FORCE_INLINE TSimd8<T> MinValue(const TSimd8<T>& other) const {
        return _mm256_min_epu8(this->Value, other.Value);
    }

    Y_FORCE_INLINE TSimd8<bool> BitsNotSet() const {
        return *this == ui8(0);
    }
    Y_FORCE_INLINE TSimd8<bool> AnyBitsSet() const {
        return ~this->BitsNotSet();
    }
    Y_FORCE_INLINE bool BitsNotSetAnywhere() const {
        return _mm256_testz_si256(this->Value, this->Value);
    }
    Y_FORCE_INLINE bool AnyBitsSetAnywhere() const {
        return !BitsNotSetAnywhere();
    }
    Y_FORCE_INLINE bool BitsNotSetAnywhere(const TSimd8<T>& bits) const {
        return _mm256_testz_si256(this->Value, bits.Value);
    }
    Y_FORCE_INLINE bool AnyBitsSetAnywhere(const TSimd8<T>& bits) const {
        return !BitsNotSetAnywhere(bits);
    }

    template<int N>
    Y_FORCE_INLINE TSimd8<T> Shr() const {
        return TSimd8<T>(_mm256_srli_epi16(this->Value, N)) & T(0xFFu >> N);
    }
    template<int N>
    Y_FORCE_INLINE TSimd8<T> Shl() const {
        return TSimd8<T>(_mm256_slli_epi16(this->Value, N)) & T(0xFFu << N);
    }

    template<int N>
    Y_FORCE_INLINE int GetBit() const {
        return _mm256_movemask_epi8(_mm256_slli_epi16(this->Value, 7-N));
    }

    friend Y_FORCE_INLINE TSimd8<bool> operator==(const TSimd8<T> &lhs, const TSimd8<T> &rhs) {
        return _mm256_cmpeq_epi8(lhs.Value, rhs.Value);
    }

    Y_FORCE_INLINE TSimd8<bool> operator<=(const TSimd8<T> &other) {
        return other.MaxValue(*this) == other;
    }

    Y_FORCE_INLINE TSimd8<bool> operator>=(const TSimd8<T> &other) {
        return other.MinValue(*this) == other;
    }

    Y_FORCE_INLINE TSimd8<bool> operator>(const TSimd8<T> &other) {
        return _mm256_cmpgt_epi8(*this, other);
    }

    Y_FORCE_INLINE TSimd8<bool> operator<(const TSimd8<T> &other) {
        return _mm256_cmpgt_epi8(other, *this);
    }

    Y_FORCE_INLINE TSimd8<T> operator|(const TSimd8<T>& other) const {
        return _mm256_or_si256(this->Value, other.Value);
    }
    Y_FORCE_INLINE TSimd8<T> operator&(const TSimd8<T>& other) const {
        return _mm256_and_si256(this->Value, other.Value);
    }
    Y_FORCE_INLINE TSimd8<T> operator^(const TSimd8<T>& other) const {
        return _mm256_xor_si256(this->Value, other.Value);
    }
    Y_FORCE_INLINE TSimd8<T> BitAndNot(const TSimd8<T>& other) const {
        return _mm256_andnot_si256(this->Value, other.Value);
    };
    Y_FORCE_INLINE TSimd8<T>& operator|=(const TSimd8<T>& other) {
        *this = *this | other;
        return *this;
    }
    Y_FORCE_INLINE TSimd8<T>& operator&=(const TSimd8<T>& other) {
        *this = *this & other;
        return *this;
    };
    Y_FORCE_INLINE TSimd8<T>& operator^=(const TSimd8<T>& other) {
        *this = *this ^ other;
        return *this;
    };

    Y_FORCE_INLINE TSimd8<T> operator+(const TSimd8<T>& other) const {
        return _mm256_add_epi8(this->Value, other.Value);
    }
    Y_FORCE_INLINE TSimd8<T> operator-(const TSimd8<T>& other) const {
        return _mm256_sub_epi8(this->Value, other.Value);
    }
    Y_FORCE_INLINE TSimd8<T>& operator+=(const TSimd8<T>& other) {
        *this = *this + other;
        return *static_cast<TSimd8<T>*>(this);
    }
    Y_FORCE_INLINE TSimd8<T>& operator-=(const TSimd8<T>& other) {
        *this = *this - other;
        return *static_cast<TSimd8<T>*>(this);
    }

    // 0xFFu = 11111111 = 2^8 - 1
    Y_FORCE_INLINE TSimd8<T> operator~() const {
        return *this ^ 0xFFu;
    }
};

template<>
Y_FORCE_INLINE TSimd8<ui64> TSimd8<ui64>::operator+(const TSimd8<ui64>& other) const {
    return _mm256_add_epi64(Value, other.Value);
}

template<>
Y_FORCE_INLINE TSimd8<ui64>& TSimd8<ui64>::operator+=(const TSimd8<ui64>& other) {
    *this = *this + other.Value;
    return *this;
}

template<>
Y_FORCE_INLINE TSimd8<bool> TSimd8<bool>::Set(bool value) {
    return _mm256_set1_epi8(ui8(-value));
}

template<>
Y_FORCE_INLINE TSimd8<bool>::TSimd8(bool value) {
    this->Value = _mm256_set1_epi8(ui8(-value));
}

template<>
Y_FORCE_INLINE TSimd8<i8> TSimd8<i8>::MaxValue(const TSimd8<i8>& other) const {
    return _mm256_max_epi8(this->Value, other.Value);
}

template<>
Y_FORCE_INLINE TSimd8<i8> TSimd8<i8>::MinValue(const TSimd8<i8>& other) const {
    return _mm256_min_epi8(this->Value, other.Value);
}

template<>
Y_FORCE_INLINE TSimd8<bool> TSimd8<ui8>::operator>(const TSimd8<ui8> &other) {
    return TSimd8<ui8>(_mm256_subs_epu8(this->Value, other.Value)).AnyBitsSet();
}

template<>
Y_FORCE_INLINE TSimd8<bool> TSimd8<ui8>::operator<(const TSimd8<ui8> &other) {
    return TSimd8<ui8>(_mm256_subs_epu8(other.Value, this->Value)).AnyBitsSet();
}


}
}

#pragma clang attribute pop
