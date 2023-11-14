#pragma once

#ifndef SIMD_FUNCTIONS_H
#define SIMD_FUNCTIONS_H

#include <stdint.h>

#if defined(_MSC_VER) && !defined(__clang__)
#include <intrin.h>
#else
#include <emmintrin.h>
#include <xmmintrin.h>
#include <immintrin.h>

#if defined(__clang_major__) && !defined(__apple_build_version__) && __clang_major__ >= 8
#   define Y_HAVE_NEW_INTRINSICS
#endif

#if !defined(Y_HAVE_NEW_INTRINSICS)
static __inline__ __m128i
_mm_loadu_si32(void const* __a) {
    struct __loadu_si32 {
        int __v;
    } __attribute__((__packed__, __may_alias__));
    int __u = ((struct __loadu_si32*)__a)->__v;
    return __extension__(__m128i)(__v4si){__u, 0, 0, 0};
}
#endif

#if !defined(__clang__) && __GNUC__ < 9
static __inline__ __m128i
_mm_loadu_si64(void const* __a) {
    struct __loadu_si64 {
        long long __v;
    } __attribute__((__packed__, __may_alias__));
    long long __u = ((struct __loadu_si64*)__a)->__v;
    return (__m128i){__u, 0L};
}
#endif

#if !defined(Y_HAVE_NEW_INTRINSICS)
static __inline__ void
_mm_storeu_si32(void const* __p, __m128i __b) {
    struct __storeu_si32 {
        int __v;
    } __attribute__((__packed__, __may_alias__));
    ((struct __storeu_si32*)__p)->__v = ((__v4si)__b)[0];
}

static __inline__ void
_mm_storeu_si64(void const* __p, __m128i __b) {
    struct __storeu_si64 {
        long long __v;
    } __attribute__((__packed__, __may_alias__));
    ((struct __storeu_si64*)__p)->__v = ((__v2di)__b)[0];
}

static __inline__ void
_mm_storeu_si16(void const* __p, __m128i __b) {
    struct __storeu_si16 {
        short __v;
    } __attribute__((__packed__, __may_alias__));
    ((struct __storeu_si16*)__p)->__v = ((__v8hi)__b)[0];
}
#endif
#endif

#ifdef _MSC_VER
#define FORCE_INLINE __forceinline
#else
#define FORCE_INLINE __attribute__((always_inline)) inline
#endif

namespace NFastOps {
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Function returns a ymm register with all floats set to 1.f
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    FORCE_INLINE __m256 YMMOneFloat() noexcept {
        return _mm256_set1_ps(1.f);
    }
    FORCE_INLINE __m256i YMMOneFloatSI() noexcept {
        return _mm256_castps_si256(YMMOneFloat());
    }
    FORCE_INLINE __m256d YMMOneDouble() noexcept {
        return _mm256_set1_pd(1.);
    }
    FORCE_INLINE __m256i YMMOneDoubleSI() noexcept {
        return _mm256_castpd_si256(YMMOneDouble());
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Function calculates horizontal sum of the YMM register. Assumes floats are stored.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    FORCE_INLINE __m128 SumYMM(__m128 x) noexcept {
        const __m128 hi_dual = _mm_movehl_ps(x, x);
        const __m128 sum_dual = _mm_add_ps(x, hi_dual);
        const __m128 hi = _mm_shuffle_ps(sum_dual, sum_dual, 0x1);
        return _mm_add_ss(sum_dual, hi);
    }
    FORCE_INLINE __m128 SumYMM(__m256 x) noexcept {
        const __m128 hi_quad = _mm256_extractf128_ps(x, 1); // hi_quad = ( x7, x6, x5, x4 )
        return SumYMM(_mm_add_ps(_mm256_castps256_ps128(x), hi_quad));
    }
    FORCE_INLINE float SumYMMR(__m128 x) noexcept {
        return _mm_cvtss_f32(SumYMM(x));
    }
    FORCE_INLINE float SumYMMR(__m256 x) noexcept {
        return _mm_cvtss_f32(SumYMM(x));
    }

    FORCE_INLINE __m128d SumYMM(__m128d x) noexcept {
        const __m128d hi_dual = _mm_castps_pd(_mm_movehl_ps(_mm_castpd_ps(x), _mm_castpd_ps(x)));
        return _mm_add_sd(x, hi_dual);
    }
    FORCE_INLINE __m128d SumYMM(__m256d x) noexcept {
        const __m128d hi_quad = _mm256_extractf128_pd(x, 1);
        return SumYMM(_mm_add_pd(_mm256_castpd256_pd128(x), hi_quad));
    }
    FORCE_INLINE double SumYMMR(__m128d x) noexcept {
        return _mm_cvtsd_f64(SumYMM(x));
    }
    FORCE_INLINE double SumYMMR(__m256d x) noexcept {
        return _mm_cvtsd_f64(SumYMM(x));
    }

//#######################################################################################################################################################################
// Macro - no other good way :-(
#define OPERATE_SEPARATELY_I(op, v, param) \
    _mm256_permute2f128_si256(_mm256_castsi128_si256(op(_mm256_castsi256_si128(v), param)), _mm256_castsi128_si256(op(_mm256_extractf128_si256(v, 1), param)), 32);

#define OPERATE_ELEMENTWISE_I(op, v1, v2)                                                             \
    _mm256_permute2f128_si256(                                                                        \
        _mm256_castsi128_si256(op(_mm256_castsi256_si128(v1), _mm256_castsi256_si128(v2))),           \
        _mm256_castsi128_si256(op(_mm256_extractf128_si256(v1, 1), _mm256_extractf128_si256(v2, 1))), \
        32);

#define FMADD_NO_AVX2()                                      \
    FORCE_INLINE static t_f FMADD(t_f v1, t_f v2, t_f v3) {  \
        return Add(Mul(v1, v2), v3);                         \
    }                                                        \
    FORCE_INLINE static t_f FMSUB(t_f v1, t_f v2, t_f v3) {  \
        return Sub(Mul(v1, v2), v3);                         \
    }                                                        \
    FORCE_INLINE static t_f FNMADD(t_f v1, t_f v2, t_f v3) { \
        return Sub(v3, Mul(v1, v2));                         \
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //  These functions only work for inputs in the range: [-2^51, 2^51]
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    FORCE_INLINE __m128i double_to_int64(__m128d x) {
        const __m256d magic_cvt_c = _mm256_set1_pd(0x0018000000000000);
        x = _mm_add_pd(x, _mm256_castpd256_pd128(magic_cvt_c));
        return _mm_sub_epi64(_mm_castpd_si128(x), _mm_castpd_si128(_mm256_castpd256_pd128(magic_cvt_c)));
    }
    FORCE_INLINE __m256i double_to_int64(__m256d x) {
        const __m256d magic_cvt_c = _mm256_set1_pd(0x0018000000000000);
        x = _mm256_add_pd(x, magic_cvt_c);
#ifndef NO_AVX2
        return _mm256_sub_epi64(_mm256_castpd_si256(x), _mm256_castpd_si256(magic_cvt_c));
#else
        return OPERATE_SEPARATELY_I(_mm_sub_epi64, _mm256_castpd_si256(x), _mm256_castsi256_si128(_mm256_castpd_si256(magic_cvt_c)));
#endif
    }
    FORCE_INLINE __m128d i64o_double(__m128i x) {
        const __m256d magic_cvt_c = _mm256_set1_pd(0x0018000000000000);
        x = _mm_add_epi64(x, _mm_castpd_si128(_mm256_castpd256_pd128(magic_cvt_c)));
        return _mm_sub_pd(_mm_castsi128_pd(x), _mm256_castpd256_pd128(magic_cvt_c));
    }
    FORCE_INLINE __m256d i64o_double(__m256i x) {
        const __m256d magic_cvt_c = _mm256_set1_pd(0x0018000000000000);
#ifndef NO_AVX2
        x = _mm256_add_epi64(x, _mm256_castpd_si256(magic_cvt_c));
#else
        x = OPERATE_SEPARATELY_I(_mm_add_epi64, x, _mm256_castsi256_si128(_mm256_castpd_si256(magic_cvt_c)));
#endif
        return _mm256_sub_pd(_mm256_castsi256_pd(x), magic_cvt_c);
    }

    //#######################################################################################################################################################################

    template <size_t I_NOfElements, size_t I_ElemSize>
    struct S_SIMDV;

    struct S_SIMDSmallBaseF {
        using t_i = __m128i;
        using t_f = __m128;
        using t_base_type = float;
        //using t_type_d = __m128d;

        FORCE_INLINE static t_i Cast(__m256i v) noexcept {
            return _mm256_castsi256_si128(v);
        }
        FORCE_INLINE static t_f Cast(__m256 v) noexcept {
            return _mm256_castps256_ps128(v);
        }
        //FORCE_INLINE static t_type_d Cast(__m256d v) noexcept { return _mm256_castpd256_pd128(v); }
        FORCE_INLINE static t_i Cast(t_i v) noexcept {
            return v;
        }
        FORCE_INLINE static t_f Cast(t_f v) noexcept {
            return v;
        }
        //FORCE_INLINE static t_type_d Cast(t_type_d v) noexcept { return v; }

        ///////////////////////////////////////////////////////////////////////////////////////////////////////

        FORCE_INLINE static t_i SetZeroI() {
            return _mm_setzero_si128();
        }
        FORCE_INLINE static t_f SetZeroF() {
            return _mm_setzero_ps();
        }
        FORCE_INLINE static t_f Set1(float v) {
            return _mm_set1_ps(v);
        }
        FORCE_INLINE static t_f Set(float v1, float v2, float v3, float v4) {
            return _mm_set_ps(v1, v2, v3, v4);
        }
        FORCE_INLINE static t_i Set1(int v) {
            return _mm_set1_epi32(v);
        }
        FORCE_INLINE static t_i Set(int v1, int v2, int v3, int v4) {
            return _mm_set_epi32(v1, v2, v3, v4);
        }

        FORCE_INLINE static t_i CastI(t_f v) {
            return _mm_castps_si128(v);
        }
        FORCE_INLINE static t_f CastF(t_i v) {
            return _mm_castsi128_ps(v);
        }
        FORCE_INLINE static t_f CVTI2F(t_i v) {
            return _mm_cvtepi32_ps(v);
        }
        FORCE_INLINE static t_i CVTF2I(t_f v) {
            return _mm_cvtps_epi32(v);
        }

        FORCE_INLINE static t_i CmpEqI(t_i v1, t_i v2) {
            return _mm_cmpeq_epi32(v1, v2);
        }
        FORCE_INLINE static t_i SRLI(t_i v, int i) {
            return _mm_srli_epi32(v, i);
        }
        FORCE_INLINE static t_i SLLI(t_i v, int i) {
            return _mm_slli_epi32(v, i);
        }
        FORCE_INLINE static t_i SRAI32(t_i v, int i) {
            return _mm_srai_epi32(v, i);
        }

        FORCE_INLINE static int TestCF(t_f v1, t_f v2) {
            return _mm_testc_ps(v1, v2);
        }
        FORCE_INLINE static int TestZF(t_f v1, t_f v2) {
            return _mm_testz_ps(v1, v2);
        }
        FORCE_INLINE static t_f AndF(t_f v1, t_f v2) {
            return _mm_and_ps(v1, v2);
        }
        FORCE_INLINE static t_f AndNotF(t_f v1, t_f v2) {
            return _mm_andnot_ps(v1, v2);
        }
        FORCE_INLINE static t_f OrF(t_f v1, t_f v2) {
            return _mm_or_ps(v1, v2);
        }
        FORCE_INLINE static t_f XorF(t_f v1, t_f v2) {
            return _mm_xor_ps(v1, v2);
        }

        FORCE_INLINE static t_i Sub(t_i v1, t_i v2) {
            return _mm_sub_epi32(v1, v2);
        }
        FORCE_INLINE static t_f BlendVF(t_f v1, t_f v2, t_f v3) {
            return _mm_blendv_ps(v1, v2, v3);
        }

        template <int I_Mode>
        FORCE_INLINE static t_f CmpFM(t_f v1, t_f v2) {
            return _mm_cmp_ps(v1, v2, I_Mode);
        }
        FORCE_INLINE static t_i Add(t_i v1, t_i v2) {
            return _mm_add_epi32(v1, v2);
        }
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    struct S_SIMDSmallMultiple: public S_SIMDSmallBaseF {
        using S_SIMDSmallBaseF::Add;
        using S_SIMDSmallBaseF::Sub;
        template <int I_Mode>
        FORCE_INLINE static t_f CmpF(t_f v1, t_f v2) {
            return _mm_cmp_ps(v1, v2, I_Mode);
        }
        FORCE_INLINE static t_f CmpEqF(t_f v1, t_f v2) {
            return _mm_cmpeq_ps(v1, v2);
        }

        FORCE_INLINE static t_f Mul(t_f v1, t_f v2) {
            return _mm_mul_ps(v1, v2);
        }
        FORCE_INLINE static t_f Div(t_f v1, t_f v2) {
            return _mm_div_ps(v1, v2);
        }
        FORCE_INLINE static t_f Add(t_f v1, t_f v2) {
            return _mm_add_ps(v1, v2);
        }
        FORCE_INLINE static t_f Sub(t_f v1, t_f v2) {
            return _mm_sub_ps(v1, v2);
        }
        FORCE_INLINE static t_f Sqrt(t_f v) {
            return _mm_sqrt_ps(v);
        }

#ifndef NO_AVX2
        FORCE_INLINE static t_f FMADD(t_f v1, t_f v2, t_f v3) {
            return _mm_fmadd_ps(v1, v2, v3);
        }
        FORCE_INLINE static t_f FMSUB(t_f v1, t_f v2, t_f v3) {
            return _mm_fmsub_ps(v1, v2, v3);
        }
        FORCE_INLINE static t_f FNMADD(t_f v1, t_f v2, t_f v3) {
            return _mm_fnmadd_ps(v1, v2, v3);
        }
#else
        FMADD_NO_AVX2();
#endif

        FORCE_INLINE static t_f Min(t_f v1, t_f v2) {
            return _mm_min_ps(v1, v2);
        }
        FORCE_INLINE static t_f Max(t_f v1, t_f v2) {
            return _mm_max_ps(v1, v2);
        }
        FORCE_INLINE static t_f Floor(t_f v) {
            return _mm_floor_ps(v);
        }
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <>
    struct S_SIMDV<1, 4>: public S_SIMDSmallBaseF {
        using S_SIMDSmallBaseF::Add;
        using S_SIMDSmallBaseF::Sub;

        FORCE_INLINE static t_f LoadU(const float* p) {
            return _mm_load_ss(p);
        }
        FORCE_INLINE static t_f Load(const float* p) {
            return _mm_load_ss(p);
        }
        FORCE_INLINE static t_i LoadU(const int* p) {
            return _mm_loadu_si32(p);
        }
        FORCE_INLINE static t_i Load(const int* p) {
            return _mm_loadu_si32(p);
        }
        FORCE_INLINE static void StoreU(float* p, t_f v) {
            return _mm_store_ss(p, v);
        }
        FORCE_INLINE static void Store(float* p, t_f v) {
            return _mm_store_ss(p, v);
        }
        FORCE_INLINE static void StoreU(int* p, t_i v) {
            return _mm_storeu_si32(p, v);
        }
        FORCE_INLINE static void Store(int* p, t_i v) {
            return _mm_storeu_si32(p, v);
        }

        template <int I_Mode>
        FORCE_INLINE static t_f CmpF(t_f v1, t_f v2) {
            return _mm_cmp_ss(v1, v2, I_Mode);
        }
        FORCE_INLINE static t_f CmpEqF(t_f v1, t_f v2) {
            return _mm_cmpeq_ss(v1, v2);
        }

        FORCE_INLINE static t_f Mul(t_f v1, t_f v2) {
            return _mm_mul_ss(v1, v2);
        }
        FORCE_INLINE static t_f Div(t_f v1, t_f v2) {
            return _mm_div_ss(v1, v2);
        }
        FORCE_INLINE static t_f Add(t_f v1, t_f v2) {
            return _mm_add_ss(v1, v2);
        }
        FORCE_INLINE static t_f Sub(t_f v1, t_f v2) {
            return _mm_sub_ss(v1, v2);
        }
        FORCE_INLINE static t_f Sqrt(t_f v) {
            return _mm_sqrt_ss(v);
        }

#ifndef NO_AVX2
        FORCE_INLINE static t_f FMADD(t_f v1, t_f v2, t_f v3) {
            return _mm_fmadd_ss(v1, v2, v3);
        }
        FORCE_INLINE static t_f FMSUB(t_f v1, t_f v2, t_f v3) {
            return _mm_fmsub_ss(v1, v2, v3);
        }
        FORCE_INLINE static t_f FNMADD(t_f v1, t_f v2, t_f v3) {
            return _mm_fnmadd_ss(v1, v2, v3);
        }
#else
        FMADD_NO_AVX2();
#endif

        FORCE_INLINE static t_f Min(t_f v1, t_f v2) {
            return _mm_min_ss(v1, v2);
        }
        FORCE_INLINE static t_f Max(t_f v1, t_f v2) {
            return _mm_max_ss(v1, v2);
        }
        FORCE_INLINE static t_f Floor(t_f v) {
            return _mm_floor_ss(v, v);
        }
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <>
    struct S_SIMDV<2, 4>: public S_SIMDSmallMultiple {
        FORCE_INLINE static t_f LoadU(const float* p) {
            return _mm_castpd_ps(_mm_load_sd((const double*)p));
        }
        FORCE_INLINE static t_f Load(const float* p) {
            return _mm_castpd_ps(_mm_load_sd((const double*)p));
        }
        FORCE_INLINE static t_i LoadU(const int* p) {
            return _mm_loadu_si64(p);
        }
        FORCE_INLINE static t_i Load(const int* p) {
            return _mm_loadu_si64(p);
        }
        FORCE_INLINE static void StoreU(float* p, t_f v) {
            return _mm_store_sd((double*)p, _mm_castps_pd(v));
        }
        FORCE_INLINE static void Store(float* p, t_f v) {
            return _mm_store_sd((double*)p, _mm_castps_pd(v));
        }
        FORCE_INLINE static void StoreU(int* p, t_i v) {
            return _mm_storeu_si64(p, v);
        }
        FORCE_INLINE static void Store(int* p, t_i v) {
            return _mm_storeu_si64(p, v);
        }
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <>
    struct S_SIMDV<4, 4>: public S_SIMDSmallMultiple {
        FORCE_INLINE static t_f LoadU(const float* p) {
            return _mm_loadu_ps(p);
        }
        FORCE_INLINE static t_f Load(const float* p) {
            return _mm_load_ps(p);
        }
        FORCE_INLINE static t_i LoadU(const int* p) {
            return _mm_loadu_si128((const t_i*)p);
        }
        FORCE_INLINE static t_i Load(const int* p) {
            return _mm_load_si128((const t_i*)p);
        }
        FORCE_INLINE static void StoreU(float* p, t_f v) {
            return _mm_storeu_ps(p, v);
        }
        FORCE_INLINE static void Store(float* p, t_f v) {
            return _mm_store_ps(p, v);
        }
        FORCE_INLINE static void StoreU(int* p, t_i v) {
            return _mm_storeu_si128((t_i*)p, v);
        }
        FORCE_INLINE static void Store(int* p, t_i v) {
            return _mm_store_si128((t_i*)p, v);
        }
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <>
    struct S_SIMDV<8, 4> {
        using t_i = __m256i;
        using t_f = __m256;
        using t_base_type = float;
        //using t_type_d = __m256d;

        FORCE_INLINE static t_i Cast(t_i v) noexcept {
            return v;
        }
        FORCE_INLINE static t_f Cast(t_f v) noexcept {
            return v;
        }
        //FORCE_INLINE static t_type_d Cast(t_type_d v) noexcept { return v; }

        ///////////////////////////////////////////////////////////////////////////////////////////////////////

        FORCE_INLINE static t_f LoadU(const float* p) {
            return _mm256_loadu_ps(p);
        }
        FORCE_INLINE static t_f Load(const float* p) {
            return _mm256_load_ps(p);
        }
        FORCE_INLINE static t_i LoadU(const int* p) {
            return _mm256_loadu_si256((const t_i*)p);
        }
        FORCE_INLINE static t_i Load(const int* p) {
            return _mm256_load_si256((const t_i*)p);
        }
        FORCE_INLINE static void StoreU(float* p, t_f v) {
            return _mm256_storeu_ps(p, v);
        }
        FORCE_INLINE static void Store(float* p, t_f v) {
            return _mm256_store_ps(p, v);
        }
        FORCE_INLINE static void StoreU(int* p, t_i v) {
            return _mm256_storeu_si256((t_i*)p, v);
        }
        FORCE_INLINE static void Store(int* p, t_i v) {
            return _mm256_store_si256((t_i*)p, v);
        }

        ///////////////////////////////////////////////////////////////////////////////////////////////////////

        FORCE_INLINE static t_i SetZeroI() {
            return _mm256_setzero_si256();
        }
        FORCE_INLINE static t_f SetZeroF() {
            return _mm256_setzero_ps();
        }
        FORCE_INLINE static t_f Set1(float v) {
            return _mm256_set1_ps(v);
        }
        FORCE_INLINE static t_i Set1(int v) {
            return _mm256_set1_epi32(v);
        }
        FORCE_INLINE static t_f Set(float v1, float v2, float v3, float v4, float v5, float v6, float v7, float v8) {
            return _mm256_set_ps(v1, v2, v3, v4, v5, v6, v7, v8);
        }
        FORCE_INLINE static t_i Set(int v1, int v2, int v3, int v4, int v5, int v6, int v7, int v8) {
            return _mm256_set_epi32(v1, v2, v3, v4, v5, v6, v7, v8);
        }

        FORCE_INLINE static t_i CastI(t_f v) {
            return _mm256_castps_si256(v);
        }
        FORCE_INLINE static t_f CastF(t_i v) {
            return _mm256_castsi256_ps(v);
        }
        FORCE_INLINE static t_f CVTI2F(t_i v) {
            return _mm256_cvtepi32_ps(v);
        }
        FORCE_INLINE static t_i CVTF2I(t_f v) {
            return _mm256_cvtps_epi32(v);
        }

#ifndef NO_AVX2
        FORCE_INLINE static t_i CmpEqI(t_i v1, t_i v2) {
            return _mm256_cmpeq_epi32(v1, v2);
        }
        FORCE_INLINE static t_i SRLI(t_i v, int i) {
            return _mm256_srli_epi32(v, i);
        }
        FORCE_INLINE static t_i SLLI(t_i v, int i) {
            return _mm256_slli_epi32(v, i);
        }
        FORCE_INLINE static t_i SRAI32(t_i v, int i) {
            return _mm256_srai_epi32(v, i);
        }
        FORCE_INLINE static t_i Add(t_i v1, t_i v2) {
            return _mm256_add_epi32(v1, v2);
        }
        FORCE_INLINE static t_i Sub(t_i v1, t_i v2) {
            return _mm256_sub_epi32(v1, v2);
        }

        FORCE_INLINE static t_f FMADD(t_f v1, t_f v2, t_f v3) {
            return _mm256_fmadd_ps(v1, v2, v3);
        }
        FORCE_INLINE static t_f FMSUB(t_f v1, t_f v2, t_f v3) {
            return _mm256_fmsub_ps(v1, v2, v3);
        }
        FORCE_INLINE static t_f FNMADD(t_f v1, t_f v2, t_f v3) {
            return _mm256_fnmadd_ps(v1, v2, v3);
        }
#else
        FORCE_INLINE static t_i CmpEqI(t_i v1, t_i v2) {
            return OPERATE_ELEMENTWISE_I(_mm_cmpeq_epi32, v1, v2);
        }
        FORCE_INLINE static t_i SRLI(t_i v, int i) {
            return OPERATE_SEPARATELY_I(_mm_srli_epi32, v, i);
        }
        FORCE_INLINE static t_i SLLI(t_i v, int i) {
            return OPERATE_SEPARATELY_I(_mm_slli_epi32, v, i);
        }
        FORCE_INLINE static t_i SRAI32(t_i v, int i) {
            return OPERATE_SEPARATELY_I(_mm_srai_epi32, v, i);
        }
        FORCE_INLINE static t_i Add(t_i v1, t_i v2) {
            return OPERATE_ELEMENTWISE_I(_mm_add_epi32, v1, v2);
        }
        FORCE_INLINE static t_i Sub(t_i v1, t_i v2) {
            return OPERATE_ELEMENTWISE_I(_mm_sub_epi32, v1, v2);
        }

        FMADD_NO_AVX2();
#endif

        FORCE_INLINE static int TestCF(t_f v1, t_f v2) {
            return _mm256_testc_ps(v1, v2);
        }
        FORCE_INLINE static int TestZF(t_f v1, t_f v2) {
            return _mm256_testz_ps(v1, v2);
        }
        FORCE_INLINE static t_f AndF(t_f v1, t_f v2) {
            return _mm256_and_ps(v1, v2);
        }
        FORCE_INLINE static t_f AndNotF(t_f v1, t_f v2) {
            return _mm256_andnot_ps(v1, v2);
        }
        FORCE_INLINE static t_f OrF(t_f v1, t_f v2) {
            return _mm256_or_ps(v1, v2);
        }
        FORCE_INLINE static t_f XorF(t_f v1, t_f v2) {
            return _mm256_xor_ps(v1, v2);
        }

        FORCE_INLINE static t_f BlendVF(t_f v1, t_f v2, t_f v3) {
            return _mm256_blendv_ps(v1, v2, v3);
        }

        template <int I_Mode>
        FORCE_INLINE static t_f CmpFM(t_f v1, t_f v2) {
            return _mm256_cmp_ps(v1, v2, I_Mode);
        }

        ///////////////////////////////////////////////////////////////////////////////////////////////////////

        template <int I_Mode>
        FORCE_INLINE static t_f CmpF(t_f v1, t_f v2) {
            return _mm256_cmp_ps(v1, v2, I_Mode);
        }
        FORCE_INLINE static t_f CmpEqF(t_f v1, t_f v2) {
            return _mm256_cmp_ps(v1, v2, _CMP_EQ_OQ);
        }

        FORCE_INLINE static t_f Mul(t_f v1, t_f v2) {
            return _mm256_mul_ps(v1, v2);
        }
        FORCE_INLINE static t_f Div(t_f v1, t_f v2) {
            return _mm256_div_ps(v1, v2);
        }
        FORCE_INLINE static t_f Add(t_f v1, t_f v2) {
            return _mm256_add_ps(v1, v2);
        }
        FORCE_INLINE static t_f Sub(t_f v1, t_f v2) {
            return _mm256_sub_ps(v1, v2);
        }
        FORCE_INLINE static t_f Sqrt(t_f v) {
            return _mm256_sqrt_ps(v);
        }

        FORCE_INLINE static t_f Min(t_f v1, t_f v2) {
            return _mm256_min_ps(v1, v2);
        }
        FORCE_INLINE static t_f Max(t_f v1, t_f v2) {
            return _mm256_max_ps(v1, v2);
        }
        FORCE_INLINE static t_f Floor(t_f v) {
            return _mm256_floor_ps(v);
        }
    };

    //############################################################################################################################################################

    struct S_SIMDSmallBaseD {
        using t_i = __m128i;
        using t_f = __m128d;
        using t_base_type = double;
        //using t_type_d = __m128d;

        FORCE_INLINE static t_i Cast(__m256i v) noexcept {
            return _mm256_castsi256_si128(v);
        }
        FORCE_INLINE static t_f Cast(__m256d v) noexcept {
            return _mm256_castpd256_pd128(v);
        }
        FORCE_INLINE static t_i Cast(t_i v) noexcept {
            return v;
        }
        FORCE_INLINE static t_f Cast(t_f v) noexcept {
            return v;
        }

        ///////////////////////////////////////////////////////////////////////////////////////////////////////

        FORCE_INLINE static t_i SetZeroI() {
            return _mm_setzero_si128();
        }
        FORCE_INLINE static t_f SetZeroF() {
            return _mm_setzero_pd();
        }
        FORCE_INLINE static t_f Set1(double v) {
            return _mm_set1_pd(v);
        }
        FORCE_INLINE static t_f Set(double v1, double v2) {
            return _mm_set_pd(v1, v2);
        }
        FORCE_INLINE static t_i Set1(int64_t v) {
            return _mm_set1_epi64x(v);
        }
        FORCE_INLINE static t_i Set(int64_t v1, int64_t v2) {
            return _mm_set_epi64x(v1, v2);
        }

        FORCE_INLINE static t_i CastI(t_f v) {
            return _mm_castpd_si128(v);
        }
        FORCE_INLINE static t_f CastF(t_i v) {
            return _mm_castsi128_pd(v);
        }
        FORCE_INLINE static t_f CVTI2F(t_i v) {
            return /*_mm_cvtepi64_pd(v);*/ i64o_double(v);
        }
        FORCE_INLINE static t_i CVTF2I(t_f v) {
            return /*_mm_cvtpd_epi64(v);*/ double_to_int64(v);
        }

        FORCE_INLINE static t_i CmpEqI(t_i v1, t_i v2) {
            return _mm_cmpeq_epi64(v1, v2);
        }
        FORCE_INLINE static t_i SRLI(t_i v, int i) {
            return _mm_srli_epi64(v, i);
        }
        FORCE_INLINE static t_i SLLI(t_i v, int i) {
            return _mm_slli_epi64(v, i);
        }
        FORCE_INLINE static t_i SRAI32(t_i v, int i) {
            return _mm_srai_epi32(v, i);
        }

        FORCE_INLINE static int TestCF(t_f v1, t_f v2) {
            return _mm_testc_pd(v1, v2);
        }
        FORCE_INLINE static int TestZF(t_f v1, t_f v2) {
            return _mm_testz_pd(v1, v2);
        }
        FORCE_INLINE static t_f AndF(t_f v1, t_f v2) {
            return _mm_and_pd(v1, v2);
        }
        FORCE_INLINE static t_f AndNotF(t_f v1, t_f v2) {
            return _mm_andnot_pd(v1, v2);
        }
        FORCE_INLINE static t_f OrF(t_f v1, t_f v2) {
            return _mm_or_pd(v1, v2);
        }
        FORCE_INLINE static t_f XorF(t_f v1, t_f v2) {
            return _mm_xor_pd(v1, v2);
        }

        FORCE_INLINE static t_i Sub(t_i v1, t_i v2) {
            return _mm_sub_epi64(v1, v2);
        }
        FORCE_INLINE static t_f BlendVF(t_f v1, t_f v2, t_f v3) {
            return _mm_blendv_pd(v1, v2, v3);
        }

        template <int I_Mode>
        FORCE_INLINE static t_f CmpFM(t_f v1, t_f v2) {
            return _mm_cmp_pd(v1, v2, I_Mode);
        }
        FORCE_INLINE static t_i Add(t_i v1, t_i v2) {
            return _mm_add_epi64(v1, v2);
        }
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    struct S_SIMDSmallMultipleD: public S_SIMDSmallBaseD {
        using S_SIMDSmallBaseD::Add;
        using S_SIMDSmallBaseD::Sub;
        template <int I_Mode>
        FORCE_INLINE static t_f CmpF(t_f v1, t_f v2) {
            return _mm_cmp_pd(v1, v2, I_Mode);
        }
        FORCE_INLINE static t_f CmpEqF(t_f v1, t_f v2) {
            return _mm_cmpeq_pd(v1, v2);
        }

        FORCE_INLINE static t_f Mul(t_f v1, t_f v2) {
            return _mm_mul_pd(v1, v2);
        }
        FORCE_INLINE static t_f Div(t_f v1, t_f v2) {
            return _mm_div_pd(v1, v2);
        }
        FORCE_INLINE static t_f Add(t_f v1, t_f v2) {
            return _mm_add_pd(v1, v2);
        }
        FORCE_INLINE static t_f Sub(t_f v1, t_f v2) {
            return _mm_sub_pd(v1, v2);
        }
        FORCE_INLINE static t_f Sqrt(t_f v) {
            return _mm_sqrt_pd(v);
        }

#ifndef NO_AVX2
        FORCE_INLINE static t_f FMADD(t_f v1, t_f v2, t_f v3) {
            return _mm_fmadd_pd(v1, v2, v3);
        }
        FORCE_INLINE static t_f FMSUB(t_f v1, t_f v2, t_f v3) {
            return _mm_fmsub_pd(v1, v2, v3);
        }
        FORCE_INLINE static t_f FNMADD(t_f v1, t_f v2, t_f v3) {
            return _mm_fnmadd_pd(v1, v2, v3);
        }
#else
        FMADD_NO_AVX2();
#endif

        FORCE_INLINE static t_f Min(t_f v1, t_f v2) {
            return _mm_min_pd(v1, v2);
        }
        FORCE_INLINE static t_f Max(t_f v1, t_f v2) {
            return _mm_max_pd(v1, v2);
        }
        FORCE_INLINE static t_f Floor(t_f v) {
            return _mm_floor_pd(v);
        }
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <>
    struct S_SIMDV<1, 8>: public S_SIMDSmallBaseD {
        using S_SIMDSmallBaseD::Add;
        using S_SIMDSmallBaseD::Sub;

        FORCE_INLINE static t_f LoadU(const double* p) {
            return _mm_load_sd(p);
        }
        FORCE_INLINE static t_f Load(const double* p) {
            return _mm_load_sd(p);
        }
        FORCE_INLINE static t_i LoadU(const int64_t* p) {
            return _mm_loadu_si64(p);
        }
        FORCE_INLINE static t_i Load(const int64_t* p) {
            return _mm_loadu_si64(p);
        }
        FORCE_INLINE static void StoreU(double* p, t_f v) {
            return _mm_store_sd(p, v);
        }
        FORCE_INLINE static void Store(double* p, t_f v) {
            return _mm_store_sd(p, v);
        }
        FORCE_INLINE static void StoreU(int64_t* p, t_i v) {
            return _mm_storeu_si64(p, v);
        }
        FORCE_INLINE static void Store(int64_t* p, t_i v) {
            return _mm_storeu_si64(p, v);
        }

        template <int I_Mode>
        FORCE_INLINE static t_f CmpF(t_f v1, t_f v2) {
            return _mm_cmp_sd(v1, v2, I_Mode);
        }
        FORCE_INLINE static t_f CmpEqF(t_f v1, t_f v2) {
            return _mm_cmpeq_sd(v1, v2);
        }

        FORCE_INLINE static t_f Mul(t_f v1, t_f v2) {
            return _mm_mul_sd(v1, v2);
        }
        FORCE_INLINE static t_f Div(t_f v1, t_f v2) {
            return _mm_div_sd(v1, v2);
        }
        FORCE_INLINE static t_f Add(t_f v1, t_f v2) {
            return _mm_add_sd(v1, v2);
        }
        FORCE_INLINE static t_f Sub(t_f v1, t_f v2) {
            return _mm_sub_sd(v1, v2);
        }
        FORCE_INLINE static t_f Sqrt(t_f v) {
            return _mm_sqrt_sd(v, v);
        }

#ifndef NO_AVX2
        FORCE_INLINE static t_f FMADD(t_f v1, t_f v2, t_f v3) {
            return _mm_fmadd_sd(v1, v2, v3);
        }
        FORCE_INLINE static t_f FMSUB(t_f v1, t_f v2, t_f v3) {
            return _mm_fmsub_sd(v1, v2, v3);
        }
        FORCE_INLINE static t_f FNMADD(t_f v1, t_f v2, t_f v3) {
            return _mm_fnmadd_sd(v1, v2, v3);
        }
#else
        FMADD_NO_AVX2();
#endif

        FORCE_INLINE static t_f Min(t_f v1, t_f v2) {
            return _mm_min_sd(v1, v2);
        }
        FORCE_INLINE static t_f Max(t_f v1, t_f v2) {
            return _mm_max_sd(v1, v2);
        }
        FORCE_INLINE static t_f Floor(t_f v) {
            return _mm_floor_sd(v, v);
        }
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <>
    struct S_SIMDV<2, 8>: public S_SIMDSmallMultipleD {
        FORCE_INLINE static t_f LoadU(const double* p) {
            return _mm_castps_pd(_mm_loadu_ps((const float*)p));
        }
        FORCE_INLINE static t_f Load(const double* p) {
            return _mm_castps_pd(_mm_load_ps((const float*)p));
        }
        FORCE_INLINE static t_i LoadU(const int64_t* p) {
            return _mm_loadu_si128((const t_i*)p);
        }
        FORCE_INLINE static t_i Load(const int64_t* p) {
            return _mm_load_si128((const t_i*)p);
        }
        FORCE_INLINE static void StoreU(double* p, t_f v) {
            return _mm_storeu_ps((float*)p, _mm_castpd_ps(v));
        }
        FORCE_INLINE static void Store(double* p, t_f v) {
            return _mm_store_ps((float*)p, _mm_castpd_ps(v));
        }
        FORCE_INLINE static void StoreU(int64_t* p, t_i v) {
            return _mm_storeu_si128((t_i*)p, v);
        }
        FORCE_INLINE static void Store(int64_t* p, t_i v) {
            return _mm_store_si128((t_i*)p, v);
        }
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <>
    struct S_SIMDV<4, 8> {
        using t_i = __m256i;
        using t_f = __m256d;
        using t_base_type = double;

        FORCE_INLINE static t_i Cast(t_i v) noexcept {
            return v;
        }
        FORCE_INLINE static t_f Cast(t_f v) noexcept {
            return v;
        }

        ///////////////////////////////////////////////////////////////////////////////////////////////////////

        FORCE_INLINE static t_f LoadU(const double* p) {
            return _mm256_loadu_pd(p);
        }
        FORCE_INLINE static t_f Load(const double* p) {
            return _mm256_load_pd(p);
        }
        FORCE_INLINE static t_i LoadU(const int64_t* p) {
            return _mm256_loadu_si256((const t_i*)p);
        }
        FORCE_INLINE static t_i Load(const int64_t* p) {
            return _mm256_load_si256((const t_i*)p);
        }
        FORCE_INLINE static void StoreU(double* p, t_f v) {
            return _mm256_storeu_pd(p, v);
        }
        FORCE_INLINE static void Store(double* p, t_f v) {
            return _mm256_store_pd(p, v);
        }
        FORCE_INLINE static void StoreU(int64_t* p, t_i v) {
            return _mm256_storeu_si256((t_i*)p, v);
        }
        FORCE_INLINE static void Store(int64_t* p, t_i v) {
            return _mm256_store_si256((t_i*)p, v);
        }

        ///////////////////////////////////////////////////////////////////////////////////////////////////////

        FORCE_INLINE static t_i SetZeroI() {
            return _mm256_setzero_si256();
        }
        FORCE_INLINE static t_f SetZeroF() {
            return _mm256_setzero_pd();
        }
        FORCE_INLINE static t_f Set1(double v) {
            return _mm256_set1_pd(v);
        }
        FORCE_INLINE static t_i Set1(int64_t v) {
            return _mm256_set1_epi64x(v);
        }
        FORCE_INLINE static t_f Set(double v1, double v2, double v3, double v4) {
            return _mm256_set_pd(v1, v2, v3, v4);
        }
        FORCE_INLINE static t_i Set(int64_t v1, int64_t v2, int64_t v3, int64_t v4) {
            return _mm256_set_epi64x(v1, v2, v3, v4);
        }

        FORCE_INLINE static t_i CastI(t_f v) {
            return _mm256_castpd_si256(v);
        }
        FORCE_INLINE static t_f CastF(t_i v) {
            return _mm256_castsi256_pd(v);
        }
        FORCE_INLINE static t_f CVTI2F(t_i v) {
            return /*_mm256_cvtepi64_pd(v);*/ i64o_double(v);
        }
        FORCE_INLINE static t_i CVTF2I(t_f v) {
            return /*_mm256_cvtpd_epi64(v);*/ double_to_int64(v);
        }

#ifndef NO_AVX2
        FORCE_INLINE static t_i CmpEqI(t_i v1, t_i v2) {
            return _mm256_cmpeq_epi64(v1, v2);
        }
        FORCE_INLINE static t_i SRLI(t_i v, int i) {
            return _mm256_srli_epi64(v, i);
        }
        FORCE_INLINE static t_i SLLI(t_i v, int i) {
            return _mm256_slli_epi64(v, i);
        }
        FORCE_INLINE static t_i SRAI32(t_i v, int i) {
            return _mm256_srai_epi32(v, i);
        }
        FORCE_INLINE static t_i Add(t_i v1, t_i v2) {
            return _mm256_add_epi64(v1, v2);
        }
        FORCE_INLINE static t_i Sub(t_i v1, t_i v2) {
            return _mm256_sub_epi64(v1, v2);
        }

        FORCE_INLINE static t_f FMADD(t_f v1, t_f v2, t_f v3) {
            return _mm256_fmadd_pd(v1, v2, v3);
        }
        FORCE_INLINE static t_f FMSUB(t_f v1, t_f v2, t_f v3) {
            return _mm256_fmsub_pd(v1, v2, v3);
        }
        FORCE_INLINE static t_f FNMADD(t_f v1, t_f v2, t_f v3) {
            return _mm256_fnmadd_pd(v1, v2, v3);
        }
#else
        FORCE_INLINE static t_i CmpEqI(t_i v1, t_i v2) {
            return OPERATE_ELEMENTWISE_I(_mm_cmpeq_epi64, v1, v2);
        }
        FORCE_INLINE static t_i SRLI(t_i v, int i) {
            return OPERATE_SEPARATELY_I(_mm_srli_epi64, v, i);
        }
        FORCE_INLINE static t_i SLLI(t_i v, int i) {
            return OPERATE_SEPARATELY_I(_mm_slli_epi64, v, i);
        }
        FORCE_INLINE static t_i SRAI32(t_i v, int i) {
            return OPERATE_SEPARATELY_I(_mm_srai_epi32, v, i);
        }
        FORCE_INLINE static t_i Add(t_i v1, t_i v2) {
            return OPERATE_ELEMENTWISE_I(_mm_add_epi64, v1, v2);
        }
        FORCE_INLINE static t_i Sub(t_i v1, t_i v2) {
            return OPERATE_ELEMENTWISE_I(_mm_sub_epi64, v1, v2);
        }

        FMADD_NO_AVX2();
#endif

        FORCE_INLINE static int TestCF(t_f v1, t_f v2) {
            return _mm256_testc_pd(v1, v2);
        }
        FORCE_INLINE static int TestZF(t_f v1, t_f v2) {
            return _mm256_testz_pd(v1, v2);
        }
        FORCE_INLINE static t_f AndF(t_f v1, t_f v2) {
            return _mm256_and_pd(v1, v2);
        }
        FORCE_INLINE static t_f AndNotF(t_f v1, t_f v2) {
            return _mm256_andnot_pd(v1, v2);
        }
        FORCE_INLINE static t_f OrF(t_f v1, t_f v2) {
            return _mm256_or_pd(v1, v2);
        }
        FORCE_INLINE static t_f XorF(t_f v1, t_f v2) {
            return _mm256_xor_pd(v1, v2);
        }

        FORCE_INLINE static t_f BlendVF(t_f v1, t_f v2, t_f v3) {
            return _mm256_blendv_pd(v1, v2, v3);
        }

        template <int I_Mode>
        FORCE_INLINE static t_f CmpFM(t_f v1, t_f v2) {
            return _mm256_cmp_pd(v1, v2, I_Mode);
        }

        ///////////////////////////////////////////////////////////////////////////////////////////////////////

        template <int I_Mode>
        FORCE_INLINE static t_f CmpF(t_f v1, t_f v2) {
            return _mm256_cmp_pd(v1, v2, I_Mode);
        }
        FORCE_INLINE static t_f CmpEqF(t_f v1, t_f v2) {
            return _mm256_cmp_pd(v1, v2, _CMP_EQ_OQ);
        }

        FORCE_INLINE static t_f Mul(t_f v1, t_f v2) {
            return _mm256_mul_pd(v1, v2);
        }
        FORCE_INLINE static t_f Div(t_f v1, t_f v2) {
            return _mm256_div_pd(v1, v2);
        }
        FORCE_INLINE static t_f Add(t_f v1, t_f v2) {
            return _mm256_add_pd(v1, v2);
        }
        FORCE_INLINE static t_f Sub(t_f v1, t_f v2) {
            return _mm256_sub_pd(v1, v2);
        }
        FORCE_INLINE static t_f Sqrt(t_f v) {
            return _mm256_sqrt_pd(v);
        }

        FORCE_INLINE static t_f Min(t_f v1, t_f v2) {
            return _mm256_min_pd(v1, v2);
        }
        FORCE_INLINE static t_f Max(t_f v1, t_f v2) {
            return _mm256_max_pd(v1, v2);
        }
        FORCE_INLINE static t_f Floor(t_f v) {
            return _mm256_floor_pd(v);
        }
    };

    template <size_t I_ElemSize>
    using S_MaxSIMD = S_SIMDV<32 / I_ElemSize, I_ElemSize>;
}

#endif
