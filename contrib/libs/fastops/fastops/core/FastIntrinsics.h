#pragma once

#ifndef FAST_INTRINSICS_H
#define FAST_INTRINSICS_H

#include "SIMDFunctions.h"

#include <limits>
#include <algorithm>
#include <cstring>
#include <type_traits>

#pragma warning(push)
#pragma warning(disable : 4100)

constexpr size_t MyMin(size_t x, size_t y) noexcept {
    return x < y ? x : y;
}

template <class T>
inline T ReadUnaligned(const void* from) noexcept {
    T ret;
    std::memcpy(&ret, from, sizeof(T));
    return ret;
}

template <class Target, class Source>
Target BitCast(const Source& source) {
    static_assert(sizeof(Source) == sizeof(Target), "Size mismatch");
    static_assert(std::is_trivially_copyable<Source>::value, "Source is not trivially copyable");
    static_assert(std::is_trivial<Target>::value, "Target is not trivial");

    using TNonvolatileSource = std::remove_volatile_t<Source>;
    using TNonvolatileTarget = std::remove_volatile_t<Target>;

    return ReadUnaligned<TNonvolatileTarget>(&const_cast<const TNonvolatileSource&>(source));
}

namespace NFastOps {
    constexpr size_t HighestPowerOf2(size_t v) noexcept {
        size_t count = 0;
        for (; v; ++count)
            v >>= 1;
        return count ? size_t(1) << (count - size_t(1)) : size_t(0);
    }

    static_assert(HighestPowerOf2(0) == 0);

    //#######################################################################################################################################################################

    namespace NFastOpsDetail {
        template <size_t I_ElementSize>
        struct S_Constants;

        static inline constexpr __m256 constexpr_mm256_set1_ps(float f) {
#if defined(_MSC_VER)
            return {f, f, f, f, f, f, f, f};
#else
            return (__m256){f, f, f, f, f, f, f, f};
#endif
        };

        static inline constexpr __m256d constexpr_mm256_set1_pd(double d) {
#if defined(_MSC_VER)
            return {d, d, d, d};
#else
            return (__m256d){d, d, d, d};
#endif
        };

        static inline constexpr __m256i constexpr_mm256_set1_epi32(int i) {
            unsigned int u = static_cast<unsigned int>(i);
#if defined(_MSC_VER) && !defined(__clang__)
            char first = u & 0xFu;
            char second = (u >> 8) & 0xFu;
            char third = (u >> 16) & 0xFu;
            char fourth = (u >> 24) & 0xFu;
            return {first, second, third, fourth,
                    first, second, third, fourth,
                    first, second, third, fourth,
                    first, second, third, fourth,
                    first, second, third, fourth,
                    first, second, third, fourth,
                    first, second, third, fourth,
                    first, second, third, fourth};
#else
            long long ul = u | (static_cast<unsigned long long>(u) << 32);
            return (__m256i){ul, ul, ul, ul};
#endif
        }

        static inline constexpr __m256i constexpr_mm256_set1_epi64x(long long l) {
#if defined(_MSC_VER) && !defined(__clang__)
            unsigned long long u = static_cast<unsigned long long>(l);
            char first = u & 0xFu;
            char second = (u >> 8) & 0xFu;
            char third = (u >> 16) & 0xFu;
            char fourth = (u >> 24) & 0xFu;
            char fifth = (u >> 32) & 0xFu;
            char sixth = (u >> 40) & 0xFu;
            char seventh = (u >> 48) & 0xFu;
            char eighth = (u >> 56) & 0xFu;
            return {first, second, third, fourth, fifth, sixth, seventh, eighth,
                    first, second, third, fourth, fifth, sixth, seventh, eighth,
                    first, second, third, fourth, fifth, sixth, seventh, eighth,
                    first, second, third, fourth, fifth, sixth, seventh, eighth};
#else
            return (__m256i){ l, l, l, l };
#endif
        }

        static inline constexpr auto constexpr_mm256_castsi256_ps = [](__m256i x) {return _mm256_castsi256_ps(x);};
        static inline constexpr auto constexpr_mm256_castps_si256 = [](__m256 x) {return _mm256_castps_si256(x);};
        static inline constexpr auto constexpr_mm256_castsi256_pd = [](__m256i x) {return _mm256_castsi256_pd(x);};
        static inline constexpr auto constexpr_mm256_castpd_si256 = [](__m256d x) {return _mm256_castpd_si256(x);};

        template <>
        struct S_Constants<4> {

            // Some constexpr workarounds for gcc, clang and msvc.
            using Source = float;
            using Target = int;
            static constexpr auto Set1 = constexpr_mm256_set1_ps;
            static constexpr auto CastSi = constexpr_mm256_castsi256_ps;
            static constexpr auto CastPf = constexpr_mm256_castps_si256;

            static constexpr __m256 c1h = Set1(4.901290717342735958569508618176166906457e-1f);
            static constexpr __m256 c1t_plus_c0t = Set1(-1.213203435596425732025330863145471178545e-1f);
            static constexpr __m256 c1h_by_c0t_minus_c2h = Set1(-1.039720770839917964125848182187264852113f);
            static constexpr __m256 c_ln_range_threshold = Set1(7.071067811865475244008443621048490392848e-1f);

            static constexpr __m256 c_ln7_c0 = Set1(3.274046088544186271578736717276955126405e-1f);
            static constexpr __m256 c_ln7_c1 = Set1(-2.460077318856183503930805541364448494063e-1f);
            static constexpr __m256 c_ln7_c2 = Set1(1.969693180733211157137504487566098634881e-1f);
            static constexpr __m256 c_ln7_c3 = Set1(-1.667744330973693530308560275865086463950e-1f);
            static constexpr __m256 c_ln7_c4 = Set1(1.510576765737534749447874102473717073429e-1f);
            static constexpr __m256 c_ln7_c5 = Set1(-1.017552258241698935203275142363246158437e-1f);

            static constexpr __m256 c_ln5_c0 = Set1(3.273555858564201849484689435773550727008e-1f);
            static constexpr __m256 c_ln5_c1 = Set1(-2.469326754162029197824769224764207256300e-1f);
            static constexpr __m256 c_ln5_c2 = Set1(2.050803141348481033461102938420647618561e-1f);
            static constexpr __m256 c_ln5_c3 = Set1(-1.441145595397930709104807611354899546141e-1f);

            static constexpr __m256 c_pow2_4_c0 = Set1(-3.068529675993459480848426056697043817499e-1f);
            static constexpr __m256 c_pow2_4_c1 = Set1(-6.662345431318903025772700509142101007024e-2f);
            static constexpr __m256 c_pow2_4_c2 = Set1(-1.113930183733997141783833210977614459718e-2f);
            static constexpr __m256 c_pow2_4_c3 = Set1(-1.461237960055165634948236381176861135936e-3f);
            static constexpr __m256 c_pow2_4_c4 = Set1(-2.171502549397975884526363201015788921121e-4f);

            static constexpr __m256 c_pow2_2_c0 = Set1(-3.069678791803394491901405992213472390777e-1f);
            static constexpr __m256 c_pow2_2_c1 = Set1(-6.558811624324781017147952441210509604385e-2f);
            static constexpr __m256 c_pow2_2_c2 = Set1(-1.355574723481491770403079319055785445381e-2f);

            static constexpr __m256 c_half_f = Set1(0.5f);
            static constexpr __m256 c_1_f = Set1(1.f);
            static constexpr __m256 c_2_f = Set1(2.f);
            static constexpr __m256 c_1_over_ln_2 = Set1(1.442695040888963407359924681001892137426f);
            static constexpr __m256 c_neg_1_over_ln_2 = Set1(-1.442695040888963407359924681001892137426f);
            static constexpr __m256 c_neg_2_over_ln_2 = Set1(float(-2. * 1.442695040888963407359924681001892137426));

            static constexpr __m256 c_ln_2 = Set1(6.931471805599453094172321214581765680755e-1f);
            static constexpr __m256i c_denorm_const = constexpr_mm256_set1_epi32(127);
            static constexpr __m256 c_inf_i = Set1(std::numeric_limits<float>::infinity());
            static constexpr __m256i c_all_ones = constexpr_mm256_set1_epi32(-1);
            static constexpr __m256i c_mantissa_mask = constexpr_mm256_set1_epi32(int(0x00'7F'FF'FF));

            static constexpr __m256 c_max_pow_2 = Set1(128.f);
            static constexpr __m256 c_min_denorm_exp_f = Set1(-150.f);
            static constexpr __m256 c_min_norm_exp_f = Set1(-127.f);
            static constexpr __m256i c_denorm_offset = constexpr_mm256_set1_epi32(-126);

            static constexpr int ci_bits_in_mantissa = 23;
            static constexpr int ci_denorm_const = (ci_bits_in_mantissa + 127) << ci_bits_in_mantissa;
            static constexpr __m256 c_neg_f_bits_in_mantissa = Set1(-float(ci_bits_in_mantissa));
            static constexpr __m256 c_neg_f_infinity = Set1(-std::numeric_limits<float>::infinity());
            static constexpr __m256 c_neg_f_zero = Set1(-0.f);
        };

        template <>
        struct S_Constants<8> {

            // Some constexpr workarounds for gcc, clang and msvc.
            using Source = double;
            using Target = size_t;
            static constexpr auto Set1 = constexpr_mm256_set1_pd;
            static constexpr auto CastSi = constexpr_mm256_castsi256_pd;
            static constexpr auto CastPf = constexpr_mm256_castpd_si256;

            static constexpr __m256d c1h = Set1(4.901290717342735958569508618176166906457e-1);
            static constexpr __m256d c1t_plus_c0t = Set1(-1.213203435596425732025330863145471178545e-1);
            static constexpr __m256d c1h_by_c0t_minus_c2h = Set1(-1.039720770839917964125848182187264852113);
            static constexpr __m256d c_ln_range_threshold = Set1(7.071067811865475244008443621048490392848e-1);

            static constexpr __m256d c_ln9_c0 = Set1(3.274040414833276642293935648031820904022e-1);
            static constexpr __m256d c_ln9_c1 = Set1(-2.460426108817215117479709510818728283515e-1);
            static constexpr __m256d c_ln9_c2 = Set1(1.971705651171856040168275563322538385840e-1);
            static constexpr __m256d c_ln9_c3 = Set1(-1.644082698894967400206460910619729462729e-1);
            static constexpr __m256d c_ln9_c4 = Set1(1.408917636407928535073460571984541868931e-1);
            static constexpr __m256d c_ln9_c5 = Set1(-1.273228141550318878611668315296447653434e-1);
            static constexpr __m256d c_ln9_c6 = Set1(1.205275963912385751945799850342567301852e-1);
            static constexpr __m256d c_ln9_c7 = Set1(-7.664829052466830813429918673961725340730e-2);

            static constexpr __m256d c_ln7_c0 = Set1(3.274046088544186271578736717276955126405e-1);
            static constexpr __m256d c_ln7_c1 = Set1(-2.460077318856183503930805541364448494063e-1);
            static constexpr __m256d c_ln7_c2 = Set1(1.969693180733211157137504487566098634881e-1);
            static constexpr __m256d c_ln7_c3 = Set1(-1.667744330973693530308560275865086463950e-1);
            static constexpr __m256d c_ln7_c4 = Set1(1.510576765737534749447874102473717073429e-1);
            static constexpr __m256d c_ln7_c5 = Set1(-1.017552258241698935203275142363246158437e-1);

            static constexpr __m256d c_ln5_c0 = Set1(3.273555858564201849484689435773550727008e-1);
            static constexpr __m256d c_ln5_c1 = Set1(-2.469326754162029197824769224764207256300e-1);
            static constexpr __m256d c_ln5_c2 = Set1(2.050803141348481033461102938420647618561e-1);
            static constexpr __m256d c_ln5_c3 = Set1(-1.441145595397930709104807611354899546141e-1);

            static constexpr __m256d c_pow2_6_c0 = Set1(-3.068528195372368372826179618775428072217e-1);
            static constexpr __m256d c_pow2_6_c1 = Set1(-6.662630929237755210810414038195547289735e-2);
            static constexpr __m256d c_pow2_6_c2 = Set1(-1.112223817301083258745885554952494883219e-2);
            static constexpr __m256d c_pow2_6_c3 = Set1(-1.503903566909095368304539146883327192756e-3);
            static constexpr __m256d c_pow2_6_c4 = Set1(-1.711643253068146019790027094116090970622e-4);
            static constexpr __m256d c_pow2_6_c5 = Set1(-1.606218523854454480443664688362539746237e-5);
            static constexpr __m256d c_pow2_6_c6 = Set1(-1.863870613873008492165005750904674527977e-6);

            static constexpr __m256d c_pow2_4_c0 = Set1(-3.068529675993459480848426056697043817499e-1);
            static constexpr __m256d c_pow2_4_c1 = Set1(-6.662345431318903025772700509142101007024e-2);
            static constexpr __m256d c_pow2_4_c2 = Set1(-1.113930183733997141783833210977614459718e-2);
            static constexpr __m256d c_pow2_4_c3 = Set1(-1.461237960055165634948236381176861135936e-3);
            static constexpr __m256d c_pow2_4_c4 = Set1(-2.171502549397975884526363201015788921121e-4);

            static constexpr __m256d c_pow2_2_c0 = Set1(-3.069678791803394491901405992213472390777e-1);
            static constexpr __m256d c_pow2_2_c1 = Set1(-6.558811624324781017147952441210509604385e-2);
            static constexpr __m256d c_pow2_2_c2 = Set1(-1.355574723481491770403079319055785445381e-2);

            static constexpr __m256d c_half_f = Set1(0.5);
            static constexpr __m256d c_1_f = Set1(1.);
            static constexpr __m256d c_2_f = Set1(2.);
            static constexpr __m256d c_1_over_ln_2 = Set1(1.442695040888963407359924681001892137426);
            static constexpr __m256d c_neg_1_over_ln_2 = Set1(-1.442695040888963407359924681001892137426);
            static constexpr __m256d c_neg_2_over_ln_2 = Set1(-2. * 1.442695040888963407359924681001892137426);

            static constexpr __m256d c_ln_2 = Set1(6.931471805599453094172321214581765680755e-1);
            static constexpr __m256i c_denorm_const = constexpr_mm256_set1_epi64x(1023);
            static constexpr __m256d c_inf_i = Set1(std::numeric_limits<double>::infinity());
            static constexpr __m256i c_all_ones = constexpr_mm256_set1_epi64x(-1);
            static constexpr __m256i c_mantissa_mask = constexpr_mm256_set1_epi64x(int64_t(0x00'0F'FF'FF'FF'FF'FF'FF));

            static constexpr __m256d c_max_pow_2 = Set1(1024.);
            static constexpr __m256d c_min_denorm_exp_f = Set1(-1075.);
            static constexpr __m256d c_min_norm_exp_f = Set1(-1023.);
            static constexpr __m256i c_denorm_offset = constexpr_mm256_set1_epi64x(-1022);

            static constexpr int ci_bits_in_mantissa = 52;
            static constexpr size_t ci_denorm_const = size_t(ci_bits_in_mantissa + 1023) << ci_bits_in_mantissa;
            static constexpr __m256d c_neg_f_bits_in_mantissa = Set1(-double(ci_bits_in_mantissa));
            static constexpr __m256d c_neg_f_infinity = Set1(-std::numeric_limits<double>::infinity());
            static constexpr __m256d c_neg_f_zero = Set1(-0.f);
        };
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <size_t I_NOfElements, size_t I_ElementSize, class P_CFT>
    FORCE_INLINE auto EvalPolynomial(const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_x, const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_x_2,
                                     const P_CFT c0_in, const P_CFT c1_in, const P_CFT c2_in, const P_CFT c3_in, const P_CFT c4_in, const P_CFT c5_in, const P_CFT c6_in, const P_CFT c7_in) noexcept {
        using t = S_SIMDV<I_NOfElements, I_ElementSize>;
        using t_f = typename t::t_f;

        //  ((c7*x + c6)*x^2 + (c5*x + c4))*x^4  +  (c3*x + c2)*x^2 + (c1*x + c0) - Estrin's scheme
        auto c0 = t::FMADD(t::Cast(c1_in), ymm_x, t::Cast(c0_in));
        auto c1 = t::FMADD(t::Cast(c3_in), ymm_x, t::Cast(c2_in));
        auto c2 = t::FMADD(t::Cast(c5_in), ymm_x, t::Cast(c4_in));
        auto c3 = t::FMADD(t::Cast(c7_in), ymm_x, t::Cast(c6_in));

        t_f ymm_x_4 = t::Mul(ymm_x_2, ymm_x_2);
        c0 = t::FMADD(c1, ymm_x_2, c0);
        c1 = t::FMADD(c3, ymm_x_2, c2);

        return t::FMADD(c1, ymm_x_4, c0);
    }
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <size_t I_NOfElements, size_t I_ElementSize, class P_CFT>
    FORCE_INLINE auto EvalPolynomial(const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_x, const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_x_2,
                                     const P_CFT c0_in, const P_CFT c1_in, const P_CFT c2_in, const P_CFT c3_in, const P_CFT c4_in, const P_CFT c5_in, const P_CFT c6_in) noexcept {
        using t = S_SIMDV<I_NOfElements, I_ElementSize>;
        using t_f = typename t::t_f;

        //  ( (c6*x + c5)*x + c4))*x^4  +  (c3*x + c2)*x^2 + (c1*x + c0) - Estrin's scheme
        auto c0 = t::FMADD(t::Cast(c1_in), ymm_x, t::Cast(c0_in));
        auto c1 = t::FMADD(t::Cast(c3_in), ymm_x, t::Cast(c2_in));
        auto c2 = t::FMADD(t::Cast(c6_in), ymm_x, t::Cast(c5_in));

        t_f ymm_x_4 = t::Mul(ymm_x_2, ymm_x_2);
        c0 = t::FMADD(c1, ymm_x_2, c0);
        c1 = t::FMADD(c2, ymm_x, t::Cast(c4_in));

        return t::FMADD(c1, ymm_x_4, c0);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <size_t I_NOfElements, size_t I_ElementSize, class P_CFT>
    FORCE_INLINE auto EvalPolynomial(const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_x, const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_x_2,
                                     const P_CFT c0_in, const P_CFT c1_in, const P_CFT c2_in, const P_CFT c3_in, const P_CFT c4_in, const P_CFT c5_in) noexcept {
        using t = S_SIMDV<I_NOfElements, I_ElementSize>;

        //  (c5*x + c4)*x^4  +  (c3*x + c2)*x^2 + (c1*x + c0)
        auto c2 = t::FMADD(t::Cast(c5_in), ymm_x, t::Cast(c4_in));
        auto c1 = t::FMADD(t::Cast(c3_in), ymm_x, t::Cast(c2_in));
        auto c0 = t::FMADD(t::Cast(c1_in), ymm_x, t::Cast(c0_in));

        c1 = t::FMADD(c2, ymm_x_2, c1);
        return t::FMADD(c1, ymm_x_2, c0);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <size_t I_NOfElements, size_t I_ElementSize, class P_CFT>
    FORCE_INLINE auto EvalPolynomial(const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_x, const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_x_2,
                                     const P_CFT c0, const P_CFT c1, const P_CFT c2, const P_CFT c3, const P_CFT c4) noexcept {
        using t = S_SIMDV<I_NOfElements, I_ElementSize>;
        using t_f = typename t::t_f;

        t_f ymm_res = t::FMADD(t::Cast(c2), ymm_x, t::Cast(c1));
        t_f ymm_res_h = t::FMADD(t::Cast(c4), ymm_x, t::Cast(c3));

        ymm_res = t::FMADD(ymm_res_h, ymm_x_2, ymm_res); // ((c4*x + c3)*x^2 + (c2*x + c1))*x + c0 - slower, but slightly more accurate
        return t::FMADD(ymm_res, ymm_x, t::Cast(c0));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <size_t I_NOfElements, size_t I_ElementSize, class P_CFT>
    FORCE_INLINE auto EvalPolynomial(const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_x, const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_x_2,
                                     const P_CFT c0_in, const P_CFT c1_in, const P_CFT c2_in, const P_CFT c3_in) noexcept {
        using t = S_SIMDV<I_NOfElements, I_ElementSize>;

        //  (c3*x + c2)*x^2 + (c1*x + c0)
        auto c0 = t::FMADD(t::Cast(c1_in), ymm_x, t::Cast(c0_in));
        auto c1 = t::FMADD(t::Cast(c3_in), ymm_x, t::Cast(c2_in));
        return t::FMADD(c1, ymm_x_2, c0);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <size_t I_NOfElements, size_t I_ElementSize, class P_CFT>
    FORCE_INLINE auto EvalPolynomial(const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_x, const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_x_2,
                                     const P_CFT c0, const P_CFT c1, const P_CFT c2) noexcept {
        using t = S_SIMDV<I_NOfElements, I_ElementSize>;
        using t_f = typename t::t_f;

        t_f ymm_res = t::FMADD(t::Cast(c1), ymm_x, t::Cast(c0));
        return t::FMADD(t::Cast(c2), ymm_x_2, ymm_res); //c2*x^2 + (c1*x + c0)
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <class P_Deriv, size_t I_ElementSize, bool I_PassToValue>
    struct S_Performer;

    //#######################################################################################################################################################################
    //It seems reads are translated into the unaligned instructions, even for the aligned intrinsics, so only leaving the customization for the output alignment
    struct S_MemCpyPerformer {
        enum : size_t {
            c_max_register_width = 32
        };

        enum : size_t {
            c_batch_size = 512
        };
        template <size_t I_BatchSize, bool I_Int, bool I_OutAligned>
        FORCE_INLINE void Run(const void* from, void* to) const noexcept {
            static_assert(I_BatchSize <= 32 && HighestPowerOf2(I_BatchSize) == I_BatchSize, "Only small power of 2 supported");
            if constexpr (I_Int) {
                if constexpr (I_BatchSize == 0)
                    ;
                else if constexpr (I_BatchSize == 1)
                    *((int8_t*)to) = *((int8_t*)from);
                else if constexpr (I_BatchSize == 2)
                    *((int16_t*)to) = *((int16_t*)from);
                else if constexpr (I_BatchSize == 4)
                    *((int32_t*)to) = *((int32_t*)from);
                else if constexpr (I_BatchSize == 8)
                    *((int64_t*)to) = *((int64_t*)from);
                else if constexpr (I_BatchSize == 16) {
                    if constexpr (I_OutAligned)
                        _mm_store_si128((__m128i*)(to), _mm_loadu_si128((const __m128i*)(from)));
                    else
                        _mm_storeu_si128((__m128i*)(to), _mm_loadu_si128((const __m128i*)(from)));
                } else if constexpr (I_BatchSize == 32) {
                    if constexpr (I_OutAligned)
                        _mm256_store_si256((__m256i*)(to), _mm256_loadu_si256((const __m256i*)(from)));
                    else
                        _mm256_storeu_si256((__m256i*)(to), _mm256_loadu_si256((const __m256i*)(from)));
                }
            } else {
                if constexpr (I_BatchSize == 0)
                    ;
                else if constexpr (I_BatchSize == 4)
                    *((float*)to) = *((float*)from);
                else if constexpr (I_BatchSize == 8)
                    *((double*)to) = *((double*)from);
                else if constexpr (I_BatchSize == 16) {
                    if constexpr (I_OutAligned)
                        _mm_store_ps((float*)(to), _mm_loadu_ps((const float*)(from)));
                    else
                        _mm_storeu_ps((float*)(to), _mm_loadu_ps((const float*)(from)));
                } else if constexpr (I_BatchSize == 32) {
                    if constexpr (I_OutAligned)
                        _mm256_store_ps((float*)(to), _mm256_loadu_ps((const float*)(from)));
                    else
                        _mm256_storeu_ps((float*)(to), _mm256_loadu_ps((const float*)(from)));
                }
            }
        }
    };

    //#######################################################################################################################################################################

    struct S_MemSetPerformer {
        enum : size_t {
            c_max_register_width = 32
        };

        enum : size_t {
            c_batch_size = 512
        };

        template <size_t I_BatchSize, bool I_Int, bool I_OutAligned>
        FORCE_INLINE void Run(const void*, void* to, const __m256i val) const noexcept {
            static_assert(I_BatchSize <= 32 && HighestPowerOf2(I_BatchSize) == I_BatchSize, "Only small power of 2 supported");
            if constexpr (I_Int) {
                if constexpr (I_BatchSize == 0)
                    ;
                else if constexpr (I_BatchSize == 1)
                    *(int8_t*)(to) = (int8_t)(_mm_cvtsi128_si32(_mm256_castsi256_si128(val)) & 255);
                else if constexpr (I_BatchSize == 2)
                    _mm_storeu_si16((__m128i*)(to), _mm256_castsi256_si128(val));
                else if constexpr (I_BatchSize == 4)
                    _mm_storeu_si32((__m128i*)(to), _mm256_castsi256_si128(val));
                else if constexpr (I_BatchSize == 8)
                    _mm_storeu_si64((__m128i*)(to), _mm256_castsi256_si128(val));
                else if constexpr (I_BatchSize == 16) {
                    if constexpr (I_OutAligned)
                        _mm_store_si128((__m128i*)(to), _mm256_castsi256_si128(val));
                    else
                        _mm_storeu_si128((__m128i*)(to), _mm256_castsi256_si128(val));
                } else if constexpr (I_BatchSize == 32) {
                    if constexpr (I_OutAligned)
                        _mm256_store_si256((__m256i*)(to), val);
                    else
                        _mm256_storeu_si256((__m256i*)(to), val);
                }
            } else {
                if constexpr (I_BatchSize == 0)
                    ;
                else if constexpr (I_BatchSize == 4)
                    _mm_store_ss((float*)(to), _mm256_castps256_ps128(_mm256_castsi256_ps(val)));
                else if constexpr (I_BatchSize == 8)
                    _mm_store_sd((double*)(to), _mm256_castpd256_pd128(_mm256_castsi256_pd(val)));
                else if constexpr (I_BatchSize == 16) {
                    if constexpr (I_OutAligned)
                        _mm_store_ps((float*)(to), _mm256_castps256_ps128(_mm256_castsi256_ps(val)));
                    else
                        _mm_storeu_ps((float*)(to), _mm256_castps256_ps128(_mm256_castsi256_ps(val)));
                } else if constexpr (I_BatchSize == 32) {
                    if constexpr (I_OutAligned)
                        _mm256_store_ps((float*)(to), _mm256_castsi256_ps(val));
                    else
                        _mm256_storeu_ps((float*)(to), _mm256_castsi256_ps(val));
                }
            }
        }
    };

    //#######################################################################################################################################################################

    template <size_t I_ElementSize>
    struct S_MulByConstAndAssignPerformer: public S_Performer<S_MulByConstAndAssignPerformer<I_ElementSize>, I_ElementSize, false> {
        enum : size_t {
            c_max_register_width = 32
        };
        enum : size_t {
            c_batch_size = 512
        };

        template <size_t I_NOfElements, bool I_Int, class P_CFT>
        FORCE_INLINE auto Calc(const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_from, const P_CFT ymm_v) const noexcept {
            static_assert(I_Int == false);
            using t = S_SIMDV<I_NOfElements, I_ElementSize>;
            return t::Mul(ymm_from, t::Cast(ymm_v));
        }
    };

    //#######################################################################################################################################################################

    template <size_t I_ElementSize>
    struct S_XMul1MinusXPerformer: public S_Performer<S_XMul1MinusXPerformer<I_ElementSize>, I_ElementSize, false> { // x * (1. - x)
        enum : size_t {
            c_max_register_width = 32
        };
        enum : size_t {
            c_batch_size = 512
        };

        template <size_t I_NOfElements, bool I_Int, class P_CFT>
        FORCE_INLINE auto Calc(const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_from, const P_CFT ymm_1_f) const noexcept {
            static_assert(I_Int == false);
            using t = S_SIMDV<I_NOfElements, I_ElementSize>;
            return t::Mul(ymm_from, t::Sub(t::Cast(ymm_1_f), ymm_from));
        }
    };
    //#######################################################################################################################################################################

    template <size_t I_ElementSize>
    struct S_1MinusX2Performer: public S_Performer<S_1MinusX2Performer<I_ElementSize>, I_ElementSize, false> { // 1. - x*x
        enum : size_t {
            c_max_register_width = 32
        };
        enum : size_t {
            c_batch_size = 512
        };

        template <size_t I_NOfElements, bool I_Int, class P_CFT>
        FORCE_INLINE auto Calc(const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_from, const P_CFT ymm_1_f) const noexcept {
            static_assert(I_Int == false);
            using t = S_SIMDV<I_NOfElements, I_ElementSize>;
            return t::FNMADD(ymm_from, ymm_from, t::Cast(ymm_1_f));
        }
    };

    //#######################################################################################################################################################################

    template <size_t I_ElementSize>
    struct S_MulByConstAndAddPerformer: public S_Performer<S_MulByConstAndAddPerformer<I_ElementSize>, I_ElementSize, true> {
        enum : size_t {
            c_max_register_width = 32
        };
        enum : size_t {
            c_batch_size = 512
        };

        template <size_t I_NOfElements, bool I_Int, class P_CFT>
        FORCE_INLINE auto Calc(const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_from, const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_to, const P_CFT ymm_v) const noexcept {
            static_assert(I_Int == false);
            using t = S_SIMDV<I_NOfElements, I_ElementSize>;
            return t::FMADD(ymm_from, t::Cast(ymm_v), ymm_to);
        }
    };

    //#######################################################################################################################################################################

    template <size_t I_ElementSize>
    struct S_AddPerformer: public S_Performer<S_AddPerformer<I_ElementSize>, I_ElementSize, true> {
        enum : size_t {
            c_max_register_width = 32
        };
        enum : size_t {
            c_batch_size = 512
        };

        template <size_t I_NOfElements, bool>
        FORCE_INLINE auto Calc(const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_from, const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_to) const noexcept {
            return S_SIMDV<I_NOfElements, I_ElementSize>::Add(ymm_from, ymm_to);
        }
    };

    //#######################################################################################################################################################################

    template <size_t I_ElementSize>
    struct S_SubPerformer: public S_Performer<S_SubPerformer<I_ElementSize>, I_ElementSize, true> {
        enum : size_t {
            c_max_register_width = 32
        };
        enum : size_t {
            c_batch_size = 512
        };

        template <size_t I_NOfElements, bool>
        FORCE_INLINE auto Calc(const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_from, const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_to) const noexcept {
            return S_SIMDV<I_NOfElements, I_ElementSize>::Sub(ymm_to, ymm_from);
        }
    };

    //#######################################################################################################################################################################

    template <size_t I_ElementSize>
    struct S_AddSquaredPerformer: public S_Performer<S_AddSquaredPerformer<I_ElementSize>, I_ElementSize, true> {
        enum : size_t {
            c_max_register_width = 32
        };
        enum : size_t {
            c_batch_size = 512
        };

        template <size_t I_NOfElements, bool>
        FORCE_INLINE auto Calc(const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_from, const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_to) const noexcept {
            return S_SIMDV<I_NOfElements, I_ElementSize>::FMADD(ymm_from, ymm_from, ymm_to);
        }
    };

    //#######################################################################################################################################################################

#pragma warning(push)
#pragma warning(disable : 4101 4100)
    template <size_t I_NOfElements, size_t I_ElementSize, bool I_Exact, class P_CTF, class P_CTI, typename... P_Params>
    FORCE_INLINE auto Pow2V(const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_x_orig, const P_CTF c_one_in, const P_CTF c_max_pow_2_in, const P_CTF c_min_denorm_exp_in,
                            const P_CTF c_min_norm_exp_in, const P_CTI c_denorm_offset_in, const P_CTI c_denorm_const_in, const P_Params... poly_coeffs) {
        using t = S_SIMDV<I_NOfElements, I_ElementSize>;
        using t_f = typename t::t_f;
        using t_i = typename t::t_i;
        const t_f c_one(t::Cast(c_one_in));
        using c = NFastOpsDetail::S_Constants<I_ElementSize>;

        t_f ymm_x = t::Min(ymm_x_orig, t::Cast(c_max_pow_2_in));
        if constexpr (I_Exact)
            ymm_x = t::Max(ymm_x, t::Cast(c_min_denorm_exp_in));
        else
            ymm_x = t::Max(ymm_x, t::Cast(c_min_norm_exp_in));

        t_f ymm_xf = t::Floor(ymm_x); //seems fine as float can represent all the integers up to a 2^(mantissa bits+1)+1 exactly
        ymm_x = t::Sub(ymm_x, ymm_xf);
        t_i ymm_xfi = t::CVTF2I(ymm_xf);

        t_f ymm_x_2 = t::Mul(ymm_x, ymm_x);
        t_f ymm_res = EvalPolynomial<I_NOfElements, I_ElementSize>(ymm_x, ymm_x_2, poly_coeffs...);

        t_f ymm_x_by_1_minus_x = t::Sub(ymm_x, ymm_x_2);
        ymm_res = t::FMADD(ymm_res, ymm_x_by_1_minus_x, ymm_x);
        ymm_res = t::Add(ymm_res, c_one); //adding ymm_x and 1 separately in the end improves accuracy

        if constexpr (I_Exact) {
            const t_i c_denorm_offset(t::Cast(c_denorm_offset_in));
            t_i ymm_denorm_flag = t::Sub(ymm_xfi, c_denorm_offset);

            if (!t::TestZF(t::CastF(ymm_denorm_flag), t::CastF(ymm_denorm_flag))) {
                ymm_xfi = t::CastI(t::BlendVF(t::CastF(ymm_xfi), t::CastF(c_denorm_offset), t::CastF(ymm_denorm_flag)));
                t_i ymm_denorm_i = t::Add(ymm_denorm_flag, t::Cast(c_denorm_const_in));
                ymm_denorm_i = t::SLLI(ymm_denorm_i, c::ci_bits_in_mantissa);
                t_f ymm_denorm = t::BlendVF(c_one, t::CastF(ymm_denorm_i), t::CastF(ymm_denorm_flag));
                ymm_xfi = t::SLLI(ymm_xfi, c::ci_bits_in_mantissa);
                ymm_res = t::CastF(t::Add(ymm_xfi, t::CastI(ymm_res)));
                ymm_res = t::Mul(ymm_res, ymm_denorm);
            } else {
                ymm_xfi = t::SLLI(ymm_xfi, c::ci_bits_in_mantissa);
                ymm_res = t::CastF(t::Add(ymm_xfi, t::CastI(ymm_res)));
            }
            t_f ymm_nan_mask = t::template CmpF<_CMP_EQ_OQ>(ymm_x_orig, ymm_x_orig); //nan's are not equal to themselves, infinities are, so here we can get a perfect behavior: nan's stay nan's, -inf -> 0.f, +inf -> +inf
            ymm_res = t::BlendVF(ymm_x_orig, ymm_res, ymm_nan_mask);
        } else {
            ymm_xfi = t::SLLI(ymm_xfi, c::ci_bits_in_mantissa);
            ymm_res = t::CastF(t::Add(ymm_xfi, t::CastI(ymm_res)));
        }
        return ymm_res;
    }
#pragma warning(pop)

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <size_t I_ElementSize, bool I_Exact = true>
    struct S_Pow: public S_Performer<S_Pow<I_ElementSize, I_Exact>, I_ElementSize, false> {
        constexpr static size_t c_max_register_width = 32; //in bytes
        constexpr static size_t c_batch_size = 128;        //in bytes

        template <size_t I_NOfElements, bool, class P_CTF, typename... P_Params>
        FORCE_INLINE auto Calc(const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_x, const P_CTF ymm_mul_factor, const P_Params... params) const noexcept {
            using t = S_SIMDV<I_NOfElements, I_ElementSize>;
            auto ymm_x_orig = t::Mul(ymm_x, t::Cast(ymm_mul_factor));
            return Pow2V<I_NOfElements, I_ElementSize, I_Exact>(ymm_x_orig, params...);
        }
    };

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <size_t I_ElementSize, bool I_Exact = true>
    struct S_Sigmoid: public S_Performer<S_Sigmoid<I_ElementSize, I_Exact>, I_ElementSize, false> {
        constexpr static size_t c_max_register_width = 32; //in bytes
        constexpr static size_t c_batch_size = 128;        //in bytes

        template <size_t I_NOfElements, bool I_Int, class P_CTF, typename... P_Params>
        FORCE_INLINE auto Calc(const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_x, const P_CTF ymm_mul_factor, const P_CTF ymm_1_f, const P_Params... params) const noexcept {
            static_assert(I_Int == false);
            using t = S_SIMDV<I_NOfElements, I_ElementSize>;

            auto ymm_x_orig = t::Mul(ymm_x, t::Cast(ymm_mul_factor));
            const auto exp_x = Pow2V<I_NOfElements, I_ElementSize, I_Exact>(ymm_x_orig, ymm_1_f, params...);
            return t::Div(t::Cast(ymm_1_f), t::Add(exp_x, t::Cast(ymm_1_f)));
        }
    };

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <size_t I_ElementSize, bool I_Exact = true>
    struct S_Tanh: public S_Performer<S_Tanh<I_ElementSize, I_Exact>, I_ElementSize, false> {
        enum : size_t {
            c_max_register_width = 32
        };
        enum : size_t {
            c_batch_size = 128
        };

        template <size_t I_NOfElements, bool I_Int, class P_CTF, typename... P_Params>
        FORCE_INLINE auto Calc(const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_x, const P_CTF ymm_mul_factor, const P_CTF ymm_2_f, const P_CTF ymm_1_f, const P_Params... params) const noexcept {
            static_assert(I_Int == false);
            using t = S_SIMDV<I_NOfElements, I_ElementSize>;

            auto ymm_x_orig = t::Mul(ymm_x, t::Cast(ymm_mul_factor));
            const auto exp_x = Pow2V<I_NOfElements, I_ElementSize, I_Exact>(ymm_x_orig, ymm_1_f, params...);
            const auto res = t::Div(t::Cast(ymm_2_f), t::Add(exp_x, t::Cast(ymm_1_f)));
            return t::Sub(res, t::Cast(ymm_1_f));
        }
    };

    //#######################################################################################################################################################################

    template <size_t I_NOfElements, size_t I_ElementSize, bool I_Exact, class P_CTF, typename... P_Params>
    FORCE_INLINE typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f LnV(typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_x_orig, const P_CTF c_one_in, const P_Params... poly_coeffs) {
        using t = S_SIMDV<I_NOfElements, I_ElementSize>;
        using t_f = typename t::t_f;
        using t_i = typename t::t_i;
        using c = NFastOpsDetail::S_Constants<I_ElementSize>;

        if constexpr (I_NOfElements == 1) {
            if constexpr (I_ElementSize == 4)
                ymm_x_orig = t::OrF(ymm_x_orig, t::Set(1.f, 1.f, 1.f, 0.f));
            else
                ymm_x_orig = t::OrF(ymm_x_orig, t::Set(1., 0.));
        } else if constexpr (I_NOfElements == 2 && I_ElementSize == 4)
            ymm_x_orig = t::OrF(ymm_x_orig, t::Set(1.f, 1.f, 0.f, 0.f));

        t_f ymm_zero_f = t::SetZeroF();
        t_i ymm_e = t::SRLI(t::CastI(ymm_x_orig), c::ci_bits_in_mantissa);

        t_f ymm_x = t::AndF(ymm_x_orig, t::CastF(t::Cast(c::c_mantissa_mask)));
        t_f ymm_e_float;

        if constexpr (I_Exact) {
            t_f ymm_denorm_mask = t::CastF(t::CmpEqI(ymm_e, t::CastI(ymm_zero_f)));
            if (!t::TestZF(ymm_denorm_mask, ymm_denorm_mask)) {
                t_f ymm_denorm_coef = t::BlendVF(t::Cast(c_one_in), t::Cast(c::Set1(BitCast<typename c::Source>(c::ci_denorm_const))), ymm_denorm_mask);
                ymm_x = t::Mul(ymm_x_orig, ymm_denorm_coef);
                ymm_e_float = t::BlendVF(ymm_zero_f, t::Cast(c::c_neg_f_bits_in_mantissa), ymm_denorm_mask);
                ymm_e = t::SRLI(t::CastI(ymm_x), c::ci_bits_in_mantissa);
                ymm_e = t::Sub(ymm_e, t::Cast(c::c_denorm_const));
                ymm_e_float = t::Add(ymm_e_float, t::CVTI2F(ymm_e));
                ymm_x = t::AndF(ymm_x, t::CastF(t::Cast(c::c_mantissa_mask)));
            } else {
                ymm_e = t::Sub(ymm_e, t::Cast(c::c_denorm_const));
                ymm_e_float = t::CVTI2F(ymm_e);
            }
        } else {
            ymm_e = t::Sub(ymm_e, t::Cast(c::c_denorm_const));
            ymm_e_float = t::CVTI2F(ymm_e);
        }

        ymm_x = t::OrF(ymm_x, t::Cast(c::c_half_f));
        t_f ymm_blend_mask = t::template CmpFM<_CMP_GT_OQ>(ymm_x, t::Cast(c::c_ln_range_threshold));
        ymm_x = t::Add(ymm_x, t::AndNotF(ymm_blend_mask, ymm_x));
        ymm_e_float = t::Add(ymm_e_float, t::AndF(ymm_blend_mask, t::Cast(c_one_in)));

        const t_f ymm_x_minus_1 = t::Sub(ymm_x, t::Cast(c_one_in));
        const t_f ymm_x_minus_1_squared = t::Mul(ymm_x_minus_1, ymm_x_minus_1);
        t_f ymm_res = EvalPolynomial<I_NOfElements, I_ElementSize>(ymm_x_minus_1, ymm_x_minus_1_squared, poly_coeffs...);

        //(r*(x+c1t) - c1h)*(x+c0t)+c2h = r*(x+c1t)*(x+c0t) - c1h*(x+c0t) + c2h = r*x*x + r*(x*(c1t+c0t) + c1t*c0t) - (c1h*x + c1h*c0t - c2h)
        //c1t+c0t == c1t*c0t => r*x*x + r*(c1t*c0t)*(x + 1) - (c1h*x + c1h*c0t - c2h)

        const t_f l = t::FMADD(ymm_x, t::Cast(c::c1t_plus_c0t), ymm_x_minus_1_squared);
        ymm_res = t::FMSUB(ymm_res, l, t::Mul(ymm_x_minus_1, t::Cast(c::c1h)));
        ymm_res = t::Sub(ymm_res, t::Cast(c::c1h_by_c0t_minus_c2h));
        ymm_res = t::FMADD(ymm_res, ymm_x_minus_1, t::Mul(ymm_e_float, t::Cast(c::c_ln_2)));

        t_f ymm_spec_case_mask = t::CastF(t::CmpEqI(t::CastI(ymm_x_orig), t::Cast(c::CastPf(c::c_inf_i)))); // if input is = inf
        t_f ymm_has_correct_values = t::template CmpFM<_CMP_GT_OQ>(ymm_x_orig, ymm_zero_f);
        ymm_has_correct_values = t::AndNotF(ymm_spec_case_mask, ymm_has_correct_values);

        if (!t::TestCF(ymm_has_correct_values, t::Cast(c::CastSi(c::c_all_ones)))) {
            t_f ymm_zero_mask = t::CmpEqF(ymm_x_orig, ymm_zero_f); // using float comparison to force -0.f to return -inf, as recommended by IEEE 754-2008
            ymm_spec_case_mask = t::OrF(ymm_spec_case_mask, ymm_zero_mask);

            t_f ymm_res_spec_case = t::BlendVF(t::CastF(t::Cast(c::CastPf(c::c_inf_i))), t::Cast(c::c_neg_f_infinity), ymm_zero_mask);
            ymm_res_spec_case = t::BlendVF(ymm_x_orig, ymm_res_spec_case, ymm_spec_case_mask);

            t_f ymm_inp_qnan_mask = t::template CmpFM<_CMP_EQ_OQ>(ymm_x_orig, ymm_x_orig); // getting an input's nan's mask (inverted)
            ymm_inp_qnan_mask = t::XorF(ymm_inp_qnan_mask, t::Cast(c::CastSi(c::c_all_ones)));        // un-inverting input's nan's mask
            ymm_spec_case_mask = t::OrF(ymm_spec_case_mask, ymm_inp_qnan_mask);            // adding the positions of the input nans' to the mask, so they are blended in the result as well
            t_f ymm_res_qnan_mask = t::AndF(ymm_x_orig, t::Cast(c::c_neg_f_zero));

            ymm_res_qnan_mask = t::CastF(t::SRAI32(t::CastI(ymm_res_qnan_mask), (int(I_ElementSize) * 8 - c::ci_bits_in_mantissa))); //shifting right to get a quiet NAN for the negatives
            ymm_res = t::Add(ymm_res, ymm_res_qnan_mask);                                                                            //NAN + any number is NAN, so OK to add here; t::OrF is faster, but you get a non-standard NAN then

            ymm_res = t::BlendVF(ymm_res, ymm_res_spec_case, ymm_spec_case_mask);
        }
        return ymm_res;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <size_t I_ElementSize, bool I_Exact = true>
    struct S_Ln: public S_Performer<S_Ln<I_ElementSize, I_Exact>, I_ElementSize, false> {
        enum : size_t {
            c_max_register_width = 32
        };
        enum : size_t {
            c_batch_size = 64
        };

        template <size_t I_NOfElements, bool I_Int, typename... P_Params>
        FORCE_INLINE auto Calc(const typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_x, const P_Params... params) const noexcept {
            static_assert(I_Int == false);
            return LnV<I_NOfElements, I_ElementSize, I_Exact>(ymm_x, params...);
        }
    };

    //#######################################################################################################################################################################

    template <class P_Deriv, size_t I_ElementSize, bool I_PassToValue>
    struct S_Performer {
        template <size_t I_BatchSize, bool I_Int, bool I_OutAligned, typename... P_Params>
        FORCE_INLINE void Run(const void* from, void* to, const P_Params... params) const noexcept {
            constexpr size_t c_n_of_elements = I_ElementSize == 4 ? (I_BatchSize >> 2) : (I_BatchSize >> 3);
            if constexpr (bool(c_n_of_elements)) {
                using t = S_SIMDV<c_n_of_elements, I_ElementSize>;
                using t_type = std::conditional_t<I_ElementSize == 4, float, double>;

                if constexpr (I_PassToValue) {
                    const auto ymm_res = static_cast<const P_Deriv*>(this)->P_Deriv::template Calc<c_n_of_elements, I_Int>(t::LoadU((const t_type*)from), t::LoadU((const t_type*)to), params...);
                    if constexpr (I_OutAligned)
                        t::Store((t_type*)to, ymm_res);
                    else
                        t::StoreU((t_type*)to, ymm_res);
                } else {
                    const auto ymm_res = static_cast<const P_Deriv*>(this)->P_Deriv::template Calc<c_n_of_elements, I_Int>(t::LoadU((const t_type*)from), params...);
                    if constexpr (I_OutAligned)
                        t::Store((t_type*)to, ymm_res);
                    else
                        t::StoreU((t_type*)to, ymm_res);
                }
            }
        }
    };

    //#######################################################################################################################################################################

    //I_CurPower is also a reverse flag
    template <size_t I_Bytes, bool I_Int, bool I_OutAligned, size_t I_CurPower, class P_PerformFunc, typename... P_Params>
    FORCE_INLINE void AVXCopyConstImpl(P_PerformFunc&& performer, const void* from, void* to, P_Params... params) noexcept {
        static_assert(I_Int || !(I_Bytes & 3), "Can't use float-point domain to copy a number of bytes not divisible by 4");
        using TT = std::remove_reference_t<P_PerformFunc>;
        constexpr size_t e1 = (I_CurPower < TT::c_max_register_width ? (I_Bytes & I_CurPower) : TT::c_max_register_width);
        constexpr size_t e2 = MyMin(TT::c_max_register_width, HighestPowerOf2(I_Bytes));
        constexpr size_t batch_size = I_CurPower ? e1 : e2;
        static_assert(HighestPowerOf2(batch_size) == batch_size);

        if constexpr (bool(batch_size)) {
            if constexpr (bool(I_CurPower))
                to = (int8_t*)to - batch_size, from = (int8_t*)from - batch_size;
            performer.template Run<batch_size, I_Int, I_OutAligned>(from, to, params...);
        }
        if constexpr (I_Bytes > batch_size) {
            if constexpr (bool(I_CurPower)) {
                constexpr size_t c_next_power = I_CurPower < performer.c_max_register_width ? (I_CurPower << 1) : performer.c_max_register_width;
                AVXCopyConstImpl<I_Bytes - batch_size, I_Int, I_OutAligned, c_next_power>(performer, (int8_t*)from, (int8_t*)to, params...);
            } else
                AVXCopyConstImpl<I_Bytes - batch_size, I_Int, I_OutAligned, size_t(0)>(performer, (int8_t*)from + batch_size, (int8_t*)to + batch_size, params...);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <size_t I_Bytes, bool I_Int, bool I_OutAligned, class P_PerformFunc, typename... P_Params>
    FORCE_INLINE void AVXReverseCopyConst(P_PerformFunc&& performer, const void* from, void* to, P_Params... params) noexcept {
        AVXCopyConstImpl<I_Bytes, I_Int, I_OutAligned, (I_Int ? 1 : 4)>(performer, (int8_t*)from + I_Bytes, (int8_t*)to + I_Bytes, params...);
    }
    template <size_t I_Bytes, bool I_Int, bool I_OutAligned, class P_PerformFunc, typename... P_Params>
    FORCE_INLINE void AVXCopyConst(P_PerformFunc&& performer, const void* from, void* to, P_Params... params) noexcept {
        AVXCopyConstImpl<I_Bytes, I_Int, I_OutAligned, size_t(0)>(performer, (int8_t*)from, (int8_t*)to, params...);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    namespace NFastOpsDetail {
        template <size_t P_MaxBatchSize, size_t P_BatchSize, bool I_Int, bool I_OutAligned, class P_PerformFunc, typename... P_Params>
        FORCE_INLINE void AVXReverseCopyImpl(P_PerformFunc&& performer, const void* from, void* to, size_t n_of_bytes, P_Params... params) noexcept {
            if constexpr (P_BatchSize <= P_MaxBatchSize) {
                static_assert(I_Int || P_BatchSize >= 4);
                if (n_of_bytes & P_BatchSize) {
                    AVXCopyConstImpl<P_BatchSize, I_Int, I_OutAligned, P_BatchSize>(performer, (int8_t*)from, (int8_t*)to, params...);
                    to = (int8_t*)to - P_BatchSize, from = (int8_t*)from - P_BatchSize;
                }
                if constexpr (P_BatchSize < P_MaxBatchSize)
                    AVXReverseCopyImpl<P_MaxBatchSize, (P_BatchSize << 1), I_Int, I_OutAligned>(performer, from, to, n_of_bytes, params...);
            }
        }
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        template <size_t P_BatchSize, bool I_Int, bool I_OutAligned, class P_PerformFunc, typename... P_Params>
        FORCE_INLINE void AVXCopyImpl(P_PerformFunc&& performer, const void* from, void* to, size_t n_of_bytes, P_Params... params) noexcept {
            if constexpr (bool(P_BatchSize)) {
                static_assert(I_Int || P_BatchSize >= 4);
                if (n_of_bytes & P_BatchSize) {
                    AVXCopyConst<P_BatchSize, I_Int, I_OutAligned>(performer, (int8_t*)from, (int8_t*)to, params...);
                    to = (int8_t*)to + P_BatchSize, from = (int8_t*)from + P_BatchSize;
                }
                if constexpr ((I_Int && P_BatchSize > 1) || P_BatchSize > 4)
                    AVXCopyImpl<(P_BatchSize >> 1), I_Int, I_OutAligned>(performer, from, to, n_of_bytes, params...);
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <bool I_Int, int I_OutAligned = -1, class P_PerformFunc, typename... P_Params>
    FORCE_INLINE void AVXReverseCopy(P_PerformFunc&& performer, const void* from, void* to, size_t n_of_bytes, P_Params... params) noexcept {
#pragma warning(push)
#pragma warning(disable : 4101)
        size_t n_of_analigned_bytes;
        if constexpr (I_OutAligned < 0) {
            n_of_analigned_bytes = ((size_t(to) + performer.c_max_register_width - 1) & ~(performer.c_max_register_width - 1)) - size_t(to);
            if (n_of_analigned_bytes >= n_of_bytes) {
                NFastOpsDetail::AVXReverseCopyImpl<performer.c_max_register_width, 1, true, false>(performer, (int8_t*)from + n_of_bytes, (int8_t*)to + n_of_bytes, n_of_bytes, params...);
                return;
            }
            from = (int8_t*)from + n_of_analigned_bytes;
            to = (int8_t*)to + n_of_analigned_bytes;
            n_of_bytes -= n_of_analigned_bytes;
        }
        size_t size = n_of_bytes & ~(performer.c_batch_size - 1);
        constexpr bool c_local_int = I_OutAligned < 0 ? true : I_Int;
        NFastOpsDetail::AVXReverseCopyImpl<(P_PerformFunc::c_batch_size >> 1), (c_local_int ? 1 : 4), c_local_int, (I_OutAligned < 0 ? true : bool(I_OutAligned))>(performer,
                                                                                                                                                                   (int8_t*)from + n_of_bytes, (int8_t*)to + n_of_bytes, n_of_bytes - size, params...);
        for (size_t i = 0; i < size; i += performer.c_batch_size)
            AVXCopyConstImpl<P_PerformFunc::c_batch_size, I_Int, (I_OutAligned < 0 ? true : bool(I_OutAligned)), P_PerformFunc::c_batch_size>(performer, (int8_t*)from + size - i, (int8_t*)to + size - i, params...);

        if constexpr (I_OutAligned < 0)
            NFastOpsDetail::AVXReverseCopyImpl<P_PerformFunc::c_max_register_width, 1, true, false>(performer, (int8_t*)from, (int8_t*)to, n_of_analigned_bytes, params...);
#pragma warning(pop)
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <bool I_Int, int I_OutAligned = -1, class P_PerformFunc, typename... P_Params>
    FORCE_INLINE void AVXCopy(P_PerformFunc&& performer, const void* from, void* to, size_t n_of_bytes, P_Params... params) noexcept {
        static_assert(P_PerformFunc::c_batch_size > 0);
        using TT = std::remove_reference_t<P_PerformFunc>;

        if constexpr (I_OutAligned < 0) {
            size_t n_of_analigned_bytes = ((size_t(to) + performer.c_max_register_width - 1) & ~(performer.c_max_register_width - 1)) - size_t(to);
            if (n_of_analigned_bytes >= n_of_bytes) {
                NFastOpsDetail::AVXCopyImpl<TT::c_max_register_width, true, false>(performer, (int8_t*)from, (int8_t*)to, n_of_bytes, params...);
                return;
            }
            NFastOpsDetail::AVXCopyImpl<TT::c_max_register_width, true, false>(performer, (int8_t*)from, (int8_t*)to, n_of_analigned_bytes, params...);
            from = (int8_t*)from + n_of_analigned_bytes;
            to = (int8_t*)to + n_of_analigned_bytes;
            n_of_bytes -= n_of_analigned_bytes;
        }
        size_t size = n_of_bytes & ~(performer.c_batch_size - 1);
        constexpr bool c_out_aligned = I_OutAligned < 0 ? true : bool(I_OutAligned);
        for (size_t i = 0; i < size; i += performer.c_batch_size)
            AVXCopyConst<P_PerformFunc::c_batch_size, I_Int, c_out_aligned>(performer, (int8_t*)from + i, (int8_t*)to + i, params...);
        NFastOpsDetail::AVXCopyImpl<(P_PerformFunc::c_batch_size >> 1), (I_OutAligned < 0 ? true : I_Int), c_out_aligned>(performer, (int8_t*)from + size, (int8_t*)to + size, n_of_bytes, params...);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <bool I_Int, int I_OutAligned = -1, class P_PerformFunc, typename... P_Params>
    FORCE_INLINE void AVXMove(P_PerformFunc&& performer, const void* from, void* to, size_t n_of_bytes, P_Params... params) noexcept {
        if (to > from && to < (int8_t*)from + n_of_bytes)
            AVXReverseCopy<I_Int, I_OutAligned>(performer, from, to, n_of_bytes, params...);
        else
            AVXCopy<I_Int, I_OutAligned>(performer, from, to, n_of_bytes, params...);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //################################################################################################################################################
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#define AVX_FUNC_NO_T_NO_P(func_name, performer_name)                                                                     \
    template <bool I_OutAligned = false, class P_Type1, class P_Type2>                                                    \
    FORCE_INLINE void func_name(const P_Type1& from, size_t size, P_Type2&& to) noexcept {                                \
        using c = NFastOpsDetail::S_Constants<sizeof(from[0])>;                                                           \
        AVXCopy<false, I_OutAligned>(performer_name<sizeof(from[0])>(), &from[0], &to[0], size * sizeof(to[0]));          \
    }                                                                                                                     \
    template <bool I_OutAligned = false, class P_Type1, class P_Type2>                                                    \
    FORCE_INLINE void func_name##Move(const P_Type1& from, size_t size, P_Type2&& to) noexcept {                          \
        using c = NFastOpsDetail::S_Constants<sizeof(from[0])>;                                                           \
        AVXMove<false, I_OutAligned>(performer_name<sizeof(from[0])>(), &from[0], &to[0], size * sizeof(to[0]));          \
    }                                                                                                                     \
    template <size_t I_Size, bool I_OutAligned = false, class P_Type1, class P_Type2>                                     \
    FORCE_INLINE void func_name##Const(const P_Type1& from, P_Type2&& to) noexcept {                                      \
        using c = NFastOpsDetail::S_Constants<sizeof(from[0])>;                                                           \
        AVXCopyConst<I_Size * sizeof(from[0]), false, I_OutAligned>(performer_name<sizeof(from[0])>(), &from[0], &to[0]); \
    }

    //Visual Studio doesn't support __VA_OPT__ yet, so have to create a separate macro
#define AVX_FUNC_NO_T_P(func_name, performer_name, params)                                                                        \
    template <bool I_OutAligned = false, class P_Type1, class P_Type2>                                                            \
    FORCE_INLINE void func_name(const P_Type1& from, size_t size, P_Type2&& to) noexcept {                                        \
        using c = NFastOpsDetail::S_Constants<sizeof(from[0])>;                                                                   \
        AVXCopy<false, I_OutAligned>(performer_name<sizeof(from[0])>(), &from[0], &to[0], size * sizeof(to[0]), params);          \
    }                                                                                                                             \
    template <bool I_OutAligned = false, class P_Type1, class P_Type2>                                                            \
    FORCE_INLINE void func_name##Move(const P_Type1& from, size_t size, P_Type2&& to) noexcept {                                  \
        using c = NFastOpsDetail::S_Constants<sizeof(from[0])>;                                                                   \
        AVXMove<false, I_OutAligned>(performer_name<sizeof(from[0])>(), &from[0], &to[0], size * sizeof(to[0]), params);          \
    }                                                                                                                             \
    template <size_t I_Size, bool I_OutAligned = false, class P_Type1, class P_Type2>                                             \
    FORCE_INLINE void func_name##Const(const P_Type1& from, P_Type2&& to) noexcept {                                              \
        using c = NFastOpsDetail::S_Constants<sizeof(from[0])>;                                                                   \
        AVXCopyConst<I_Size * sizeof(from[0]), false, I_OutAligned>(performer_name<sizeof(from[0])>(), &from[0], &to[0], params); \
    }
    //Visual Studio doesn't support __VA_OPT__ yet, so have to create a separate macro
#define AVX_FUNC_T_P(func_name, performer_name, params)                                                                           \
    template <bool I_OutAligned = false, class P_Type1, class P_Type2, class P_FT>                                                \
    FORCE_INLINE void func_name(const P_Type1& from, const P_FT v, size_t size, P_Type2&& to) noexcept {                          \
        using c = NFastOpsDetail::S_Constants<sizeof(from[0])>;                                                                   \
        AVXCopy<false, I_OutAligned>(performer_name<sizeof(from[0])>(), &from[0], &to[0], size * sizeof(to[0]), params);          \
    }                                                                                                                             \
    template <bool I_OutAligned = false, class P_Type1, class P_Type2, class P_FT>                                                \
    FORCE_INLINE void func_name##Move(const P_Type1& from, const P_FT v, size_t size, P_Type2&& to) noexcept {                    \
        using c = NFastOpsDetail::S_Constants<sizeof(from[0])>;                                                                   \
        AVXMove<false, I_OutAligned>(performer_name<sizeof(from[0])>(), &from[0], &to[0], size * sizeof(to[0]), params);          \
    }                                                                                                                             \
    template <size_t I_Size, bool I_OutAligned = false, class P_Type1, class P_Type2, class P_FT>                                 \
    FORCE_INLINE void func_name##Const(const P_Type1& from, const P_FT v, P_Type2&& to) noexcept {                                \
        using c = NFastOpsDetail::S_Constants<sizeof(from[0])>;                                                                   \
        AVXCopyConst<I_Size * sizeof(from[0]), false, I_OutAligned>(performer_name<sizeof(from[0])>(), &from[0], &to[0], params); \
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    template <bool I_Int, int I_OutAligned = -1, class P_Type1, class P_Type2>
    FORCE_INLINE void AVXMemMove(const P_Type1& from, size_t size, P_Type2&& to) noexcept {
        static_assert(std::is_same<std::remove_cv_t<std::remove_pointer_t<decltype(&from[0])>>, std::remove_cv_t<std::remove_pointer_t<decltype(&to[0])>>>::value);
        AVXMove<I_Int, I_OutAligned>(S_MemCpyPerformer(), &from[0], &to[0], size * sizeof(from[0]));
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    template <bool I_Int, int I_OutAligned = -1, class P_Type1, class P_Type2>
    FORCE_INLINE void AVXMemCopy(const P_Type1& from, size_t size, P_Type2&& to) noexcept {
        static_assert(std::is_same<std::remove_cv_t<std::remove_pointer_t<decltype(&from[0])>>, std::remove_cv_t<std::remove_pointer_t<decltype(&to[0])>>>::value);
        AVXCopy<I_Int, I_OutAligned>(S_MemCpyPerformer(), &from[0], &to[0], size * sizeof(from[0]));
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    template <size_t I_Size, bool I_Int, bool I_OutAligned, class P_Type1, class P_Type2>
    FORCE_INLINE void AVXMemCopyConst(const P_Type1& from, P_Type2&& to) noexcept {
        static_assert(std::is_same<std::remove_cv_t<std::remove_pointer_t<decltype(&from[0])>>, std::remove_cv_t<std::remove_pointer_t<decltype(&to[0])>>>::value);
        AVXCopyConst<I_Size * sizeof(to[0]), I_Int, I_OutAligned>(S_MemCpyPerformer(), &from[0], &to[0]);
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    namespace NFastOpsDetail {
        FORCE_INLINE __m256i MemSetBroadcastValue() noexcept {
            return _mm256_setzero_si256();
        }
        FORCE_INLINE __m256i MemSetBroadcastValue(unsigned char val) noexcept {
            return _mm256_set1_epi8(val);
        }
        FORCE_INLINE __m256i MemSetBroadcastValue(char val) noexcept {
            return _mm256_set1_epi8(val);
        }
        FORCE_INLINE __m256i MemSetBroadcastValue(float val) noexcept {
            return _mm256_castps_si256(_mm256_set1_ps(val));
        }
        FORCE_INLINE __m256i MemSetBroadcastValue(double val) noexcept {
            return _mm256_castpd_si256(_mm256_set1_pd(val));
        }
    }

    template <bool I_Int, int I_OutAligned = -1, class P_ValType, class P_Type>
    FORCE_INLINE void AVXMemSet(P_ValType v, size_t size, P_Type&& to) noexcept {
        AVXCopy<I_Int, I_OutAligned>(S_MemSetPerformer(), &to[0], &to[0], size * sizeof(to[0]), NFastOpsDetail::MemSetBroadcastValue(v));
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    template <size_t I_Size, bool I_Int, bool I_OutAligned, class P_ValType, class P_Type>
    FORCE_INLINE void AVXMemSetConst(P_ValType v, P_Type&& to) noexcept {
        AVXCopyConst<I_Size * sizeof(to[0]), I_Int, I_OutAligned>(S_MemSetPerformer(), &to[0], &to[0], NFastOpsDetail::MemSetBroadcastValue(v));
    }
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    AVX_FUNC_T_P(AVXMulByConstAndAssign, S_MulByConstAndAssignPerformer, S_MaxSIMD<sizeof(from[0])>::Set1(v));
    AVX_FUNC_T_P(AVXMulByConstAndAdd, S_MulByConstAndAddPerformer, S_MaxSIMD<sizeof(from[0])>::Set1(v));
    AVX_FUNC_NO_T_P(AVXXMul1MinusX, S_XMul1MinusXPerformer, c::c_1_f);
    AVX_FUNC_NO_T_P(AVX1MinusX2, S_1MinusX2Performer, c::c_1_f);
    AVX_FUNC_NO_T_NO_P(AVXAdd, S_AddPerformer);
    AVX_FUNC_NO_T_NO_P(AVXSub, S_SubPerformer);
    AVX_FUNC_NO_T_NO_P(AVXAddSquared, S_AddSquaredPerformer);

    //#######################################################################################################################################################################
#define AVX_FLOAT_MATH_FUNC_CALL(func_name, performer_name, params_common, params_double, params_float, params_approx) \
    constexpr size_t c_elem_size = sizeof(from[0]);                                                                    \
    using c = NFastOpsDetail::S_Constants<c_elem_size>;                                                                \
    using t_performer = performer_name<c_elem_size, I_Exact>;                                                          \
    if constexpr (bool(I_Exact)) {                                                                                     \
        if constexpr (sizeof(to[0]) >= 8)                                                                              \
            func_name(t_performer(), &from[0], &to[0], params_common, params_double);                                  \
        else                                                                                                           \
            func_name(t_performer(), &from[0], &to[0], params_common, params_float);                                   \
    } else {                                                                                                           \
        func_name(t_performer(), &from[0], &to[0], params_common, params_approx);                                      \
    }
#define AVX_FLOAT_MATH_FUNC_CALL_INTR(c_elem_size, func_name, var_name, params_common, params_double, params_float, params_approx) \
    using c = NFastOpsDetail::S_Constants<c_elem_size>;                                                                            \
    if constexpr (bool(I_Exact)) {                                                                                                 \
        if constexpr (c_elem_size >= 8)                                                                                            \
            return func_name(var_name, params_common, params_double);                                                              \
        return func_name(var_name, params_common, params_float);                                                                   \
    }                                                                                                                              \
    return func_name(var_name, params_common, params_approx);

#define EXP_PARAMS_COMMON(...) __VA_ARGS__, c::c_1_f, c::c_max_pow_2, c::c_min_denorm_exp_f, c::c_min_norm_exp_f, c::c_denorm_offset, c::c_denorm_const
#define EXP_PARAMS_DOUBLE c::c_pow2_6_c0, c::c_pow2_6_c1, c::c_pow2_6_c2, c::c_pow2_6_c3, c::c_pow2_6_c4, c::c_pow2_6_c5, c::c_pow2_6_c6
#define EXP_PARAMS_FLOAT c::c_pow2_4_c0, c::c_pow2_4_c1, c::c_pow2_4_c2, c::c_pow2_4_c3, c::c_pow2_4_c4
#define EXP_PARAMS_APPROX c::c_pow2_2_c0, c::c_pow2_2_c1, c::c_pow2_2_c2

#define AVX_FLOAT_EXP_LIKE_FUNC_CALL(func_name, performer_name, ...) \
    AVX_FLOAT_MATH_FUNC_CALL(func_name, performer_name, EXP_PARAMS_COMMON(__VA_ARGS__), EXP_PARAMS_DOUBLE, EXP_PARAMS_FLOAT, EXP_PARAMS_APPROX)

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <bool I_Exact = true, bool I_OutAligned = false, class P_Type1, class P_Type2>
    FORCE_INLINE void AVXExp(const P_Type1& from, size_t size, P_Type2&& to) noexcept {
        AVX_FLOAT_EXP_LIKE_FUNC_CALL((AVXCopy<false, I_OutAligned>), S_Pow, size * c_elem_size, c::c_1_over_ln_2);
    }
    template <bool I_Exact = true, bool I_OutAligned = false, class P_Type1, class P_Type2>
    FORCE_INLINE void AVXExpMove(const P_Type1& from, size_t size, P_Type2&& to) noexcept {
        AVX_FLOAT_EXP_LIKE_FUNC_CALL((AVXMove<false, I_OutAligned>), S_Pow, size * c_elem_size, c::c_1_over_ln_2);
    }
    template <size_t I_Size, bool I_Exact = true, bool I_OutAligned = false, class P_Type1, class P_Type2>
    FORCE_INLINE void AVXExpConst(const P_Type1& from, P_Type2&& to) noexcept {
        AVX_FLOAT_EXP_LIKE_FUNC_CALL((AVXCopyConst<I_Size * c_elem_size, false, I_OutAligned>), S_Pow, c::c_1_over_ln_2);
    }

    template <size_t I_NOfElements, size_t I_ElementSize, bool I_Exact = true>
    FORCE_INLINE typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f YmmExp(typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_x) {
#define LOC_FUNC_NAME S_Pow<I_ElementSize, I_Exact>().Calc<I_NOfElements, false>
        AVX_FLOAT_MATH_FUNC_CALL_INTR(I_ElementSize, LOC_FUNC_NAME, ymm_x, EXP_PARAMS_COMMON(c::c_1_over_ln_2), EXP_PARAMS_DOUBLE, EXP_PARAMS_FLOAT, EXP_PARAMS_APPROX);
#undef LOC_FUNC_NAME
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <bool I_Exact = true, bool I_OutAligned = false, class P_Type1, class P_Type2>
    FORCE_INLINE void AVXSigmoid(const P_Type1& from, size_t size, P_Type2&& to) noexcept {
        AVX_FLOAT_EXP_LIKE_FUNC_CALL((AVXCopy<false, I_OutAligned>), S_Sigmoid, size * c_elem_size, c::c_neg_1_over_ln_2);
    }
    template <bool I_Exact = true, bool I_OutAligned = false, class P_Type1, class P_Type2>
    FORCE_INLINE void AVXSigmoidMove(const P_Type1& from, size_t size, P_Type2&& to) noexcept {
        AVX_FLOAT_EXP_LIKE_FUNC_CALL((AVXMove<false, I_OutAligned>), S_Sigmoid, size * c_elem_size, c::c_neg_1_over_ln_2);
    }
    template <size_t I_Size, bool I_Exact = true, bool I_OutAligned = false, class P_Type1, class P_Type2>
    FORCE_INLINE void AVXSigmoidConst(const P_Type1& from, P_Type2&& to) noexcept {
        AVX_FLOAT_EXP_LIKE_FUNC_CALL((AVXCopyConst<I_Size * c_elem_size, false, I_OutAligned>), S_Sigmoid, c::c_neg_1_over_ln_2);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <bool I_Exact = true, bool I_OutAligned = false, class P_Type1, class P_Type2>
    FORCE_INLINE void AVXTanh(const P_Type1& from, size_t size, P_Type2&& to) noexcept {
        AVX_FLOAT_EXP_LIKE_FUNC_CALL((AVXCopy<false, I_OutAligned>), S_Tanh, size * c_elem_size, c::c_neg_2_over_ln_2, c::c_2_f);
    }
    template <bool I_Exact = true, bool I_OutAligned = false, class P_Type1, class P_Type2>
    FORCE_INLINE void AVXTanhMove(const P_Type1& from, size_t size, P_Type2&& to) noexcept {
        AVX_FLOAT_EXP_LIKE_FUNC_CALL((AVXMove<false, I_OutAligned>), S_Tanh, size * c_elem_size, c::c_neg_2_over_ln_2, c::c_2_f);
    }
    template <size_t I_Size, bool I_Exact = true, bool I_OutAligned = false, class P_Type1, class P_Type2>
    FORCE_INLINE void AVXTanhConst(const P_Type1& from, P_Type2&& to) noexcept {
        AVX_FLOAT_EXP_LIKE_FUNC_CALL((AVXCopyConst<I_Size * c_elem_size, false, I_OutAligned>), S_Tanh, c::c_neg_2_over_ln_2, c::c_2_f);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#define LN_PARAMS_COMMON_SIZE size *c_elem_size, c::c_1_f
#define LN_PARAMS_COMMON c::c_1_f
#define LN_PARAMS_DOUBLE c::c_ln9_c0, c::c_ln9_c1, c::c_ln9_c2, c::c_ln9_c3, c::c_ln9_c4, c::c_ln9_c5, c::c_ln9_c6, c::c_ln9_c7
#define LN_PARAMS_FLOAT c::c_ln7_c0, c::c_ln7_c1, c::c_ln7_c2, c::c_ln7_c3, c::c_ln7_c4, c::c_ln7_c5
#define LN_PARAMS_APPROX c::c_ln5_c0, c::c_ln5_c1, c::c_ln5_c2, c::c_ln5_c3

    template <bool I_Exact = true, bool I_OutAligned = false, class P_Type1, class P_Type2>
    FORCE_INLINE void AVXLn(const P_Type1& from, size_t size, P_Type2&& to) noexcept {
        AVX_FLOAT_MATH_FUNC_CALL((AVXCopy<false, I_OutAligned>), S_Ln, LN_PARAMS_COMMON_SIZE, LN_PARAMS_DOUBLE, LN_PARAMS_FLOAT, LN_PARAMS_APPROX);
    }
    template <bool I_Exact = true, bool I_OutAligned = false, class P_Type1, class P_Type2>
    FORCE_INLINE void AVXLnMove(const P_Type1& from, size_t size, P_Type2&& to) noexcept {
        AVX_FLOAT_MATH_FUNC_CALL((AVXMove<false, I_OutAligned>), S_Ln, LN_PARAMS_COMMON_SIZE, LN_PARAMS_DOUBLE, LN_PARAMS_FLOAT, LN_PARAMS_APPROX);
    }
    template <size_t I_Size, bool I_Exact = true, bool I_OutAligned = false, class P_Type1, class P_Type2>
    FORCE_INLINE void AVXLnConst(const P_Type1& from, P_Type2&& to) noexcept {
        AVX_FLOAT_MATH_FUNC_CALL((AVXCopyConst<I_Size * c_elem_size, false, I_OutAligned>), S_Ln, LN_PARAMS_COMMON, LN_PARAMS_DOUBLE, LN_PARAMS_FLOAT, LN_PARAMS_APPROX);
    }

    template <size_t I_NOfElements, size_t I_ElementSize, bool I_Exact = true>
    FORCE_INLINE typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f YmmLn(typename S_SIMDV<I_NOfElements, I_ElementSize>::t_f ymm_x) {
        AVX_FLOAT_MATH_FUNC_CALL_INTR(I_ElementSize, (LnV<I_NOfElements, I_ElementSize, I_Exact>), ymm_x, LN_PARAMS_COMMON, LN_PARAMS_DOUBLE, LN_PARAMS_FLOAT, LN_PARAMS_APPROX);
    }
}
#pragma warning(pop)
#endif
