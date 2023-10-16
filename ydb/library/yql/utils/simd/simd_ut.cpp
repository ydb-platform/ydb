#include <library/cpp/testing/unittest/registar.h>
#include <util/system/cpu_id.h>

#pragma clang attribute push(__attribute__((target("avx2"))), apply_to=function)
#include <ydb/library/yql/utils/simd/simd_avx2.h>
Y_UNIT_TEST_SUITE(TSimdAVX2) {
    using namespace NSimd::NAVX2;
    Y_UNIT_TEST(SimdBool) {
        if (!NX86::HaveAVX2()) {
            return;
        }
        TSimd8<bool> tr(true);
        TSimd8<bool> fal(false);
        UNIT_ASSERT_EQUAL(tr.Any(), true);
        UNIT_ASSERT_EQUAL(fal.Any(), false);
        UNIT_ASSERT_UNEQUAL(tr.Any(), fal.Any());
        UNIT_ASSERT_EQUAL(tr.Any(), (tr ^ fal).Any());
        UNIT_ASSERT_EQUAL(fal.Any(), (tr ^ tr).Any());
        UNIT_ASSERT_EQUAL(fal.Any(), (tr & fal).Any());
        UNIT_ASSERT_EQUAL((~tr).Any(), fal.Any());
        UNIT_ASSERT_EQUAL((~fal).Any(), tr.Any());

        TSimd8<bool> bit_or = tr | fal;
        UNIT_ASSERT_EQUAL(bit_or.Any(), tr.Any());
        
        TSimd8<bool> tr_m(_mm256_set_epi32(-1, -1, -1, -1, -1, -1, -1, -1));
        UNIT_ASSERT_EQUAL((tr_m == tr).Any(), TSimd8<bool>(true).Any());
    }
    Y_UNIT_TEST(SimdUInt8) {
        if (!NX86::HaveAVX2()) {
            return;
        }
        __m256i x = _mm256_set1_epi8(0U);
        ui8 arr[32];
        for (auto &i : arr) {
            i = 0;
        }
        TSimd8<ui8> a(x), b(arr), c(ui8(0));
        UNIT_ASSERT_EQUAL((a == b).Any(), true);
        UNIT_ASSERT_EQUAL((b == c).Any(), true);
        UNIT_ASSERT_EQUAL((c == TSimd8<ui8>::Zero()).Any(), true);
        
        a = TSimd8<ui8>(ui8(50));
        b = TSimd8<ui8>(ui8(49));
        UNIT_ASSERT_EQUAL((a.MaxValue(b) == a).Any(), true);
        UNIT_ASSERT_EQUAL((a.MinValue(b) == b).Any(), true);
        UNIT_ASSERT_EQUAL((a.MinValue(b) == a).Any(), false);

        UNIT_ASSERT_EQUAL(c.BitsNotSet().Any(), true);
        UNIT_ASSERT_EQUAL(a.BitsNotSet().Any(), false);
        UNIT_ASSERT_EQUAL(a.AnyBitsSet().Any(), true);

        
        TSimd8<ui8> a2(ui8(100));
        TSimd8<ui8> a3(ui8(25));
        UNIT_ASSERT_EQUAL((a.Shl<1>() == a2).Any(), true);
        UNIT_ASSERT_EQUAL((a.Shr<1>() == a3).Any(), true);
        UNIT_ASSERT_EQUAL((a.Shr<8>() == c).Any(), true);
    }

    Y_UNIT_TEST(SimdInt8) {
        if (!NX86::HaveAVX2()) {
            return;
        }
        __m256i x = _mm256_set1_epi8(0);
        i8 arr[32];
        for (auto &i : arr) {
            i = 0;
        }
        TSimd8<i8> a(x), b(arr), c(i8(0));
        UNIT_ASSERT_EQUAL((a == b).Any(), true);
        UNIT_ASSERT_EQUAL((b == c).Any(), true);
        UNIT_ASSERT_EQUAL((c == TSimd8<i8>::Zero()).Any(), true);
        
        a = TSimd8<i8>(i8(50));
        b = TSimd8<i8>(i8(49));
        UNIT_ASSERT_EQUAL((a.MaxValue(b) == a).Any(), true);
        UNIT_ASSERT_EQUAL((a.MinValue(b) == b).Any(), true);
        UNIT_ASSERT_EQUAL((a.MinValue(b) == a).Any(), false);

        
        TSimd8<i8> a2(i8(5));
        TSimd8<i8> a3(i8(25));
        a = TSimd8<i8>(i8(15));
        b = TSimd8<i8>(i8(10));
        UNIT_ASSERT_EQUAL(((a + b) == a3).Any(), true);
        UNIT_ASSERT_EQUAL(((a - b) == a2).Any(), true);
    }
}
#pragma clang attribute pop

#pragma clang attribute push(__attribute__((target("sse4.2"))), apply_to=function)
#include <ydb/library/yql/utils/simd/simd_sse42.h>
Y_UNIT_TEST_SUITE(TSimdSSE42) {
    using namespace NSimd::NSSE42;
    Y_UNIT_TEST(SimdBool) {
        if (!NX86::HaveSSE42()) {
            return;
        }
        TSimd8<bool> tr(true);
        TSimd8<bool> fal(false);
        UNIT_ASSERT_EQUAL(tr.Any(), true);
        UNIT_ASSERT_EQUAL(fal.Any(), false);
        UNIT_ASSERT_UNEQUAL(tr.Any(), fal.Any());
        UNIT_ASSERT_EQUAL(tr.Any(), (tr ^ fal).Any());
        UNIT_ASSERT_EQUAL(fal.Any(), (tr ^ tr).Any());
        UNIT_ASSERT_EQUAL(fal.Any(), (tr & fal).Any());
        UNIT_ASSERT_EQUAL((~tr).Any(), fal.Any());
        UNIT_ASSERT_EQUAL((~fal).Any(), tr.Any());

        TSimd8<bool> bit_or = tr | fal;
        UNIT_ASSERT_EQUAL(bit_or.Any(), tr.Any());
        
        TSimd8<bool> tr_m(_mm_set_epi32(-1, -1, -1, -1));
        UNIT_ASSERT_EQUAL((tr_m == tr).Any(), TSimd8<bool>(true).Any());
    }
    Y_UNIT_TEST(SimdUInt8) {
        if (!NX86::HaveSSE42()) {
            return;
        }
        __m128i x = _mm_set1_epi8(0U);
        ui8 arr[16];
        for (auto &i : arr) {
            i = 0;
        }
        TSimd8<ui8> a(x), b(arr), c(ui8(0));
        UNIT_ASSERT_EQUAL((a == b).Any(), true);
        UNIT_ASSERT_EQUAL((b == c).Any(), true);
        UNIT_ASSERT_EQUAL((c == TSimd8<ui8>::Zero()).Any(), true);
        
        a = TSimd8<ui8>(ui8(50));
        b = TSimd8<ui8>(ui8(49));
        UNIT_ASSERT_EQUAL((a.MaxValue(b) == a).Any(), true);
        UNIT_ASSERT_EQUAL((a.MinValue(b) == b).Any(), true);
        UNIT_ASSERT_EQUAL((a.MinValue(b) == a).Any(), false);

        UNIT_ASSERT_EQUAL(c.BitsNotSet().Any(), true);
        UNIT_ASSERT_EQUAL(a.BitsNotSet().Any(), false);
        UNIT_ASSERT_EQUAL(a.AnyBitsSet().Any(), true);

        
        TSimd8<ui8> a2(ui8(100));
        TSimd8<ui8> a3(ui8(25));
        UNIT_ASSERT_EQUAL((a.Shl<1>() == a2).Any(), true);
        UNIT_ASSERT_EQUAL((a.Shr<1>() == a3).Any(), true);
        UNIT_ASSERT_EQUAL((a.Shr<8>() == c).Any(), true);
    }

    Y_UNIT_TEST(SimdInt8) {
        if (!NX86::HaveSSE42()) {
            return;
        }
        __m128i x = _mm_set1_epi8(0);
        i8 arr[16];
        for (auto &i : arr) {
            i = 0;
        }
        TSimd8<i8> a(x), b(arr), c(i8(0));
        UNIT_ASSERT_EQUAL((a == b).Any(), true);
        UNIT_ASSERT_EQUAL((b == c).Any(), true);
        UNIT_ASSERT_EQUAL((c == TSimd8<i8>::Zero()).Any(), true);
        
        a = TSimd8<i8>(i8(50));
        b = TSimd8<i8>(i8(49));
        UNIT_ASSERT_EQUAL((a.MaxValue(b) == a).Any(), true);
        UNIT_ASSERT_EQUAL((a.MinValue(b) == b).Any(), true);
        UNIT_ASSERT_EQUAL((a.MinValue(b) == a).Any(), false);

        
        TSimd8<i8> a2(i8(5));
        TSimd8<i8> a3(i8(25));
        a = TSimd8<i8>(i8(15));
        b = TSimd8<i8>(i8(10));
        UNIT_ASSERT_EQUAL(((a + b) == a3).Any(), true);
        UNIT_ASSERT_EQUAL(((a - b) == a2).Any(), true);
    }
}
#pragma clang attribute pop