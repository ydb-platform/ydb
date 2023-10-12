#include <library/cpp/testing/unittest/registar.h>
#include <util/system/cpu_id.h>

#if __AVX2__
#include <ydb/library/yql/minikql/comp_nodes/block_join/avx2/begin.h>
Y_UNIT_TEST_SUITE(TMiniKQLBlockJoinHaswell) {
    using namespace NKikimr::NMiniKQL::NBlockJoin::NAVX2::NSIMD;
    Y_UNIT_TEST(SimdBool) {
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
    Y_UNIT_TEST(SimdUInt) {
        __m256i x = _mm256_set1_epi8(0U);
        uint8_t arr[32];
        for (auto &i : arr) {
            i = 0;
        }
        TSimd8<uint8_t> a(x), b(arr), c(uint8_t(0));
        UNIT_ASSERT_EQUAL((a == b).Any(), true);
        UNIT_ASSERT_EQUAL((b == c).Any(), true);
        UNIT_ASSERT_EQUAL((c == TSimd8<uint8_t>::Zero()).Any(), true);
        
        a = TSimd8<uint8_t>(uint8_t(50));
        b = TSimd8<uint8_t>(uint8_t(49));
        UNIT_ASSERT_EQUAL((a.MaxValue(b) == a).Any(), true);
        UNIT_ASSERT_EQUAL((a.MinValue(b) == b).Any(), true);
        UNIT_ASSERT_EQUAL((a.MinValue(b) == a).Any(), false);

        UNIT_ASSERT_EQUAL(c.BitsNotSet().Any(), true);
        UNIT_ASSERT_EQUAL(a.BitsNotSet().Any(), false);
        UNIT_ASSERT_EQUAL(a.AnyBitsSet().Any(), true);

        
        TSimd8<uint8_t> a2(uint8_t(100));
        TSimd8<uint8_t> a3(uint8_t(25));
        UNIT_ASSERT_EQUAL((a.Shl<1>() == a2).Any(), true);
        UNIT_ASSERT_EQUAL((a.Shr<1>() == a3).Any(), true);
        UNIT_ASSERT_EQUAL((a.Shr<8>() == c).Any(), true);
    }

    Y_UNIT_TEST(SimdInt) {
        __m256i x = _mm256_set1_epi8(0);
        int8_t arr[32];
        for (auto &i : arr) {
            i = 0;
        }
        TSimd8<int8_t> a(x), b(arr), c(int8_t(0));
        UNIT_ASSERT_EQUAL((a == b).Any(), true);
        UNIT_ASSERT_EQUAL((b == c).Any(), true);
        UNIT_ASSERT_EQUAL((c == TSimd8<int8_t>::Zero()).Any(), true);
        
        a = TSimd8<int8_t>(int8_t(50));
        b = TSimd8<int8_t>(int8_t(49));
        UNIT_ASSERT_EQUAL((a.MaxValue(b) == a).Any(), true);
        UNIT_ASSERT_EQUAL((a.MinValue(b) == b).Any(), true);
        UNIT_ASSERT_EQUAL((a.MinValue(b) == a).Any(), false);

        
        TSimd8<int8_t> a2(int8_t(5));
        TSimd8<int8_t> a3(int8_t(25));
        a = TSimd8<int8_t>(int8_t(15));
        b = TSimd8<int8_t>(int8_t(10));
        UNIT_ASSERT_EQUAL(((a + b) == a3).Any(), true);
        UNIT_ASSERT_EQUAL(((a - b) == a2).Any(), true);
    }
}

#include <ydb/library/yql/minikql/comp_nodes/block_join/avx2/end.h>
#else
Y_UNIT_TEST_SUITE(TMiniKQLBlockJoinHaswell) {
    Y_UNIT_TEST(SimdBool) {}
    Y_UNIT_TEST(SimdUInt) {}
    Y_UNIT_TEST(SimdInt) {}
}
#endif

#if __SSE4_2__
#include <ydb/library/yql/minikql/comp_nodes/block_join/sse42/begin.h>
Y_UNIT_TEST_SUITE(TMiniKQLBlockJoinWestmere) {
    using namespace NKikimr::NMiniKQL::NBlockJoin::NSSE42::NSIMD;
    Y_UNIT_TEST(SimdBool) {
        
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
    Y_UNIT_TEST(SimdUInt) {
        __m128i x = _mm_set1_epi8(0U);
        uint8_t arr[16];
        for (auto &i : arr) {
            i = 0;
        }
        TSimd8<uint8_t> a(x), b(arr), c(uint8_t(0));
        UNIT_ASSERT_EQUAL((a == b).Any(), true);
        UNIT_ASSERT_EQUAL((b == c).Any(), true);
        UNIT_ASSERT_EQUAL((c == TSimd8<uint8_t>::Zero()).Any(), true);
        
        a = TSimd8<uint8_t>(uint8_t(50));
        b = TSimd8<uint8_t>(uint8_t(49));
        UNIT_ASSERT_EQUAL((a.MaxValue(b) == a).Any(), true);
        UNIT_ASSERT_EQUAL((a.MinValue(b) == b).Any(), true);
        UNIT_ASSERT_EQUAL((a.MinValue(b) == a).Any(), false);

        UNIT_ASSERT_EQUAL(c.BitsNotSet().Any(), true);
        UNIT_ASSERT_EQUAL(a.BitsNotSet().Any(), false);
        UNIT_ASSERT_EQUAL(a.AnyBitsSet().Any(), true);

        
        TSimd8<uint8_t> a2(uint8_t(100));
        TSimd8<uint8_t> a3(uint8_t(25));
        UNIT_ASSERT_EQUAL((a.Shl<1>() == a2).Any(), true);
        UNIT_ASSERT_EQUAL((a.Shr<1>() == a3).Any(), true);
        UNIT_ASSERT_EQUAL((a.Shr<8>() == c).Any(), true);
    }

    Y_UNIT_TEST(SimdInt) {
        __m128i x = _mm_set1_epi8(0);
        int8_t arr[16];
        for (auto &i : arr) {
            i = 0;
        }
        TSimd8<int8_t> a(x), b(arr), c(int8_t(0));
        UNIT_ASSERT_EQUAL((a == b).Any(), true);
        UNIT_ASSERT_EQUAL((b == c).Any(), true);
        UNIT_ASSERT_EQUAL((c == TSimd8<int8_t>::Zero()).Any(), true);
        
        a = TSimd8<int8_t>(int8_t(50));
        b = TSimd8<int8_t>(int8_t(49));
        UNIT_ASSERT_EQUAL((a.MaxValue(b) == a).Any(), true);
        UNIT_ASSERT_EQUAL((a.MinValue(b) == b).Any(), true);
        UNIT_ASSERT_EQUAL((a.MinValue(b) == a).Any(), false);

        
        TSimd8<int8_t> a2(int8_t(5));
        TSimd8<int8_t> a3(int8_t(25));
        a = TSimd8<int8_t>(int8_t(15));
        b = TSimd8<int8_t>(int8_t(10));
        UNIT_ASSERT_EQUAL(((a + b) == a3).Any(), true);
        UNIT_ASSERT_EQUAL(((a - b) == a2).Any(), true);
    }
}
#include <ydb/library/yql/minikql/comp_nodes/block_join/sse42/end.h>
#else
Y_UNIT_TEST_SUITE(TMiniKQLBlockJoinWestmere) {
    Y_UNIT_TEST(SimdBool) {}
    Y_UNIT_TEST(SimdUInt) {}
    Y_UNIT_TEST(SimdInt) {}
}
#endif