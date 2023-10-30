#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ptr.h>
#include <util/system/cpu_id.h>
#include <util/system/types.h>

#include "simd.h"

template<typename TTraits>
void Reverse(ui8* buf, ui8* result_buf, int len) {
    using TSimdUI8 = typename TTraits::template TSimd8<ui8>;
    int id = 0;
    while (id + TTraits::Size <= len) {
        TSimdUI8 x(buf + id);
        (~x).Store(result_buf + id);
        id += TTraits::Size;
    }
    while (id < len) {
        *(result_buf + id) = ~(*(buf + id));
        id += 1;
    }
}

struct TTestFactory {

    template<typename T>
    int Create() const {
        return T::Size;
    }
};

#pragma clang attribute push(__attribute__((target("avx2"))), apply_to=function)
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

    Y_UNIT_TEST(SimdTrait) {
        if (!NX86::HaveAVX2()) {
            return;
        }

        ui8 buf[1000];
        for (int i = 0; i < 1000; i += 1) {
            buf[i] = i;
        }
        ui8 result_buf[1000] = {0};
        Reverse<NSimd::TSimdAVX2Traits>(buf, result_buf, 1000);
        for (int i = 0; i < 1000; i += 1) {
            UNIT_ASSERT_EQUAL(result_buf[i], ui8(~buf[i]));
        }
    }

    Y_UNIT_TEST(Shuffle) {
        TSimd8<i8> index(31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17,
                        16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0);
        TSimd8<i8> tmp( 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7,
                        0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7);
        TSimd8<i8> result = tmp.Shuffle(index);
        UNIT_ASSERT_EQUAL((result == TSimd8<i8>(7, 6, 5, 4, 3, 2, 1, 0, 7, 6, 5, 4, 3, 2, 1, 0,
                                                7, 6, 5, 4, 3, 2, 1, 0, 7, 6, 5, 4, 3, 2, 1, 0)).Any(), true);
    }

    Y_UNIT_TEST(Shuffle128) {
        TSimd8<i8> index(   0, -1, 2, -1, 4, -1, 6, -1, 0, -1, 2, -1, 4, -1, 6, -1,
                            0, -1, 2, -1, 4, -1, 6, -1, 0, -1, 2, -1, 4, -1, 6, -1);
        TSimd8<i8> tmp( 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7,
                        0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7);
        TSimd8<i8> result = tmp.Shuffle128(index);
        UNIT_ASSERT_EQUAL((result == TSimd8<i8>(0, 0, 2, 0, 4, 0, 6, 0, 0, 0, 2, 0, 4, 0, 6, 0,
                                                0, 0, 2, 0, 4, 0, 6, 0, 0, 0, 2, 0, 4, 0, 6, 0)).Any(), true);
    }

    Y_UNIT_TEST(ShiftBytes) {
        auto mask0 = NSimd::AdvanceBytesMask<NSimd::TSimdAVX2Traits>(5);
        auto mask1 = NSimd::AdvanceBytesMask<NSimd::TSimdAVX2Traits>(-5);

        TSimd8<i8> arr( 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
                        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
        TSimd8<i8> Shift5(  5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4,
                            5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 0, 0, 0, 0);
        TSimd8<i8> Shift5Right( 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                                11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        UNIT_ASSERT_EQUAL((Shift5Right == arr.Shuffle(mask0)).Any(), true);
        UNIT_ASSERT_EQUAL((Shift5 == arr.Shuffle(mask1)).Any(), true);
    }

    Y_UNIT_TEST(UnpackMask) {
        TSimd8<i8> unpackMask = NSimd::CreateUnpackMask<NSimd::TSimdAVX2Traits>(2, 1, false);
        unpackMask.Log<i8>(Cerr);
        UNIT_ASSERT_EQUAL((unpackMask == TSimd8<i8>(0, 1, -1, 2, 3, -1, 4, 5, -1, 6, 7, -1, 8, 9, -1, 10, 11,
                                                        -1, 12, 13, -1, 14, 15, -1, 16, 17, -1, 18, 19, -1, 20, 21)).Any(), true);
    }
}

#pragma clang attribute pop

#pragma clang attribute push(__attribute__((target("sse4.2"))), apply_to=function)
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

    Y_UNIT_TEST(SimdTrait) {
        if (!NX86::HaveSSE42()) {
            return;
        }

        ui8 buf[1000];
        for (int i = 0; i < 1000; i += 1) {
            buf[i] = i;
        }
        ui8 result_buf[1000] = {0};
        Reverse<NSimd::TSimdSSE42Traits>(buf, result_buf, 1000);
        for (int i = 0; i < 1000; i += 1) {
            UNIT_ASSERT_EQUAL(result_buf[i], ui8(~buf[i]));
        }
    }

    Y_UNIT_TEST(ShiftBytes) {
        auto mask0 = NSimd::AdvanceBytesMask<NSimd::TSimdSSE42Traits>(5);
        auto mask1 = NSimd::AdvanceBytesMask<NSimd::TSimdSSE42Traits>(-5);

        TSimd8<i8> arr(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
        TSimd8<i8> Shift5(5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 0, 0, 0, 0);
        TSimd8<i8> Shift5Right(0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        UNIT_ASSERT_EQUAL((Shift5Right == arr.Shuffle(mask0)).Any(), true);
        UNIT_ASSERT_EQUAL((Shift5 == arr.Shuffle(mask1)).Any(), true);
    }


    Y_UNIT_TEST(Shuffle) {
        TSimd8<i8> index(0, -1, 2, -1, 4, -1, 6, -1, 0, -1, 2, -1, 4, -1, 6, -1);
        TSimd8<i8> tmp(0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7);
        TSimd8<i8> result = tmp.Shuffle(index);
        UNIT_ASSERT_EQUAL((result == TSimd8<i8>(0, 0, 2, 0, 4, 0, 6, 0, 0, 0, 2, 0, 4, 0, 6, 0)).Any(), true);
    }

    Y_UNIT_TEST(UnpackMask) {
        TSimd8<i8> unpackMask = NSimd::CreateUnpackMask<NSimd::TSimdSSE42Traits>(2, 1, false);
        UNIT_ASSERT_EQUAL((unpackMask == TSimd8<i8>(0, 1, -1, 2, 3, -1, 4, 5, -1, 6, 7, -1, 8, 9, -1, 10)).Any(), true);
    }


}
#pragma clang attribute pop

Y_UNIT_TEST_SUITE(SimdFallback) {
    using namespace NSimd::NFallback;
    Y_UNIT_TEST(SimdTrait) {
        ui8 buf[1000];
        for (int i = 0; i < 1000; i += 1) {
            buf[i] = i;
        }
        ui8 result_buf[1000] = {0};
        Reverse<NSimd::TSimdFallbackTraits>(buf, result_buf, 1000);
        for (int i = 0; i < 1000; i += 1) {
            UNIT_ASSERT_EQUAL(result_buf[i], ui8(~buf[i]));
        }
    }

    Y_UNIT_TEST(BestTrait) {
        TTestFactory x;
        if (NX86::HaveAVX2()) {
            auto y = NSimd::SelectSimdTraits<TTestFactory>(x);
            UNIT_ASSERT_EQUAL(y, 32);
        } else if (NX86::HaveSSE42()) {
            auto y = NSimd::SelectSimdTraits<TTestFactory>(x);
            UNIT_ASSERT_EQUAL(y, 16);
        } else {
            auto y = NSimd::SelectSimdTraits<TTestFactory>(x);
            UNIT_ASSERT_EQUAL(y, 8);
        }
    }

    Y_UNIT_TEST(ShiftBytes) {
        auto mask0 = NSimd::AdvanceBytesMask<NSimd::TSimdFallbackTraits>(5);
        auto mask1 = NSimd::AdvanceBytesMask<NSimd::TSimdFallbackTraits>(-5);

        TSimd8<i8> arr(0, 1, 2, 3, 4, 5, 6, 7);
        TSimd8<i8> shift5(5, 6, 7, 0, 0, 0, 0, 0);
        TSimd8<i8> shift5Right(0, 0, 0, 0, 0, 0, 1, 2);

        UNIT_ASSERT_EQUAL((shift5Right == arr.Shuffle(mask0)).Any(), true);
        UNIT_ASSERT_EQUAL((shift5 == arr.Shuffle(mask1)).Any(), true);
    }

    Y_UNIT_TEST(Shuffle) {
        TSimd8<i8> index(0, -1, 2, -1, 4, -1, 6, -1);
        TSimd8<i8> tmp(0, 1, 2, 3, 4, 5, 6, 7);
        TSimd8<i8> result = tmp.Shuffle(index);
        UNIT_ASSERT_EQUAL((result == TSimd8<i8>(0, 0, 2, 0, 4, 0, 6, 0)).Any(), true);
    }

    Y_UNIT_TEST(UnpackMask) {
        TSimd8<i8> unpackMask = NSimd::CreateUnpackMask<NSimd::TSimdFallbackTraits>(2, 1, false);
        UNIT_ASSERT_EQUAL((unpackMask == TSimd8<i8>(0, 1, -1, 2, 3, -1, 4, 5)).Any(), true);
    }

}