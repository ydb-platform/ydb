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


template
__attribute__((target("avx2")))
void Reverse<NSimd::TSimdAVX2Traits>(ui8* buf, ui8* result_buf, int len);


template
__attribute__((target("sse4.2")))
void Reverse<NSimd::TSimdSSE42Traits>(ui8* buf, ui8* result_buf, int len);


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
        UNIT_ASSERT_EQUAL(tr.All(), true);
        UNIT_ASSERT_EQUAL(fal.All(), false);
        UNIT_ASSERT_UNEQUAL(tr.All(), fal.All());
        UNIT_ASSERT_EQUAL(tr.All(), (tr ^ fal).All());
        UNIT_ASSERT_EQUAL(fal.All(), (tr ^ tr).All());
        UNIT_ASSERT_EQUAL(fal.All(), (tr & fal).All());
        UNIT_ASSERT_EQUAL((~tr).All(), fal.All());
        UNIT_ASSERT_EQUAL((~fal).All(), tr.All());

        TSimd8<bool> bit_or = tr | fal;
        UNIT_ASSERT_EQUAL(bit_or.All(), tr.All());

        TSimd8<bool> tr_m(_mm256_set_epi32(-1, -1, -1, -1, -1, -1, -1, -1));
        UNIT_ASSERT_EQUAL((tr_m == tr).All(), TSimd8<bool>(true).All());
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
        UNIT_ASSERT_EQUAL((a == b).All(), true);
        UNIT_ASSERT_EQUAL((b == c).All(), true);
        UNIT_ASSERT_EQUAL((c == TSimd8<ui8>::Zero()).All(), true);

        a = TSimd8<ui8>(ui8(50));
        b = TSimd8<ui8>(ui8(49));
        UNIT_ASSERT_EQUAL((a.MaxValue(b) == a).All(), true);
        UNIT_ASSERT_EQUAL((a.MinValue(b) == b).All(), true);
        UNIT_ASSERT_EQUAL((a.MinValue(b) == a).All(), false);

        UNIT_ASSERT_EQUAL(c.BitsNotSet().All(), true);
        UNIT_ASSERT_EQUAL(a.BitsNotSet().All(), false);
        UNIT_ASSERT_EQUAL(a.AnyBitsSet().All(), true);


        TSimd8<ui8> a2(ui8(100));
        TSimd8<ui8> a3(ui8(25));
        UNIT_ASSERT_EQUAL((a.Shl<1>() == a2).All(), true);
        UNIT_ASSERT_EQUAL((a.Shr<1>() == a3).All(), true);
        UNIT_ASSERT_EQUAL((a.Shr<8>() == c).All(), true);
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
        UNIT_ASSERT_EQUAL((a == b).All(), true);
        UNIT_ASSERT_EQUAL((b == c).All(), true);
        UNIT_ASSERT_EQUAL((c == TSimd8<i8>::Zero()).All(), true);

        a = TSimd8<i8>(i8(50));
        b = TSimd8<i8>(i8(49));
        UNIT_ASSERT_EQUAL((a.MaxValue(b) == a).All(), true);
        UNIT_ASSERT_EQUAL((a.MinValue(b) == b).All(), true);
        UNIT_ASSERT_EQUAL((a.MinValue(b) == a).All(), false);


        TSimd8<i8> a2(i8(5));
        TSimd8<i8> a3(i8(25));
        a = TSimd8<i8>(i8(15));
        b = TSimd8<i8>(i8(10));
        UNIT_ASSERT_EQUAL(((a + b) == a3).All(), true);
        UNIT_ASSERT_EQUAL(((a - b) == a2).All(), true);
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
                                                7, 6, 5, 4, 3, 2, 1, 0, 7, 6, 5, 4, 3, 2, 1, 0)).All(), true);
    }

    Y_UNIT_TEST(Shuffle128) {
        TSimd8<i8> index(   0, -1, 2, -1, 4, -1, 6, -1, 0, -1, 2, -1, 4, -1, 6, -1,
                            0, -1, 2, -1, 4, -1, 6, -1, 0, -1, 2, -1, 4, -1, 6, -1);
        TSimd8<i8> tmp( 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7,
                        0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7);
        TSimd8<i8> result = tmp.Shuffle128(index);
        UNIT_ASSERT_EQUAL((result == TSimd8<i8>(0, 0, 2, 0, 4, 0, 6, 0, 0, 0, 2, 0, 4, 0, 6, 0,
                                                0, 0, 2, 0, 4, 0, 6, 0, 0, 0, 2, 0, 4, 0, 6, 0)).All(), true);
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
        UNIT_ASSERT_EQUAL((Shift5Right == arr.Shuffle(mask0)).All(), true);
        UNIT_ASSERT_EQUAL((Shift5 == arr.Shuffle(mask1)).All(), true);

        UNIT_ASSERT_EQUAL((Shift5Right == arr.template ByteShift<5>()).All(), true);
        UNIT_ASSERT_EQUAL((Shift5 == arr.template ByteShift<-5>()).All(), true);


        TSimd8<i8> Rotate5( 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4,
                            5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4);
        UNIT_ASSERT_EQUAL((Rotate5 == arr.template Rotate<-5>()).All(), true);
    }

    Y_UNIT_TEST(UnpackMask) {
        TSimd8<i8> unpackMask = NSimd::CreateUnpackMask<NSimd::TSimdAVX2Traits>(2, 1, false);
        unpackMask.Log<i8>(Cerr);
        UNIT_ASSERT_EQUAL((unpackMask == TSimd8<i8>(0, 1, -1, 2, 3, -1, 4, 5, -1, 6, 7, -1, 8, 9, -1, 10, 11,
                                                    -1, 12, 13, -1, 14, 15, -1, 16, 17, -1, 18, 19, -1, 20, 21)).All(), true);
    }

    Y_UNIT_TEST(CRC) {
        ui32 val = 0x454234;
        UNIT_ASSERT_EQUAL(TSimd8<i8>::CRC32u32(0, val), 1867938110);
    }

    Y_UNIT_TEST(Blend) {
        ui8 to[32], from[32];
        for (ui8 i = 0; i < 32; i += 1) {
            to[i] = i;
            from[i] = 32 - i;
        }

        ui16 need[16];
        for (ui8 i = 0; i < 16; i += 1) {
            need[i] = i % 2 == 0 ? ((ui16*)to)[i] : ((ui16*)from)[i];
        }

        const int mask = 0b10101010;
        TSimd8<ui8> v1(to);
        TSimd8<ui8> v2(from);
        TSimd8<ui8> res((ui8*) need);

        ui16 maskBuf[16];
        for (ui8 i = 0; i < 16; i += 1) {
            maskBuf[i] = i % 2 == 0 ? 0 : ui16(-1);
        }
        TSimd8<ui8> blend((ui8*) maskBuf);
        UNIT_ASSERT((v1.Blend16<mask>(v2) == res).All());
        UNIT_ASSERT((v1.BlendVar(v2, blend) == res).All());
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
        UNIT_ASSERT_EQUAL(tr.All(), true);
        UNIT_ASSERT_EQUAL(fal.All(), false);
        UNIT_ASSERT_UNEQUAL(tr.All(), fal.All());
        UNIT_ASSERT_EQUAL(tr.All(), (tr ^ fal).All());
        UNIT_ASSERT_EQUAL(fal.All(), (tr ^ tr).All());
        UNIT_ASSERT_EQUAL(fal.All(), (tr & fal).All());
        UNIT_ASSERT_EQUAL((~tr).All(), fal.All());
        UNIT_ASSERT_EQUAL((~fal).All(), tr.All());

        TSimd8<bool> bit_or = tr | fal;
        UNIT_ASSERT_EQUAL(bit_or.All(), tr.All());

        TSimd8<bool> tr_m(_mm_set_epi32(-1, -1, -1, -1));
        UNIT_ASSERT_EQUAL((tr_m == tr).All(), TSimd8<bool>(true).All());
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
        UNIT_ASSERT_EQUAL((a == b).All(), true);
        UNIT_ASSERT_EQUAL((b == c).All(), true);
        UNIT_ASSERT_EQUAL((c == TSimd8<ui8>::Zero()).All(), true);

        a = TSimd8<ui8>(ui8(50));
        b = TSimd8<ui8>(ui8(49));
        UNIT_ASSERT_EQUAL((a.MaxValue(b) == a).All(), true);
        UNIT_ASSERT_EQUAL((a.MinValue(b) == b).All(), true);
        UNIT_ASSERT_EQUAL((a.MinValue(b) == a).All(), false);

        UNIT_ASSERT_EQUAL(c.BitsNotSet().All(), true);
        UNIT_ASSERT_EQUAL(a.BitsNotSet().All(), false);
        UNIT_ASSERT_EQUAL(a.AnyBitsSet().All(), true);


        TSimd8<ui8> a2(ui8(100));
        TSimd8<ui8> a3(ui8(25));
        UNIT_ASSERT_EQUAL((a.Shl<1>() == a2).All(), true);
        UNIT_ASSERT_EQUAL((a.Shr<1>() == a3).All(), true);
        UNIT_ASSERT_EQUAL((a.Shr<8>() == c).All(), true);
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
        UNIT_ASSERT_EQUAL((a == b).All(), true);
        UNIT_ASSERT_EQUAL((b == c).All(), true);
        UNIT_ASSERT_EQUAL((c == TSimd8<i8>::Zero()).All(), true);

        a = TSimd8<i8>(i8(50));
        b = TSimd8<i8>(i8(49));
        UNIT_ASSERT_EQUAL((a.MaxValue(b) == a).All(), true);
        UNIT_ASSERT_EQUAL((a.MinValue(b) == b).All(), true);
        UNIT_ASSERT_EQUAL((a.MinValue(b) == a).All(), false);


        TSimd8<i8> a2(i8(5));
        TSimd8<i8> a3(i8(25));
        a = TSimd8<i8>(i8(15));
        b = TSimd8<i8>(i8(10));
        UNIT_ASSERT_EQUAL(((a + b) == a3).All(), true);
        UNIT_ASSERT_EQUAL(((a - b) == a2).All(), true);
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
        UNIT_ASSERT_EQUAL((Shift5Right == arr.Shuffle(mask0)).All(), true);
        UNIT_ASSERT_EQUAL((Shift5 == arr.Shuffle(mask1)).All(), true);

        UNIT_ASSERT_EQUAL((Shift5Right == arr.template ByteShift<5>()).All(), true);
        UNIT_ASSERT_EQUAL((Shift5 == arr.template ByteShift<-5>()).All(), true);

        TSimd8<i8> Rotate5(5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4);
        UNIT_ASSERT_EQUAL((Rotate5 == arr.template Rotate<-5>()).All(), true);

        TSimd8<i8> a(i8(0));
        TSimd8<i8> b(i8(1));
        TSimd8<i8> c(1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        UNIT_ASSERT_EQUAL((a.ByteShiftWithCarry<3>(b) == c).All(), true);
    }


    Y_UNIT_TEST(Shuffle) {
        TSimd8<i8> index(0, -1, 2, -1, 4, -1, 6, -1, 0, -1, 2, -1, 4, -1, 6, -1);
        TSimd8<i8> tmp(0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7);
        TSimd8<i8> result = tmp.Shuffle(index);
        UNIT_ASSERT_EQUAL((result == TSimd8<i8>(0, 0, 2, 0, 4, 0, 6, 0, 0, 0, 2, 0, 4, 0, 6, 0)).All(), true);
    }

    Y_UNIT_TEST(UnpackMask) {
        TSimd8<i8> unpackMask = NSimd::CreateUnpackMask<NSimd::TSimdSSE42Traits>(2, 1, false);
        UNIT_ASSERT_EQUAL((unpackMask == TSimd8<i8>(0, 1, -1, 2, 3, -1, 4, 5, -1, 6, 7, -1, 8, 9, -1, 10)).All(), true);
    }

    Y_UNIT_TEST(Blend) {
        ui8 to[16], from[16];
        for (ui8 i = 0; i < 16; i += 1) {
            to[i] = i;
            from[i] = 16 - i;
        }
        ui16 need[8];
        for (ui8 i = 0; i < 8; i += 1) {
            need[i] = i % 2 == 0 ? ((ui16*)to)[i] : ((ui16*)from)[i];
        }

        const int mask = 0b10101010;
        TSimd8<ui8> v1(to);
        TSimd8<ui8> v2(from);
        TSimd8<ui8> res((ui8*) need);

        ui16 maskBuf[8];
        for (ui8 i = 0; i < 8; i += 1) {
            maskBuf[i] = i % 2 == 0 ? 0 : ui16(-1);
        }
        TSimd8<ui8> blend((ui8*) maskBuf);

        UNIT_ASSERT_EQUAL((v1.Blend16<mask>(v2) == res).All(), true);
        UNIT_ASSERT_EQUAL((v1.BlendVar(v2, blend) == res).All(), true);
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

        UNIT_ASSERT_EQUAL((shift5Right == arr.Shuffle(mask0)).All(), true);
        UNIT_ASSERT_EQUAL((shift5 == arr.Shuffle(mask1)).All(), true);
    }

    Y_UNIT_TEST(Shuffle) {
        TSimd8<i8> index(0, -1, 2, -1, 4, -1, 6, -1);
        TSimd8<i8> tmp(0, 1, 2, 3, 4, 5, 6, 7);
        TSimd8<i8> result = tmp.Shuffle(index);
        UNIT_ASSERT_EQUAL((result == TSimd8<i8>(0, 0, 2, 0, 4, 0, 6, 0)).All(), true);
    }

    Y_UNIT_TEST(UnpackMask) {
        TSimd8<i8> unpackMask = NSimd::CreateUnpackMask<NSimd::TSimdFallbackTraits>(2, 1, false);
        unpackMask.Log<i8>(Cerr);
        TSimd8<ui8>((unpackMask == TSimd8<i8>(0, 1, -1, 2, 3, -1, 4, 5)).Value).Log<ui8>(Cerr);
        UNIT_ASSERT_EQUAL((unpackMask == TSimd8<i8>(0, 1, -1, 2, 3, -1, 4, 5)).All(), true);
    }

    Y_UNIT_TEST(CRC) {
        ui32 val = 0x454234;

        UNIT_ASSERT_EQUAL(TSimd8<i8>::CRC32u32(0, val), 1867938110);
    }

    Y_UNIT_TEST(Blend) {
        ui8 to[8], from[8];
        for (ui8 i = 0; i < 8; i += 1) {
            to[i] = i;
            from[i] = 8 - i;
        }
        ui16 need[8];
        for (ui8 i = 0; i < 8; i += 1) {
            need[i] = i % 2 == 0 ? ((ui16*)to)[i] : ((ui16*)from)[i];
        }

        const int mask = 0b10101010;
        TSimd8<ui8> v1(to);
        TSimd8<ui8> v2(from);
        TSimd8<ui8> res((ui8*) need);

        ui16 maskBuf[4];
        for (ui8 i = 0; i < 4; i += 1) {
            maskBuf[i] = i % 2 == 0 ? 0 : ui16(-1);
        }
        TSimd8<ui8> blend((ui8*) maskBuf);

        UNIT_ASSERT_EQUAL((v1.Blend16<mask>(v2) == res).All(), true);
        UNIT_ASSERT_EQUAL((v1.BlendVar(v2, blend) == res).All(), true);
    }
}
