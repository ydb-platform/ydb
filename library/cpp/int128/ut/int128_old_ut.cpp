#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/int128/int128.h>

#include "int128_ut_helpers.h"

class TUInt128Test: public TTestBase {
    UNIT_TEST_SUITE(TUInt128Test);
    UNIT_TEST(Create);
    UNIT_TEST(Minus);
    UNIT_TEST(Plus);
    UNIT_TEST(Shift)
    UNIT_TEST(Overflow);
    UNIT_TEST(Underflow);
    UNIT_TEST(ToStringTest);
    UNIT_TEST(FromStringTest);
#if defined(Y_HAVE_INT128)
    UNIT_TEST(FromSystemUint128);
#endif
    UNIT_TEST_SUITE_END();

private:
    void Create();
    void Minus();
    void Plus();
    void Shift();
    void Overflow();
    void Underflow();
    void ToStringTest();
    void FromStringTest();
#if defined(Y_HAVE_INT128)
    void FromSystemUint128();
#endif
};

UNIT_TEST_SUITE_REGISTRATION(TUInt128Test);

void TUInt128Test::Create() {
    const ui128 n1 = 10;
    UNIT_ASSERT_EQUAL(n1, 10);

    const ui128 n2 = n1;
    UNIT_ASSERT_EQUAL(n2, 10);

    const ui128 n3(10);
    UNIT_ASSERT_EQUAL(n3, 10);
}
void TUInt128Test::Minus() {
    const ui128 n2 = 20;
    const ui128 n3 = 30;

    ui128 n4 = n3 - n2;
    UNIT_ASSERT_EQUAL(n4, 10);

    n4 = n4 - 2;
    UNIT_ASSERT_EQUAL(n4, 8);

    n4 -= 2;
    UNIT_ASSERT_EQUAL(n4, 6);

    n4 = 10 - n4;
    UNIT_ASSERT_EQUAL(n4, 4);
}
void TUInt128Test::Plus() {
    const ui128 n2 = 20;
    const ui128 n3 = 30;

    ui128 n4 = n3 + n2;
    UNIT_ASSERT_EQUAL(n4, 50);

    n4 = n4 + 2;
    UNIT_ASSERT_EQUAL(n4, 52);

    n4 += 2;
    UNIT_ASSERT_EQUAL(n4, 54);

    n4 = 10 + n4;
    UNIT_ASSERT_EQUAL(n4, 64);
}
void TUInt128Test::Shift() {
    ui128 n = 1;

    const ui128 n4 = n << 4;
    UNIT_ASSERT_EQUAL(n4, ui128(0x0, 0x0000000000000010));
    UNIT_ASSERT_EQUAL(n4 >> 4, 1);

    const ui128 n8 = n << 8;
    UNIT_ASSERT_EQUAL(n8, ui128(0x0, 0x0000000000000100));
    UNIT_ASSERT_EQUAL(n8 >> 8, 1);

    const ui128 n60 = n << 60;
    UNIT_ASSERT_EQUAL(n60, ui128(0x0, 0x1000000000000000));
    UNIT_ASSERT_EQUAL(n60 >> 60, 1);

    const ui128 n64 = n << 64;
    UNIT_ASSERT_EQUAL(n64, ui128(0x1, 0x0000000000000000));
    UNIT_ASSERT_EQUAL(n64 >> 64, 1);

    const ui128 n124 = n << 124;
    UNIT_ASSERT_EQUAL(n124, ui128(0x1000000000000000, 0x0000000000000000));
    UNIT_ASSERT_EQUAL(n124 >> 124, 1);
}

void TUInt128Test::Overflow() {
    ui128 n = ui128(0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF);
    const ui128 n2 = n + 2;
    UNIT_ASSERT_EQUAL(n2, 1);
}
void TUInt128Test::Underflow() {
    ui128 n = 1;
    const ui128 n128 = n - 2;
    UNIT_ASSERT_EQUAL(n128, ui128(0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF));
}

void TUInt128Test::ToStringTest() {
    ui128 n(0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF);
    TString correct = "340282366920938463463374607431768211455";
    UNIT_ASSERT_EQUAL(correct, ::ToString(n));
}

void TUInt128Test::FromStringTest() {
    {
        const TString originalString = "37778931862957161709568";
        const ui128 number = FromString<ui128>(originalString);
        UNIT_ASSERT_EQUAL(ToString(number), originalString);
    }

    {
        const TString originalString = "1024";
        const ui128 number = FromString<ui128>(originalString);
        UNIT_ASSERT_EQUAL(ToString(number), originalString);
        UNIT_ASSERT_EQUAL(GetHigh(number), 0);
        UNIT_ASSERT_EQUAL(GetLow(number), 1024);
    }

    {
        const TString originalString = "18446744073709551616"; // 2^64, i.e. UINT64_MAX + 1
        const ui128 number = FromString<ui128>(originalString);
        UNIT_ASSERT_EQUAL(ToString(number), originalString);
        UNIT_ASSERT_EQUAL(GetHigh(number), 1);
        UNIT_ASSERT_EQUAL(GetLow(number), 0);
    }

    {
        const TString originalString = "340282366920938463463374607431768211455"; // 2^128-1, i.e. UINT128_MAX
        const ui128 number = FromString<ui128>(originalString);
        UNIT_ASSERT_EQUAL(ToString(number), originalString);
        UNIT_ASSERT_EQUAL(GetHigh(number), 0xFFFFFFFFFFFFFFFF);
        UNIT_ASSERT_EQUAL(GetLow(number), 0xFFFFFFFFFFFFFFFF);
    }
}

#if defined(Y_HAVE_INT128)
void TUInt128Test::FromSystemUint128() {
    unsigned __int128 n = 1;
    ui128 number{n};

    UNIT_ASSERT_EQUAL(GetLow(number), 1);
    UNIT_ASSERT_EQUAL(GetHigh(number), 0);

    auto byteArray = NInt128Private::GetAsArray(number);
#ifdef _little_endian_
    UNIT_ASSERT_EQUAL(byteArray[0], 1);
    for (size_t i = 1; i < 16; i++) {
        UNIT_ASSERT_EQUAL(byteArray[i], 0);
    }
#elif defined(_big_endian_)
    UNIT_ASSERT_EQUAL(byteArray[15], 1);
    for (size_t i = 0; i < 15; i++) {
        UNIT_ASSERT_EQUAL(byteArray[i], 0);
    }
#endif

    UNIT_ASSERT_EQUAL(std::memcmp((void*)&n, (void*)&number, 16), 0);

    UNIT_ASSERT_EQUAL(ToString(n), "1");

    UNIT_ASSERT_EQUAL(FromString<unsigned __int128>(ToString(n)), n);
}
#endif
