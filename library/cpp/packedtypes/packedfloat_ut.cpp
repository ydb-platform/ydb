#include "packedfloat.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ylimits.h>

class TPackedFloatTest: public TTestBase {
    UNIT_TEST_SUITE(TPackedFloatTest);
    UNIT_TEST(F16Test);
    UNIT_TEST(Uf16Test);
    UNIT_TEST(F8dTest);
    UNIT_TEST(F8Test);
    UNIT_TEST(Uf8dTest);
    UNIT_TEST(Uf8Test);
    UNIT_TEST_SUITE_END();

private:
    template <typename F>
    void TestF(float f, float rf) {
        UNIT_ASSERT(F::New(f) == rf);
    }

    void TestF16(float f, float rf) {
        TestF<f16>(f, rf);
    }

    void F16Test() {
        TestF16(f16(), 0.);
        TestF16(0, 0);
        TestF16(1.5, 1.5);
        TestF16(-1.5, -1.5);
        TestF16(f16::max(), f16::max());
        TestF16(f16::min(), f16::min());
        TestF16(2.0f * f16::max(), std::numeric_limits<float>::infinity());
        TestF16(0.5 * f16::min(), 0.5 * f16::min());
        TestF16(0.5 * f16::denorm_min(), 0);
        TestF16(-0.5 * f16::denorm_min(), 0);
        TestF16(FLT_MIN, FLT_MIN);
        TestF16(FLT_MAX, f16::max());
        TestF16(f16::min(), FLT_MIN);
        TestF16(f16::denorm_min(), (FLT_MIN / (1 << 23)) * (1 << 16));
    }

    void TestUf16(float f, float rf) {
        TestF<uf16>(f, rf);
    }

    void Uf16Test() {
        UNIT_ASSERT(uf16() == 0.);

        TestUf16(0, 0);
        TestUf16(1.5, 1.5);
        TestUf16(uf16::max(), uf16::max());
        TestUf16(uf16::min(), uf16::min());
        TestUf16(2.0f * uf16::max(), std::numeric_limits<float>::infinity());
        TestUf16(0.5 * uf16::min(), 0.5 * uf16::min());
        TestUf16(0.5 * uf16::denorm_min(), 0);
        TestUf16(FLT_MIN, FLT_MIN);
        TestUf16(FLT_MAX, uf16::max());
        TestUf16(uf16::min(), FLT_MIN);
        TestUf16(uf16::denorm_min(), (FLT_MIN / (1 << 23)) * (1 << 15));
    }

    void TestF8d(float f, float rf) {
        TestF<f8d>(f, rf);
    }

    void F8dTest() {
        UNIT_ASSERT(f8d() == 0.);

        TestF8d(0, 0);
        TestF8d(1.5, 1.5);
        TestF8d(-1.5, -1.5);
        TestF8d(f8d::max(), f8d::max());
        TestF8d(f8d::min(), f8d::min());
        TestF8d(f8d::denorm_min(), f8d::denorm_min());
        TestF8d(2.0 * f8d::max(), f8d::max());
        TestF8d(0.5 * f8d::min(), 0.5 * f8d::min());
        TestF8d(0.5 * f8d::denorm_min(), 0);
    }

    void TestF8(float f, float rf) {
        TestF<f8>(f, rf);
    }

    void F8Test() {
        UNIT_ASSERT(f8() == 0.);

        TestF8(0, 0);
        TestF8(1.5, 1.5);
        TestF8(-1.5, -1.5);
        TestF8(f8::max(), f8::max());
        TestF8(f8::min(), f8::min());
        TestF8(2.0 * f8::max(), f8::max());
        TestF8(0.5 * f8::min(), 0);
    }

    void TestUf8d(float f, float rf) {
        TestF<uf8d>(f, rf);
    }

    void Uf8dTest() {
        UNIT_ASSERT(uf8d() == 0.);

        TestUf8d(0, 0);
        TestUf8d(1.5, 1.5);
        TestUf8d(uf8d::max(), uf8d::max());
        TestUf8d(uf8d::min(), uf8d::min());
        TestUf8d(uf8d::denorm_min(), uf8d::denorm_min());
        TestUf8d(2.0 * uf8d::max(), uf8d::max());
        TestUf8d(0.5 * uf8d::min(), 0.5 * uf8d::min());
        TestUf8d(0.5 * uf8d::denorm_min(), 0);
    }

    void TestUf8(float f, float rf) {
        TestF<uf8>(f, rf);
    }

    void Uf8Test() {
        UNIT_ASSERT(uf8() == 0.);

        TestUf8(0, 0);
        TestUf8(1.5, 1.5);
        TestUf8(uf8::max(), uf8::max());
        TestUf8(uf8::min(), uf8::min());
        TestUf8(2.0 * uf8::max(), uf8::max());
        TestUf8(0.5 * uf8::min(), 0);
    }
};
UNIT_TEST_SUITE_REGISTRATION(TPackedFloatTest);
