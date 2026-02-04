#include "mkql_saturated_math.h"

#include <library/cpp/testing/unittest/registar.h>
#include <limits>

namespace NKikimr::NMiniKQL {

Y_UNIT_TEST_SUITE(SaturatedMathTest) {

Y_UNIT_TEST(IsBelongToInterval_RightDirection_Normal) {
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, 10, 5, 15));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, 10, 5, 10));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, 10, 5, 0));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, 10, 5, 16));

    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Following, 10, 5, 15));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Following, 10, 5, 16));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Right>(EDirection::Following, 10, 5, 14));
}

Y_UNIT_TEST(IsBelongToInterval_RightDirection_Overflow) {
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, std::numeric_limits<ui32>::max(), 1u, std::numeric_limits<ui32>::max()));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, std::numeric_limits<ui32>::max(), 1u, 0u));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, std::numeric_limits<ui32>::max(), 1u, 100u));

    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Right>(EDirection::Following, std::numeric_limits<ui32>::max(), 1u, std::numeric_limits<ui32>::max()));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Right>(EDirection::Following, std::numeric_limits<ui32>::max(), 1u, 0u));
}

Y_UNIT_TEST(IsBelongToInterval_LeftDirection_Normal) {
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10, 5, 5));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10, 5, 0));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10, 5, -100));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10, 5, 6));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10, 5, 10));

    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Preceding, 10, 5, 5));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Preceding, 10, 5, 10));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Right>(EDirection::Preceding, 10, 5, 4));
}

Y_UNIT_TEST(IsBelongToInterval_LeftDirection_Underflow_Unsigned) {
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 5u, 10u, 0u));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 5u, 10u, 1u));

    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Preceding, 5u, 10u, 0u));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Preceding, 5u, 10u, 100u));
}

Y_UNIT_TEST(IsBelongToInterval_LeftDirection_Underflow_Signed) {
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, std::numeric_limits<i32>::min(), 1, std::numeric_limits<i32>::min()));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, std::numeric_limits<i32>::min(), 1, 0));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, std::numeric_limits<i32>::min(), 1, std::numeric_limits<i32>::min() + 1));

    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Preceding, std::numeric_limits<i32>::min(), 1, std::numeric_limits<i32>::min()));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Preceding, std::numeric_limits<i32>::min(), 1, 0));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Preceding, std::numeric_limits<i32>::min(), 1, std::numeric_limits<i32>::max()));
}

Y_UNIT_TEST(IsBelongToInterval_BoundaryValues_ui64) {
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, std::numeric_limits<ui64>::max(), ui64(0), std::numeric_limits<ui64>::max()));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, std::numeric_limits<ui64>::max(), ui64(0), ui64(0)));

    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 0ul, 0ul, 0ul));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 0ul, 0ul, 1ul));

    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Following, std::numeric_limits<ui64>::max(), ui64(0), std::numeric_limits<ui64>::max()));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Right>(EDirection::Following, std::numeric_limits<ui64>::max(), ui64(0), ui64(0)));

    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Preceding, 0ul, 0ul, 0ul));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Preceding, 0ul, 0ul, 1ul));
}

Y_UNIT_TEST(IsBelongToInterval_BoundaryValues_i64) {
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i64>(EDirection::Following, std::numeric_limits<i64>::max(), 0ll, std::numeric_limits<i64>::max())));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i64>(EDirection::Following, std::numeric_limits<i64>::max(), 0ll, 0ll)));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i64>(EDirection::Following, std::numeric_limits<i64>::max(), 0ll, std::numeric_limits<i64>::min())));

    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i64>(EDirection::Preceding, std::numeric_limits<i64>::min(), 0ll, std::numeric_limits<i64>::min())));
    UNIT_ASSERT((!IsBelongToInterval<EInfBoundary::Left, i64>(EDirection::Preceding, std::numeric_limits<i64>::min(), 0ll, std::numeric_limits<i64>::min() + 1)));

    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Right, i64>(EDirection::Following, std::numeric_limits<i64>::max(), 0ll, std::numeric_limits<i64>::max())));
    UNIT_ASSERT((!IsBelongToInterval<EInfBoundary::Right, i64>(EDirection::Following, std::numeric_limits<i64>::max(), 0ll, 0ll)));
    UNIT_ASSERT((!IsBelongToInterval<EInfBoundary::Right, i64>(EDirection::Following, std::numeric_limits<i64>::max(), 0ll, std::numeric_limits<i64>::min())));

    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Right, i64>(EDirection::Preceding, std::numeric_limits<i64>::min(), 0ll, std::numeric_limits<i64>::min())));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Right, i64>(EDirection::Preceding, std::numeric_limits<i64>::min(), 0ll, std::numeric_limits<i64>::min() + 1)));
}

Y_UNIT_TEST(IsBelongToInterval_Float) {
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, 10.0f, 5.0f, 15.0f));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, 10.0f, 5.0f, 10.0f));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, 10.0f, 5.0f, 15.1f));

    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10.0f, 5.0f, 5.0f));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10.0f, 5.0f, 0.0f));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10.0f, 5.0f, 5.1f));

    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Following, 10.0f, 5.0f, 15.0f));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Following, 10.0f, 5.0f, 15.1f));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Right>(EDirection::Following, 10.0f, 5.0f, 14.9f));

    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Preceding, 10.0f, 5.0f, 5.0f));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Preceding, 10.0f, 5.0f, 5.1f));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Right>(EDirection::Preceding, 10.0f, 5.0f, 4.9f));
}

Y_UNIT_TEST(IsBelongToInterval_Double) {
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, 10.0, 5.0, 15.0));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, 10.0, 5.0, 10.0));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, 10.0, 5.0, 15.1));

    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10.0, 5.0, 5.0));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10.0, 5.0, 0.0));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10.0, 5.0, 5.1));

    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Following, 10.0, 5.0, 15.0));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Following, 10.0, 5.0, 15.1));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Right>(EDirection::Following, 10.0, 5.0, 14.9));

    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Preceding, 10.0, 5.0, 5.0));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Preceding, 10.0, 5.0, 5.1));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Right>(EDirection::Preceding, 10.0, 5.0, 4.9));
}

Y_UNIT_TEST(IsBelongToInterval_Float_BoundaryValues) {
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, std::numeric_limits<float>::max(), 5.f, std::numeric_limits<float>::max()));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, std::numeric_limits<float>::max(), 0.0f, 0.0f));

    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, std::numeric_limits<float>::lowest(), 0.0f, std::numeric_limits<float>::lowest()));

    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Following, std::numeric_limits<float>::max(), 7.0, std::numeric_limits<float>::max()));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Right>(EDirection::Following, std::numeric_limits<float>::max(), 0.0f, 0.0f));

    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Preceding, std::numeric_limits<float>::lowest(), 0.0f, std::numeric_limits<float>::lowest()));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Preceding, std::numeric_limits<float>::lowest(), 0.0f, 0.0f));
}

Y_UNIT_TEST(IsBelongToInterval_Double_BoundaryValues) {
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, std::numeric_limits<double>::max(), 0.0, std::numeric_limits<double>::max()));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, std::numeric_limits<double>::max(), 0.0, 0.0));

    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, std::numeric_limits<double>::lowest(), 0.0, std::numeric_limits<double>::lowest()));

    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Following, std::numeric_limits<double>::max(), 0.0, std::numeric_limits<double>::max()));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Right>(EDirection::Following, std::numeric_limits<double>::max(), 0.0, 0.0));

    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Preceding, std::numeric_limits<double>::lowest(), 0.0, std::numeric_limits<double>::lowest()));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Right>(EDirection::Preceding, std::numeric_limits<double>::lowest(), 0.0, 0.0));
}

Y_UNIT_TEST(IsBelongToInterval_MixedTypes_i64_ui32) {
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i64, ui32>(EDirection::Following, i64(1000000000000ll), ui32(500), i64(1000000000500ll))));
    UNIT_ASSERT((!IsBelongToInterval<EInfBoundary::Left, i64, ui32>(EDirection::Following, i64(1000000000000ll), ui32(500), i64(1000000000501ll))));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i64, ui32>(EDirection::Preceding, i64(1000000000000ll), ui32(500), i64(999999999500ll))));

    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i64, ui32>(EDirection::Following, i64(-100), ui32(50), i64(-50))));
    UNIT_ASSERT((!IsBelongToInterval<EInfBoundary::Left, i64, ui32>(EDirection::Following, i64(-100), ui32(50), i64(-49))));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i64, ui32>(EDirection::Preceding, i64(-100), ui32(50), i64(-150))));

    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i64, ui32>(EDirection::Following, std::numeric_limits<i64>::max(), std::numeric_limits<ui32>::max(), std::numeric_limits<i64>::max())));
    UNIT_ASSERT(!(IsBelongToInterval<EInfBoundary::Left, i64, ui32>(EDirection::Preceding, std::numeric_limits<i64>::min(), std::numeric_limits<ui32>::max(), std::numeric_limits<i64>::min())));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i64, ui32>(EDirection::Following, i64(0), ui32(0), i64(0))));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i64, ui32>(EDirection::Preceding, i64(0), ui32(0), i64(0))));

    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Right, i64, ui32>(EDirection::Following, i64(1000000000000ll), ui32(500), i64(1000000000500ll))));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Right, i64, ui32>(EDirection::Following, i64(1000000000000ll), ui32(500), i64(1000000000501ll))));
    UNIT_ASSERT((!IsBelongToInterval<EInfBoundary::Right, i64, ui32>(EDirection::Following, i64(1000000000000ll), ui32(500), i64(1000000000499ll))));

    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Right, i64, ui32>(EDirection::Preceding, i64(1000000000000ll), ui32(500), i64(999999999500ll))));
    UNIT_ASSERT((!IsBelongToInterval<EInfBoundary::Right, i64, ui32>(EDirection::Preceding, i64(1000000000000ll), ui32(500), i64(999999999499ll))));
}

Y_UNIT_TEST(IsBelongToInterval_MixedTypes_i16_ui64) {
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i16, ui64>(EDirection::Following, i16(100), ui64(50), i16(150))));
    UNIT_ASSERT((!IsBelongToInterval<EInfBoundary::Left, i16, ui64>(EDirection::Following, i16(100), ui64(50), i16(151))));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i16, ui64>(EDirection::Preceding, i16(100), ui64(50), i16(50))));

    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i16, ui64>(EDirection::Following, i16(-50), ui64(100), i16(50))));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i16, ui64>(EDirection::Preceding, i16(-50), ui64(100), i16(-150))));

    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i16, ui64>(EDirection::Following, std::numeric_limits<i16>::max(), std::numeric_limits<ui64>::max(), std::numeric_limits<i16>::max())));
    UNIT_ASSERT(!(IsBelongToInterval<EInfBoundary::Left, i16, ui64>(EDirection::Preceding, std::numeric_limits<i16>::min(), std::numeric_limits<ui64>::max(), std::numeric_limits<i16>::min())));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i16, ui64>(EDirection::Following, i16(0), ui64(0), i16(0))));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i16, ui64>(EDirection::Preceding, i16(0), ui64(0), i16(0))));

    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Right, i16, ui64>(EDirection::Following, i16(100), ui64(50), i16(150))));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Right, i16, ui64>(EDirection::Following, i16(100), ui64(50), i16(151))));
    UNIT_ASSERT((!IsBelongToInterval<EInfBoundary::Right, i16, ui64>(EDirection::Following, i16(100), ui64(50), i16(149))));

    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Right, i16, ui64>(EDirection::Preceding, i16(100), ui64(50), i16(50))));
    UNIT_ASSERT((!IsBelongToInterval<EInfBoundary::Right, i16, ui64>(EDirection::Preceding, i16(100), ui64(50), i16(49))));
}

Y_UNIT_TEST(IsBelongToInterval_MixedTypes_float_i32) {
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, float, i32>(EDirection::Following, 10.5f, i32(5), 15.5f)));
    UNIT_ASSERT((!IsBelongToInterval<EInfBoundary::Left, float, i32>(EDirection::Following, 10.5f, i32(5), 15.6f)));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, float, i32>(EDirection::Preceding, 10.5f, i32(5), 5.5f)));

    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, float, i32>(EDirection::Following, -10.5f, i32(5), -5.5f)));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, float, i32>(EDirection::Preceding, -10.5f, i32(5), -15.5f)));

    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, float, i32>(EDirection::Following, std::numeric_limits<float>::max(), std::numeric_limits<i32>::max(), std::numeric_limits<float>::max())));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, float, i32>(EDirection::Preceding, std::numeric_limits<float>::lowest(), std::numeric_limits<i32>::max(), std::numeric_limits<float>::lowest())));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, float, i32>(EDirection::Following, 0.0f, i32(0), 0.0f)));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, float, i32>(EDirection::Preceding, 0.0f, i32(0), 0.0f)));

    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Right, float, i32>(EDirection::Following, 10.5f, i32(5), 15.5f)));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Right, float, i32>(EDirection::Following, 10.5f, i32(5), 15.6f)));
    UNIT_ASSERT((!IsBelongToInterval<EInfBoundary::Right, float, i32>(EDirection::Following, 10.5f, i32(5), 15.4f)));

    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Right, float, i32>(EDirection::Preceding, 10.5f, i32(5), 5.5f)));
    UNIT_ASSERT((!IsBelongToInterval<EInfBoundary::Right, float, i32>(EDirection::Preceding, 10.5f, i32(5), 5.4f)));
}

Y_UNIT_TEST(IsBelongToInterval_MixedTypes_i32_float) {
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i32, float>(EDirection::Following, i32(10), 5.5f, i32(15))));
    UNIT_ASSERT((!IsBelongToInterval<EInfBoundary::Left, i32, float>(EDirection::Following, i32(10), 5.5f, i32(16))));
    UNIT_ASSERT((!IsBelongToInterval<EInfBoundary::Left, i32, float>(EDirection::Preceding, i32(10), 5.5f, i32(5))));

    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i32, float>(EDirection::Following, i32(-10), 5.5f, i32(-5))));
    UNIT_ASSERT((!IsBelongToInterval<EInfBoundary::Left, i32, float>(EDirection::Preceding, i32(-10), 5.5f, i32(-15))));

    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i32, float>(EDirection::Following, std::numeric_limits<i32>::max(), std::numeric_limits<float>::max(), std::numeric_limits<i32>::max())));
    UNIT_ASSERT((!IsBelongToInterval<EInfBoundary::Left, i32, float>(EDirection::Preceding, std::numeric_limits<i32>::min(), std::numeric_limits<float>::max(), std::numeric_limits<i32>::min())));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i32, float>(EDirection::Following, i32(0), 0.0f, i32(0))));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i32, float>(EDirection::Preceding, i32(0), 0.0f, i32(0))));

    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Right, i32, float>(EDirection::Following, i32(10), 5.5f, i32(16))));
    UNIT_ASSERT((!IsBelongToInterval<EInfBoundary::Right, i32, float>(EDirection::Following, i32(10), 5.5f, i32(15))));

    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Right, i32, float>(EDirection::Preceding, i32(10), 5.5f, i32(5))));
    UNIT_ASSERT((!IsBelongToInterval<EInfBoundary::Right, i32, float>(EDirection::Preceding, i32(10), 5.5f, i32(4))));
}

Y_UNIT_TEST(IsBelongToInterval_MixedTypes_double_float) {
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, double, float>(EDirection::Following, 10.0, 5.0f, 15.0)));
    UNIT_ASSERT((!IsBelongToInterval<EInfBoundary::Left, double, float>(EDirection::Following, 10.0, 5.0f, 15.1)));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, double, float>(EDirection::Preceding, 10.0, 5.0f, 5.0)));

    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, double, float>(EDirection::Following, -10.0, 5.0f, -5.0)));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, double, float>(EDirection::Preceding, -10.0, 5.0f, -15.0)));

    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, double, float>(EDirection::Following, std::numeric_limits<double>::max(), std::numeric_limits<float>::max(), std::numeric_limits<double>::max())));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, double, float>(EDirection::Preceding, std::numeric_limits<double>::lowest(), std::numeric_limits<float>::max(), std::numeric_limits<double>::lowest())));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, double, float>(EDirection::Following, 0.0, 0.0f, 0.0)));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, double, float>(EDirection::Preceding, 0.0, 0.0f, 0.0)));

    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Right, double, float>(EDirection::Following, 10.0, 5.0f, 15.0)));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Right, double, float>(EDirection::Following, 10.0, 5.0f, 15.1)));
    UNIT_ASSERT((!IsBelongToInterval<EInfBoundary::Right, double, float>(EDirection::Following, 10.0, 5.0f, 14.9)));

    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Right, double, float>(EDirection::Preceding, 10.0, 5.0f, 5.0)));
    UNIT_ASSERT((!IsBelongToInterval<EInfBoundary::Right, double, float>(EDirection::Preceding, 10.0, 5.0f, 4.9)));
}

} // Y_UNIT_TEST_SUITE(SaturatedMathTest)

} // namespace NKikimr::NMiniKQL
