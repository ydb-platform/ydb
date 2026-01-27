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
}

Y_UNIT_TEST(IsBelongToInterval_RightDirection_Overflow) {
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, std::numeric_limits<ui32>::max(), 1u, std::numeric_limits<ui32>::max()));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, std::numeric_limits<ui32>::max(), 1u, 0u));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, std::numeric_limits<ui32>::max(), 1u, 100u));
}

Y_UNIT_TEST(IsBelongToInterval_LeftDirection_Normal) {
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10, 5, 5));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10, 5, 0));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10, 5, -100));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10, 5, 6));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10, 5, 10));
}

Y_UNIT_TEST(IsBelongToInterval_LeftDirection_Underflow_Unsigned) {
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 5u, 10u, 0u));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 5u, 10u, 1u));
}

Y_UNIT_TEST(IsBelongToInterval_LeftDirection_Underflow_Signed) {
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, std::numeric_limits<i32>::min(), 1, std::numeric_limits<i32>::min()));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, std::numeric_limits<i32>::min(), 1, 0));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, std::numeric_limits<i32>::min(), 1, std::numeric_limits<i32>::min() + 1));
}

Y_UNIT_TEST(IsBelongToInterval_RightDirection_NegativeDelta_Signed) {
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, 10, -5, 5));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, 10, -5, 0));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, 10, -5, 6));
}

Y_UNIT_TEST(IsBelongToInterval_LeftDirection_NegativeDelta_Signed) {
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10, -5, 15));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10, -5, 10));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10, -5, 16));
}

Y_UNIT_TEST(IsBelongToInterval_BoundaryValues_ui64) {
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, std::numeric_limits<ui64>::max(), ui64(0), std::numeric_limits<ui64>::max()));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, std::numeric_limits<ui64>::max(), ui64(0), ui64(0)));

    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 0ul, 0ul, 0ul));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 0ul, 0ul, 1ul));
}

Y_UNIT_TEST(IsBelongToInterval_BoundaryValues_i64) {
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i64>(EDirection::Following, std::numeric_limits<i64>::max(), 0ll, std::numeric_limits<i64>::max())));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i64>(EDirection::Following, std::numeric_limits<i64>::max(), 0ll, 0ll)));
    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i64>(EDirection::Following, std::numeric_limits<i64>::max(), 0ll, std::numeric_limits<i64>::min())));

    UNIT_ASSERT((IsBelongToInterval<EInfBoundary::Left, i64>(EDirection::Preceding, std::numeric_limits<i64>::min(), 0ll, std::numeric_limits<i64>::min())));
    UNIT_ASSERT((!IsBelongToInterval<EInfBoundary::Left, i64>(EDirection::Preceding, std::numeric_limits<i64>::min(), 0ll, std::numeric_limits<i64>::min() + 1)));
}

Y_UNIT_TEST(IsBelongToInterval_Float) {
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, 10.0f, 5.0f, 15.0f));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, 10.0f, 5.0f, 10.0f));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, 10.0f, 5.0f, 15.1f));

    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10.0f, 5.0f, 5.0f));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10.0f, 5.0f, 0.0f));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10.0f, 5.0f, 5.1f));
}

Y_UNIT_TEST(IsBelongToInterval_Double) {
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, 10.0, 5.0, 15.0));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, 10.0, 5.0, 10.0));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, 10.0, 5.0, 15.1));

    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10.0, 5.0, 5.0));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10.0, 5.0, 0.0));
    UNIT_ASSERT(!IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, 10.0, 5.0, 5.1));
}

Y_UNIT_TEST(IsBelongToInterval_Float_BoundaryValues) {
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, std::numeric_limits<float>::max(), 0.0f, std::numeric_limits<float>::max()));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, std::numeric_limits<float>::max(), 0.0f, 0.0f));

    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, std::numeric_limits<float>::lowest(), 0.0f, std::numeric_limits<float>::lowest()));
}

Y_UNIT_TEST(IsBelongToInterval_Double_BoundaryValues) {
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, std::numeric_limits<double>::max(), 0.0, std::numeric_limits<double>::max()));
    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Following, std::numeric_limits<double>::max(), 0.0, 0.0));

    UNIT_ASSERT(IsBelongToInterval<EInfBoundary::Left>(EDirection::Preceding, std::numeric_limits<double>::lowest(), 0.0, std::numeric_limits<double>::lowest()));
}

} // Y_UNIT_TEST_SUITE(SaturatedMathTest)

} // namespace NKikimr::NMiniKQL
