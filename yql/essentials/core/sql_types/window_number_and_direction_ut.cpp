#include "window_number_and_direction.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql::NWindow;

Y_UNIT_TEST_SUITE(TNumberAndDirectionTest) {

Y_UNIT_TEST(Comparison_LeftInf_vs_LeftInf) {
    TNumberAndDirection<i64> a(TNumberAndDirection<i64>::TUnbounded{}, EDirection::Preceding);
    TNumberAndDirection<i64> b(TNumberAndDirection<i64>::TUnbounded{}, EDirection::Preceding);
    UNIT_ASSERT(!(a < b));
    UNIT_ASSERT(a <= b);
    UNIT_ASSERT(a == b);
    UNIT_ASSERT(!(a != b));
    UNIT_ASSERT(!(a > b));
    UNIT_ASSERT(a >= b);
}

Y_UNIT_TEST(Comparison_LeftInf_vs_LeftSmallNum) {
    TNumberAndDirection<i64> leftInf(TNumberAndDirection<i64>::TUnbounded{}, EDirection::Preceding);
    TNumberAndDirection<i64> leftSmall(100, EDirection::Preceding);
    UNIT_ASSERT(leftInf < leftSmall);
    UNIT_ASSERT(leftInf <= leftSmall);
    UNIT_ASSERT(!(leftInf == leftSmall));
    UNIT_ASSERT(leftInf != leftSmall);
    UNIT_ASSERT(!(leftInf > leftSmall));
    UNIT_ASSERT(!(leftInf >= leftSmall));
}

Y_UNIT_TEST(Comparison_LeftInf_vs_LeftLargeNum) {
    TNumberAndDirection<i64> leftInf(TNumberAndDirection<i64>::TUnbounded{}, EDirection::Preceding);
    TNumberAndDirection<i64> leftLarge(200, EDirection::Preceding);
    UNIT_ASSERT(leftInf < leftLarge);
    UNIT_ASSERT(leftInf <= leftLarge);
    UNIT_ASSERT(!(leftInf == leftLarge));
    UNIT_ASSERT(leftInf != leftLarge);
    UNIT_ASSERT(!(leftInf > leftLarge));
    UNIT_ASSERT(!(leftInf >= leftLarge));
}

Y_UNIT_TEST(Comparison_LeftInf_vs_RightInf) {
    TNumberAndDirection<i64> leftInf(TNumberAndDirection<i64>::TUnbounded{}, EDirection::Preceding);
    TNumberAndDirection<i64> rightInf(TNumberAndDirection<i64>::TUnbounded{}, EDirection::Following);
    UNIT_ASSERT(leftInf < rightInf);
    UNIT_ASSERT(leftInf <= rightInf);
    UNIT_ASSERT(!(leftInf == rightInf));
    UNIT_ASSERT(leftInf != rightInf);
    UNIT_ASSERT(!(leftInf > rightInf));
    UNIT_ASSERT(!(leftInf >= rightInf));
}

Y_UNIT_TEST(Comparison_LeftInf_vs_RightSmallNum) {
    TNumberAndDirection<i64> leftInf(TNumberAndDirection<i64>::TUnbounded{}, EDirection::Preceding);
    TNumberAndDirection<i64> rightSmall(100, EDirection::Following);
    UNIT_ASSERT(leftInf < rightSmall);
    UNIT_ASSERT(leftInf <= rightSmall);
    UNIT_ASSERT(!(leftInf == rightSmall));
    UNIT_ASSERT(leftInf != rightSmall);
    UNIT_ASSERT(!(leftInf > rightSmall));
    UNIT_ASSERT(!(leftInf >= rightSmall));
}

Y_UNIT_TEST(Comparison_LeftInf_vs_RightLargeNum) {
    TNumberAndDirection<i64> leftInf(TNumberAndDirection<i64>::TUnbounded{}, EDirection::Preceding);
    TNumberAndDirection<i64> rightLarge(200, EDirection::Following);
    UNIT_ASSERT(leftInf < rightLarge);
    UNIT_ASSERT(leftInf <= rightLarge);
    UNIT_ASSERT(!(leftInf == rightLarge));
    UNIT_ASSERT(leftInf != rightLarge);
    UNIT_ASSERT(!(leftInf > rightLarge));
    UNIT_ASSERT(!(leftInf >= rightLarge));
}

Y_UNIT_TEST(Comparison_LeftSmallNum_vs_LeftInf) {
    TNumberAndDirection<i64> leftSmall(100, EDirection::Preceding);
    TNumberAndDirection<i64> leftInf(TNumberAndDirection<i64>::TUnbounded{}, EDirection::Preceding);
    UNIT_ASSERT(!(leftSmall < leftInf));
    UNIT_ASSERT(!(leftSmall <= leftInf));
    UNIT_ASSERT(!(leftSmall == leftInf));
    UNIT_ASSERT(leftSmall != leftInf);
    UNIT_ASSERT(leftSmall > leftInf);
    UNIT_ASSERT(leftSmall >= leftInf);
}

Y_UNIT_TEST(Comparison_LeftSmallNum_vs_LeftSmallNum_Equal) {
    TNumberAndDirection<i64> a(100, EDirection::Preceding);
    TNumberAndDirection<i64> b(100, EDirection::Preceding);
    UNIT_ASSERT(!(a < b));
    UNIT_ASSERT(a <= b);
    UNIT_ASSERT(a == b);
    UNIT_ASSERT(!(a != b));
    UNIT_ASSERT(!(a > b));
    UNIT_ASSERT(a >= b);
}

Y_UNIT_TEST(Comparison_LeftSmallNum_vs_LeftLargeNum) {
    TNumberAndDirection<i64> leftSmall(100, EDirection::Preceding);
    TNumberAndDirection<i64> leftLarge(200, EDirection::Preceding);
    // For Left: larger value is "less" (200 < 100)
    UNIT_ASSERT(!(leftSmall < leftLarge));
    UNIT_ASSERT(!(leftSmall <= leftLarge));
    UNIT_ASSERT(!(leftSmall == leftLarge));
    UNIT_ASSERT(leftSmall != leftLarge);
    UNIT_ASSERT(leftSmall > leftLarge);
    UNIT_ASSERT(leftSmall >= leftLarge);
}

Y_UNIT_TEST(Comparison_LeftSmallNum_vs_RightInf) {
    TNumberAndDirection<i64> leftSmall(100, EDirection::Preceding);
    TNumberAndDirection<i64> rightInf(TNumberAndDirection<i64>::TUnbounded{}, EDirection::Following);
    UNIT_ASSERT(leftSmall < rightInf);
    UNIT_ASSERT(leftSmall <= rightInf);
    UNIT_ASSERT(!(leftSmall == rightInf));
    UNIT_ASSERT(leftSmall != rightInf);
    UNIT_ASSERT(!(leftSmall > rightInf));
    UNIT_ASSERT(!(leftSmall >= rightInf));
}

Y_UNIT_TEST(Comparison_LeftSmallNum_vs_RightSmallNum) {
    TNumberAndDirection<i64> leftSmall(100, EDirection::Preceding);
    TNumberAndDirection<i64> rightSmall(100, EDirection::Following);
    // Left always < Right
    UNIT_ASSERT(leftSmall < rightSmall);
    UNIT_ASSERT(leftSmall <= rightSmall);
    UNIT_ASSERT(!(leftSmall == rightSmall));
    UNIT_ASSERT(leftSmall != rightSmall);
    UNIT_ASSERT(!(leftSmall > rightSmall));
    UNIT_ASSERT(!(leftSmall >= rightSmall));
}

Y_UNIT_TEST(Comparison_LeftSmallNum_vs_RightLargeNum) {
    TNumberAndDirection<i64> leftSmall(100, EDirection::Preceding);
    TNumberAndDirection<i64> rightLarge(200, EDirection::Following);
    // Left always < Right
    UNIT_ASSERT(leftSmall < rightLarge);
    UNIT_ASSERT(leftSmall <= rightLarge);
    UNIT_ASSERT(!(leftSmall == rightLarge));
    UNIT_ASSERT(leftSmall != rightLarge);
    UNIT_ASSERT(!(leftSmall > rightLarge));
    UNIT_ASSERT(!(leftSmall >= rightLarge));
}

Y_UNIT_TEST(Comparison_LeftLargeNum_vs_LeftInf) {
    TNumberAndDirection<i64> leftLarge(200, EDirection::Preceding);
    TNumberAndDirection<i64> leftInf(TNumberAndDirection<i64>::TUnbounded{}, EDirection::Preceding);
    UNIT_ASSERT(!(leftLarge < leftInf));
    UNIT_ASSERT(!(leftLarge <= leftInf));
    UNIT_ASSERT(!(leftLarge == leftInf));
    UNIT_ASSERT(leftLarge != leftInf);
    UNIT_ASSERT(leftLarge > leftInf);
    UNIT_ASSERT(leftLarge >= leftInf);
}

Y_UNIT_TEST(Comparison_LeftLargeNum_vs_LeftSmallNum) {
    TNumberAndDirection<i64> leftLarge(200, EDirection::Preceding);
    TNumberAndDirection<i64> leftSmall(100, EDirection::Preceding);
    // For Left: larger value is "less" (200 < 100)
    UNIT_ASSERT(leftLarge < leftSmall);
    UNIT_ASSERT(leftLarge <= leftSmall);
    UNIT_ASSERT(!(leftLarge == leftSmall));
    UNIT_ASSERT(leftLarge != leftSmall);
    UNIT_ASSERT(!(leftLarge > leftSmall));
    UNIT_ASSERT(!(leftLarge >= leftSmall));
}

Y_UNIT_TEST(Comparison_LeftLargeNum_vs_LeftLargeNum_Equal) {
    TNumberAndDirection<i64> a(200, EDirection::Preceding);
    TNumberAndDirection<i64> b(200, EDirection::Preceding);
    UNIT_ASSERT(!(a < b));
    UNIT_ASSERT(a <= b);
    UNIT_ASSERT(a == b);
    UNIT_ASSERT(!(a != b));
    UNIT_ASSERT(!(a > b));
    UNIT_ASSERT(a >= b);
}

Y_UNIT_TEST(Comparison_LeftLargeNum_vs_RightInf) {
    TNumberAndDirection<i64> leftLarge(200, EDirection::Preceding);
    TNumberAndDirection<i64> rightInf(TNumberAndDirection<i64>::TUnbounded{}, EDirection::Following);
    UNIT_ASSERT(leftLarge < rightInf);
    UNIT_ASSERT(leftLarge <= rightInf);
    UNIT_ASSERT(!(leftLarge == rightInf));
    UNIT_ASSERT(leftLarge != rightInf);
    UNIT_ASSERT(!(leftLarge > rightInf));
    UNIT_ASSERT(!(leftLarge >= rightInf));
}

Y_UNIT_TEST(Comparison_LeftLargeNum_vs_RightSmallNum) {
    TNumberAndDirection<i64> leftLarge(200, EDirection::Preceding);
    TNumberAndDirection<i64> rightSmall(100, EDirection::Following);
    // Left always < Right
    UNIT_ASSERT(leftLarge < rightSmall);
    UNIT_ASSERT(leftLarge <= rightSmall);
    UNIT_ASSERT(!(leftLarge == rightSmall));
    UNIT_ASSERT(leftLarge != rightSmall);
    UNIT_ASSERT(!(leftLarge > rightSmall));
    UNIT_ASSERT(!(leftLarge >= rightSmall));
}

Y_UNIT_TEST(Comparison_LeftLargeNum_vs_RightLargeNum) {
    TNumberAndDirection<i64> leftLarge(200, EDirection::Preceding);
    TNumberAndDirection<i64> rightLarge(200, EDirection::Following);
    // Left always < Right
    UNIT_ASSERT(leftLarge < rightLarge);
    UNIT_ASSERT(leftLarge <= rightLarge);
    UNIT_ASSERT(!(leftLarge == rightLarge));
    UNIT_ASSERT(leftLarge != rightLarge);
    UNIT_ASSERT(!(leftLarge > rightLarge));
    UNIT_ASSERT(!(leftLarge >= rightLarge));
}

Y_UNIT_TEST(Comparison_RightInf_vs_LeftInf) {
    TNumberAndDirection<i64> rightInf(TNumberAndDirection<i64>::TUnbounded{}, EDirection::Following);
    TNumberAndDirection<i64> leftInf(TNumberAndDirection<i64>::TUnbounded{}, EDirection::Preceding);
    UNIT_ASSERT(!(rightInf < leftInf));
    UNIT_ASSERT(!(rightInf <= leftInf));
    UNIT_ASSERT(!(rightInf == leftInf));
    UNIT_ASSERT(rightInf != leftInf);
    UNIT_ASSERT(rightInf > leftInf);
    UNIT_ASSERT(rightInf >= leftInf);
}

Y_UNIT_TEST(Comparison_RightInf_vs_LeftSmallNum) {
    TNumberAndDirection<i64> rightInf(TNumberAndDirection<i64>::TUnbounded{}, EDirection::Following);
    TNumberAndDirection<i64> leftSmall(100, EDirection::Preceding);
    UNIT_ASSERT(!(rightInf < leftSmall));
    UNIT_ASSERT(!(rightInf <= leftSmall));
    UNIT_ASSERT(!(rightInf == leftSmall));
    UNIT_ASSERT(rightInf != leftSmall);
    UNIT_ASSERT(rightInf > leftSmall);
    UNIT_ASSERT(rightInf >= leftSmall);
}

Y_UNIT_TEST(Comparison_RightInf_vs_LeftLargeNum) {
    TNumberAndDirection<i64> rightInf(TNumberAndDirection<i64>::TUnbounded{}, EDirection::Following);
    TNumberAndDirection<i64> leftLarge(200, EDirection::Preceding);
    UNIT_ASSERT(!(rightInf < leftLarge));
    UNIT_ASSERT(!(rightInf <= leftLarge));
    UNIT_ASSERT(!(rightInf == leftLarge));
    UNIT_ASSERT(rightInf != leftLarge);
    UNIT_ASSERT(rightInf > leftLarge);
    UNIT_ASSERT(rightInf >= leftLarge);
}

Y_UNIT_TEST(Comparison_RightInf_vs_RightInf) {
    TNumberAndDirection<i64> a(TNumberAndDirection<i64>::TUnbounded{}, EDirection::Following);
    TNumberAndDirection<i64> b(TNumberAndDirection<i64>::TUnbounded{}, EDirection::Following);
    UNIT_ASSERT(!(a < b));
    UNIT_ASSERT(a <= b);
    UNIT_ASSERT(a == b);
    UNIT_ASSERT(!(a != b));
    UNIT_ASSERT(!(a > b));
    UNIT_ASSERT(a >= b);
}

Y_UNIT_TEST(Comparison_RightInf_vs_RightSmallNum) {
    TNumberAndDirection<i64> rightInf(TNumberAndDirection<i64>::TUnbounded{}, EDirection::Following);
    TNumberAndDirection<i64> rightSmall(100, EDirection::Following);
    UNIT_ASSERT(!(rightInf < rightSmall));
    UNIT_ASSERT(!(rightInf <= rightSmall));
    UNIT_ASSERT(!(rightInf == rightSmall));
    UNIT_ASSERT(rightInf != rightSmall);
    UNIT_ASSERT(rightInf > rightSmall);
    UNIT_ASSERT(rightInf >= rightSmall);
}

Y_UNIT_TEST(Comparison_RightInf_vs_RightLargeNum) {
    TNumberAndDirection<i64> rightInf(TNumberAndDirection<i64>::TUnbounded{}, EDirection::Following);
    TNumberAndDirection<i64> rightLarge(200, EDirection::Following);
    UNIT_ASSERT(!(rightInf < rightLarge));
    UNIT_ASSERT(!(rightInf <= rightLarge));
    UNIT_ASSERT(!(rightInf == rightLarge));
    UNIT_ASSERT(rightInf != rightLarge);
    UNIT_ASSERT(rightInf > rightLarge);
    UNIT_ASSERT(rightInf >= rightLarge);
}

Y_UNIT_TEST(Comparison_RightSmallNum_vs_LeftInf) {
    TNumberAndDirection<i64> rightSmall(100, EDirection::Following);
    TNumberAndDirection<i64> leftInf(TNumberAndDirection<i64>::TUnbounded{}, EDirection::Preceding);
    UNIT_ASSERT(!(rightSmall < leftInf));
    UNIT_ASSERT(!(rightSmall <= leftInf));
    UNIT_ASSERT(!(rightSmall == leftInf));
    UNIT_ASSERT(rightSmall != leftInf);
    UNIT_ASSERT(rightSmall > leftInf);
    UNIT_ASSERT(rightSmall >= leftInf);
}

Y_UNIT_TEST(Comparison_RightSmallNum_vs_LeftSmallNum) {
    TNumberAndDirection<i64> rightSmall(100, EDirection::Following);
    TNumberAndDirection<i64> leftSmall(100, EDirection::Preceding);
    // Right always > Left
    UNIT_ASSERT(!(rightSmall < leftSmall));
    UNIT_ASSERT(!(rightSmall <= leftSmall));
    UNIT_ASSERT(!(rightSmall == leftSmall));
    UNIT_ASSERT(rightSmall != leftSmall);
    UNIT_ASSERT(rightSmall > leftSmall);
    UNIT_ASSERT(rightSmall >= leftSmall);
}

Y_UNIT_TEST(Comparison_RightSmallNum_vs_LeftLargeNum) {
    TNumberAndDirection<i64> rightSmall(100, EDirection::Following);
    TNumberAndDirection<i64> leftLarge(200, EDirection::Preceding);
    // Right always > Left
    UNIT_ASSERT(!(rightSmall < leftLarge));
    UNIT_ASSERT(!(rightSmall <= leftLarge));
    UNIT_ASSERT(!(rightSmall == leftLarge));
    UNIT_ASSERT(rightSmall != leftLarge);
    UNIT_ASSERT(rightSmall > leftLarge);
    UNIT_ASSERT(rightSmall >= leftLarge);
}

Y_UNIT_TEST(Comparison_RightSmallNum_vs_RightInf) {
    TNumberAndDirection<i64> rightSmall(100, EDirection::Following);
    TNumberAndDirection<i64> rightInf(TNumberAndDirection<i64>::TUnbounded{}, EDirection::Following);
    UNIT_ASSERT(rightSmall < rightInf);
    UNIT_ASSERT(rightSmall <= rightInf);
    UNIT_ASSERT(!(rightSmall == rightInf));
    UNIT_ASSERT(rightSmall != rightInf);
    UNIT_ASSERT(!(rightSmall > rightInf));
    UNIT_ASSERT(!(rightSmall >= rightInf));
}

Y_UNIT_TEST(Comparison_RightSmallNum_vs_RightSmallNum_Equal) {
    TNumberAndDirection<i64> a(100, EDirection::Following);
    TNumberAndDirection<i64> b(100, EDirection::Following);
    UNIT_ASSERT(!(a < b));
    UNIT_ASSERT(a <= b);
    UNIT_ASSERT(a == b);
    UNIT_ASSERT(!(a != b));
    UNIT_ASSERT(!(a > b));
    UNIT_ASSERT(a >= b);
}

Y_UNIT_TEST(Comparison_RightSmallNum_vs_RightLargeNum) {
    TNumberAndDirection<i64> rightSmall(100, EDirection::Following);
    TNumberAndDirection<i64> rightLarge(200, EDirection::Following);
    // For Right: 100 < 200
    UNIT_ASSERT(rightSmall < rightLarge);
    UNIT_ASSERT(rightSmall <= rightLarge);
    UNIT_ASSERT(!(rightSmall == rightLarge));
    UNIT_ASSERT(rightSmall != rightLarge);
    UNIT_ASSERT(!(rightSmall > rightLarge));
    UNIT_ASSERT(!(rightSmall >= rightLarge));
}

Y_UNIT_TEST(Comparison_RightLargeNum_vs_LeftInf) {
    TNumberAndDirection<i64> rightLarge(200, EDirection::Following);
    TNumberAndDirection<i64> leftInf(TNumberAndDirection<i64>::TUnbounded{}, EDirection::Preceding);
    UNIT_ASSERT(!(rightLarge < leftInf));
    UNIT_ASSERT(!(rightLarge <= leftInf));
    UNIT_ASSERT(!(rightLarge == leftInf));
    UNIT_ASSERT(rightLarge != leftInf);
    UNIT_ASSERT(rightLarge > leftInf);
    UNIT_ASSERT(rightLarge >= leftInf);
}

Y_UNIT_TEST(Comparison_RightLargeNum_vs_LeftSmallNum) {
    TNumberAndDirection<i64> rightLarge(200, EDirection::Following);
    TNumberAndDirection<i64> leftSmall(100, EDirection::Preceding);
    // Right always > Left
    UNIT_ASSERT(!(rightLarge < leftSmall));
    UNIT_ASSERT(!(rightLarge <= leftSmall));
    UNIT_ASSERT(!(rightLarge == leftSmall));
    UNIT_ASSERT(rightLarge != leftSmall);
    UNIT_ASSERT(rightLarge > leftSmall);
    UNIT_ASSERT(rightLarge >= leftSmall);
}

Y_UNIT_TEST(Comparison_RightLargeNum_vs_LeftLargeNum) {
    TNumberAndDirection<i64> rightLarge(200, EDirection::Following);
    TNumberAndDirection<i64> leftLarge(200, EDirection::Preceding);
    // Right always > Left
    UNIT_ASSERT(!(rightLarge < leftLarge));
    UNIT_ASSERT(!(rightLarge <= leftLarge));
    UNIT_ASSERT(!(rightLarge == leftLarge));
    UNIT_ASSERT(rightLarge != leftLarge);
    UNIT_ASSERT(rightLarge > leftLarge);
    UNIT_ASSERT(rightLarge >= leftLarge);
}

Y_UNIT_TEST(Comparison_RightLargeNum_vs_RightInf) {
    TNumberAndDirection<i64> rightLarge(200, EDirection::Following);
    TNumberAndDirection<i64> rightInf(TNumberAndDirection<i64>::TUnbounded{}, EDirection::Following);
    UNIT_ASSERT(rightLarge < rightInf);
    UNIT_ASSERT(rightLarge <= rightInf);
    UNIT_ASSERT(!(rightLarge == rightInf));
    UNIT_ASSERT(rightLarge != rightInf);
    UNIT_ASSERT(!(rightLarge > rightInf));
    UNIT_ASSERT(!(rightLarge >= rightInf));
}

Y_UNIT_TEST(Comparison_RightLargeNum_vs_RightSmallNum) {
    TNumberAndDirection<i64> rightLarge(200, EDirection::Following);
    TNumberAndDirection<i64> rightSmall(100, EDirection::Following);
    // For Right: 200 > 100
    UNIT_ASSERT(!(rightLarge < rightSmall));
    UNIT_ASSERT(!(rightLarge <= rightSmall));
    UNIT_ASSERT(!(rightLarge == rightSmall));
    UNIT_ASSERT(rightLarge != rightSmall);
    UNIT_ASSERT(rightLarge > rightSmall);
    UNIT_ASSERT(rightLarge >= rightSmall);
}

Y_UNIT_TEST(Comparison_RightLargeNum_vs_RightLargeNum_Equal) {
    TNumberAndDirection<i64> a(200, EDirection::Following);
    TNumberAndDirection<i64> b(200, EDirection::Following);
    UNIT_ASSERT(!(a < b));
    UNIT_ASSERT(a <= b);
    UNIT_ASSERT(a == b);
    UNIT_ASSERT(!(a != b));
    UNIT_ASSERT(!(a > b));
    UNIT_ASSERT(a >= b);
}

Y_UNIT_TEST(Comparison_DifferentZeroes_Equal) {
    TNumberAndDirection<i64> a(0, EDirection::Preceding);
    TNumberAndDirection<i64> b(0, EDirection::Following);
    UNIT_ASSERT(!(a < b));
    UNIT_ASSERT(a <= b);
    UNIT_ASSERT(a == b);
    UNIT_ASSERT(!(a != b));
    UNIT_ASSERT(!(a > b));
    UNIT_ASSERT(a >= b);
}
} // Y_UNIT_TEST_SUITE(TNumberAndDirectionTest)
