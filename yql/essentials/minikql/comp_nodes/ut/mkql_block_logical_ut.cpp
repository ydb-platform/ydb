#include <yql/essentials/minikql/comp_nodes/mkql_block_logical.h>

#include <yql/essentials/minikql/comp_nodes/ut/mkql_block_test_helper.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>

namespace NKikimr::NMiniKQL {

namespace {

template <typename T, typename U, typename V>
void TestAndKernel(T left, U right, V expected) {
    TBlockHelper().TestKernelFuzzied(left, right, expected,
                                     [](TSetup<false>& setup, TRuntimeNode left, TRuntimeNode right) {
                                         return setup.PgmBuilder->BlockAnd(left, right);
                                     });
}

template <typename T, typename U, typename V>
void TestOrKernel(T left, U right, V expected) {
    TBlockHelper().TestKernelFuzzied(left, right, expected,
                                     [](TSetup<false>& setup, TRuntimeNode left, TRuntimeNode right) {
                                         return setup.PgmBuilder->BlockOr(left, right);
                                     });
}

template <typename T, typename U, typename V>
void TestXorKernel(T left, U right, V expected) {
    TBlockHelper().TestKernelFuzzied(left, right, expected,
                                     [](TSetup<false>& setup, TRuntimeNode left, TRuntimeNode right) {
                                         return setup.PgmBuilder->BlockXor(left, right);
                                     });
}

template <typename T, typename V>
void TestNotKernel(T input, V expected) {
    TBlockHelper().TestKernelFuzzied(input, expected,
                                     [](TSetup<false>& setup, TRuntimeNode input) {
                                         return setup.PgmBuilder->BlockNot(input);
                                     });
}

} // namespace

// Tests for BlockAnd operation
Y_UNIT_TEST_SUITE(TMiniKQLBlockLogicalAndTest) {

Y_UNIT_TEST(NonNullValues) {
    // Test with non-null values
    TestAndKernel(
        std::vector<bool>{true, true, false, false},
        std::vector<bool>{true, false, true, false},
        std::vector<bool>{true, false, false, false});
}

Y_UNIT_TEST(ScalarTrueRightOperand) {
    // Test with scalar true right operand
    TestAndKernel(
        std::vector<bool>{true, true, false, false},
        true,
        std::vector<bool>{true, true, false, false});
}

Y_UNIT_TEST(ScalarFalseRightOperand) {
    // Test with scalar false right operand
    TestAndKernel(
        std::vector<bool>{true, true, false, false},
        false,
        std::vector<bool>{false, false, false, false});
}

Y_UNIT_TEST(MixedNullNonNull) {
    // Test with mixed null/non-null values
    // In SQL: NULL AND TRUE -> NULL, NULL AND FALSE -> FALSE
    TestAndKernel(
        std::vector<TMaybe<bool>>{Nothing(), true, false, Nothing()},
        std::vector<bool>{true, true, true, false},
        std::vector<TMaybe<bool>>{Nothing(), true, false, false});
}

Y_UNIT_TEST(BothOperandsWithNulls) {
    // Test with both operands having nulls
    TestAndKernel(
        std::vector<TMaybe<bool>>{Nothing(), true, false, Nothing()},
        std::vector<TMaybe<bool>>{true, Nothing(), Nothing(), false},
        std::vector<TMaybe<bool>>{Nothing(), Nothing(), false, false});
}

Y_UNIT_TEST(ScalarNullRightOperand) {
    // Test with scalar null right operand
    TestAndKernel(
        std::vector<TMaybe<bool>>{Nothing(), true, false, Nothing()},
        TMaybe<bool>{Nothing()},
        std::vector<TMaybe<bool>>{Nothing(), Nothing(), false, Nothing()});
}

Y_UNIT_TEST(AllNullLeftOperands) {
    // Test with all null left operands
    TestAndKernel(
        std::vector<TMaybe<bool>>{Nothing(), Nothing(), Nothing(), Nothing()},
        std::vector<bool>{true, false, true, false},
        std::vector<TMaybe<bool>>{Nothing(), false, Nothing(), false});
}

Y_UNIT_TEST(AllNullRightOperands) {
    // Test with all null right operands
    TestAndKernel(
        std::vector<bool>{true, false, true, false},
        std::vector<TMaybe<bool>>{Nothing(), Nothing(), Nothing(), Nothing()},
        std::vector<TMaybe<bool>>{Nothing(), false, Nothing(), false});
}

Y_UNIT_TEST(ScalarTrueLeftOperand) {
    // Test with scalar true left operand
    TestAndKernel(
        true,
        std::vector<bool>{true, false, true, false},
        std::vector<bool>{true, false, true, false});
}

Y_UNIT_TEST(ScalarFalseLeftOperand) {
    // Test with scalar false left operand
    TestAndKernel(
        false,
        std::vector<bool>{true, false, true, false},
        std::vector<bool>{false, false, false, false});
}

Y_UNIT_TEST(ScalarNullLeftOperand) {
    // Test with scalar null left operand
    TestAndKernel(
        TMaybe<bool>{Nothing()},
        std::vector<bool>{true, false, true, false},
        std::vector<TMaybe<bool>>{Nothing(), false, Nothing(), false});
}

} // Y_UNIT_TEST_SUITE(TMiniKQLBlockLogicalAndTest)

// Tests for BlockOr operation
Y_UNIT_TEST_SUITE(TMiniKQLBlockLogicalOrTest) {

Y_UNIT_TEST(NonNullValues) {
    // Test with non-null values
    TestOrKernel(
        std::vector<bool>{true, true, false, false},
        std::vector<bool>{true, false, true, false},
        std::vector<bool>{true, true, true, false});
}

Y_UNIT_TEST(ScalarTrueRightOperand) {
    // Test with scalar true right operand
    TestOrKernel(
        std::vector<bool>{true, true, false, false},
        true,
        std::vector<bool>{true, true, true, true});
}

Y_UNIT_TEST(ScalarFalseRightOperand) {
    // Test with scalar false right operand
    TestOrKernel(
        std::vector<bool>{true, true, false, false},
        false,
        std::vector<bool>{true, true, false, false});
}

Y_UNIT_TEST(MixedNullNonNull) {
    // Test with mixed null/non-null values
    // In SQL: NULL OR TRUE -> TRUE, NULL OR FALSE -> NULL
    TestOrKernel(
        std::vector<TMaybe<bool>>{Nothing(), true, false, Nothing()},
        std::vector<bool>{true, true, false, false},
        std::vector<TMaybe<bool>>{true, true, false, Nothing()});
}

Y_UNIT_TEST(BothOperandsWithNulls) {
    // Test with both operands having nulls
    TestOrKernel(
        std::vector<TMaybe<bool>>{Nothing(), true, false, Nothing()},
        std::vector<TMaybe<bool>>{true, Nothing(), Nothing(), false},
        std::vector<TMaybe<bool>>{true, true, Nothing(), Nothing()});
}

Y_UNIT_TEST(ScalarNullRightOperand) {
    // Test with scalar null right operand
    TestOrKernel(
        std::vector<TMaybe<bool>>{Nothing(), true, false, Nothing()},
        TMaybe<bool>{Nothing()},
        std::vector<TMaybe<bool>>{Nothing(), true, Nothing(), Nothing()});
}

Y_UNIT_TEST(AllNullLeftOperands) {
    // Test with all null left operands
    TestOrKernel(
        std::vector<TMaybe<bool>>{Nothing(), Nothing(), Nothing(), Nothing()},
        std::vector<bool>{true, false, true, false},
        std::vector<TMaybe<bool>>{true, Nothing(), true, Nothing()});
}

Y_UNIT_TEST(AllNullRightOperands) {
    // Test with all null right operands
    TestOrKernel(
        std::vector<bool>{true, false, true, false},
        std::vector<TMaybe<bool>>{Nothing(), Nothing(), Nothing(), Nothing()},
        std::vector<TMaybe<bool>>{true, Nothing(), true, Nothing()});
}

Y_UNIT_TEST(ScalarTrueLeftOperand) {
    // Test with scalar true left operand
    TestOrKernel(
        true,
        std::vector<bool>{true, false, true, false},
        std::vector<bool>{true, true, true, true});
}

Y_UNIT_TEST(ScalarFalseLeftOperand) {
    // Test with scalar false left operand
    TestOrKernel(
        false,
        std::vector<bool>{true, false, true, false},
        std::vector<bool>{true, false, true, false});
}

Y_UNIT_TEST(ScalarNullLeftOperand) {
    // Test with scalar null left operand
    TestOrKernel(
        TMaybe<bool>{Nothing()},
        std::vector<bool>{true, false, true, false},
        std::vector<TMaybe<bool>>{true, Nothing(), true, Nothing()});
}

} // Y_UNIT_TEST_SUITE(TMiniKQLBlockLogicalOrTest)

// Tests for BlockXor operation
Y_UNIT_TEST_SUITE(TMiniKQLBlockLogicalXorTest) {

Y_UNIT_TEST(NonNullValues) {
    // Test with non-null values
    TestXorKernel(
        std::vector<bool>{true, true, false, false},
        std::vector<bool>{true, false, true, false},
        std::vector<bool>{false, true, true, false});
}

Y_UNIT_TEST(ScalarTrueRightOperand) {
    // Test with scalar true right operand
    TestXorKernel(
        std::vector<bool>{true, true, false, false},
        true,
        std::vector<bool>{false, false, true, true});
}

Y_UNIT_TEST(ScalarFalseRightOperand) {
    // Test with scalar false right operand
    TestXorKernel(
        std::vector<bool>{true, true, false, false},
        false,
        std::vector<bool>{true, true, false, false});
}

Y_UNIT_TEST(MixedNullNonNull) {
    // Test with mixed null/non-null values
    // In SQL: NULL XOR anything -> NULL
    TestXorKernel(
        std::vector<TMaybe<bool>>{Nothing(), true, false, Nothing()},
        std::vector<bool>{true, true, false, false},
        std::vector<TMaybe<bool>>{Nothing(), false, false, Nothing()});
}

Y_UNIT_TEST(BothOperandsWithNulls) {
    // Test with both operands having nulls
    TestXorKernel(
        std::vector<TMaybe<bool>>{Nothing(), true, false, Nothing()},
        std::vector<TMaybe<bool>>{true, Nothing(), Nothing(), false},
        std::vector<TMaybe<bool>>{Nothing(), Nothing(), Nothing(), Nothing()});
}

Y_UNIT_TEST(ScalarNullRightOperand) {
    // Test with scalar null right operand
    TestXorKernel(
        std::vector<TMaybe<bool>>{Nothing(), true, false, Nothing()},
        TMaybe<bool>{Nothing()},
        std::vector<TMaybe<bool>>{Nothing(), Nothing(), Nothing(), Nothing()});
}

Y_UNIT_TEST(AllNullLeftOperands) {
    // Test with all null left operands
    TestXorKernel(
        std::vector<TMaybe<bool>>{Nothing(), Nothing(), Nothing(), Nothing()},
        std::vector<bool>{true, false, true, false},
        std::vector<TMaybe<bool>>{Nothing(), Nothing(), Nothing(), Nothing()});
}

Y_UNIT_TEST(AllNullRightOperands) {
    // Test with all null right operands
    TestXorKernel(
        std::vector<bool>{true, false, true, false},
        std::vector<TMaybe<bool>>{Nothing(), Nothing(), Nothing(), Nothing()},
        std::vector<TMaybe<bool>>{Nothing(), Nothing(), Nothing(), Nothing()});
}

Y_UNIT_TEST(ScalarTrueLeftOperand) {
    // Test with scalar true left operand
    TestXorKernel(
        true,
        std::vector<bool>{true, false, true, false},
        std::vector<bool>{false, true, false, true});
}

Y_UNIT_TEST(ScalarFalseLeftOperand) {
    // Test with scalar false left operand
    TestXorKernel(
        false,
        std::vector<bool>{true, false, true, false},
        std::vector<bool>{true, false, true, false});
}

Y_UNIT_TEST(ScalarNullLeftOperand) {
    // Test with scalar null left operand
    TestXorKernel(
        TMaybe<bool>{Nothing()},
        std::vector<bool>{true, false, true, false},
        std::vector<TMaybe<bool>>{Nothing(), Nothing(), Nothing(), Nothing()});
}

} // Y_UNIT_TEST_SUITE(TMiniKQLBlockLogicalXorTest)

// Tests for BlockNot operation
Y_UNIT_TEST_SUITE(TMiniKQLBlockNotTest) {

Y_UNIT_TEST(NonNullValues) {
    // Test with non-null values
    TestNotKernel(
        std::vector<bool>{true, false, true, false},
        std::vector<bool>{false, true, false, true});
}

Y_UNIT_TEST(MixedNullNonNull) {
    // Test with mixed null/non-null values
    // In SQL: NOT NULL -> NULL
    TestNotKernel(
        std::vector<TMaybe<bool>>{Nothing(), true, false, Nothing()},
        std::vector<TMaybe<bool>>{Nothing(), false, true, Nothing()});
}

Y_UNIT_TEST(AllNullValues) {
    // Test with all null values
    TestNotKernel(
        std::vector<TMaybe<bool>>{Nothing(), Nothing(), Nothing(), Nothing()},
        std::vector<TMaybe<bool>>{Nothing(), Nothing(), Nothing(), Nothing()});
}

} // Y_UNIT_TEST_SUITE(TMiniKQLBlockNotTest)

} // namespace NKikimr::NMiniKQL
