#include "mkql_computation_node_ut.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_block_builder.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_block_test_helper.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <typename T, typename V>
void TestExistKernel(T operand, V expected) {
    TBlockHelper().TestKernel(operand, expected,
                              [](TSetup<false>& setup, TRuntimeNode node) {
                                  return setup.PgmBuilder->BlockExists(node);
                              });
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockExistsTest) {

Y_UNIT_TEST(TestOptionalVector) {
    // Test with vector of optionals - mixed defined/undefined
    TestExistKernel(
        std::vector<TMaybe<ui32>>{TMaybe<ui32>(1), TMaybe<ui32>(), TMaybe<ui32>(3), TMaybe<ui32>()},
        std::vector<bool>{true, false, true, false});

    // Test with vector of optionals - all defined
    TestExistKernel(
        std::vector<TMaybe<ui32>>{TMaybe<ui32>(1), TMaybe<ui32>(2), TMaybe<ui32>(3)},
        std::vector<bool>{true, true, true});

    // Test with vector of optionals - all undefined
    TestExistKernel(
        std::vector<TMaybe<ui32>>{TMaybe<ui32>(), TMaybe<ui32>(), TMaybe<ui32>()},
        std::vector<bool>{false, false, false});
}

Y_UNIT_TEST(TestNestedOptionalVector) {
    // Test with vector of nested optionals - various combinations
    TestExistKernel(
        std::vector<TMaybe<TMaybe<ui32>>>{
            TMaybe<TMaybe<ui32>>(TMaybe<ui32>(1)), // outer and inner defined
            TMaybe<TMaybe<ui32>>(TMaybe<ui32>()),  // outer defined, inner undefined
            TMaybe<TMaybe<ui32>>(),                // outer undefined
            TMaybe<TMaybe<ui32>>(TMaybe<ui32>(4))  // outer and inner defined
        },
        std::vector<bool>{true, true, false, true});
}

Y_UNIT_TEST(TestPgIntVector) {
    // Test with vector of PgInt - mixed defined/undefined
    TestExistKernel(
        std::vector<TPgInt>{TPgInt(1), TPgInt(), TPgInt(3), TPgInt()},
        std::vector<bool>{true, false, true, false});

    // Test with vector of PgInt - all defined
    TestExistKernel(
        std::vector<TPgInt>{TPgInt(1), TPgInt(2), TPgInt(3)},
        std::vector<bool>{true, true, true});

    // Test with vector of PgInt - all undefined
    TestExistKernel(
        std::vector<TPgInt>{TPgInt(), TPgInt(), TPgInt()},
        std::vector<bool>{false, false, false});

    // Test with vector of optional PgInt - mixed
    TestExistKernel(
        std::vector<TMaybe<TPgInt>>{
            TMaybe<TPgInt>(TPgInt(1)),
            TMaybe<TPgInt>(),
            TMaybe<TPgInt>(TPgInt()),
            TMaybe<TPgInt>(TPgInt(4))},
        std::vector<bool>{true, false, true, true});
}

Y_UNIT_TEST(TestWithStrings) {
    // Test with optional strings
    TestExistKernel(
        std::vector<TMaybe<TString>>{
            TMaybe<TString>(TString("hello")),
            TMaybe<TString>(),
            TMaybe<TString>(TString("world"))},
        std::vector<bool>{true, false, true});
}

Y_UNIT_TEST(TestWithTaggedTypes) {
    using TaggedIntA = TTagged<ui32, TTag::A>;
    using TaggedIntB = TTagged<ui32, TTag::B>;

    // Test with tagged types
    TestExistKernel(
        std::vector<TMaybe<TaggedIntA>>{
            TMaybe<TaggedIntA>(TaggedIntA(1)),
            TMaybe<TaggedIntA>(),
            TMaybe<TaggedIntA>(TaggedIntA(3))},
        std::vector<bool>{true, false, true});

    // Test with different tagged types
    TestExistKernel(
        std::vector<TMaybe<TaggedIntB>>{
            TMaybe<TaggedIntB>(TaggedIntB(1)),
            TMaybe<TaggedIntB>(),
            TMaybe<TaggedIntB>(TaggedIntB(3))},
        std::vector<bool>{true, false, true});
}

Y_UNIT_TEST(TestWithSingularTypes) {
    // Test with singular void type
    TestExistKernel(
        std::vector<TMaybe<TSingularVoid>>{
            TMaybe<TSingularVoid>(TSingularVoid()),
            TMaybe<TSingularVoid>(),
            TMaybe<TSingularVoid>(TSingularVoid())},
        std::vector<bool>{true, false, true});

    // Test with singular null type
    TestExistKernel(
        std::vector<TMaybe<TSingularNull>>{
            TMaybe<TSingularNull>(TSingularNull()),
            TMaybe<TSingularNull>(),
            TMaybe<TSingularNull>(TSingularNull())},
        std::vector<bool>{true, false, true});
}

} // Y_UNIT_TEST_SUITE(TMiniKQLBlockExistsTest)

} // namespace NMiniKQL
} // namespace NKikimr
