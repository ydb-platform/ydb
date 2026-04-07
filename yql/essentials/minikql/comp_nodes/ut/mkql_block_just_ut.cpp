#include <yql/essentials/minikql/comp_nodes/mkql_block_just.h>

#include <yql/essentials/minikql/comp_nodes/ut/mkql_block_test_helper.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>

namespace NKikimr::NMiniKQL {

namespace {

template <typename T, typename V>
void TestJustKernelFuzzied(T input, V expected) {
    TBlockHelper().TestKernelFuzzied(input, expected,
                                     [&](TSetup<false>& setup, TRuntimeNode input) {
                                         return setup.PgmBuilder->BlockJust(input);
                                     });
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockJustTest) {

// Tests for non-optional input
Y_UNIT_TEST(NotOptIntTest) {
    // For non-optional types, BlockJust should add one level of optionality
    std::vector<ui32> input{1, 2, 3, 4};
    std::vector<TMaybe<ui32>> expected{1, 2, 3, 4};
    TestJustKernelFuzzied(input, expected);
}

Y_UNIT_TEST(NotOptSingularNullTest) {
    // Test with non-optional TSingularNull
    std::vector<TSingularNull> input{TSingularNull(), TSingularNull(), TSingularNull(), TSingularNull()};
    std::vector<TMaybe<TSingularNull>> expected{TSingularNull(), TSingularNull(), TSingularNull(), TSingularNull()};
    TestJustKernelFuzzied(input, expected);
}

Y_UNIT_TEST(NotOptTaggedTest) {
    // Test with non-optional tagged values
    auto input = TagVector<TTag::A>(std::vector<ui32>{1, 2, 3, 4});
    std::vector<TMaybe<TTagged<ui32, TTag::A>>> expected{
        TTagged<ui32, TTag::A>(1),
        TTagged<ui32, TTag::A>(2),
        TTagged<ui32, TTag::A>(3),
        TTagged<ui32, TTag::A>(4)};
    TestJustKernelFuzzied(input, expected);
}

// Tests for already optional input
Y_UNIT_TEST(OptIntTest) {
    // For optional types, BlockJust should add another level of optionality
    std::vector<TMaybe<ui32>> input{1, 2, Nothing(), 4};
    // The top level TMaybe should always be defined when BlockJust adds a level of optionality
    std::vector<TMaybe<TMaybe<ui32>>> expected{
        TMaybe<TMaybe<ui32>>{TMaybe<ui32>{1}},
        TMaybe<TMaybe<ui32>>{TMaybe<ui32>{2}},
        TMaybe<TMaybe<ui32>>{TMaybe<ui32>()},
        TMaybe<TMaybe<ui32>>{TMaybe<ui32>{4}}};
    TestJustKernelFuzzied(input, expected);
}

Y_UNIT_TEST(OptSingularVoidTest) {
    // Test with optional TSingularVoid
    std::vector<TMaybe<TSingularVoid>> input{TSingularVoid(), TSingularVoid(), Nothing(), TSingularVoid()};
    // The top level TMaybe should always be defined when BlockJust adds a level of optionality
    std::vector<TMaybe<TMaybe<TSingularVoid>>> expected{
        TMaybe<TMaybe<TSingularVoid>>{TMaybe<TSingularVoid>{TSingularVoid()}},
        TMaybe<TMaybe<TSingularVoid>>{TMaybe<TSingularVoid>{TSingularVoid()}},
        TMaybe<TMaybe<TSingularVoid>>{TMaybe<TSingularVoid>()},
        TMaybe<TMaybe<TSingularVoid>>{TMaybe<TSingularVoid>{TSingularVoid()}}};
    TestJustKernelFuzzied(input, expected);
}

Y_UNIT_TEST(OptSingularNullTest) {
    // Test with optional TSingularNull
    std::vector<TMaybe<TSingularNull>> input{TSingularNull(), TSingularNull(), Nothing(), TSingularNull()};
    // The top level TMaybe should always be defined when BlockJust adds a level of optionality
    std::vector<TMaybe<TMaybe<TSingularNull>>> expected{
        TMaybe<TMaybe<TSingularNull>>{TMaybe<TSingularNull>{TSingularNull()}},
        TMaybe<TMaybe<TSingularNull>>{TMaybe<TSingularNull>{TSingularNull()}},
        TMaybe<TMaybe<TSingularNull>>{TMaybe<TSingularNull>()},
        TMaybe<TMaybe<TSingularNull>>{TMaybe<TSingularNull>{TSingularNull()}}};
    TestJustKernelFuzzied(input, expected);
}

Y_UNIT_TEST(OptTaggedTest) {
    // Test with optional tagged values
    std::vector<TMaybe<TTagged<ui32, TTag::A>>> input{
        TMaybe<TTagged<ui32, TTag::A>>{TTagged<ui32, TTag::A>(1)},
        TMaybe<TTagged<ui32, TTag::A>>{TTagged<ui32, TTag::A>(2)},
        Nothing(),
        TMaybe<TTagged<ui32, TTag::A>>{TTagged<ui32, TTag::A>(4)}};
    // The top level TMaybe should always be defined when BlockJust adds a level of optionality
    std::vector<TMaybe<TMaybe<TTagged<ui32, TTag::A>>>> expected{
        TMaybe<TMaybe<TTagged<ui32, TTag::A>>>{TMaybe<TTagged<ui32, TTag::A>>{TTagged<ui32, TTag::A>(1)}},
        TMaybe<TMaybe<TTagged<ui32, TTag::A>>>{TMaybe<TTagged<ui32, TTag::A>>{TTagged<ui32, TTag::A>(2)}},
        TMaybe<TMaybe<TTagged<ui32, TTag::A>>>{TMaybe<TTagged<ui32, TTag::A>>()},
        TMaybe<TMaybe<TTagged<ui32, TTag::A>>>{TMaybe<TTagged<ui32, TTag::A>>{TTagged<ui32, TTag::A>(4)}}};
    TestJustKernelFuzzied(input, expected);
}

Y_UNIT_TEST(PgIntTest) {
    // Test with TPgInt (which is internally optional)
    std::vector<TPgInt> input{TPgInt(1), TPgInt(2), TPgInt(), TPgInt(4)};
    std::vector<TMaybe<TPgInt>> expected{TPgInt(1), TPgInt(2), TPgInt(), TPgInt(4)};
    TestJustKernelFuzzied(input, expected);
}

Y_UNIT_TEST(TaggedOptTest) {
    // Test with tagged optional values (TTagged<TMaybe<ui32>, TTag::A>)
    auto input = TagVector<TTag::A>(std::vector<TMaybe<ui32>>{1, 2, Nothing(), 4});

    // Expected: tagged optional values wrapped in Maybe
    std::vector<TMaybe<TTagged<TMaybe<ui32>, TTag::A>>> expected;
    for (const auto& item : input) {
        expected.push_back(TMaybe<TTagged<TMaybe<ui32>, TTag::A>>(item));
    }

    TestJustKernelFuzzied(input, expected);
}

} // Y_UNIT_TEST_SUITE(TMiniKQLBlockJustTest)

} // namespace NKikimr::NMiniKQL
