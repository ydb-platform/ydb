#include <yql/essentials/minikql/comp_nodes/mkql_block_getelem.h>

#include <yql/essentials/minikql/comp_nodes/ut/mkql_block_test_helper.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>

namespace NKikimr::NMiniKQL {

namespace {

template <typename T, typename V>
void TestNthKernelFuzzied(T tuple, ui32 idx, V expected) {
    TBlockHelper().TestKernelFuzzied(tuple, expected,
                                     [&](TSetup<false>& setup, TRuntimeNode tuple) {
                                         return setup.PgmBuilder->BlockNth(tuple, idx);
                                     });
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockNthTest) {

Y_UNIT_TEST(NotOptTupleNotOptElementTest) {
    TestNthKernelFuzzied(TupleZip(std::vector<ui32>{1, 2, 3, 4}), 0, std::vector<ui32>{1, 2, 3, 4});
    TestNthKernelFuzzied(TupleZip(std::vector<ui32>{1}), 0, std::vector<ui32>{1});
}

Y_UNIT_TEST(NotOptTupleOptElementTest) {
    TestNthKernelFuzzied(TupleZip(std::vector<TMaybe<ui32>>{1, 2, Nothing(), 4}), 0, std::vector<TMaybe<ui32>>{1, 2, Nothing(), 4});
}

Y_UNIT_TEST(OptTupleNotOptElementTest) {
    auto tuple = TupleZip(std::vector<ui32>{1, 2, 3, 4});
    std::vector<TMaybe<std::tuple<ui32>>> optTuple(tuple.begin(), tuple.end());
    optTuple[1] = Nothing();
    TestNthKernelFuzzied(optTuple, 0, std::vector<TMaybe<ui32>>{1, Nothing(), 3, 4});
}

Y_UNIT_TEST(OptTupleOptElementTest) {
    auto tuple = TupleZip(std::vector<TMaybe<ui32>>{1, 2, 3, Nothing()});
    std::vector<TMaybe<std::tuple<TMaybe<ui32>>>> optTuple(tuple.begin(), tuple.end());
    optTuple[1] = Nothing();
    TestNthKernelFuzzied(optTuple, 0, std::vector<TMaybe<ui32>>{1, Nothing(), 3, Nothing()});
}

Y_UNIT_TEST(OptTupleDoubleOptElementTest) {
    std::vector<TMaybe<TMaybe<ui32>>> doubleOptValues{TMaybe<TMaybe<ui32>>{TMaybe<ui32>{1}},
                                                      TMaybe<TMaybe<ui32>>{TMaybe<ui32>{2}},
                                                      TMaybe<TMaybe<ui32>>{Nothing()},
                                                      Nothing()};

    auto tuple = TupleZip(doubleOptValues);
    std::vector<TMaybe<std::tuple<TMaybe<TMaybe<ui32>>>>> optTuple(tuple.begin(), tuple.end());
    optTuple[1] = Nothing();

    std::vector<TMaybe<TMaybe<ui32>>> expected{TMaybe<TMaybe<ui32>>{TMaybe<ui32>{1}},
                                               Nothing(),
                                               TMaybe<TMaybe<ui32>>{Nothing()},
                                               Nothing()};

    TestNthKernelFuzzied(optTuple, 0, expected);
}

Y_UNIT_TEST(NotOptTupleDoubleOptElementTest) {
    std::vector<TMaybe<TMaybe<ui32>>> doubleOptValues{TMaybe<TMaybe<ui32>>{TMaybe<ui32>{1}},
                                                      TMaybe<TMaybe<ui32>>{TMaybe<ui32>{2}},
                                                      TMaybe<TMaybe<ui32>>{Nothing()},
                                                      Nothing()};

    auto tuple = TupleZip(doubleOptValues);
    TestNthKernelFuzzied(tuple, 0, doubleOptValues);
}

Y_UNIT_TEST(OptTupleNotOptSingularVoidTest) {
    // Create a tuple with non-optional TSingularVoid elements
    auto tuple = TupleZip(std::vector<TSingularVoid>{TSingularVoid(), TSingularVoid(), TSingularVoid(), TSingularVoid()});

    // Convert to optional tuple and make one element null
    std::vector<TMaybe<std::tuple<TSingularVoid>>> optTuple(tuple.begin(), tuple.end());
    optTuple[1] = Nothing();

    // Expected result: optional TSingularVoid with one element being Nothing
    std::vector<TMaybe<TSingularVoid>> expected{TSingularVoid(), Nothing(), TSingularVoid(), TSingularVoid()};

    TestNthKernelFuzzied(optTuple, 0, expected);
}

Y_UNIT_TEST(OptTupleNotOptSingularNullTest) {
    // Create a tuple with non-optional TSingularNull elements
    auto tuple = TupleZip(std::vector<TSingularNull>{TSingularNull(), TSingularNull(), TSingularNull(), TSingularNull()});

    // Convert to optional tuple and make one element null
    std::vector<TMaybe<std::tuple<TSingularNull>>> optTuple(tuple.begin(), tuple.end());
    optTuple[1] = Nothing();

    // Expected result: optional TSingularNull with one element being Nothing
    std::vector<TSingularNull> expected{TSingularNull(), TSingularNull(), TSingularNull(), TSingularNull()};

    TestNthKernelFuzzied(optTuple, 0, expected);
}

Y_UNIT_TEST(OptTupleOptSingularVoidTest) {
    // Create a tuple with optional TSingularVoid elements
    std::vector<TMaybe<TSingularVoid>> values{TSingularVoid(), TSingularVoid(), Nothing(), TSingularVoid()};
    auto tuple = TupleZip(values);

    // Convert to optional tuple and make one element null
    std::vector<TMaybe<std::tuple<TMaybe<TSingularVoid>>>> optTuple(tuple.begin(), tuple.end());
    optTuple[1] = Nothing();

    // Expected result: optional TSingularVoid with two elements being Nothing
    std::vector<TMaybe<TSingularVoid>> expected{TSingularVoid(), Nothing(), Nothing(), TSingularVoid()};

    TestNthKernelFuzzied(optTuple, 0, expected);
}

Y_UNIT_TEST(OptTupleOptSingularNullTest) {
    // Create a tuple with optional TSingularNull elements
    std::vector<TMaybe<TSingularNull>> values{TSingularNull(), TSingularNull(), Nothing(), TSingularNull()};
    auto tuple = TupleZip(values);

    // Convert to optional tuple and make one element null
    std::vector<TMaybe<std::tuple<TMaybe<TSingularNull>>>> optTuple(tuple.begin(), tuple.end());
    optTuple[1] = Nothing();

    // Expected result: optional TSingularNull with two elements being Nothing
    std::vector<TMaybe<TSingularNull>> expected{TSingularNull(), Nothing(), Nothing(), TSingularNull()};

    TestNthKernelFuzzied(optTuple, 0, expected);
}

Y_UNIT_TEST(OptTupleDoubleOptSingularVoidTest) {
    // Create a tuple with double-optional TSingularVoid elements
    std::vector<TMaybe<TMaybe<TSingularVoid>>> doubleOptValues{
        TMaybe<TMaybe<TSingularVoid>>{TMaybe<TSingularVoid>{TSingularVoid()}},
        TMaybe<TMaybe<TSingularVoid>>{TMaybe<TSingularVoid>{TSingularVoid()}},
        TMaybe<TMaybe<TSingularVoid>>{Nothing()},
        Nothing()};

    auto tuple = TupleZip(doubleOptValues);
    std::vector<TMaybe<std::tuple<TMaybe<TMaybe<TSingularVoid>>>>> optTuple(tuple.begin(), tuple.end());
    optTuple[1] = Nothing();

    std::vector<TMaybe<TMaybe<TSingularVoid>>> expected{
        TMaybe<TMaybe<TSingularVoid>>{TMaybe<TSingularVoid>{TSingularVoid()}},
        Nothing(),
        TMaybe<TMaybe<TSingularVoid>>{Nothing()},
        Nothing()};

    TestNthKernelFuzzied(optTuple, 0, expected);
}

Y_UNIT_TEST(OptTupleDoubleOptSingularNullTest) {
    // Create a tuple with double-optional TSingularNull elements
    std::vector<TMaybe<TMaybe<TSingularNull>>> doubleOptValues{
        TMaybe<TMaybe<TSingularNull>>{TMaybe<TSingularNull>{TSingularNull()}},
        TMaybe<TMaybe<TSingularNull>>{TMaybe<TSingularNull>{TSingularNull()}},
        TMaybe<TMaybe<TSingularNull>>{Nothing()},
        Nothing()};

    auto tuple = TupleZip(doubleOptValues);
    std::vector<TMaybe<std::tuple<TMaybe<TMaybe<TSingularNull>>>>> optTuple(tuple.begin(), tuple.end());
    optTuple[1] = Nothing();

    std::vector<TMaybe<TMaybe<TSingularNull>>> expected{
        TMaybe<TMaybe<TSingularNull>>{TMaybe<TSingularNull>{TSingularNull()}},
        Nothing(),
        TMaybe<TMaybe<TSingularNull>>{Nothing()},
        Nothing()};

    TestNthKernelFuzzied(optTuple, 0, expected);
}

Y_UNIT_TEST(NotOptTupleDoubleOptSingularVoidTest) {
    // Create a tuple with double-optional TSingularVoid elements
    std::vector<TMaybe<TMaybe<TSingularVoid>>> doubleOptValues{
        TMaybe<TMaybe<TSingularVoid>>{TMaybe<TSingularVoid>{TSingularVoid()}},
        TMaybe<TMaybe<TSingularVoid>>{TMaybe<TSingularVoid>{TSingularVoid()}},
        TMaybe<TMaybe<TSingularVoid>>{Nothing()},
        Nothing()};

    auto tuple = TupleZip(doubleOptValues);

    // Test with non-optional tuple
    TestNthKernelFuzzied(tuple, 0, doubleOptValues);
}

Y_UNIT_TEST(NotOptTupleDoubleOptSingularNullTest) {
    // Create a tuple with double-optional TSingularNull elements
    std::vector<TMaybe<TMaybe<TSingularNull>>> doubleOptValues{
        TMaybe<TMaybe<TSingularNull>>{TMaybe<TSingularNull>{TSingularNull()}},
        TMaybe<TMaybe<TSingularNull>>{TMaybe<TSingularNull>{TSingularNull()}},
        TMaybe<TMaybe<TSingularNull>>{Nothing()},
        Nothing()};

    auto tuple = TupleZip(doubleOptValues);

    // Test with non-optional tuple
    TestNthKernelFuzzied(tuple, 0, doubleOptValues);
}

Y_UNIT_TEST(TupleSingularNullTest) {
    // Create a tuple with non-optional TSingularNull elements
    auto values = std::vector<TSingularNull>{TSingularNull(), TSingularNull(), TSingularNull(), TSingularNull()};
    auto tuple = TupleZip(values);

    // Test with non-optional tuple
    TestNthKernelFuzzied(tuple, 0, values);
}

Y_UNIT_TEST(OptTupleSingularNullTest) {
    // Create a tuple with non-optional TSingularNull elements
    auto values = std::vector<TSingularNull>{TSingularNull(), TSingularNull(), TSingularNull(), TSingularNull()};
    auto tuple = TupleZip(values);
    // Convert to optional tuple and make one element null
    std::vector<TMaybe<std::tuple<TSingularNull>>> optTuple(tuple.begin(), tuple.end());
    optTuple[1] = Nothing();
    TestNthKernelFuzzied(optTuple, 0, values);
}

Y_UNIT_TEST(TupleTaggedIntTest) {
    auto values = TagVector<TTag::A>(std::vector<ui32>{1, 2, 3, 4});
    TestNthKernelFuzzied(TupleZip(values), 0, values);
}

Y_UNIT_TEST(OptTupleTaggedTest) {
    // Create a tuple with tagged non-optional elements
    auto values = TagVector<TTag::A>(std::vector<ui32>{1, 2, 3, 4});
    auto tuple = TupleZip(values);

    // Convert to optional tuple and make one element null
    std::vector<TMaybe<std::tuple<TTagged<ui32, TTag::A>>>> optTuple(tuple.begin(), tuple.end());
    optTuple[1] = Nothing();

    // Expected result: optional tagged values with one element being Nothing
    std::vector<TMaybe<TTagged<ui32, TTag::A>>> expected{
        TTagged<ui32, TTag::A>(1),
        Nothing(),
        TTagged<ui32, TTag::A>(3),
        TTagged<ui32, TTag::A>(4)};

    TestNthKernelFuzzied(optTuple, 0, expected);
}

Y_UNIT_TEST(OptTupleTaggedOptTest) {
    // Create a tuple with tagged optional elements
    auto values = TagVector<TTag::A>(std::vector<TMaybe<ui32>>{1, 2, Nothing(), 4});
    auto tuple = TupleZip(values);

    // Convert to optional tuple and make one element null
    std::vector<TMaybe<std::tuple<TTagged<TMaybe<ui32>, TTag::A>>>> optTuple(tuple.begin(), tuple.end());
    optTuple[1] = Nothing();

    // Expected result: optional tagged optional values with two elements being Nothing
    std::vector<TMaybe<TTagged<TMaybe<ui32>, TTag::A>>> expected{
        TTagged<TMaybe<ui32>, TTag::A>(1),
        Nothing(),
        TTagged<TMaybe<ui32>, TTag::A>(Nothing()),
        TTagged<TMaybe<ui32>, TTag::A>(4)};

    TestNthKernelFuzzied(optTuple, 0, expected);
}

Y_UNIT_TEST(OptTupleTaggedSingularTest) {
    // Create a tuple with tagged singular elements
    auto values = TagVector<TTag::A>(std::vector<TSingularVoid>{TSingularVoid(), TSingularVoid(), TSingularVoid(), TSingularVoid()});
    auto tuple = TupleZip(values);

    // Convert to optional tuple and make one element null
    std::vector<TMaybe<std::tuple<TTagged<TSingularVoid, TTag::A>>>> optTuple(tuple.begin(), tuple.end());
    optTuple[1] = Nothing();

    // Expected result: optional tagged singular values with one element being Nothing
    std::vector<TMaybe<TTagged<TSingularVoid, TTag::A>>> expected{
        TTagged<TSingularVoid, TTag::A>(TSingularVoid()),
        Nothing(),
        TTagged<TSingularVoid, TTag::A>(TSingularVoid()),
        TTagged<TSingularVoid, TTag::A>(TSingularVoid())};

    TestNthKernelFuzzied(optTuple, 0, expected);
}

Y_UNIT_TEST(OptTupleOptTaggedTest) {
    // Create a vector of optional tagged elements
    std::vector<TMaybe<TTagged<ui32, TTag::A>>> values{
        TMaybe<TTagged<ui32, TTag::A>>{TTagged<ui32, TTag::A>(1)},
        TMaybe<TTagged<ui32, TTag::A>>{TTagged<ui32, TTag::A>(2)},
        Nothing(),
        TMaybe<TTagged<ui32, TTag::A>>{TTagged<ui32, TTag::A>(4)}};

    auto tuple = TupleZip(values);

    // Convert to optional tuple and make one element null
    std::vector<TMaybe<std::tuple<TMaybe<TTagged<ui32, TTag::A>>>>> optTuple(tuple.begin(), tuple.end());
    optTuple[1] = Nothing();

    // Expected result: optional optional tagged values with two elements being Nothing
    std::vector<TMaybe<TTagged<ui32, TTag::A>>> expected{
        TMaybe<TTagged<ui32, TTag::A>>{TMaybe<TTagged<ui32, TTag::A>>{TTagged<ui32, TTag::A>(1)}},
        Nothing(),
        Nothing(),
        TMaybe<TTagged<ui32, TTag::A>>{TMaybe<TTagged<ui32, TTag::A>>{TTagged<ui32, TTag::A>(4)}}};

    TestNthKernelFuzzied(optTuple, 0, expected);
}

Y_UNIT_TEST(OptTuplePgIntTest) {
    // Create a tuple with non-optional TPgInt elements
    auto values = std::vector<TPgInt>{TPgInt(1), TPgInt(2), TPgInt(), TPgInt(4)};
    auto tuple = TupleZip(values);

    // Convert to optional tuple and make one element null
    std::vector<TMaybe<std::tuple<TPgInt>>> optTuple(tuple.begin(), tuple.end());
    optTuple[1] = Nothing();

    // Expected result: optional TPgInt with one element being Nothing
    std::vector<TPgInt> expected{TPgInt(1), TPgInt(), TPgInt(), TPgInt(4)};

    TestNthKernelFuzzied(optTuple, 0, expected);
}

} // Y_UNIT_TEST_SUITE(TMiniKQLBlockNthTest)

} // namespace NKikimr::NMiniKQL
