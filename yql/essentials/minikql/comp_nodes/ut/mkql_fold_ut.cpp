#include "mkql_computation_node_ut.h"
#include "mkql_program_builder_test_utils.h"

#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

#include <random>
#include <ctime>
#include <algorithm>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLFoldNodeTest) {
Y_UNIT_TEST_LLVM(TestFoldOverList) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{1, 2, 3});
    auto pgmReturn = pb.Fold(list, NTest::ConvertValueToLiteralNode(pb, ui32(0)),
                             [&](TRuntimeNode item, TRuntimeNode state) {
                                 return pb.Add(item, state);
                             });

    auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), ui32(6));
}

Y_UNIT_TEST_LLVM(TestFold1OverEmptyList) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto list = pb.NewEmptyList(NTest::ConvertToMinikqlType<ui32>(pb));
    auto data2 = NTest::ConvertValueToLiteralNode(pb, ui32(2));
    auto pgmReturn = pb.Fold1(list, [&](TRuntimeNode item) { return pb.Mul(item, data2); },
                              [&](TRuntimeNode item, TRuntimeNode state) { return pb.Add(item, state); });

    auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TMaybe<ui32>{});
}

Y_UNIT_TEST_LLVM(TestFold1OverSingleElementList) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto data2 = NTest::ConvertValueToLiteralNode(pb, ui32(2));
    auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{1});
    auto pgmReturn = pb.Fold1(list,
                              [&](TRuntimeNode item) { return pb.Mul(item, data2); },
                              [&](TRuntimeNode item, TRuntimeNode state) { return pb.Add(item, state); });

    auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TMaybe<ui32>{ui32(2)});
}

Y_UNIT_TEST_LLVM(TestFold1OverManyElementList) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto data2 = NTest::ConvertValueToLiteralNode(pb, ui32(2));
    auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{1, 2});
    auto pgmReturn = pb.Fold1(list,
                              [&](TRuntimeNode item) { return pb.Mul(item, data2); },
                              [&](TRuntimeNode item, TRuntimeNode state) { return pb.Add(item, state); });

    auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TMaybe<ui32>{ui32(4)});
}

Y_UNIT_TEST_LLVM(TestFoldWithAggrAdd) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto data0 = NTest::ConvertValueToLiteralNode(pb, TMaybe<float>{42.f});
    auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<float>>{TMaybe<float>{}, 3.f, 2.f, 1.f});

    auto pgmReturn = pb.Fold(list, data0,
                             [&](TRuntimeNode item, TRuntimeNode state) {
                                 return pb.AggrAdd(pb.Increment(item), pb.Decrement(state));
                             });

    auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), float(47));
}

Y_UNIT_TEST_LLVM(TestNestedApply) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto list = NTest::ConvertValueToLiteralNode(pb, TVector<i32>{1, 2, 3});

    const auto callType = TCallableTypeBuilder(pb.GetTypeEnvironment(), "TEST", NTest::ConvertToMinikqlType<i32>(pb)).Add(NTest::ConvertToMinikqlType<i32>(pb)).Add(NTest::ConvertToMinikqlType<i32>(pb)).Build();

    auto pgmReturn = pb.Fold(list, NTest::ConvertValueToLiteralNode(pb, i32(100)),
                             [&](TRuntimeNode item, TRuntimeNode state) {
                                 return pb.Apply(pb.Callable(callType,
                                                             [&](const TArrayRef<const TRuntimeNode>& args) {
                                                                 return pb.Sub(args[1], args[0]);
                                                             }),
                                                 {item, state});
                             });

    auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), i32(94));
}

Y_UNIT_TEST_LLVM(TestLogicalOpts) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto args = NTest::ConvertValueToLiteralNode(pb, TVector<bool>{true, false});
    auto opts = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<bool>>{{}, true, false});

    auto pgmReturn = pb.Fold(opts, pb.NewEmptyList(NTest::ConvertToMinikqlType<TMaybe<bool>>(pb)),
                             [&](TRuntimeNode item, TRuntimeNode state) {
                                 const auto append = pb.Append(state, pb.Not(item));

                                 const auto one = pb.Fold(args, pb.NewEmptyList(NTest::ConvertToMinikqlType<TMaybe<bool>>(pb)),
                                                          [&](TRuntimeNode item2, TRuntimeNode state2) {
                                                              state2 = pb.Append(state2, pb.And({item, item2}));
                                                              state2 = pb.Append(state2, pb.And({item2, item}));
                                                              state2 = pb.Append(state2, pb.Or({item, item2}));
                                                              state2 = pb.Append(state2, pb.Or({item2, item}));
                                                              state2 = pb.Append(state2, pb.Xor({item, item2}));
                                                              state2 = pb.Append(state2, pb.Xor({item2, item}));
                                                              return state2;
                                                          });

                                 const auto two = pb.Fold(opts, pb.NewEmptyList(NTest::ConvertToMinikqlType<TMaybe<bool>>(pb)),
                                                          [&](TRuntimeNode item2, TRuntimeNode state2) {
                                                              state2 = pb.Append(state2, pb.And({item, item2}));
                                                              state2 = pb.Append(state2, pb.Or({item, item2}));
                                                              state2 = pb.Append(state2, pb.Xor({item, item2}));
                                                              return state2;
                                                          });

                                 return pb.Extend({append, one, two});
                             });

    auto graph = setup.BuildGraph(pgmReturn);

    // clang-format off
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<bool>>{
        // empty: not
        {},
        // empty/true: and, and
        {}, {},
        // empty/true: or, or
        true, true,
        // empty/true: xor, xor
        {}, {},
        // empty/false: and, and
        false, false,
        // empty/false: or, or
        {}, {},
        // empty/false: xor, xor
        {}, {},
        // empty/empty: and
        {},
        // empty/empty: or
        {},
        // empty/empty: xor
        {},
        // empty/true: and
        {},
        // empty/true: or
        true,
        // empty/true: xor
        {},
        // empty/false: and
        false,
        // empty/false: or
        {},
        // empty/false: xor
        {},
        // true: not
        false,
        // true/true: and, and
        true, true,
        // true/true: or, or
        true, true,
        // true/true: xor, xor
        false, false,
        // true/false: and, and
        false, false,
        // true/false: or, or
        true, true,
        // true/false: xor, xor
        true, true,
        // true/empty: and
        {},
        // true/true: or
        true,
        // true/empty: xor
        {},
        // true/true: and
        true,
        // true/true: or
        true,
        // true/true: xor
        false,
        // true/false: and
        false,
        // true/false: or
        true,
        // true/false: xor
        true,
        // false: not
        true,
        // false/true: and, and
        false, false,
        // false/true: or, or
        true, true,
        // false/true: xor, xor
        true, true,
        // false/false: and, and
        false, false,
        // false/false: or, or
        false, false,
        // false/false: xor, xor
        false, false,
        // false/empty: and
        false,
        // false/empty: or
        {},
        // false/empty: xor
        {},
        // false/true: and
        false,
        // false/true: or
        true,
        // false/true: xor
        true,
        // false/false: and
        false,
        // false/false: or
        false,
        // false/false: xor
        false,
    });
    // clang-format on
}

Y_UNIT_TEST_LLVM(TestFoldWithListInState) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{1, 2, 3});
    auto empty = pb.AddMember(pb.AddMember(
                                  pb.NewEmptyStruct(), "Max", pb.NewEmptyOptional(NTest::ConvertToMinikqlType<TMaybe<ui32>>(pb))),
                              "List", pb.NewEmptyList(NTest::ConvertToMinikqlType<ui32>(pb)));

    auto pgmReturn = pb.Fold(list, empty,
                             [&](TRuntimeNode item, TRuntimeNode state) {
                                 return pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Max",
                                                                  pb.IfPresent({pb.Member(state, "Max")},
                                                                               [&](TRuntimeNode::TList oldMax) {
                                                                                   return pb.NewOptional(pb.Max(oldMax.front(), item));
                                                                               }, pb.NewOptional(item))),
                                                     "List", pb.Append(pb.Member(state, "List"), item));
                             });

    auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue().GetElement(0), TVector<ui32>{1, 2, 3});
    AssertUnboxedValueElementEqual(graph->GetValue().GetElement(1), ui32(3));
}

Y_UNIT_TEST_LLVM(TestManyAppend) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto zeroList = pb.NewEmptyList(NTest::ConvertToMinikqlType<ui32>(pb));
    zeroList = pb.Append(zeroList, NTest::ConvertValueToLiteralNode(pb, ui32(0)));
    const ui32 n = 13;
    for (ui32 i = 0; i < n; ++i) {
        zeroList = pb.Extend({zeroList, zeroList});
    }

    auto state = pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Counter",
                                           NTest::ConvertValueToLiteralNode(pb, ui32(0))), "NewList",
                              pb.NewEmptyList(NTest::ConvertToMinikqlType<ui32>(pb)));

    auto fold = pb.Fold(zeroList, state,
                        [&](TRuntimeNode item, TRuntimeNode state) {
                            Y_UNUSED(item);
                            auto oldList = pb.Member(state, "NewList");
                            auto oldCounter = pb.Member(state, "Counter");
                            return pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Counter",
                                                             pb.Add(oldCounter, NTest::ConvertValueToLiteralNode(pb, ui32(1)))),
                                                "NewList", pb.Append(oldList, oldCounter));
                        });

    auto pgmReturn = pb.Member(fold, "NewList");

    auto graph = setup.BuildGraph(pgmReturn);
    UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetListLength(), 1 << n);

    auto iterator = graph->GetValue().GetListIterator();
    ui32 i = 0;
    for (NUdf::TUnboxedValue item; iterator.Next(item); ++i) {
        AssertUnboxedValueElementEqual(item, ui32(i));
    }
    UNIT_ASSERT(!iterator.Skip());
    UNIT_ASSERT_VALUES_EQUAL(i, 1 << n);
}

Y_UNIT_TEST_LLVM(TestManyPrepend) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto zeroList = pb.NewEmptyList(NTest::ConvertToMinikqlType<ui32>(pb));
    zeroList = pb.Append(zeroList, NTest::ConvertValueToLiteralNode(pb, ui32(0)));
    const ui32 n = 13;
    for (ui32 i = 0; i < n; ++i) {
        zeroList = pb.Extend({zeroList, zeroList});
    }

    auto state = pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Counter",
                                           NTest::ConvertValueToLiteralNode(pb, ui32(0))), "NewList",
                              pb.NewEmptyList(NTest::ConvertToMinikqlType<ui32>(pb)));

    auto fold = pb.Fold(zeroList, state,
                        [&](TRuntimeNode item, TRuntimeNode state) {
                            Y_UNUSED(item);
                            auto oldList = pb.Member(state, "NewList");
                            auto oldCounter = pb.Member(state, "Counter");
                            return pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Counter",
                                                             pb.Add(oldCounter, NTest::ConvertValueToLiteralNode(pb, ui32(1)))),
                                                "NewList", pb.Prepend(oldCounter, oldList));
                        });

    auto pgmReturn = pb.Member(fold, "NewList");

    auto graph = setup.BuildGraph(pgmReturn);
    UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetListLength(), 1 << n);

    auto iterator = graph->GetValue().GetListIterator();
    ui32 i = 1 << n;
    for (NUdf::TUnboxedValue item; iterator.Next(item);) {
        AssertUnboxedValueElementEqual(item, ui32(--i));
    }
    UNIT_ASSERT(!iterator.Skip());
    UNIT_ASSERT_VALUES_EQUAL(i, 0);
}

Y_UNIT_TEST_LLVM(TestManyExtend) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto zeroList = pb.NewEmptyList(NTest::ConvertToMinikqlType<ui32>(pb));
    zeroList = pb.Append(zeroList, NTest::ConvertValueToLiteralNode(pb, ui32(0)));
    const ui32 n = 13;
    for (ui32 i = 0; i < n; ++i) {
        zeroList = pb.Extend({zeroList, zeroList});
    }

    auto state = pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Counter",
                                           NTest::ConvertValueToLiteralNode(pb, ui32(0))), "NewList",
                              pb.NewEmptyList(NTest::ConvertToMinikqlType<ui32>(pb)));

    auto fold = pb.Fold(zeroList, state,
                        [&](TRuntimeNode item, TRuntimeNode state) {
                            Y_UNUSED(item);
                            auto oldList = pb.Member(state, "NewList");
                            auto oldCounter = pb.Member(state, "Counter");
                            auto oldCounterMul2 = pb.Mul(oldCounter, NTest::ConvertValueToLiteralNode(pb, ui32(2)));
                            auto extList = pb.NewEmptyList(NTest::ConvertToMinikqlType<ui32>(pb));
                            extList = pb.Append(extList, oldCounterMul2);
                            extList = pb.Append(extList, pb.Increment(oldCounterMul2));
                            return pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Counter",
                                                             pb.Add(oldCounter, NTest::ConvertValueToLiteralNode(pb, ui32(1)))),
                                                "NewList", pb.Extend({oldList, extList}));
                        });

    auto pgmReturn = pb.Member(fold, "NewList");

    auto graph = setup.BuildGraph(pgmReturn);
    UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetListLength(), 1 << (n + 1));

    auto iterator = graph->GetValue().GetListIterator();
    ui32 i = 0;
    for (NUdf::TUnboxedValue item; iterator.Next(item); ++i) {
        AssertUnboxedValueElementEqual(item, ui32(i));
    }
    UNIT_ASSERT(!iterator.Skip());
    UNIT_ASSERT_VALUES_EQUAL(i, 1 << (n + 1));
}

Y_UNIT_TEST_LLVM(TestFoldSingular) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{1, 2, 3});
    auto fold1 = pb.Fold(list, NTest::ConvertValueToLiteralNode(pb, ui32(0)),
                         [&](TRuntimeNode item, TRuntimeNode state) {
                             Y_UNUSED(state);
                             return item;
                         });

    auto fold2 = pb.Fold(list, NTest::ConvertValueToLiteralNode(pb, ui32(0)),
                         [&](TRuntimeNode item, TRuntimeNode state) {
                             Y_UNUSED(item);
                             return state;
                         });

    auto pgmReturn = pb.NewList(NTest::ConvertToMinikqlType<ui32>(pb), {fold1, fold2});

    auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{3, 0});
}

Y_UNIT_TEST_LLVM(TestSumListSizes) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto item = NTest::ConvertValueToLiteralNode(pb, float(0.f));

    auto data0 = pb.NewEmptyList(NTest::ConvertToMinikqlType<float>(pb));
    auto data1 = pb.NewList(NTest::ConvertToMinikqlType<float>(pb), {item});
    auto data2 = pb.NewList(NTest::ConvertToMinikqlType<float>(pb), {item, item, item});
    auto data3 = pb.NewList(NTest::ConvertToMinikqlType<float>(pb), {item, item, item, item, item});

    auto list = pb.NewList(NTest::ConvertToMinikqlType<TVector<float>>(pb), {data0, data1, data2, data3});

    auto pgmReturn = pb.Fold1(list,
                              [&](TRuntimeNode item) { return pb.Length(item); },
                              [&](TRuntimeNode item, TRuntimeNode state) { return pb.AggrAdd(state, pb.Length(item)); });

    auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TMaybe<ui64>{ui64(9)});
}

Y_UNIT_TEST_LLVM(TestHasListsItems) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto item = NTest::ConvertValueToLiteralNode(pb, float(0.f));

    auto data0 = pb.NewEmptyList(NTest::ConvertToMinikqlType<float>(pb));
    auto data1 = pb.NewList(NTest::ConvertToMinikqlType<float>(pb), {item});
    auto data2 = pb.NewEmptyList(NTest::ConvertToMinikqlType<float>(pb));

    auto list = pb.NewList(NTest::ConvertToMinikqlType<TVector<float>>(pb), {data0, data1, data2});

    auto pgmReturn = pb.Fold(list, NTest::ConvertValueToLiteralNode(pb, TMaybe<bool>{false}),
                             [&](TRuntimeNode item, TRuntimeNode state) { return pb.Or({state, pb.HasItems(item)}); });

    auto graph = setup.BuildGraph(pgmReturn);
    UNIT_ASSERT(graph->GetValue().template Get<bool>());
}

Y_UNIT_TEST_LLVM(TestConcat) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto data0 = NTest::ConvertValueToLiteralNode(pb, TStringBuf("X"));
    auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TStringBuf>{"aa", "bbb", "zzzz"});
    auto pgmReturn = pb.Fold(list, data0,
                             [&](TRuntimeNode item, TRuntimeNode state) {
                                 return pb.Concat(state, item);
                             });

    auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TStringBuf("Xaabbbzzzz"));
}

Y_UNIT_TEST_LLVM(TestConcatOpt) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto data0 = NTest::ConvertValueToLiteralNode(pb, TMaybe<TStringBuf>{""});
    auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<TStringBuf>>{
                                                         "very large string", " + ", "small", ""});
    auto pgmReturn = pb.Fold(list, data0,
                             [&](TRuntimeNode item, TRuntimeNode state) {
                                 return pb.Concat(state, item);
                             });

    auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TStringBuf("very large string + small"));
}

Y_UNIT_TEST_LLVM(TestAggrConcat) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, TMaybe<NTest::TUtf8>{});
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<NTest::TUtf8>>{
                                                               {},
                                                               NTest::TUtf8{"PREFIX:"},
                                                               {},
                                                               NTest::TUtf8{"very large string"},
                                                               NTest::TUtf8{":SUFFIX"},
                                                               {}});

    const auto pgmReturn = pb.Fold1(list,
                                    [&](TRuntimeNode item) { return item; },
                                    [&](TRuntimeNode item, TRuntimeNode state) { return pb.AggrConcat(state, item); });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TStringBuf("PREFIX:very large string:SUFFIX"));
}

Y_UNIT_TEST_LLVM(TestLongFold) {
    for (ui32 i = 0; i < 10; ++i) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const ui32 n = 1000;

        auto firstList = pb.Replicate(NTest::ConvertValueToLiteralNode(pb, ui32(0)),
                                      NTest::ConvertValueToLiteralNode(pb, ui64(n)), "", 0, 0);

        auto secondList = pb.Replicate(firstList, NTest::ConvertValueToLiteralNode(pb, ui64(n)), "", 0, 0);

        auto pgmReturn = pb.Fold(secondList, NTest::ConvertValueToLiteralNode(pb, ui32(0)),
                                 [&](TRuntimeNode item, TRuntimeNode state) {
                                     auto partialSum = pb.Fold(item, NTest::ConvertValueToLiteralNode(pb, ui32(0)),
                                                               [&](TRuntimeNode item, TRuntimeNode state) {
                                                                   Y_UNUSED(item);
                                                                   return pb.AggrAdd(state, NTest::ConvertValueToLiteralNode(pb, ui32(1)));
                                                               });

                                     return pb.AggrAdd(state, partialSum);
                                 });

        auto graph = setup.BuildGraph(pgmReturn);
        AssertUnboxedValueElementEqual(graph->GetValue(), ui32(n * n));
    }
}

Y_UNIT_TEST_LLVM(TestFoldAggrAddIntervals) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto upper = i64(+1000LL);
    const auto lower = i64(-1000LL);
    const auto part = i64(100LL);
    const auto from = pb.NewDataLiteral<NUdf::EDataSlot::Interval>(NUdf::TStringRef((const char*)&lower, sizeof(lower)));
    const auto stop = pb.NewDataLiteral<NUdf::EDataSlot::Interval>(NUdf::TStringRef((const char*)&upper, sizeof(upper)));
    const auto step = pb.NewDataLiteral<NUdf::EDataSlot::Interval>(NUdf::TStringRef((const char*)&part, sizeof(part)));
    const auto list = pb.ListFromRange(from, stop, step);

    const auto pgmReturn = pb.Fold1(pb.ListFromRange(from, stop, step),
                                    [&](TRuntimeNode item) { return pb.NewOptional(item); },
                                    [&](TRuntimeNode item, TRuntimeNode state) { return pb.AggrAdd(pb.NewOptional(item), state); });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), i64(-1000LL));
}

Y_UNIT_TEST_LLVM(TestFoldFoldPerf) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const ui32 n = 3333U;

    const auto firstList = pb.Replicate(NTest::ConvertValueToLiteralNode(pb, ui32(1)), NTest::ConvertValueToLiteralNode(pb, ui64(n)), "", 0, 0);

    const auto secondList = pb.Replicate(firstList, NTest::ConvertValueToLiteralNode(pb, ui64(n)), "", 0, 0);

    const auto pgmReturn = pb.Fold(secondList, NTest::ConvertValueToLiteralNode(pb, ui32(0)),
                                   [&](TRuntimeNode item, TRuntimeNode state) {
                                       const auto partialSum = pb.Fold(item, NTest::ConvertValueToLiteralNode(pb, ui32(0)),
                                                                       [&](TRuntimeNode i2, TRuntimeNode state) {
                                                                           return pb.AggrAdd(state, i2);
                                                                       });

                                       return pb.AggrAdd(state, partialSum);
                                   });

    const auto t1 = TInstant::Now();
    const auto graph = setup.BuildGraph(pgmReturn);
    const auto t2 = TInstant::Now();
    const auto value = graph->GetValue().template Get<ui32>();
    const auto t3 = TInstant::Now();
    Cerr << "Time is " << t3 - t1 << " (" << t2 - t1 << " + " << t3 - t2 << ")." << Endl;
    UNIT_ASSERT_VALUES_EQUAL(value, n * n);
}

std::vector<double> MakeSamples() {
    std::default_random_engine eng;
    std::uniform_real_distribution<double> unif(-999.0, +999.0);

    std::vector<double> samples(3333333U);

    eng.seed(std::time(nullptr));
    std::generate(samples.begin(), samples.end(), std::bind(std::move(unif), std::move(eng)));
    return samples;
}

static const auto Samples = MakeSamples();

Y_UNIT_TEST_LLVM(TestSumDoubleArrayListPerf) {
    TSetup<LLVM> setup;

    const auto t = TInstant::Now();
    const double sum = std::accumulate(Samples.cbegin(), Samples.cend(), 0.0);
    const auto cppTime = TInstant::Now() - t;

    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto listType = NTest::ConvertToMinikqlType<TVector<double>>(pb);
    const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

    const auto pgmReturn = pb.Fold1(pb.Collect(TRuntimeNode(list, false)),
                                    [&](TRuntimeNode item) { return item; },
                                    [&](TRuntimeNode item, TRuntimeNode state) { return pb.AggrAdd(state, item); });

    const auto t1 = TInstant::Now();
    const auto graph = setup.BuildGraph(pgmReturn, {list});
    NUdf::TUnboxedValue* items = nullptr;
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(Samples.size(), items));
    std::transform(Samples.cbegin(), Samples.cend(), items, &ToValue<double>);
    const auto t2 = TInstant::Now();
    const auto& value = graph->GetValue();
    const auto t3 = TInstant::Now();
    Cerr << "Time is " << t3 - t1 << " (" << t2 - t1 << " + " << t3 - t2 << ") vs C++ " << cppTime << Endl;
    UNIT_ASSERT_VALUES_EQUAL(value.template Get<double>(), sum);
}

Y_UNIT_TEST_LLVM(TestSumDoubleLazyListPerf) {
    TSetup<LLVM> setup;

    const auto t = TInstant::Now();
    const double sum = std::accumulate(Samples.cbegin(), Samples.cend(), 0.0);
    const auto cppTime = TInstant::Now() - t;

    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto listType = NTest::ConvertToMinikqlType<TVector<double>>(pb);
    const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

    const auto pgmReturn = pb.Fold1(pb.LazyList(TRuntimeNode(list, false)),
                                    [&](TRuntimeNode item) { return item; },
                                    [&](TRuntimeNode item, TRuntimeNode state) { return pb.AggrAdd(state, item); });

    const auto t1 = TInstant::Now();
    const auto graph = setup.BuildGraph(pgmReturn, {list});
    NUdf::TUnboxedValue* items = nullptr;
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(Samples.size(), items));
    std::transform(Samples.cbegin(), Samples.cend(), items, &ToValue<double>);
    const auto t2 = TInstant::Now();
    const auto& value = graph->GetValue();
    const auto t3 = TInstant::Now();
    Cerr << "Time is " << t3 - t1 << " (" << t2 - t1 << " + " << t3 - t2 << ") vs C++ " << cppTime << Endl;
    UNIT_ASSERT_VALUES_EQUAL(value.template Get<double>(), sum);
}

Y_UNIT_TEST_LLVM(TestSumDoubleFilteredArrayListPerf) {
    TSetup<LLVM> setup;

    const auto t = TInstant::Now();
    const double sum = std::accumulate(Samples.cbegin(), Samples.cend(), 0.0, [](double s, double v) { return v > 0.0 ? s + v : s; });
    const auto cppTime = TInstant::Now() - t;

    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto listType = NTest::ConvertToMinikqlType<TVector<double>>(pb);
    const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

    const auto pgmReturn = pb.Fold1(
        pb.Filter(pb.Collect(TRuntimeNode(list, false)),
                  [&](TRuntimeNode item) { return pb.AggrGreater(item, NTest::ConvertValueToLiteralNode(pb, double(0.0))); }),
        [&](TRuntimeNode item) { return item; },
        [&](TRuntimeNode item, TRuntimeNode state) { return pb.AggrAdd(state, item); });

    const auto t1 = TInstant::Now();
    const auto graph = setup.BuildGraph(pgmReturn, {list});
    NUdf::TUnboxedValue* items = nullptr;
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(Samples.size(), items));
    std::transform(Samples.cbegin(), Samples.cend(), items, &ToValue<double>);
    const auto t2 = TInstant::Now();
    const auto& value = graph->GetValue();
    const auto t3 = TInstant::Now();
    Cerr << "Time is " << t3 - t1 << " (" << t2 - t1 << " + " << t3 - t2 << ") vs C++ " << cppTime << Endl;
    UNIT_ASSERT_VALUES_EQUAL(value.template Get<double>(), sum);
}

Y_UNIT_TEST_LLVM(TestSumDoubleFilteredLazyListPerf) {
    TSetup<LLVM> setup;

    const auto t = TInstant::Now();
    const double sum = std::accumulate(Samples.cbegin(), Samples.cend(), 0.0, [](double s, double v) { return v > 0.0 ? s + v : s; });
    const auto cppTime = TInstant::Now() - t;

    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto listType = NTest::ConvertToMinikqlType<TVector<double>>(pb);
    const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

    const auto pgmReturn = pb.Fold1(
        pb.Filter(pb.LazyList(TRuntimeNode(list, false)),
                  [&](TRuntimeNode item) { return pb.AggrGreater(item, NTest::ConvertValueToLiteralNode(pb, double(0.0))); }),
        [&](TRuntimeNode item) { return item; },
        [&](TRuntimeNode item, TRuntimeNode state) { return pb.AggrAdd(state, item); });

    const auto t1 = TInstant::Now();
    const auto graph = setup.BuildGraph(pgmReturn, {list});
    NUdf::TUnboxedValue* items = nullptr;
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(Samples.size(), items));
    std::transform(Samples.cbegin(), Samples.cend(), items, &ToValue<double>);
    const auto t2 = TInstant::Now();
    const auto& value = graph->GetValue();
    const auto t3 = TInstant::Now();
    Cerr << "Time is " << t3 - t1 << " (" << t2 - t1 << " + " << t3 - t2 << ") vs C++ " << cppTime << Endl;
    UNIT_ASSERT_VALUES_EQUAL(value.template Get<double>(), sum);
}

Y_UNIT_TEST_LLVM(TestMinMaxSumDoubleArrayListPerf) {
    TSetup<LLVM> setup;

    double min(Samples.front()), max(Samples.front()), sum(0.0);

    const auto t = TInstant::Now();
    for (const auto v : Samples) {
        min = std::fmin(min, v);
        max = std::fmax(max, v);
        sum += v;
    }
    const auto cppTime = TInstant::Now() - t;

    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto listType = NTest::ConvertToMinikqlType<TVector<double>>(pb);
    const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

    const auto pgmReturn = pb.Fold1(pb.Collect(TRuntimeNode(list, false)),
                                    [&](TRuntimeNode item) { return pb.NewTuple({item, item, item}); },
                                    [&](TRuntimeNode item, TRuntimeNode state) { return pb.NewTuple({pb.AggrMin(pb.Nth(state, 0U), item), pb.AggrMax(pb.Nth(state, 1U), item), pb.AggrAdd(pb.Nth(state, 2U), item)}); });

    const auto t1 = TInstant::Now();
    const auto graph = setup.BuildGraph(pgmReturn, {list});
    NUdf::TUnboxedValue* items = nullptr;
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(Samples.size(), items));
    std::transform(Samples.cbegin(), Samples.cend(), items, &ToValue<double>);
    const auto t2 = TInstant::Now();
    const auto& value = graph->GetValue();
    const auto t3 = TInstant::Now();
    Cerr << "Time is " << t3 - t1 << " (" << t2 - t1 << " + " << t3 - t2 << ") vs C++ " << cppTime << Endl;
    AssertUnboxedValueElementEqual(value, TMaybe<std::tuple<double, double, double>>{std::tuple<double, double, double>{min, max, sum}});
}

Y_UNIT_TEST_LLVM(TestMinMaxSumDoubleLazyListPerf) {
    TSetup<LLVM> setup;

    double min(Samples.front()), max(Samples.front()), sum(0.0);

    const auto t = TInstant::Now();
    for (const auto v : Samples) {
        min = std::fmin(min, v);
        max = std::fmax(max, v);
        sum += v;
    }
    const auto cppTime = TInstant::Now() - t;

    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto listType = NTest::ConvertToMinikqlType<TVector<double>>(pb);
    const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

    const auto pgmReturn = pb.Fold1(pb.LazyList(TRuntimeNode(list, false)),
                                    [&](TRuntimeNode item) { return pb.NewTuple({item, item, item}); },
                                    [&](TRuntimeNode item, TRuntimeNode state) { return pb.NewTuple({pb.AggrMin(pb.Nth(state, 0U), item), pb.AggrMax(pb.Nth(state, 1U), item), pb.AggrAdd(pb.Nth(state, 2U), item)}); });

    const auto t1 = TInstant::Now();
    const auto graph = setup.BuildGraph(pgmReturn, {list});
    NUdf::TUnboxedValue* items = nullptr;
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(Samples.size(), items));
    std::transform(Samples.cbegin(), Samples.cend(), items, &ToValue<double>);
    const auto t2 = TInstant::Now();
    const auto& value = graph->GetValue();
    const auto t3 = TInstant::Now();
    Cerr << "Time is " << t3 - t1 << " (" << t2 - t1 << " + " << t3 - t2 << ") vs C++ " << cppTime << Endl;
    AssertUnboxedValueElementEqual(value, TMaybe<std::tuple<double, double, double>>{std::tuple<double, double, double>{min, max, sum}});
}

Y_UNIT_TEST_LLVM(TestMinMaxSumDoubleFilteredArrayListPerf) {
    TSetup<LLVM> setup;

    double min(std::nan("")), max(std::nan("")), sum(0.0);

    const auto t = TInstant::Now();
    for (const auto v : Samples) {
        if (v < 0.0) {
            min = std::fmin(min, v);
            max = std::fmax(max, v);
            sum += v;
        }
    }
    const auto cppTime = TInstant::Now() - t;

    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto listType = NTest::ConvertToMinikqlType<TVector<double>>(pb);
    const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

    const auto pgmReturn = pb.Fold1(
        pb.Filter(pb.Collect(TRuntimeNode(list, false)),
                  [&](TRuntimeNode item) { return pb.AggrLess(item, NTest::ConvertValueToLiteralNode(pb, double(0.0))); }),
        [&](TRuntimeNode item) { return pb.NewTuple({item, item, item}); },
        [&](TRuntimeNode item, TRuntimeNode state) {
            return pb.NewTuple({pb.AggrMin(pb.Nth(state, 0U), item), pb.AggrMax(pb.Nth(state, 1U), item), pb.AggrAdd(pb.Nth(state, 2U), item)});
        });

    const auto t1 = TInstant::Now();
    const auto graph = setup.BuildGraph(pgmReturn, {list});
    NUdf::TUnboxedValue* items = nullptr;
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(Samples.size(), items));
    std::transform(Samples.cbegin(), Samples.cend(), items, &ToValue<double>);
    const auto t2 = TInstant::Now();
    const auto& value = graph->GetValue();
    const auto t3 = TInstant::Now();
    Cerr << "Time is " << t3 - t1 << " (" << t2 - t1 << " + " << t3 - t2 << ") vs C++ " << cppTime << Endl;
    AssertUnboxedValueElementEqual(value, TMaybe<std::tuple<double, double, double>>{std::tuple<double, double, double>{min, max, sum}});
}

Y_UNIT_TEST_LLVM(TestMinMaxSumDoubleFilteredLazyListPerf) {
    TSetup<LLVM> setup;

    double min(std::nan("")), max(std::nan("")), sum(0.0);

    const auto t = TInstant::Now();
    for (const auto v : Samples) {
        if (v < 0.0) {
            min = std::fmin(min, v);
            max = std::fmax(max, v);
            sum += v;
        }
    }
    const auto cppTime = TInstant::Now() - t;

    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto listType = NTest::ConvertToMinikqlType<TVector<double>>(pb);
    const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

    const auto pgmReturn = pb.Fold1(
        pb.Filter(pb.LazyList(TRuntimeNode(list, false)),
                  [&](TRuntimeNode item) { return pb.AggrLess(item, NTest::ConvertValueToLiteralNode(pb, double(0.0))); }),
        [&](TRuntimeNode item) { return pb.NewTuple({item, item, item}); },
        [&](TRuntimeNode item, TRuntimeNode state) {
            return pb.NewTuple({pb.AggrMin(pb.Nth(state, 0U), item), pb.AggrMax(pb.Nth(state, 1U), item), pb.AggrAdd(pb.Nth(state, 2U), item)});
        });

    const auto t1 = TInstant::Now();
    const auto graph = setup.BuildGraph(pgmReturn, {list});
    NUdf::TUnboxedValue* items = nullptr;
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(Samples.size(), items));
    std::transform(Samples.cbegin(), Samples.cend(), items, &ToValue<double>);
    const auto t2 = TInstant::Now();
    const auto& value = graph->GetValue();
    const auto t3 = TInstant::Now();
    Cerr << "Time is " << t3 - t1 << " (" << t2 - t1 << " + " << t3 - t2 << ") vs C++ " << cppTime << Endl;
    AssertUnboxedValueElementEqual(value, TMaybe<std::tuple<double, double, double>>{std::tuple<double, double, double>{min, max, sum}});
}

Y_UNIT_TEST_LLVM(TestAvgDoubleByTupleFoldArrayListPerf) {
    TSetup<LLVM> setup;

    const auto t = TInstant::Now();
    const double avg = std::accumulate(Samples.cbegin(), Samples.cend(), 0.0) / Samples.size();
    const auto cppTime = TInstant::Now() - t;

    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto listType = NTest::ConvertToMinikqlType<TVector<double>>(pb);
    const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

    const auto fold = pb.Fold(pb.Collect(TRuntimeNode(list, false)),
                              NTest::ConvertValueToLiteralNode(pb, std::tuple<double, ui64>{double(0.0), ui64(0ULL)}),
                              [&](TRuntimeNode item, TRuntimeNode state) {
                                  return pb.NewTuple({pb.AggrAdd(pb.Nth(state, 0), item), pb.Increment(pb.Nth(state, 1))});
                              });

    const auto pgmReturn = pb.Div(pb.Nth(fold, 0U), pb.Nth(fold, 1U));

    const auto t1 = TInstant::Now();
    const auto graph = setup.BuildGraph(pgmReturn, {list});
    NUdf::TUnboxedValue* items = nullptr;
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(Samples.size(), items));
    std::transform(Samples.cbegin(), Samples.cend(), items, &ToValue<double>);
    const auto t2 = TInstant::Now();
    const auto& value = graph->GetValue();
    const auto t3 = TInstant::Now();
    Cerr << "Time is " << t3 - t1 << " (" << t2 - t1 << " + " << t3 - t2 << ") vs C++ " << cppTime << Endl;
    UNIT_ASSERT_VALUES_EQUAL(value.template Get<double>(), avg);
}

Y_UNIT_TEST_LLVM(TestAvgDoubleByTupleFoldLazyListPerf) {
    TSetup<LLVM> setup;

    const auto t = TInstant::Now();
    const double avg = std::accumulate(Samples.cbegin(), Samples.cend(), 0.0) / Samples.size();
    const auto cppTime = TInstant::Now() - t;

    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto listType = NTest::ConvertToMinikqlType<TVector<double>>(pb);
    const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

    const auto fold = pb.Fold(pb.LazyList(TRuntimeNode(list, false)),
                              NTest::ConvertValueToLiteralNode(pb, std::tuple<double, ui64>{double(0.0), ui64(0ULL)}),
                              [&](TRuntimeNode item, TRuntimeNode state) {
                                  return pb.NewTuple({pb.AggrAdd(pb.Nth(state, 0U), item), pb.Increment(pb.Nth(state, 1U))});
                              });

    const auto pgmReturn = pb.Div(pb.Nth(fold, 0U), pb.Nth(fold, 1U));

    const auto t1 = TInstant::Now();
    const auto graph = setup.BuildGraph(pgmReturn, {list});
    NUdf::TUnboxedValue* items = nullptr;
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(Samples.size(), items));
    std::transform(Samples.cbegin(), Samples.cend(), items, &ToValue<double>);
    const auto t2 = TInstant::Now();
    const auto& value = graph->GetValue();
    const auto t3 = TInstant::Now();
    Cerr << "Time is " << t3 - t1 << " (" << t2 - t1 << " + " << t3 - t2 << ") vs C++ " << cppTime << Endl;
    UNIT_ASSERT_VALUES_EQUAL(value.template Get<double>(), avg);
}

Y_UNIT_TEST_LLVM(TestAvgDoubleByCollectFoldLazyListPerf) {
    TSetup<LLVM> setup;

    const auto t = TInstant::Now();
    const double avg = std::accumulate(Samples.cbegin(), Samples.cend(), 0.0) / Samples.size();
    const auto cppTime = TInstant::Now() - t;

    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto listType = NTest::ConvertToMinikqlType<TVector<double>>(pb);
    const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

    const auto src = pb.Collect(pb.LazyList(TRuntimeNode(list, false)));
    const auto pgmReturn = pb.Div(
        pb.Fold(src, NTest::ConvertValueToLiteralNode(pb, double(0.0)),
                [&](TRuntimeNode item, TRuntimeNode state) { return pb.AggrAdd(state, item); }),
        pb.Length(src));

    const auto t1 = TInstant::Now();
    const auto graph = setup.BuildGraph(pgmReturn, {list});
    NUdf::TUnboxedValue* items = nullptr;
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(Samples.size(), items));
    std::transform(Samples.cbegin(), Samples.cend(), items, &ToValue<double>);
    const auto t2 = TInstant::Now();
    const auto& value = graph->GetValue();
    const auto t3 = TInstant::Now();
    Cerr << "Time is " << t3 - t1 << " (" << t2 - t1 << " + " << t3 - t2 << ") vs C++ " << cppTime << Endl;
    UNIT_ASSERT_VALUES_EQUAL(value.template Get<double>(), avg);
}
} // Y_UNIT_TEST_SUITE(TMiniKQLFoldNodeTest)

} // namespace NMiniKQL
} // namespace NKikimr
