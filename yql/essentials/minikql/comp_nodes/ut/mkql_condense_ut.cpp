#include "mkql_computation_node_ut.h"
#include "mkql_program_builder_test_utils.h"

#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLCondenseNodeTest) {
Y_UNIT_TEST_LLVM(TestSqueeze) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<double>>{{}, 233.8, -53.2, 3.8});
    const auto data0 = NTest::ConvertValueToLiteralNode(pb, TMaybe<double>{HUGE_VAL});

    const auto pgmReturn = pb.Squeeze(pb.Iterator(list, {}), data0,
                                      [&](TRuntimeNode item, TRuntimeNode state) {
                                          return pb.AggrMin(item, state);
                                      });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<double>({-53.2}));
}

Y_UNIT_TEST_LLVM(TestSqueezeOnEmpty) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = pb.NewEmptyList(NTest::ConvertToMinikqlType<TMaybe<double>>(pb));
    const auto data0 = NTest::ConvertValueToLiteralNode(pb, TMaybe<double>{HUGE_VAL});

    const auto pgmReturn = pb.Squeeze(pb.Iterator(list, {}), data0,
                                      [&](TRuntimeNode item, TRuntimeNode state) {
                                          return pb.AggrMin(item, state);
                                      });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<double>({HUGE_VAL}));
}

Y_UNIT_TEST_LLVM(TestSqueeze1OverEmpty) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = pb.NewEmptyList(NTest::ConvertToMinikqlType<i32>(pb));
    const auto pgmReturn = pb.Squeeze1(pb.Iterator(list, {}),
                                       [&](TRuntimeNode item) { return pb.Minus(item); },
                                       [&](TRuntimeNode item, TRuntimeNode state) { return pb.Mul(item, state); });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<i32>({}));
}

Y_UNIT_TEST_LLVM(TestSqueeze1OverSingle) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<i32>{1});
    const auto pgmReturn = pb.Squeeze1(pb.Iterator(list, {}),
                                       [&](TRuntimeNode item) { return pb.Minus(item); },
                                       [&](TRuntimeNode item, TRuntimeNode state) { return pb.Mul(item, state); });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<i32>({-1}));
}

Y_UNIT_TEST_LLVM(TestSqueeze1OverMany) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<i32>{1, 2, 7});
    const auto pgmReturn = pb.Squeeze1(pb.Iterator(list, {}),
                                       [&](TRuntimeNode item) { return pb.Minus(item); },
                                       [&](TRuntimeNode item, TRuntimeNode state) { return pb.Mul(item, state); });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<i32>({-14}));
}

Y_UNIT_TEST_LLVM(TestCondense) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<double>>{0.0, 3.8, {}, -53.2, 233.8, 3.14, -73.12, {}, 233.8, 1221.8});
    const auto data0 = NTest::ConvertValueToLiteralNode(pb, TMaybe<double>{0.0});

    const auto pgmReturn = pb.Condense(pb.Iterator(list, {}), data0,
                                       [&](TRuntimeNode item, TRuntimeNode state) { return pb.Or({pb.Not(pb.Exists(item)), pb.Less(state, data0)}); },
                                       [&](TRuntimeNode item, TRuntimeNode state) { return pb.AggrAdd(item, state); });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<double>({3.8, -53.2, 163.82, 1455.6}));
}

Y_UNIT_TEST_LLVM(TestCondense1) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<i32>{-1, 2, 0, 7, 5, -7, -6, 4, 8, 9});
    const auto pgmReturn = pb.Condense1(pb.Iterator(list, {}),
                                        [&](TRuntimeNode item) { return pb.Minus(item); },
                                        [&](TRuntimeNode item, TRuntimeNode state) { return pb.LessOrEqual(item, state); },
                                        [&](TRuntimeNode item, TRuntimeNode state) { return pb.Mul(item, state); });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<i32>({2, 0, 7, 6, -288}));
}

Y_UNIT_TEST_LLVM(TestCondenseOverList) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<double>>{0.0, 3.8, {}, -53.2, 233.8, 3.14, -73.12, {}, 233.8, 1221.8});
    const auto data0 = NTest::ConvertValueToLiteralNode(pb, TMaybe<double>{0.0});

    const auto pgmReturn = pb.Condense(list, data0,
                                       [&](TRuntimeNode item, TRuntimeNode state) { return pb.Or({pb.Not(pb.Exists(item)), pb.Less(state, data0)}); },
                                       [&](TRuntimeNode item, TRuntimeNode state) { return pb.AggrAdd(item, state); });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<double>{3.8, -53.2, 163.82, 1455.6});
}

Y_UNIT_TEST_LLVM(TestCondense1OverList) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<i32>{-1, 2, 0, 7, 5, -7, -6, 4, 8, 9});
    const auto pgmReturn = pb.Condense1(list,
                                        [&](TRuntimeNode item) { return pb.Minus(item); },
                                        [&](TRuntimeNode item, TRuntimeNode state) { return pb.LessOrEqual(item, state); },
                                        [&](TRuntimeNode item, TRuntimeNode state) { return pb.Mul(item, state); });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<i32>{2, 0, 7, 6, -288});
}

Y_UNIT_TEST_LLVM(TestCondenseInterrupt) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<bool>>{false, false, false, false, true, {}, {}});

    const auto pgmReturn = pb.FromFlow(pb.Condense(pb.ToFlow(list), NTest::ConvertValueToLiteralNode(pb, bool(false)),
                                                   [&](TRuntimeNode, TRuntimeNode state) { return pb.If(state, NTest::ConvertValueToLiteralNode(pb, TMaybe<bool>{}), NTest::ConvertValueToLiteralNode(pb, bool(false))); },
                                                   [&](TRuntimeNode item, TRuntimeNode state) { return pb.Or({pb.Unwrap(item, NTest::ConvertValueToLiteralNode(pb, TStringBuf("")), "", 0, 0), state}); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<bool>({true}));
}

Y_UNIT_TEST_LLVM(TestCondense1Interrupt) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<bool>>{true, true, true, true, false, {}, {}});

    const auto pgmReturn = pb.FromFlow(pb.Condense1(pb.ToFlow(list),
                                                    [&](TRuntimeNode item) { return pb.Unwrap(item, NTest::ConvertValueToLiteralNode(pb, TStringBuf("")), "", 0, 0); },
                                                    [&](TRuntimeNode, TRuntimeNode state) { return pb.If(state, NTest::ConvertValueToLiteralNode(pb, bool(false)), NTest::ConvertValueToLiteralNode(pb, TMaybe<bool>{})); },
                                                    [&](TRuntimeNode item, TRuntimeNode state) { return pb.And({pb.Unwrap(item, NTest::ConvertValueToLiteralNode(pb, TStringBuf("")), "", 0, 0), state}); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<bool>({false}));
}

Y_UNIT_TEST_LLVM(TestCondenseInterruptEndlessStream) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto pgmReturn = pb.Condense(pb.SourceOf(pb.NewStreamType(pb.NewNull().GetStaticType())), NTest::ConvertValueToLiteralNode(pb, ui32(0U)),
                                       [&](TRuntimeNode, TRuntimeNode state) { return pb.If(pb.AggrLess(state, NTest::ConvertValueToLiteralNode(pb, ui32(123456U))), NTest::ConvertValueToLiteralNode(pb, bool(false)), NTest::ConvertValueToLiteralNode(pb, TMaybe<bool>{})); },
                                       [&](TRuntimeNode, TRuntimeNode state) { return pb.AggrAdd(NTest::ConvertValueToLiteralNode(pb, ui32(1U)), state); });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<ui32>({123456U}));
}

Y_UNIT_TEST_LLVM(TestCondense1InterruptEndlessFlow) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto pgmReturn = pb.FromFlow(pb.Condense1(pb.SourceOf(pb.NewFlowType(pb.NewNull().GetStaticType())),
                                                    [&](TRuntimeNode) { return NTest::ConvertValueToLiteralNode(pb, ui32(0U)); },
                                                    [&](TRuntimeNode, TRuntimeNode state) { return pb.If(pb.AggrLess(state, NTest::ConvertValueToLiteralNode(pb, ui32(123456U))), NTest::ConvertValueToLiteralNode(pb, bool(false)), NTest::ConvertValueToLiteralNode(pb, TMaybe<bool>{})); },
                                                    [&](TRuntimeNode, TRuntimeNode state) { return pb.AggrAdd(NTest::ConvertValueToLiteralNode(pb, ui32(1U)), state); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<ui32>({123456U}));
}

Y_UNIT_TEST_LLVM(TestCondenseListeralListInMap) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<TStringBuf>{"foo", "bar"});
    const auto list0 = NTest::ConvertValueToLiteralNode(pb, TVector<TStringBuf>{"foo", "bar", "other"});

    const auto pgmReturn = pb.Map(list0,
                                  [&](TRuntimeNode item) {
                                      return pb.Head(pb.Condense(list2,
                                                                 NTest::ConvertValueToLiteralNode(pb, bool(false)),
                                                                 [&](TRuntimeNode, TRuntimeNode) { return NTest::ConvertValueToLiteralNode(pb, bool(false)); },
                                                                 [&](TRuntimeNode it, TRuntimeNode state) { return pb.Or({state, pb.AggrEquals(item, it)}); }));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<bool>{true, true, false});
}

Y_UNIT_TEST_LLVM(TestCondense1ListeralListInMap) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<TStringBuf>{"foo", "bar"});
    const auto list0 = NTest::ConvertValueToLiteralNode(pb, TVector<TStringBuf>{"foo", "bar", "other"});

    const auto pgmReturn = pb.Map(list0,
                                  [&](TRuntimeNode item) {
                                      return pb.Head(pb.Condense1(list2,
                                                                  [&](TRuntimeNode it) { return pb.AggrEquals(item, it); },
                                                                  [&](TRuntimeNode, TRuntimeNode) { return NTest::ConvertValueToLiteralNode(pb, bool(false)); },
                                                                  [&](TRuntimeNode it, TRuntimeNode state) { return pb.Or({state, pb.AggrEquals(item, it)}); }));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<bool>{true, true, false});
}
} // Y_UNIT_TEST_SUITE(TMiniKQLCondenseNodeTest)

} // namespace NMiniKQL
} // namespace NKikimr
