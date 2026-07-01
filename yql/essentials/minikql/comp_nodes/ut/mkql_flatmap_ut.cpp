#include "mkql_computation_node_ut.h"
#include "mkql_program_builder_test_utils.h"
#include <yql/essentials/minikql/mkql_runtime_version.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLFlatMapTest) {
Y_UNIT_TEST_LLVM(TestOverListAndPartialLists) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data1 = NTest::ConvertValueToLiteralNode(pb, ui32(1));
    const auto data2 = NTest::ConvertValueToLiteralNode(pb, ui32(2));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{1, 2});
    const auto pgmReturn = pb.FlatMap(list,
                                      [&](TRuntimeNode item) {
                                          return pb.NewList(NTest::ConvertToMinikqlType<ui32>(pb), {pb.Add(item, data1), pb.Mul(item, data2)});
                                      });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{2, 2, 3, 4});
}

Y_UNIT_TEST_LLVM(TestOverListAndStreams) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<i8>{3, -7});
    const auto pgmReturn = pb.FlatMap(list,
                                      [&](TRuntimeNode item) {
                                          return pb.Iterator(pb.NewList(NTest::ConvertToMinikqlType<i8>(pb), {pb.Plus(item), pb.Minus(item)}), {});
                                      });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<i8>{3, -3, -7, 7});
}

Y_UNIT_TEST_LLVM(TestOverStreamAndPartialLists) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data1 = NTest::ConvertValueToLiteralNode(pb, ui16(10));
    const auto data2 = NTest::ConvertValueToLiteralNode(pb, ui16(20));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui16>{10, 20});
    const auto pgmReturn = pb.FlatMap(pb.Iterator(list, {}),
                                      [&](TRuntimeNode item) {
                                          return pb.NewList(NTest::ConvertToMinikqlType<ui16>(pb), {pb.Sub(item, data1), pb.Unwrap(pb.Div(item, data2), NTest::ConvertValueToLiteralNode(pb, TStringBuf("")), "", 0, 0)});
                                      });

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, NYql::NUdf::TUnboxedValueComparatorStreamView<ui16>({0, 0, 10, 1}));
}

Y_UNIT_TEST_LLVM(TestOverFlowAndPartialLists) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data1 = NTest::ConvertValueToLiteralNode(pb, ui16(10));
    const auto data2 = NTest::ConvertValueToLiteralNode(pb, ui16(20));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui16>{10, 20});
    const auto pgmReturn = pb.FromFlow(pb.FlatMap(pb.ToFlow(pb.Iterator(list, {})),
                                                  [&](TRuntimeNode item) {
                                                      return pb.NewList(NTest::ConvertToMinikqlType<ui16>(pb), {pb.Sub(item, data1), pb.Unwrap(pb.Div(item, data2), NTest::ConvertValueToLiteralNode(pb, TStringBuf("")), "", 0, 0)});
                                                  }));

    const auto& graph = setup.BuildGraph(pgmReturn);
    const NUdf::TUnboxedValue& iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, NYql::NUdf::TUnboxedValueComparatorStreamView<ui16>({0, 0, 10, 1}));
}

Y_UNIT_TEST_LLVM(TestOverStreamAndStreams) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data = NTest::ConvertValueToLiteralNode(pb, i32(-100));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<i32>{0, 3, 7});
    const auto pgmReturn = pb.FlatMap(pb.Iterator(list, {}),
                                      [&](TRuntimeNode item) {
                                          return pb.Iterator(pb.NewList(NTest::ConvertToMinikqlType<TMaybe<i32>>(pb),
                                                                        {pb.Mod(data, item), pb.Div(data, item)}),
                                                             {});
                                      });

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, NYql::NUdf::TUnboxedValueComparatorStreamView<TMaybe<i32>>({{}, {}, i32(-1), i32(-33), i32(-2), i32(-14)}));
}

Y_UNIT_TEST_LLVM(TestOverFlowAndStreams) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data = NTest::ConvertValueToLiteralNode(pb, i32(-100));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<i32>{0, 3, 7});
    const auto pgmReturn = pb.FromFlow(pb.FlatMap(pb.ToFlow(list),
                                                  [&](TRuntimeNode item) {
                                                      return pb.Iterator(pb.NewList(NTest::ConvertToMinikqlType<TMaybe<i32>>(pb),
                                                                                    {pb.Mod(data, item), pb.Div(data, item)}),
                                                                         {});
                                                  }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, NYql::NUdf::TUnboxedValueComparatorStreamView<TMaybe<i32>>({{}, {}, i32(-1), i32(-33), i32(-2), i32(-14)}));
}

Y_UNIT_TEST_LLVM(TestOverFlowAndFlows) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data = NTest::ConvertValueToLiteralNode(pb, i32(-100));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<i32>{0, 3, 7});
    const auto pgmReturn = pb.FromFlow(pb.FlatMap(pb.ToFlow(list),
                                                  [&](TRuntimeNode item) {
                                                      return pb.ToFlow(pb.NewList(NTest::ConvertToMinikqlType<TMaybe<i32>>(pb),
                                                                                  {pb.Mod(data, item), pb.Div(data, item)}));
                                                  }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, NYql::NUdf::TUnboxedValueComparatorStreamView<TMaybe<i32>>({{}, {}, i32(-1), i32(-33), i32(-2), i32(-14)}));
}

Y_UNIT_TEST_LLVM(TestOverListAndFlows) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data = NTest::ConvertValueToLiteralNode(pb, i32(-100));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<i32>{0, 3, 7});
    const auto pgmReturn = pb.FromFlow(pb.FlatMap(list,
                                                  [&](TRuntimeNode item) {
                                                      return pb.ToFlow(pb.NewList(NTest::ConvertToMinikqlType<TMaybe<i32>>(pb),
                                                                                  {pb.Mod(data, item), pb.Div(data, item)}));
                                                  }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, NYql::NUdf::TUnboxedValueComparatorStreamView<TMaybe<i32>>({{}, {}, i32(-1), i32(-33), i32(-2), i32(-14)}));
}

Y_UNIT_TEST_LLVM(TestOverFlowAndIndependentFlows) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<i32>{0, 3, 7});
    const auto pgmReturn = pb.FromFlow(pb.FlatMap(pb.ToFlow(list),
                                                  [&](TRuntimeNode) {
                                                      return pb.Map(pb.ToFlow(NTest::ConvertValueToLiteralNode(pb, TVector<i32>{-100, -100})), [&](TRuntimeNode it) { return pb.Abs(it); });
                                                  }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, NYql::NUdf::TUnboxedValueComparatorStreamView<i32>({100, 100, 100, 100, 100, 100}));
}

Y_UNIT_TEST_LLVM(TestOverListAndIndependentFlows) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<i32>{0, 3, 7});
    const auto pgmReturn = pb.FromFlow(pb.FlatMap(list,
                                                  [&](TRuntimeNode) {
                                                      return pb.Map(pb.ToFlow(NTest::ConvertValueToLiteralNode(pb, TVector<i32>{-100, -100})), [&](TRuntimeNode it) { return pb.Minus(it); });
                                                  }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, NYql::NUdf::TUnboxedValueComparatorStreamView<i32>({100, 100, 100, 100, 100, 100}));
}

Y_UNIT_TEST_LLVM(TestOverFlowAndPartialOptionals) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data = NTest::ConvertValueToLiteralNode(pb, i64(-100));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<i64>{0, 3, 7});
    const auto pgmReturn = pb.FromFlow(pb.FlatMap(pb.ToFlow(pb.Iterator(list, {})),
                                                  [&](TRuntimeNode item) {
                                                      return pb.Div(data, item);
                                                  }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, NYql::NUdf::TUnboxedValueComparatorStreamView<i64>({-33, -14}));
}

Y_UNIT_TEST_LLVM(TestOverStreamAndPartialOptionals) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data = NTest::ConvertValueToLiteralNode(pb, i64(-100));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<i64>{0, 3, 7});
    const auto pgmReturn = pb.FlatMap(pb.Iterator(list, {}),
                                      [&](TRuntimeNode item) {
                                          return pb.Div(data, item);
                                      });

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, NYql::NUdf::TUnboxedValueComparatorStreamView<i64>({-33, -14}));
}

Y_UNIT_TEST_LLVM(TestOverListAndPartialOptionals) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data2 = NTest::ConvertValueToLiteralNode(pb, ui32(2));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{0, 1, 2});
    const auto pgmReturn = pb.FlatMap(list,
                                      [&](TRuntimeNode item) {
                                          return pb.Div(data2, item);
                                      });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{2, 1});
}

Y_UNIT_TEST_LLVM(TestOverListAndDoubleOptionals) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data2 = NTest::ConvertValueToLiteralNode(pb, ui32(2));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{0, 1, 2});
    const auto pgmReturn = pb.FlatMap(list,
                                      [&](TRuntimeNode item) {
                                          return pb.NewOptional(pb.Div(data2, item));
                                      });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<ui32>>{{}, ui32(2), ui32(1)});
}

Y_UNIT_TEST_LLVM(TestOverOptionalAndPartialOptionals) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data2 = NTest::ConvertValueToLiteralNode(pb, ui32(2));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TMaybe<ui32>{ui32(2)});
    const auto pgmReturn = pb.FlatMap(list,
                                      [&](TRuntimeNode item) {
                                          return pb.Div(item, data2);
                                      });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TMaybe<ui32>{ui32(1)});
}

Y_UNIT_TEST_LLVM(TestOverOptionalAndPartialLists) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data1 = NTest::ConvertValueToLiteralNode(pb, ui32(1));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TMaybe<ui32>{ui32(2)});
    const auto pgmReturn = pb.FlatMap(list,
                                      [&](TRuntimeNode item) {
                                          return pb.Append(pb.AsList(item), data1);
                                      });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{2, 1});
}

Y_UNIT_TEST_LLVM(TestOverListAndPartialListsLazy) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data1 = NTest::ConvertValueToLiteralNode(pb, ui32(1U));
    const auto data2 = NTest::ConvertValueToLiteralNode(pb, ui32(2U));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{1, 2});

    const auto pgmReturn = pb.FlatMap(pb.LazyList(list),
                                      [&](TRuntimeNode item) {
                                          return pb.NewList(NTest::ConvertToMinikqlType<ui32>(pb), {pb.Add(item, data1), pb.Mul(item, data2)});
                                      });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{2, 2, 3, 4});
}

Y_UNIT_TEST_LLVM(TestOverListAndPartialOptionalsLazy) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data2 = NTest::ConvertValueToLiteralNode(pb, ui32(2U));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{0, 2});

    const auto pgmReturn = pb.FlatMap(pb.LazyList(list),
                                      [&](TRuntimeNode item) { return pb.Div(data2, item); });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{1});
}

Y_UNIT_TEST_LLVM(TestNarrowWithList) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                               {i32(1), TMaybe<i32>{}, i32(-1)},
                                                               {TMaybe<i32>{}, i32(2), i32(-2)},
                                                               {i32(3), TMaybe<i32>{}, i32(-3)}});

    const auto pgmReturn = pb.Collect(pb.NarrowFlatMap(pb.ExpandMap(pb.ToFlow(list),
                                                                    [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                       [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.FlatMap(pb.NewList(NTest::ConvertToMinikqlType<TMaybe<i32>>(pb), items), [](TRuntimeNode item) { return item; }); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<i32>{1, -1, 2, -2, 3, -3});
}

Y_UNIT_TEST_LLVM(TestNarrowWithFlow) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                               {i32(1), TMaybe<i32>{}, i32(-1)},
                                                               {TMaybe<i32>{}, i32(2), i32(-2)},
                                                               {i32(3), TMaybe<i32>{}, i32(-3)}});

    const auto pgmReturn = pb.Collect(pb.NarrowFlatMap(pb.ExpandMap(pb.ToFlow(list),
                                                                    [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                       [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.FlatMap(pb.ToFlow(pb.NewList(NTest::ConvertToMinikqlType<TMaybe<i32>>(pb), items)), [](TRuntimeNode item) { return item; }); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<i32>{1, -1, 2, -2, 3, -3});
}

Y_UNIT_TEST_LLVM(TestNarrowWithIndependentFlow) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data = NTest::ConvertValueToLiteralNode(pb, std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>{i32(1), TMaybe<i32>{}, i32(-1)});
    const auto list = pb.NewList(NTest::ConvertToMinikqlType<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>(pb), {data, data, data});

    const auto pgmReturn = pb.Collect(pb.NarrowFlatMap(pb.ExpandMap(pb.ToFlow(list),
                                                                    [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                       [&](TRuntimeNode::TList) { return pb.Map(
                                                                                      pb.ToFlow(NTest::ConvertValueToLiteralNode(pb, TVector<float>{+1.f, -1.f})),
                                                                                      [&](TRuntimeNode item) { return pb.Minus(item); }); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<float>{-1.f, +1.f, -1.f, +1.f, -1.f, +1.f});
}

Y_UNIT_TEST_LLVM(TestThinNarrowWithList) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data = NTest::ConvertValueToLiteralNode(pb, std::tuple<>{});
    const auto list = pb.NewList(NTest::ConvertToMinikqlType<std::tuple<>>(pb), {data, data, data});

    const auto pgmReturn = pb.Collect(pb.NarrowFlatMap(pb.ExpandMap(pb.ToFlow(list),
                                                                    [&](TRuntimeNode) -> TRuntimeNode::TList { return {}; }),
                                                       [&](TRuntimeNode::TList) -> TRuntimeNode { return pb.Replicate(NTest::ConvertValueToLiteralNode(pb, i32(7)), NTest::ConvertValueToLiteralNode(pb, ui64(3)), __FILE__, __LINE__, 0); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<i32>{7, 7, 7, 7, 7, 7, 7, 7, 7});
}

Y_UNIT_TEST_LLVM(TestOverFlowAndWideFlows) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data = NTest::ConvertValueToLiteralNode(pb, i32(-100));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<i32>{0, 3, 7});
    const auto pgmReturn = pb.FromFlow(pb.NarrowMap(pb.FlatMap(pb.ToFlow(list),
                                                               [&](TRuntimeNode item) {
                                                                   return pb.ExpandMap(pb.ToFlow(pb.NewList(NTest::ConvertToMinikqlType<TMaybe<i32>>(pb),
                                                                                                            {pb.Mod(data, item), pb.Div(data, item)})),
                                                                                       [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Plus(item), pb.Minus(item)}; });
                                                               }),
                                                    [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, NYql::NUdf::TUnboxedValueComparatorStreamView<std::tuple<TMaybe<i32>, TMaybe<i32>>>(
                                                 {{{}, {}}, {{}, {}}, {i32(-1), i32(+1)}, {i32(-33), i32(+33)}, {i32(-2), i32(+2)}, {i32(-14), i32(+14)}}));
}

Y_UNIT_TEST_LLVM(TestOverListAndWideFlows) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data = NTest::ConvertValueToLiteralNode(pb, i32(-100));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<i32>{0, 3, 7});
    const auto pgmReturn = pb.FromFlow(pb.NarrowMap(pb.FlatMap(list,
                                                               [&](TRuntimeNode item) {
                                                                   return pb.ExpandMap(pb.ToFlow(pb.NewList(NTest::ConvertToMinikqlType<TMaybe<i32>>(pb),
                                                                                                            {pb.Mod(data, item), pb.Div(data, item)})),
                                                                                       [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Minus(item), pb.Plus(item)}; });
                                                               }),
                                                    [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, NYql::NUdf::TUnboxedValueComparatorStreamView<std::tuple<TMaybe<i32>, TMaybe<i32>>>(
                                                 {{{}, {}}, {{}, {}}, {i32(+1), i32(-1)}, {i32(+33), i32(-33)}, {i32(+2), i32(-2)}, {i32(+14), i32(-14)}}));
}

Y_UNIT_TEST_LLVM(TestOverFlowAndIndependentWideFlows) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<i32>{0, 3, 7});
    const auto pgmReturn = pb.FromFlow(pb.NarrowMap(pb.FlatMap(pb.ToFlow(list),
                                                               [&](TRuntimeNode) {
                                                                   return pb.ExpandMap(pb.ToFlow(NTest::ConvertValueToLiteralNode(pb, TVector<i32>{-100, -100})),
                                                                                       [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Plus(item), pb.Minus(item)}; });
                                                               }),
                                                    [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, NYql::NUdf::TUnboxedValueComparatorStreamView<std::tuple<i32, i32>>({{i32(-100), i32(+100)},
                                                                                                                  {i32(-100), i32(+100)},
                                                                                                                  {i32(-100), i32(+100)},
                                                                                                                  {i32(-100), i32(+100)},
                                                                                                                  {i32(-100), i32(+100)},
                                                                                                                  {i32(-100), i32(+100)}}));
}

Y_UNIT_TEST_LLVM(TestOverListAndIndependentWideFlows) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<i32>{0, 3, 7});
    const auto pgmReturn = pb.FromFlow(pb.NarrowMap(pb.FlatMap(list,
                                                               [&](TRuntimeNode) {
                                                                   return pb.ExpandMap(pb.ToFlow(NTest::ConvertValueToLiteralNode(pb, TVector<i32>{-100, -100})),
                                                                                       [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Minus(item), pb.Plus(item)}; });
                                                               }),
                                                    [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, NYql::NUdf::TUnboxedValueComparatorStreamView<std::tuple<i32, i32>>({{i32(+100), i32(-100)},
                                                                                                                  {i32(+100), i32(-100)},
                                                                                                                  {i32(+100), i32(-100)},
                                                                                                                  {i32(+100), i32(-100)},
                                                                                                                  {i32(+100), i32(-100)},
                                                                                                                  {i32(+100), i32(-100)}}));
}

template <bool LLVM>
void TestRecursiveFlatMapOverLazyZip(bool WithCollect) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TVector<double>>{{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}, {7.0, 8.0, 9.0}});

    const auto init = pb.Replicate(NTest::ConvertValueToLiteralNode(pb, double(0.0)), NTest::ConvertValueToLiteralNode(pb, ui64(3ULL)), __FILE__, __LINE__, 0);
    const auto pgmReturn = pb.Fold(list, WithCollect ? pb.Collect(init) : init,
                                   [&pb](TRuntimeNode item, TRuntimeNode state) {
                                       return pb.OrderedFlatMap(pb.Zip({item, state}),
                                                                [&pb](TRuntimeNode item) {
                                                                    return pb.NewOptional(pb.Add(pb.Nth(item, 0), pb.Nth(item, 1)));
                                                                });
                                   });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<double>{12.0, 15.0, 18.0});
}

Y_UNIT_TEST_LLVM(TestRecursiveFlatMapOverLazyZipWithCollect) {
    TestRecursiveFlatMapOverLazyZip<LLVM>(true);
}
Y_UNIT_TEST_LLVM(TestRecursiveFlatMapOverLazyZipWithoutCollect) {
    TestRecursiveFlatMapOverLazyZip<LLVM>(false);
}
} // Y_UNIT_TEST_SUITE(TMiniKQLFlatMapTest)

} // namespace NMiniKQL
} // namespace NKikimr
