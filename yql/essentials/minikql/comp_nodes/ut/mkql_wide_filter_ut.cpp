#include "mkql_computation_node_ut.h"
#include <yql/essentials/minikql/mkql_runtime_version.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_program_builder_test_utils.h>

namespace NKikimr {
namespace NMiniKQL {

using NYql::NUdf::TUnboxedValueComparatorStreamView;

Y_UNIT_TEST_SUITE(TMiniKQLWideFilterTest) {
Y_UNIT_TEST_LLVM(TestPredicateExpression) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>>>{
                                                               {{}, i32(-1)},
                                                               {i32(2), i32(-2)},
                                                               {i32(3), i32(-3)},
                                                           });

    const auto pgmReturn = pb.FromFlow(
        pb.NarrowMap(
            pb.WideFilter(
                pb.ExpandMap(
                    pb.ToFlow(list),
                    [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                [&](TRuntimeNode::TList items) -> TRuntimeNode {
                    const auto v = pb.If(
                        pb.Exists(items.front()),
                        NTest::ConvertValueToLiteralNode(pb, TMaybe<bool>{true}),
                        pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<bool>::Id));
                    return pb.Coalesce(v, NTest::ConvertValueToLiteralNode(pb, false));
                }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    NUdf::TUnboxedValue value = graph->GetValue();

    AssertUnboxedValueElementEqual(value, TUnboxedValueComparatorStreamView<std::tuple<TMaybe<i32>, TMaybe<i32>>>({
                                              std::tuple<TMaybe<i32>, TMaybe<i32>>{i32(2), i32(-2)},
                                              std::tuple<TMaybe<i32>, TMaybe<i32>>{i32(3), i32(-3)},
                                          }));
}

Y_UNIT_TEST_LLVM(TestCheckedFieldPasstrought) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                               {i32(1), {}, i32(-1)},
                                                               {{}, i32(2), i32(-2)},
                                                               {i32(3), {}, i32(-3)},
                                                               {i32(4), i32(4), {}},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideFilter(pb.ExpandMap(pb.ToFlow(list),
                                                                              [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                                 [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.Exists(items[1U]); }),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                          {{}, i32(2), i32(-2)},
                                                          {i32(4), i32(4), {}},
                                                      });
}

Y_UNIT_TEST_LLVM(TestCheckedFieldUnusedAfter) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                               {i32(1), {}, i32(-1)},
                                                               {{}, i32(2), i32(-2)},
                                                               {i32(3), {}, i32(-3)},
                                                               {i32(4), i32(4), {}},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideFilter(pb.ExpandMap(pb.ToFlow(list),
                                                                              [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                                 [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.Exists(items[1U]); }),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple({items.back(), items.front()}); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TMaybe<i32>, TMaybe<i32>>>{
                                                          {i32(-2), {}},
                                                          {{}, i32(4)},
                                                      });
}

Y_UNIT_TEST_LLVM(TestDotCalculateUnusedField) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                               {i32(1), {}, i32(-1)},
                                                               {{}, i32(2), i32(-2)},
                                                               {i32(3), {}, i32(-3)},
                                                               {i32(4), i32(4), {}},
                                                           });

    const auto landmine = NTest::ConvertValueToLiteralNode(pb, TStringBuf("ACHTUNG MINEN!"));

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideFilter(pb.ExpandMap(pb.ToFlow(list),
                                                                              [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Unwrap(pb.Nth(item, 2U), landmine, __FILE__, __LINE__, 0)}; }),
                                                                 [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.Exists(items[1U]); }),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return items.front(); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<i32>>{
                                                          {},
                                                          i32(4),
                                                      });
}

Y_UNIT_TEST_LLVM(TestWithLimitCheckedFieldUsedAfter) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                               {i32(9), {}, i32(-1)},
                                                               {{}, i32(2), i32(-2)},
                                                               {i32(7), {}, i32(-3)},
                                                               {i32(6), i32(4), {}},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideFilter(pb.ExpandMap(pb.ToFlow(list),
                                                                              [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                                 NTest::ConvertValueToLiteralNode(pb, ui64(2)), [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.Exists(items.front()); }),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.AggrAdd(items.back(), items.front()); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<i32>>{
                                                          i32(8),
                                                          i32(4),
                                                      });
}

Y_UNIT_TEST_LLVM(TestWithLimitCheckedFieldUnusedAfter) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                               {i32(1), {}, i32(-1)},
                                                               {{}, i32(2), i32(-2)},
                                                               {i32(3), {}, i32(-3)},
                                                               {i32(4), i32(4), {}},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideFilter(pb.ExpandMap(pb.ToFlow(list),
                                                                              [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                                 NTest::ConvertValueToLiteralNode(pb, ui64(2)), [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.Exists(items.front()); }),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return items.back(); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<i32>>{
                                                          i32(-1),
                                                          i32(-3),
                                                      });
}

Y_UNIT_TEST_LLVM(TestTakeWhile) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                               {i32(1), {}, i32(-1)},
                                                               {{}, i32(2), i32(-2)},
                                                               {i32(3), {}, i32(-3)},
                                                               {i32(4), i32(4), {}},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTakeWhile(pb.ExpandMap(pb.ToFlow(list),
                                                                                 [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                                    [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.Exists(items.front()); }),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                          {i32(1), {}, i32(-1)},
                                                      });
}

Y_UNIT_TEST_LLVM(TestTakeWhileInclusive) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                               {i32(1), {}, i32(-1)},
                                                               {{}, i32(2), i32(-2)},
                                                               {i32(3), {}, i32(-3)},
                                                               {i32(4), i32(4), {}},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTakeWhileInclusive(pb.ExpandMap(pb.ToFlow(list),
                                                                                          [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                                             [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.Exists(items.front()); }),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                          {i32(1), {}, i32(-1)},
                                                          {{}, i32(2), i32(-2)},
                                                      });
}

Y_UNIT_TEST_LLVM(TestTakeWhileInclusiveSingular) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTakeWhileInclusive(pb.Source(),
                                                                             [&](TRuntimeNode::TList) -> TRuntimeNode {
                                                                                 return NTest::ConvertValueToLiteralNode(pb, false);
                                                                             }),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto length = graph->GetValue().GetListLength();
    UNIT_ASSERT_VALUES_EQUAL(length, 1);
}

Y_UNIT_TEST_LLVM(TestSkipWhile) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                               {i32(1), {}, i32(-1)},
                                                               {{}, i32(2), i32(-2)},
                                                               {i32(3), {}, i32(-3)},
                                                               {i32(4), i32(4), {}},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideSkipWhile(pb.ExpandMap(pb.ToFlow(list),
                                                                                 [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                                    [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.AggrGreater(items.back(), NTest::ConvertValueToLiteralNode(pb, TMaybe<i32>{-3})); }),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                          {i32(3), {}, i32(-3)},
                                                          {i32(4), i32(4), {}},
                                                      });
}

Y_UNIT_TEST_LLVM(TestSkipWhileInclusive) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                               {i32(1), {}, i32(-1)},
                                                               {{}, i32(2), i32(-2)},
                                                               {i32(3), {}, i32(-3)},
                                                               {i32(4), i32(4), {}},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideSkipWhileInclusive(pb.ExpandMap(pb.ToFlow(list),
                                                                                          [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                                             [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.AggrGreater(items.back(), NTest::ConvertValueToLiteralNode(pb, TMaybe<i32>{-3})); }),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                          {i32(4), i32(4), {}},
                                                      });
}

Y_UNIT_TEST_LLVM(TestSkipWhileInclusiveSingular) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    constexpr ui64 limit = 2;

    const auto limitedSource = pb.ExpandMap(pb.Take(pb.NarrowMap(pb.Source(),
                                                                 [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }),
                                                    NTest::ConvertValueToLiteralNode(pb, limit)),
                                            [&](TRuntimeNode) -> TRuntimeNode::TList { return {}; });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideSkipWhileInclusive(limitedSource,
                                                                             [&](TRuntimeNode::TList) -> TRuntimeNode {
                                                                                 return NTest::ConvertValueToLiteralNode(pb, false);
                                                                             }),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto length = graph->GetValue().GetListLength();
    UNIT_ASSERT_VALUES_EQUAL(length, limit - 1);
}

Y_UNIT_TEST_LLVM(TestFilterByBooleanField) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, bool, TMaybe<i32>>>{
                                                               {i32(1), true, i32(-1)},
                                                               {{}, false, i32(-2)},
                                                               {i32(3), true, i32(-3)},
                                                               {i32(4), false, {}},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideFilter(pb.ExpandMap(pb.ToFlow(list),
                                                                              [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                                 [&](TRuntimeNode::TList items) -> TRuntimeNode { return items[1U]; }),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple({items.back(), items.front()}); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TMaybe<i32>, TMaybe<i32>>>{
                                                          {i32(-1), i32(1)},
                                                          {i32(-3), i32(3)},
                                                      });
}
} // Y_UNIT_TEST_SUITE(TMiniKQLWideFilterTest)

} // namespace NMiniKQL
} // namespace NKikimr
