#include "mkql_computation_node_ut.h"
#include <yql/essentials/minikql/mkql_runtime_version.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_program_builder_test_utils.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLWideCondense1Test) {
Y_UNIT_TEST_LLVM(TestConcatItemsToKey) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TStringBuf, TStringBuf>>{
                                                               {"key one", "very long value 1"},
                                                               {"key two", "very long value 2"},
                                                               {"key two", "very long value 3"},
                                                               {"very long key one", "very long value 4"},
                                                               {"very long key two", "very long value 5"},
                                                               {"very long key two", "very long value 6"},
                                                               {"very long key two", "very long value 7"},
                                                               {"very long key two", "very long value 8"},
                                                               {"very long key two", "very long value 9"},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideCondense1(pb.ExpandMap(pb.ToFlow(list),
                                                                                 [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                    [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front(), pb.AggrConcat(pb.AggrConcat(items.front(), NTest::ConvertValueToLiteralNode(pb, TStringBuf(": "))), items.back())}; },
                                                                    [&](TRuntimeNode::TList items, TRuntimeNode::TList state) { return pb.AggrNotEquals(items.front(), state.front()); },
                                                                    [&](TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {state.front(), pb.AggrConcat(pb.AggrConcat(state.back(), NTest::ConvertValueToLiteralNode(pb, TStringBuf(", "))), items.back())}; }),
                                                   [&](TRuntimeNode::TList items) { return items.back(); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TStringBuf>{
                                                          "key one: very long value 1",
                                                          "key two: very long value 2, very long value 3",
                                                          "very long key one: very long value 4",
                                                          "very long key two: very long value 5, very long value 6, very long value 7, very long value 8, very long value 9",
                                                      });
}

Y_UNIT_TEST_LLVM(TestSwitchByBoolFieldAndDontUseKey) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<TStringBuf>, TStringBuf, bool>>{
                                                               {{}, "value 1", true},
                                                               {"one", "value 2", false},
                                                               {"two", "value 3", false},
                                                               {{}, "value 4", true},
                                                               {"one", "value 5", false},
                                                               {"two", "value 6", false},
                                                               {{}, "value 7", false},
                                                               {"one", "value 8", false},
                                                               {"two", "value 9", true},
                                                           });

    const auto landmine = NTest::ConvertValueToLiteralNode(pb, TStringBuf("ACHTUNG MINEN!"));

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideCondense1(pb.ExpandMap(pb.ToFlow(list),
                                                                                 [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Unwrap(pb.Nth(item, 0U), landmine, __FILE__, __LINE__, 0), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                                    [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[1U]}; },
                                                                    [&](TRuntimeNode::TList items, TRuntimeNode::TList) { return items.back(); },
                                                                    [&](TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {pb.AggrConcat(pb.AggrConcat(state.front(), NTest::ConvertValueToLiteralNode(pb, TStringBuf("; "))), items[1U])}; }),
                                                   [&](TRuntimeNode::TList items) { return items.front(); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TStringBuf>{
                                                          "value 1; value 2; value 3",
                                                          "value 4; value 5; value 6; value 7; value 8",
                                                          "value 9",
                                                      });
}

Y_UNIT_TEST_LLVM(TestThinAllLambdas) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<>>(pb);
    const auto data = pb.NewTuple({});
    const auto list = pb.NewList(tupleType, {data, data, data, data});

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideCondense1(pb.ExpandMap(pb.ToFlow(list),
                                                                                 [](TRuntimeNode) -> TRuntimeNode::TList { return {}; }),
                                                                    [](TRuntimeNode::TList items) { return items; },
                                                                    [&](TRuntimeNode::TList, TRuntimeNode::TList) { return pb.NewDataLiteral<bool>(true); },
                                                                    [](TRuntimeNode::TList, TRuntimeNode::TList state) { return state; }),
                                                   [&](TRuntimeNode::TList) { return pb.NewTuple({}); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<>>{{}, {}, {}, {}});
}
} // Y_UNIT_TEST_SUITE(TMiniKQLWideCondense1Test)

} // namespace NMiniKQL
} // namespace NKikimr
