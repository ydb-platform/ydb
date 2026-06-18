#include "mkql_computation_node_ut.h"
#include <yql/essentials/minikql/mkql_runtime_version.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_program_builder_test_utils.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLWideMapTest) {
Y_UNIT_TEST_LLVM(TestSimpleSwap) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                               {i32(1), {}, i32(-1)},
                                                               {{}, i32(2), i32(-2)},
                                                               {i32(3), {}, i32(-3)},
                                                               {i32(4), i32(4), {}},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.ExpandMap(pb.ToFlow(list),
                                                                [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple({items[2U], items[1U], items[0U]}); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                          {i32(-1), {}, i32(1)},
                                                          {i32(-2), i32(2), {}},
                                                          {i32(-3), {}, i32(3)},
                                                          {{}, i32(4), i32(4)},
                                                      });
}

Y_UNIT_TEST_LLVM(TestThinLambda) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>>>{
                                                               {i32(1)},
                                                               {{}},
                                                               {i32(3)},
                                                               {i32(4)},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideMap(pb.ExpandMap(pb.ToFlow(list),
                                                                           [&](TRuntimeNode) -> TRuntimeNode::TList { return {}; }),
                                                              [&](TRuntimeNode::TList items) { return items; }),
                                                   [&](TRuntimeNode::TList) -> TRuntimeNode { return pb.NewTuple({}); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetListLength(), 4ULL);
}

Y_UNIT_TEST_LLVM(TestWideMap) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                               {i32(1), {}, i32(-1)},
                                                               {{}, i32(2), i32(-2)},
                                                               {i32(3), {}, i32(-3)},
                                                               {i32(4), i32(4), {}},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideMap(pb.ExpandMap(pb.ToFlow(list),
                                                                           [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                              [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {pb.AggrMin(items[0], items[1]), pb.AggrMax(items[1], items[2]), pb.AggrAdd(items[0], items[2])}; }),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                          {i32(1), i32(-1), i32(0)},
                                                          {i32(2), i32(2), i32(-2)},
                                                          {i32(3), i32(-3), i32(0)},
                                                          {i32(4), i32(4), i32(4)},
                                                      });
}

Y_UNIT_TEST_LLVM(TestDotCalculateUnusedField) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                               {i32(1), i32(0), i32(-1)},
                                                               {i32(2), {}, i32(-2)},
                                                               {i32(3), {}, i32(-3)},
                                                               {i32(4), {}, i32(-4)},
                                                           });

    const auto landmine = NTest::ConvertValueToLiteralNode(pb, TStringBuf("ACHTUNG MINEN!"));

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideMap(pb.ExpandMap(pb.ToFlow(list),
                                                                           [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                              [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {pb.Mul(items.front(), items.back()), pb.Unwrap(items[1], landmine, __FILE__, __LINE__, 0), pb.Add(items.front(), items.back())}; }),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple({items.back(), items.front()}); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TMaybe<i32>, TMaybe<i32>>>{
                                                          {i32(0), i32(-1)},
                                                          {i32(0), i32(-4)},
                                                          {i32(0), i32(-9)},
                                                          {i32(0), i32(-16)},
                                                      });
}

Y_UNIT_TEST_LLVM(TestPasstroughtFieldsAsIs) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                               {i32(1), i32(-5), i32(-1)},
                                                               {i32(2), i32(-4), i32(-2)},
                                                               {i32(3), i32(-7), i32(-3)},
                                                               {i32(4), {}, i32(-4)},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideMap(pb.ExpandMap(pb.ToFlow(list),
                                                                           [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                              [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front(), pb.Minus(items[1u]), items.back()}; }),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                          {i32(1), i32(5), i32(-1)},
                                                          {i32(2), i32(4), i32(-2)},
                                                          {i32(3), i32(7), i32(-3)},
                                                          {i32(4), {}, i32(-4)},
                                                      });
}

Y_UNIT_TEST_LLVM(TestPasstroughtFieldSplitAsIs) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                               {i32(1), i32(-5), i32(-1)},
                                                               {i32(2), i32(-4), i32(-2)},
                                                               {i32(3), i32(-7), i32(-3)},
                                                               {i32(4), {}, i32(-4)},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideMap(pb.ExpandMap(pb.ToFlow(list),
                                                                           [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                              [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[1U], pb.Mul(items.front(), items.back()), items[1U]}; }),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                          {i32(-5), i32(-1), i32(-5)},
                                                          {i32(-4), i32(-4), i32(-4)},
                                                          {i32(-7), i32(-9), i32(-7)},
                                                          {{}, i32(-16), {}},
                                                      });
}

Y_UNIT_TEST_LLVM(TestFieldBothWayPasstroughtAndArg) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                               {i32(1), i32(-5), i32(-1)},
                                                               {i32(2), i32(-4), i32(-2)},
                                                               {i32(3), i32(-7), i32(-3)},
                                                               {i32(4), {}, i32(-4)},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideMap(pb.ExpandMap(pb.ToFlow(list),
                                                                           [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                              [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[1U], pb.Sub(items.front(), items.back()), pb.Minus(items[1U])}; }),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                          {i32(-5), i32(2), i32(5)},
                                                          {i32(-4), i32(4), i32(4)},
                                                          {i32(-7), i32(6), i32(7)},
                                                          {{}, i32(8), {}},
                                                      });
}

Y_UNIT_TEST_LLVM(TestPasstroughtFieldSplitAndFirstUnused) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>>{
                                                               {i32(1), i32(-5), i32(-3)},
                                                               {i32(2), i32(-4), i32(-6)},
                                                               {i32(3), i32(-7), i32(-9)},
                                                               {i32(4), {}, i32(-1)},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideMap(pb.ExpandMap(pb.ToFlow(list),
                                                                           [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                              [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[1U], pb.AggrAdd(items.front(), items.back()), items[1U]}; }),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple({items[1], items[2]}); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TMaybe<i32>, TMaybe<i32>>>{
                                                          {i32(-2), i32(-5)},
                                                          {i32(-4), i32(-4)},
                                                          {i32(-6), i32(-7)},
                                                          {i32(3), {}},
                                                      });
}
} // Y_UNIT_TEST_SUITE(TMiniKQLWideMapTest)

} // namespace NMiniKQL
} // namespace NKikimr
