#include "mkql_computation_node_ut.h"
#include <yql/essentials/minikql/mkql_runtime_version.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_program_builder_test_utils.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

namespace NKikimr {
namespace NMiniKQL {

using NTest::TUtf8;
using NYql::NUdf::TUnboxedValueComparatorStreamView;

Y_UNIT_TEST_SUITE(TMiniKQLWideChain1MapTest) {
Y_UNIT_TEST_LLVM(TestThinLambda) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>>>{
                                                               {TMaybe<i32>{1}}, {TMaybe<i32>{}}, {TMaybe<i32>{3}}, {TMaybe<i32>{4}}});

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideChain1Map(pb.ExpandMap(pb.ToFlow(list),
                                                                                 [&](TRuntimeNode) -> TRuntimeNode::TList { return {}; }),
                                                                    [&](TRuntimeNode::TList inputs) { return inputs; },
                                                                    [&](TRuntimeNode::TList, TRuntimeNode::TList outputs) { return outputs; }),
                                                   [&](TRuntimeNode::TList) -> TRuntimeNode { return pb.NewTuple({}); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<>>{{}, {}, {}, {}});
}

Y_UNIT_TEST_LLVM(TestSimpleSwap) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TTuple = std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>;
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TTuple>{
                                                               {TMaybe<i32>{1}, TMaybe<i32>{}, TMaybe<i32>{-1}},
                                                               {TMaybe<i32>{}, TMaybe<i32>{2}, TMaybe<i32>{-2}},
                                                               {TMaybe<i32>{3}, TMaybe<i32>{}, TMaybe<i32>{-3}},
                                                               {TMaybe<i32>{4}, TMaybe<i32>{4}, TMaybe<i32>{}},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideChain1Map(pb.ExpandMap(pb.ToFlow(list),
                                                                                 [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                                    [&](TRuntimeNode::TList inputs) { return inputs; },
                                                                    [&](TRuntimeNode::TList inputs, TRuntimeNode::TList outputs) -> TRuntimeNode::TList { return {inputs.back(), outputs[1U], inputs.front()}; }),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTuple>{
                                                          {TMaybe<i32>{1}, TMaybe<i32>{}, TMaybe<i32>{-1}},
                                                          {TMaybe<i32>{-2}, TMaybe<i32>{}, TMaybe<i32>{}},
                                                          {TMaybe<i32>{-3}, TMaybe<i32>{}, TMaybe<i32>{3}},
                                                          {TMaybe<i32>{}, TMaybe<i32>{}, TMaybe<i32>{4}},
                                                      });
}

Y_UNIT_TEST_LLVM(TestSimpleChain) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto dataType = NTest::ConvertToMinikqlType<TMaybe<i32>>(pb);

    using TTuple = std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>;
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TTuple>{
                                                               {TMaybe<i32>{1}, TMaybe<i32>{-5}, TMaybe<i32>{-6}},
                                                               {TMaybe<i32>{2}, TMaybe<i32>{-4}, TMaybe<i32>{-7}},
                                                               {TMaybe<i32>{3}, TMaybe<i32>{-7}, TMaybe<i32>{-8}},
                                                               {TMaybe<i32>{4}, TMaybe<i32>{}, TMaybe<i32>{-9}},
                                                           });

    const auto pgmReturn = pb.FromFlow(pb.NarrowMap(pb.WideChain1Map(pb.ExpandMap(pb.ToFlow(list),
                                                                                  [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                                     [&](TRuntimeNode::TList inputs) -> TRuntimeNode::TList { return {pb.Add(inputs.front(), inputs[1U]), pb.NewEmptyOptional(dataType), pb.Sub(inputs.back(), inputs[1U])}; },
                                                                     [&](TRuntimeNode::TList inputs, TRuntimeNode::TList outputs) -> TRuntimeNode::TList { return {pb.AggrAdd(outputs.back(), inputs[1U]), outputs.front(), pb.Decrement(outputs[1])}; }),
                                                    [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, TUnboxedValueComparatorStreamView<TTuple>({
                                                 {TMaybe<i32>{-4}, TMaybe<i32>{}, TMaybe<i32>{-1}},
                                                 {TMaybe<i32>{-5}, TMaybe<i32>{-4}, TMaybe<i32>{}},
                                                 {TMaybe<i32>{-7}, TMaybe<i32>{-5}, TMaybe<i32>{-5}},
                                                 {TMaybe<i32>{-5}, TMaybe<i32>{-7}, TMaybe<i32>{-6}},
                                             }));
}

Y_UNIT_TEST_LLVM(TestAgrregateWithPrevious) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TTuple = std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>;
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TTuple>{
                                                               {TMaybe<i32>{1}, TMaybe<i32>{}, TMaybe<i32>{-1}},
                                                               {TMaybe<i32>{}, TMaybe<i32>{2}, TMaybe<i32>{-2}},
                                                               {TMaybe<i32>{3}, TMaybe<i32>{}, TMaybe<i32>{-3}},
                                                               {TMaybe<i32>{4}, TMaybe<i32>{5}, TMaybe<i32>{}},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideChain1Map(pb.ExpandMap(pb.ToFlow(list),
                                                                                 [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                                    [&](TRuntimeNode::TList inputs) -> TRuntimeNode::TList { return {inputs[1U], inputs[2U], inputs[0U]}; },
                                                                    [&](TRuntimeNode::TList inputs, TRuntimeNode::TList outputs) -> TRuntimeNode::TList { return {pb.AggrMin(inputs[0U], outputs[1U]), pb.AggrMax(inputs[1U], outputs[2U]), pb.AggrAdd(outputs[0U], inputs[2U])}; }),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTuple>{
                                                          {TMaybe<i32>{}, TMaybe<i32>{-1}, TMaybe<i32>{1}},
                                                          {TMaybe<i32>{-1}, TMaybe<i32>{2}, TMaybe<i32>{-2}},
                                                          {TMaybe<i32>{2}, TMaybe<i32>{-2}, TMaybe<i32>{-4}},
                                                          {TMaybe<i32>{-2}, TMaybe<i32>{5}, TMaybe<i32>{2}},
                                                      });
}

Y_UNIT_TEST_LLVM(TestPasstroughtFieldSplitAsIs) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TTuple = std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>;
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TTuple>{
                                                               {TMaybe<i32>{1}, TMaybe<i32>{-6}, TMaybe<i32>{-5}},
                                                               {TMaybe<i32>{2}, TMaybe<i32>{-4}, TMaybe<i32>{-4}},
                                                               {TMaybe<i32>{3}, TMaybe<i32>{-7}, TMaybe<i32>{-3}},
                                                               {TMaybe<i32>{4}, TMaybe<i32>{}, TMaybe<i32>{0}},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideChain1Map(pb.ExpandMap(pb.ToFlow(list),
                                                                                 [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                                    [&](TRuntimeNode::TList inputs) -> TRuntimeNode::TList { return {inputs[1U], pb.Mul(inputs.front(), inputs.back()), inputs[1U]}; },
                                                                    [&](TRuntimeNode::TList inputs, TRuntimeNode::TList outputs) -> TRuntimeNode::TList { return {inputs[1U], pb.Mul(outputs[1U], pb.Add(inputs.back(), inputs.front())), inputs[1U]}; }),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTuple>{
                                                          {TMaybe<i32>{-6}, TMaybe<i32>{-5}, TMaybe<i32>{-6}},
                                                          {TMaybe<i32>{-4}, TMaybe<i32>{10}, TMaybe<i32>{-4}},
                                                          {TMaybe<i32>{-7}, TMaybe<i32>{0}, TMaybe<i32>{-7}},
                                                          {TMaybe<i32>{}, TMaybe<i32>{0}, TMaybe<i32>{}},
                                                      });
}

Y_UNIT_TEST_LLVM(TestFieldBothWayPasstroughtAndArg) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TTuple = std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>;
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TTuple>{
                                                               {TMaybe<i32>{1}, TMaybe<i32>{-5}, TMaybe<i32>{-6}},
                                                               {TMaybe<i32>{2}, TMaybe<i32>{-4}, TMaybe<i32>{-7}},
                                                               {TMaybe<i32>{3}, TMaybe<i32>{-7}, TMaybe<i32>{-8}},
                                                               {TMaybe<i32>{4}, TMaybe<i32>{}, TMaybe<i32>{-9}},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideChain1Map(pb.ExpandMap(pb.ToFlow(list),
                                                                                 [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                                    [&](TRuntimeNode::TList inputs) -> TRuntimeNode::TList { return {inputs[1U], pb.Sub(inputs.front(), inputs.back()), pb.Minus(inputs[1U])}; },
                                                                    [&](TRuntimeNode::TList inputs, TRuntimeNode::TList outputs) -> TRuntimeNode::TList { return {inputs[1U], pb.Sub(outputs[1U], pb.Add(inputs.back(), inputs.front())), pb.Minus(inputs[1U])}; }),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTuple>{
                                                          {TMaybe<i32>{-5}, TMaybe<i32>{7}, TMaybe<i32>{5}},
                                                          {TMaybe<i32>{-4}, TMaybe<i32>{12}, TMaybe<i32>{4}},
                                                          {TMaybe<i32>{-7}, TMaybe<i32>{17}, TMaybe<i32>{7}},
                                                          {TMaybe<i32>{}, TMaybe<i32>{22}, TMaybe<i32>{}},
                                                      });
}

Y_UNIT_TEST_LLVM(TestDotCalculateUnusedField) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TTuple = std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>;
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TTuple>{
                                                               {TMaybe<i32>{1}, TMaybe<i32>{}, TMaybe<i32>{-1}},
                                                               {TMaybe<i32>{2}, TMaybe<i32>{}, TMaybe<i32>{-2}},
                                                               {TMaybe<i32>{3}, TMaybe<i32>{}, TMaybe<i32>{-3}},
                                                               {TMaybe<i32>{4}, TMaybe<i32>{}, TMaybe<i32>{-4}},
                                                           });

    const auto landmine = NTest::ConvertValueToLiteralNode(pb, TUtf8{"Veszély! Aknák!"});

    const auto pgmReturn = pb.FromFlow(pb.NarrowMap(pb.WideChain1Map(pb.ExpandMap(pb.ToFlow(list),
                                                                                  [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                                     [&](TRuntimeNode::TList inputs) -> TRuntimeNode::TList { return {inputs.back(), pb.Unwrap(inputs[1U], landmine, __FILE__, __LINE__, 0), inputs.front()}; },
                                                                     [&](TRuntimeNode::TList inputs, TRuntimeNode::TList outputs) -> TRuntimeNode::TList { return {pb.Mul(outputs.front(), inputs.back()), pb.Unwrap(inputs[1U], landmine, __FILE__, __LINE__, 0), pb.Add(inputs.front(), outputs.back())}; }),
                                                    [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple({items.back(), items.front()}); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, TUnboxedValueComparatorStreamView<std::tuple<i32, i32>>({
                                                 {1, -1},
                                                 {3, 2},
                                                 {6, -6},
                                                 {10, 24},
                                             }));
}
} // Y_UNIT_TEST_SUITE(TMiniKQLWideChain1MapTest)

} // namespace NMiniKQL
} // namespace NKikimr
