#include "mkql_computation_node_ut.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_program_builder_test_utils.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLWideStreamTest) {

Y_UNIT_TEST_LLVM(TestSimple) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<ui64, ui64>>{
                                                               {ui64(1), ui64(10)},
                                                               {ui64(2), ui64(20)},
                                                               {ui64(3), ui64(30)},
                                                           });
    const auto flow = pb.ToFlow(list);

    const auto wideFlow = pb.ExpandMap(flow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
        return {pb.Nth(item, 0U), pb.Nth(item, 1U)};
    });

    const auto wideStream = pb.FromFlow(wideFlow);
    const auto newWideFlow = pb.ToFlow(wideStream);

    const auto narrowFlow = pb.NarrowMap(newWideFlow, [&](TRuntimeNode::TList items) -> TRuntimeNode {
        return pb.Sub(items[1], items[0]);
    });
    const auto pgmReturn = pb.Collect(narrowFlow);

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui64>{
                                                          ui64(9),
                                                          ui64(18),
                                                          ui64(27),
                                                      });
}
} // Y_UNIT_TEST_SUITE(TMiniKQLWideStreamTest)

} // namespace NMiniKQL
} // namespace NKikimr
