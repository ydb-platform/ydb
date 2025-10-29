#include "mkql_computation_node_ut.h"

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLLinearTest) {
Y_UNIT_TEST_LLVM(TestDynamicConvert) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
    const auto arg = pb.Arg(pb.NewLinearType(dataType, false));
    const auto pgmReturn = pb.FromDynamicLinear(pb.ToDynamicLinear(arg), "foo.sql", 5, 6);
    const auto graph = setup.BuildGraph(pgmReturn, {arg.GetNode()});
    auto& ctx = graph->GetContext();
    graph->GetEntryPoint(0, true)->SetValue(ctx, NUdf::TUnboxedValuePod(1));
    UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().template Get<ui32>(), 1);
}

Y_UNIT_TEST_LLVM(TestDynamicConvertTwice) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
    const auto arg = pb.Arg(pb.NewLinearType(dataType, false));
    const auto linear = pb.ToDynamicLinear(arg);
    const auto use1 = pb.FromDynamicLinear(linear, "foo.sql", 5, 6);
    const auto use2 = pb.FromDynamicLinear(linear, "foo.sql", 8, 9);
    const auto pgmReturn = pb.NewTuple({use1, use2});
    const auto graph = setup.BuildGraph(pgmReturn, {arg.GetNode()});
    auto& ctx = graph->GetContext();
    graph->GetEntryPoint(0, true)->SetValue(ctx, NUdf::TUnboxedValuePod(1));
    UNIT_ASSERT_EXCEPTION_CONTAINS(graph->GetValue(), std::exception, R"(Terminate was called, reason(51): foo.sql:8:9: The linear value has already been used)");
}
} // Y_UNIT_TEST_SUITE(TMiniKQLLinearTest)

} // namespace NMiniKQL
} // namespace NKikimr
