#include "mkql_computation_node_ut.h"
#include "mkql_program_builder_test_utils.h"

#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

namespace NKikimr::NMiniKQL {

Y_UNIT_TEST_SUITE(TErasedTest) {

Y_UNIT_TEST_LLVM(TestPeekErasedSuccessfulExtraction) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto value = NTest::ConvertValueToLiteralNode(pb, ui32(42));
    const auto erased = pb.AsErased(value);
    const auto pgmReturn = pb.PeekErased(erased, value.GetStaticType());

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TMaybe<ui32>{ui32(42)});
}

Y_UNIT_TEST_LLVM(TestPeekErasedUnsuccessfulExtraction) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto value = NTest::ConvertValueToLiteralNode(pb, ui32(42));
    const auto erased = pb.AsErased(value);
    // Request a different type (ui64) than the stored one (ui32).
    const auto ui64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto pgmReturn = pb.PeekErased(erased, ui64Type);

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TMaybe<ui64>{});
}

} // Y_UNIT_TEST_SUITE(TErasedTest)

} // namespace NKikimr::NMiniKQL
