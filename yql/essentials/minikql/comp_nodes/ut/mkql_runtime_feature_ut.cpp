#include "mkql_computation_node_ut.h"
#include <yql/essentials/minikql/comp_nodes/ut/mkql_program_builder_test_utils.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

namespace NKikimr::NMiniKQL {

Y_UNIT_TEST_SUITE(TRuntimeFeatureTest) {

Y_UNIT_TEST_LLVM(HostRuntimeSetting) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    setup.RuntimeSettings->TestHostSetting.Set(true);

    const auto lookup = [&](TStringBuf name) {
        auto graph = setup.BuildGraph(pb.HostRuntimeSetting(NTest::ConvertValueToLiteralNode(pb, name)));
        return graph->GetValue();
    };

    const auto found = lookup("TestHostSetting");
    AssertUnboxedValueElementEqual(found, TMaybe<TStringBuf>{"true"});

    AssertUnboxedValueElementEqual(lookup("NonExistentSetting"), TMaybe<TStringBuf>{});
}

Y_UNIT_TEST_LLVM(UdfRuntimeSetting) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    setup.RuntimeSettings->SetUdfSetting("MyModule", "myKey", "myValue");

    const auto lookup = [&](TStringBuf module, TStringBuf key) {
        auto graph = setup.BuildGraph(pb.UdfRuntimeSetting(
            NTest::ConvertValueToLiteralNode(pb, module),
            NTest::ConvertValueToLiteralNode(pb, key)));
        return graph->GetValue();
    };

    const auto found = lookup("MyModule", "myKey");
    AssertUnboxedValueElementEqual(found, TMaybe<TStringBuf>{"myValue"});

    AssertUnboxedValueElementEqual(lookup("MyModule", "missingKey"), TMaybe<TStringBuf>{});
    AssertUnboxedValueElementEqual(lookup("UnknownModule", "myKey"), TMaybe<TStringBuf>{});
}

} // Y_UNIT_TEST_SUITE(TRuntimeFeatureTest)

} // namespace NKikimr::NMiniKQL
