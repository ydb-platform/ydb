#include "mkql_computation_node_ut.h"

namespace NKikimr::NMiniKQL {

Y_UNIT_TEST_SUITE(TRuntimeFeatureTest) {

Y_UNIT_TEST_LLVM(HostRuntimeSetting) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    setup.RuntimeSettings->TestHostSetting.Set(true);

    const auto lookup = [&](TStringBuf name) {
        auto graph = setup.BuildGraph(pb.HostRuntimeSetting(pb.NewDataLiteral<NUdf::EDataSlot::String>(name)));
        return graph->GetValue();
    };

    const auto found = lookup("TestHostSetting");
    UNIT_ASSERT(found);
    const NUdf::TUnboxedValue foundValue = found.GetOptionalValue();
    UNIT_ASSERT_VALUES_EQUAL(TString(foundValue.AsStringRef()), "true");

    UNIT_ASSERT(!lookup("NonExistentSetting"));
}

Y_UNIT_TEST_LLVM(UdfRuntimeSetting) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    setup.RuntimeSettings->SetUdfSetting("MyModule", "myKey", "myValue");

    const auto lookup = [&](TStringBuf module, TStringBuf key) {
        auto graph = setup.BuildGraph(pb.UdfRuntimeSetting(
            pb.NewDataLiteral<NUdf::EDataSlot::String>(module),
            pb.NewDataLiteral<NUdf::EDataSlot::String>(key)));
        return graph->GetValue();
    };

    const auto found = lookup("MyModule", "myKey");
    UNIT_ASSERT(found);
    const NUdf::TUnboxedValue foundValue = found.GetOptionalValue();
    UNIT_ASSERT_VALUES_EQUAL(TString(foundValue.AsStringRef()), "myValue");

    UNIT_ASSERT(!lookup("MyModule", "missingKey"));
    UNIT_ASSERT(!lookup("UnknownModule", "myKey"));
}

} // Y_UNIT_TEST_SUITE(TRuntimeFeatureTest)

} // namespace NKikimr::NMiniKQL
