#include <yql/essentials/minikql/comp_nodes/mkql_block_cast.h>

#include <yql/essentials/minikql/comp_nodes/ut/mkql_block_test_helper.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr::NMiniKQL {

using namespace NTest;

namespace {

TRuntimeNode BlockCast(TSetup<false>& setup, TRuntimeNode input, NUdf::EDataSlot targetSlot, bool safe) {
    const auto inputType = AS_TYPE(TBlockType, input.GetStaticType());
    bool isOptional;
    UnpackOptionalData(inputType->GetItemType(), isOptional);
    const auto targetType = setup.PgmBuilder->NewBlockType(
        setup.PgmBuilder->NewDataType(targetSlot, isOptional), inputType->GetShape());
    return setup.PgmBuilder->BlockCast(input, targetType, safe);
}

TRuntimeNode CastUtf8ToString(TSetup<false>& setup, TRuntimeNode input) {
    return BlockCast(setup, input, NUdf::EDataSlot::String, /*safe=*/true);
}

}

Y_UNIT_TEST_SUITE(TMiniKQLBlockCastTest) {

Y_UNIT_TEST(Utf8ToString) {
    TBlockHelper().TestKernelFuzzied(
        TVector<TUtf8>{TUtf8{"one"}, TUtf8{"two"}},
        TVector<TString>{"one", "two"},
        CastUtf8ToString);
}

Y_UNIT_TEST(OptionalUtf8ToString) {
    TBlockHelper().TestKernelFuzzied(
        TVector<TMaybe<TUtf8>>{TUtf8{"one"}, Nothing(), TUtf8{"two"}},
        TVector<TMaybe<TString>>{"one", Nothing(), "two"},
        CastUtf8ToString);
}

Y_UNIT_TEST(SafeFloatToIntFails) {
    TSetup<false> setup;
    const auto input = setup.PgmBuilder->AsScalar(
        NTest::ConvertValueToLiteralNode(*setup.PgmBuilder, 1.5));
    const auto graph = setup.BuildGraph(BlockCast(setup, input, NUdf::EDataSlot::Int64, /*safe=*/true));

    UNIT_ASSERT_EXCEPTION(graph->GetValue(), yexception);
}

Y_UNIT_TEST(UnsafeFloatToIntTruncates) {
    TBlockHelper().TestKernelFuzzied(
        TVector<double>{1.5, 2.5},
        TVector<i64>{1, 2},
        [](TSetup<false>& setup, TRuntimeNode input) {
            return BlockCast(setup, input, NUdf::EDataSlot::Int64, /*safe=*/false);
        });
}

} // Y_UNIT_TEST_SUITE(TMiniKQLBlockCastTest)

} // namespace NKikimr::NMiniKQL
