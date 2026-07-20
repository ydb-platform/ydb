#include <yql/essentials/minikql/comp_nodes/mkql_block_variant.h>

#include <yql/essentials/minikql/comp_nodes/ut/mkql_block_test_helper.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>

namespace NKikimr::NMiniKQL {

using namespace NTest;

Y_UNIT_TEST_SUITE(TMiniKQLBlockVariantNodeTest) {

Y_UNIT_TEST(TupleVariant_Ui32_IntoIndex0) {
    using TVariant = std::variant<ui32, ui64>;
    TVector<ui32> data = {1u, 2u, 3u};
    TVector<TVariant> expected = {TVariant{ui32{1u}}, TVariant{ui32{2u}}, TVariant{ui32{3u}}};
    TBlockHelper().TestKernelFuzzied(data, expected, [](TSetup<false>& setup, TRuntimeNode payloadValue) {
        auto& pb = *setup.PgmBuilder;
        return pb.BlockVariant(payloadValue, 0u, ConvertToMinikqlType<TVariant>(pb));
    });
}

Y_UNIT_TEST(TupleVariant_Ui64_IntoIndex1) {
    using TVariant = std::variant<ui32, ui64>;
    TVector<ui64> data = {10ull, 20ull, 30ull};
    TVector<TVariant> expected = {TVariant{ui64{10ull}}, TVariant{ui64{20ull}}, TVariant{ui64{30ull}}};
    TBlockHelper().TestKernelFuzzied(data, expected, [](TSetup<false>& setup, TRuntimeNode payloadValue) {
        auto& pb = *setup.PgmBuilder;
        return pb.BlockVariant(payloadValue, 1u, ConvertToMinikqlType<TVariant>(pb));
    });
}

Y_UNIT_TEST(TupleVariant_StringAlternative) {
    using TVariant = std::variant<ui32, TString>;
    TVector<TString> data = {TString{"hello"}, TString{"world"}};
    TVector<TVariant> expected = {TVariant{TString{"hello"}}, TVariant{TString{"world"}}};
    TBlockHelper().TestKernelFuzzied(data, expected, [](TSetup<false>& setup, TRuntimeNode payloadValue) {
        auto& pb = *setup.PgmBuilder;
        return pb.BlockVariant(payloadValue, 1u, ConvertToMinikqlType<TVariant>(pb));
    });
}

Y_UNIT_TEST(TupleVariant_TuplePayload) {
    using TInner = std::tuple<ui32, TString>;
    using TVariant = std::variant<TInner, ui64>;
    TVector<TInner> data = {TInner{ui32{1}, TString{"a"}}, TInner{ui32{2}, TString{"b"}}};
    TVector<TVariant> expected = {TVariant{TInner{ui32{1}, TString{"a"}}}, TVariant{TInner{ui32{2}, TString{"b"}}}};
    TBlockHelper().TestKernelFuzzied(data, expected, [](TSetup<false>& setup, TRuntimeNode payloadValue) {
        auto& pb = *setup.PgmBuilder;
        return pb.BlockVariant(payloadValue, 0u, ConvertToMinikqlType<TVariant>(pb));
    });
}

Y_UNIT_TEST(StructVariant_ByMemberName_XMember) {
    using TVariant = TStructVariant<NTest::TStructMember<"x", ui32>, NTest::TStructMember<"y", ui64>>;

    TVector<ui32> xData = {7u, 8u};
    TVector<TMaybe<ui32>> xExpected = {ui32{7u}, ui32{8u}};
    TBlockHelper().TestKernelFuzzied(xData, xExpected, [](TSetup<false>& setup, TRuntimeNode payloadValue) {
        auto& pb = *setup.PgmBuilder;
        auto varNode = pb.BlockVariant(payloadValue, NUdf::TStringRef("x"), ConvertToMinikqlType<TVariant>(pb));
        return pb.BlockGuess(varNode, NUdf::TStringRef("x"));
    });
}

Y_UNIT_TEST(StructVariant_ByMemberName_YMember) {
    using TVariant = TStructVariant<NTest::TStructMember<"x", ui32>, NTest::TStructMember<"y", ui64>>;

    TVector<ui64> yData = {100ull, 200ull};
    TVector<TMaybe<ui64>> yExpected = {ui64{100ull}, ui64{200ull}};
    TBlockHelper().TestKernelFuzzied(yData, yExpected, [](TSetup<false>& setup, TRuntimeNode payloadValue) {
        auto& pb = *setup.PgmBuilder;
        auto varNode = pb.BlockVariant(payloadValue, NUdf::TStringRef("y"), ConvertToMinikqlType<TVariant>(pb));
        return pb.BlockGuess(varNode, NUdf::TStringRef("y"));
    });
}

} // Y_UNIT_TEST_SUITE(TMiniKQLBlockVariantNodeTest)

} // namespace NKikimr::NMiniKQL
