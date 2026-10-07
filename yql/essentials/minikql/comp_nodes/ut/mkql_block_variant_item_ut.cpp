#include <yql/essentials/minikql/comp_nodes/mkql_block_variant_item.h>

#include <yql/essentials/minikql/comp_nodes/ut/mkql_block_test_helper.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/utils/random_data_generator/random_data_generator.h>

#include <library/cpp/random_provider/random_provider.h>

#include <tuple>
#include <variant>

namespace NKikimr::NMiniKQL {

using namespace NTest;

namespace {

constexpr size_t LargeStringLength = MaxBlockSizeInBytes / 10;

template <typename TInputData, typename TExpectedData>
void TestBlockVariantItem(const TVector<TInputData>& data, const TVector<TExpectedData>& expected) {
    TBlockHelper().TestKernelFuzzied(data, expected, [](TSetup<false>& setup, TRuntimeNode variantValue) {
        return setup.PgmBuilder->BlockVariantItem(variantValue);
    });
}

template <typename... Ts>
auto ExtractCommonAlternative(const std::variant<Ts...>& variant) {
    return std::visit([](const auto& alternativeValue) { return alternativeValue; }, variant);
}

template <typename TVariant>
TVector<decltype(ExtractCommonAlternative(std::declval<TVariant>()))> ExtractCommonAlternatives(const TVector<TVariant>& data) {
    TVector<decltype(ExtractCommonAlternative(std::declval<TVariant>()))> expected;
    expected.reserve(data.size());
    for (const auto& variant : data) {
        expected.push_back(ExtractCommonAlternative(variant));
    }
    return expected;
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockVariantItemTest) {

Y_UNIT_TEST(TupleVariant_Ui32_TwoAlternatives) {
    using TVariant = std::variant<ui32, ui32>;
    TVector<TVariant> data = {
        TVariant(std::in_place_index<0>, 1U),
        TVariant(std::in_place_index<1>, 2U),
        TVariant(std::in_place_index<0>, 3U),
    };
    TVector<ui32> expected = {1U, 2U, 3U};
    TestBlockVariantItem(data, expected);
}

Y_UNIT_TEST(TupleVariant_Ui32_ThreeAlternatives) {
    using TVariant = std::variant<ui32, ui32, ui32>;
    TVector<TVariant> data = {
        TVariant(std::in_place_index<0>, 1U),
        TVariant(std::in_place_index<1>, 2U),
        TVariant(std::in_place_index<2>, 3U),
        TVariant(std::in_place_index<0>, 4U),
    };
    TVector<ui32> expected = {1U, 2U, 3U, 4U};
    TestBlockVariantItem(data, expected);
}

Y_UNIT_TEST(TupleVariant_TString_TwoAlternatives) {
    using TVariant = std::variant<TString, TString>;
    TVector<TVariant> data = {
        TVariant(std::in_place_index<0>, TString{"hello"}),
        TVariant(std::in_place_index<1>, TString{"world"}),
        TVariant(std::in_place_index<0>, TString{""}),
    };
    TVector<TString> expected = {"hello", "world", ""};
    TestBlockVariantItem(data, expected);
}

Y_UNIT_TEST(TupleVariant_NestedTuple_TwoAlternatives) {
    using TInner = std::tuple<ui32, TString>;
    using TVariant = std::variant<TInner, TInner>;
    TVector<TVariant> data = {
        TVariant(std::in_place_index<0>, TInner{1U, "a"}),
        TVariant(std::in_place_index<1>, TInner{2U, "b"}),
        TVariant(std::in_place_index<0>, TInner{3U, "c"}),
    };
    TVector<TInner> expected = {TInner{1U, "a"}, TInner{2U, "b"}, TInner{3U, "c"}};
    TestBlockVariantItem(data, expected);
}

Y_UNIT_TEST(TupleVariant_NestedVariant_TwoAlternatives) {
    using TInner = std::variant<ui32, ui32>;
    using TVariant = std::variant<TInner, TInner>;
    TVector<TVariant> data = {
        TVariant(std::in_place_index<0>, TInner(std::in_place_index<0>, 1U)),
        TVariant(std::in_place_index<1>, TInner(std::in_place_index<1>, 2U)),
        TVariant(std::in_place_index<0>, TInner(std::in_place_index<1>, 3U)),
    };
    TVector<TInner> expected = ExtractCommonAlternatives(data);
    TestBlockVariantItem(data, expected);
}

Y_UNIT_TEST(TupleVariant_OptionalInnerUi32_TwoAlternatives) {
    using TInner = TMaybe<ui32>;
    using TVariant = std::variant<TInner, TInner>;
    TVector<TVariant> data = {
        TVariant(std::in_place_index<0>, TInner{10U}),
        TVariant(std::in_place_index<1>, TInner{}),
        TVariant(std::in_place_index<0>, TInner{20U}),
    };
    TVector<TMaybe<ui32>> expected = {TInner{10U}, TInner{}, TInner{20U}};
    TestBlockVariantItem(data, expected);
}

Y_UNIT_TEST(TupleVariant_OptionalInnerString_TwoAlternatives) {
    using TInner = TMaybe<TString>;
    using TVariant = std::variant<TInner, TInner>;
    TVector<TVariant> data = {
        TVariant(std::in_place_index<0>, TInner{"first"}),
        TVariant(std::in_place_index<1>, TInner{}),
        TVariant(std::in_place_index<1>, TInner{"second"}),
    };
    TVector<TMaybe<TString>> expected = {TInner{"first"}, TInner{}, TInner{"second"}};
    TestBlockVariantItem(data, expected);
}

Y_UNIT_TEST(OptionalTupleVariant_Ui32_NullRows) {
    using TVariant = std::variant<ui32, ui32>;
    TVector<TMaybe<TVariant>> data = {
        TMaybe<TVariant>{TVariant(std::in_place_index<0>, 1U)},
        TMaybe<TVariant>{},
        TMaybe<TVariant>{TVariant(std::in_place_index<1>, 3U)},
    };
    TVector<TMaybe<ui32>> expected = {ui32{1U}, Nothing(), ui32{3U}};
    TestBlockVariantItem(data, expected);
}

Y_UNIT_TEST(OptionalTupleVariant_TString_MixedNull) {
    using TVariant = std::variant<TString, TString>;
    TVector<TMaybe<TVariant>> data = {
        TMaybe<TVariant>{TVariant(std::in_place_index<0>, TString{"hello"})},
        TMaybe<TVariant>{},
        TMaybe<TVariant>{TVariant(std::in_place_index<1>, TString{"world"})},
        TMaybe<TVariant>{},
    };
    TVector<TMaybe<TString>> expected = {TString{"hello"}, Nothing(), TString{"world"}, Nothing()};
    TestBlockVariantItem(data, expected);
}

Y_UNIT_TEST(OptionalTupleVariant_NestedTuple_MixedNull) {
    using TInner = std::tuple<ui32, TString>;
    using TVariant = std::variant<TInner, TInner>;
    TVector<TMaybe<TVariant>> data = {
        TMaybe<TVariant>{TVariant(std::in_place_index<0>, TInner{1U, "a"})},
        TMaybe<TVariant>{},
        TMaybe<TVariant>{TVariant(std::in_place_index<1>, TInner{2U, "b"})},
    };
    TVector<TMaybe<TInner>> expected = {TInner{1U, "a"}, Nothing(), TInner{2U, "b"}};
    TestBlockVariantItem(data, expected);
}

Y_UNIT_TEST(OptionalTupleVariant_OptionalInnerUi32_MixedNull) {
    using TInner = TMaybe<ui32>;
    using TVariant = std::variant<TInner, TInner>;
    TVector<TMaybe<TVariant>> data = {
        TMaybe<TVariant>{TVariant(std::in_place_index<0>, TInner{10U})},
        TMaybe<TVariant>{TVariant(std::in_place_index<0>, TInner{})},
        TMaybe<TVariant>{},
        TMaybe<TVariant>{TVariant(std::in_place_index<1>, TInner{20U})},
        TMaybe<TVariant>{TVariant(std::in_place_index<1>, TInner{})},
    };
    TVector<TMaybe<TInner>> expected = {
        TMaybe<TInner>{TInner{10U}},
        TMaybe<TInner>{TInner{}},
        TMaybe<TInner>{},
        TMaybe<TInner>{TInner{20U}},
        TMaybe<TInner>{TInner{}},
    };
    TestBlockVariantItem(data, expected);
}

Y_UNIT_TEST(OptionalTupleVariant_Ui32_NoNull) {
    using TVariant = std::variant<ui32, ui32>;
    TVector<TMaybe<TVariant>> data = {
        TMaybe<TVariant>{TVariant(std::in_place_index<0>, 1U)},
        TMaybe<TVariant>{TVariant(std::in_place_index<1>, 2U)},
        TMaybe<TVariant>{TVariant(std::in_place_index<0>, 3U)},
    };
    TVector<TMaybe<ui32>> expected = {ui32{1U}, ui32{2U}, ui32{3U}};
    TestBlockVariantItem(data, expected);
}

Y_UNIT_TEST(StructVariant_Ui32_TwoMembers) {
    using TMemberA = NTest::TStructMember<"a", ui32>;
    using TMemberB = NTest::TStructMember<"b", ui32>;
    using TVariant = NTest::TStructVariant<TMemberA, TMemberB>;

    TVector<TVariant> data = {TVariant{TMemberA{1}}, TVariant{TMemberB{2}}, TVariant{TMemberA{3}}};
    TVector<ui32> expected = {1U, 2U, 3U};
    TestBlockVariantItem(data, expected);
}

Y_UNIT_TEST(StructVariant_Ui32_ThreeMembers) {
    using TMemberA = NTest::TStructMember<"a", ui32>;
    using TMemberB = NTest::TStructMember<"b", ui32>;
    using TMemberC = NTest::TStructMember<"c", ui32>;
    using TVariant = NTest::TStructVariant<TMemberA, TMemberB, TMemberC>;

    TVector<TVariant> data = {TVariant{TMemberA{1}}, TVariant{TMemberB{2}}, TVariant{TMemberC{3}}, TVariant{TMemberA{4}}};
    TVector<ui32> expected = {1U, 2U, 3U, 4U};
    TestBlockVariantItem(data, expected);
}

Y_UNIT_TEST(StructVariant_TString_TwoMembers) {
    using TMemberA = NTest::TStructMember<"a", TString>;
    using TMemberB = NTest::TStructMember<"b", TString>;
    using TVariant = NTest::TStructVariant<TMemberA, TMemberB>;

    TVector<TVariant> data = {TVariant{TMemberA{"hello"}}, TVariant{TMemberB{"world"}}, TVariant{TMemberA{""}}};
    TVector<TString> expected = {"hello", "world", ""};
    TestBlockVariantItem(data, expected);
}

Y_UNIT_TEST(StructVariant_NestedTuple_TwoMembers) {
    using TInner = std::tuple<ui32, TString>;
    using TMemberA = NTest::TStructMember<"a", TInner>;
    using TMemberB = NTest::TStructMember<"b", TInner>;
    using TVariant = NTest::TStructVariant<TMemberA, TMemberB>;

    TVector<TVariant> data = {TVariant{TMemberA{TInner{1U, "a"}}}, TVariant{TMemberB{TInner{2U, "b"}}}};
    TVector<TInner> expected = {TInner{1U, "a"}, TInner{2U, "b"}};
    TestBlockVariantItem(data, expected);
}

Y_UNIT_TEST(OptionalStructVariant_Ui32_MixedNull) {
    using TMemberA = NTest::TStructMember<"a", ui32>;
    using TMemberB = NTest::TStructMember<"b", ui32>;
    using TVariant = NTest::TStructVariant<TMemberA, TMemberB>;

    TVector<TMaybe<TVariant>> data = {
        TMaybe<TVariant>{TVariant{TMemberA{1}}},
        TMaybe<TVariant>{},
        TMaybe<TVariant>{TVariant{TMemberB{2}}},
    };
    TVector<TMaybe<ui32>> expected = {ui32{1U}, Nothing(), ui32{2U}};
    TestBlockVariantItem(data, expected);
}

Y_UNIT_TEST(OptionalStructVariant_TString_MixedNull) {
    using TMemberA = NTest::TStructMember<"a", TString>;
    using TMemberB = NTest::TStructMember<"b", TString>;
    using TVariant = NTest::TStructVariant<TMemberA, TMemberB>;

    TVector<TMaybe<TVariant>> data = {
        TMaybe<TVariant>{TVariant{TMemberA{"hello"}}},
        TMaybe<TVariant>{},
        TMaybe<TVariant>{TVariant{TMemberB{"world"}}},
    };
    TVector<TMaybe<TString>> expected = {TString{"hello"}, Nothing(), TString{"world"}};
    TestBlockVariantItem(data, expected);
}

Y_UNIT_TEST(OptionalStructVariant_OptionalInnerUi32_MixedNull) {
    using TInner = TMaybe<ui32>;
    using TMemberA = NTest::TStructMember<"a", TInner>;
    using TMemberB = NTest::TStructMember<"b", TInner>;
    using TVariant = NTest::TStructVariant<TMemberA, TMemberB>;

    TVector<TMaybe<TVariant>> data = {
        TMaybe<TVariant>{TVariant{TMemberA{TInner{10U}}}},
        TMaybe<TVariant>{TVariant{TMemberA{TInner{}}}},
        TMaybe<TVariant>{},
        TMaybe<TVariant>{TVariant{TMemberB{TInner{20U}}}},
        TMaybe<TVariant>{TVariant{TMemberB{TInner{}}}},
    };
    TVector<TMaybe<TInner>> expected = {
        TMaybe<TInner>{TInner{10U}},
        TMaybe<TInner>{TInner{}},
        TMaybe<TInner>{},
        TMaybe<TInner>{TInner{20U}},
        TMaybe<TInner>{TInner{}},
    };
    TestBlockVariantItem(data, expected);
}

Y_UNIT_TEST(OptionalStructVariant_Ui32_NoNull) {
    using TMemberA = NTest::TStructMember<"a", ui32>;
    using TMemberB = NTest::TStructMember<"b", ui32>;
    using TVariant = NTest::TStructVariant<TMemberA, TMemberB>;

    TVector<TMaybe<TVariant>> data = {
        TMaybe<TVariant>{TVariant{TMemberA{1}}},
        TMaybe<TVariant>{TVariant{TMemberB{2}}},
        TMaybe<TVariant>{TVariant{TMemberA{3}}},
    };
    TVector<TMaybe<ui32>> expected = {ui32{1U}, ui32{2U}, ui32{3U}};
    TestBlockVariantItem(data, expected);
}

Y_UNIT_TEST(TupleVariant_Ui32_Fuzzied) {
    using TVariant = std::variant<ui32, ui32>;
    auto rng = CreateDeterministicRandomProvider(200);
    const TVector<TVariant> data = NYql::GenerateRandomData<TVariant>(rng, 7);
    const TVector<ui32> expected = ExtractCommonAlternatives(data);
    TestBlockVariantItem(data, expected);
}

Y_UNIT_TEST(TupleVariant_LargeString_ChunkedOutput) {
    using TVariant = std::variant<TString, TString>;
    auto rng = CreateDeterministicRandomProvider(201);
    TBlockHelper helper;

    helper.WithScopedFuzzers([&]() {
        const NYql::TGeneratorSettings<TVariant> settings{
            .Weights = {1.0, 1.0},
            .InnerSettings = {
                NYql::TGeneratorSettings<TString>{.MinSize = 0, .MaxSize = LargeStringLength},
                NYql::TGeneratorSettings<TString>{.MinSize = 0, .MaxSize = LargeStringLength},
            },
        };
        const TVector<TVariant> data = NYql::GenerateRandomData<TVariant>(rng, settings, 50);
        const TVector<TString> expected = ExtractCommonAlternatives(data);

        helper.RunNodeOverWideStream(
            expected,
            [](TSetup<false>& setup, TRuntimeNode variantBlock) {
                return setup.PgmBuilder->BlockVariantItem(variantBlock);
            },
            data);
    });
}

} // Y_UNIT_TEST_SUITE(TMiniKQLBlockVariantItemTest)

} // namespace NKikimr::NMiniKQL
