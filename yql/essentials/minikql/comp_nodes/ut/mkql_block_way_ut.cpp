#include <yql/essentials/minikql/comp_nodes/mkql_block_way.h>

#include <yql/essentials/minikql/comp_nodes/ut/mkql_block_test_helper.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/utils/random_data_generator/random_data_generator.h>

#include <library/cpp/random_provider/random_provider.h>

#include <array>
#include <string_view>

namespace NKikimr::NMiniKQL {

using namespace NTest;

namespace {

template <typename TInputData, typename TExpectedData>
void TestBlockWay(const TVector<TInputData>& data, const TVector<TExpectedData>& expected) {
    TBlockHelper().TestKernelFuzzied(data, expected, [](TSetup<false>& setup, TRuntimeNode variantValue) {
        return setup.PgmBuilder->BlockWay(variantValue);
    });
}

template <char Fill, size_t Length>
using TRepeatedNameMember = NTest::TStructMember<NTest::TStructMemberName<Length + 1>(Fill), ui32>;

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockWayTest) {

Y_UNIT_TEST(TupleVariant_Ui32Ui64_Mixed) {
    using TVariant = std::variant<ui32, ui64>;
    TVector<TVariant> data = {TVariant{ui32{1}}, TVariant{ui64{2}}, TVariant{ui32{3}}};
    TVector<ui32> expected = {0u, 1u, 0u};
    TestBlockWay(data, expected);
}

Y_UNIT_TEST(TupleVariant_AllIndex0) {
    using TVariant = std::variant<ui32, ui64>;
    TVector<TVariant> data = {TVariant{ui32{10}}, TVariant{ui32{20}}, TVariant{ui32{30}}};
    TVector<ui32> expected = {0u, 0u, 0u};
    TestBlockWay(data, expected);
}

Y_UNIT_TEST(TupleVariant_AllIndex1) {
    using TVariant = std::variant<ui32, ui64>;
    TVector<TVariant> data = {TVariant{ui64{10}}, TVariant{ui64{20}}, TVariant{ui64{30}}};
    TVector<ui32> expected = {1u, 1u, 1u};
    TestBlockWay(data, expected);
}

Y_UNIT_TEST(TupleVariant_StringAlternative) {
    using TVariant = std::variant<ui32, TString>;
    TVector<TVariant> data = {TVariant{ui32{42}}, TVariant{TString{"hello"}}, TVariant{ui32{7}}};
    TVector<ui32> expected = {0u, 1u, 0u};
    TestBlockWay(data, expected);
}

Y_UNIT_TEST(TupleVariant_InnerOptional) {
    using TVariant = std::variant<TMaybe<ui32>, ui64>;
    TVector<TVariant> data = {TVariant{TMaybe<ui32>{10u}}, TVariant{ui64{20u}}, TVariant{TMaybe<ui32>{}}};
    TVector<ui32> expected = {0u, 1u, 0u};
    TestBlockWay(data, expected);
}

Y_UNIT_TEST(OptionalTupleVariant_MixedNull) {
    using TVariant = std::variant<ui32, ui64>;
    TVector<TMaybe<TVariant>> data = {TMaybe<TVariant>{TVariant{ui32{1}}}, TMaybe<TVariant>{}, TMaybe<TVariant>{TVariant{ui64{2}}}};
    TVector<TMaybe<ui32>> expected = {ui32{0}, Nothing(), ui32{1}};
    TestBlockWay(data, expected);
}

Y_UNIT_TEST(OptionalTupleVariant_AllNull) {
    using TVariant = std::variant<ui32, ui64>;
    TVector<TMaybe<TVariant>> data = {TMaybe<TVariant>{}, TMaybe<TVariant>{}, TMaybe<TVariant>{}};
    TVector<TMaybe<ui32>> expected = {Nothing(), Nothing(), Nothing()};
    TestBlockWay(data, expected);
}

Y_UNIT_TEST(OptionalTupleVariant_NoNull) {
    using TVariant = std::variant<ui32, ui64>;
    TVector<TMaybe<TVariant>> data = {TMaybe<TVariant>{TVariant{ui32{1}}}, TMaybe<TVariant>{TVariant{ui64{2}}}, TMaybe<TVariant>{TVariant{ui32{3}}}};
    TVector<TMaybe<ui32>> expected = {ui32{0}, ui32{1}, ui32{0}};
    TestBlockWay(data, expected);
}

Y_UNIT_TEST(StructVariant_NonOptional_MemberNames) {
    using TMemberA = NTest::TStructMember<"a", ui32>;
    using TMemberB = NTest::TStructMember<"b_name_longer_than_sixteen_bytes", ui64>;
    using TMemberC = NTest::TStructMember<"c", double>;
    using TVariant = NTest::TStructVariant<TMemberA, TMemberB, TMemberC>;

    TVector<TVariant> data = {TVariant{TMemberA{1}}, TVariant{TMemberB{2}}, TVariant{TMemberC{3}}, TVariant{TMemberA{4}}, TVariant{TMemberB{5}}};
    TVector<TUtf8> expected = {TUtf8("a"), TUtf8("b_name_longer_than_sixteen_bytes"), TUtf8("c"), TUtf8("a"), TUtf8("b_name_longer_than_sixteen_bytes")};

    TestBlockWay(data, expected);
}

Y_UNIT_TEST(StructVariant_Optional_MixedNull) {
    using TFirstMember = NTest::TStructMember<"x", ui32>;
    using TSecondMember = NTest::TStructMember<"y", ui64>;
    using TVariant = NTest::TStructVariant<TFirstMember, TSecondMember>;

    auto elem1 = TMaybe<TVariant>(TFirstMember{1});
    auto elem2 = Nothing();
    auto elem3 = TMaybe<TVariant>(TSecondMember{2});
    auto elem4 = TMaybe<TVariant>(TFirstMember{3});

    TVector<TMaybe<TVariant>> items = {elem1, elem2, elem3, elem4};

    TVector<TMaybe<TUtf8>> expected = {
        TMaybe<TUtf8>{"x"},
        TMaybe<TUtf8>{},
        TMaybe<TUtf8>{"y"},
        TMaybe<TUtf8>{"x"},
    };

    TestBlockWay(items, expected);
}

Y_UNIT_TEST(StructVariant_LongNames_ChunkedOutput) {
    constexpr size_t ItemsPerIteration = 10000;
    constexpr size_t NameLength = 100;
    static_assert(NameLength * ItemsPerIteration > MaxBlockSizeInBytes, "Output must overflow a single block");

    using TVariant = NTest::TStructVariant<
        TRepeatedNameMember<'a', NameLength>,
        TRepeatedNameMember<'b', NameLength>,
        TRepeatedNameMember<'c', NameLength>>;

    auto rng = CreateDeterministicRandomProvider(7);
    TBlockHelper helper;

    helper.WithScopedFuzzers([&] {
        size_t randomSize = rng->GenRand() % ItemsPerIteration;
        const TVector<TVariant> data = NYql::GenerateRandomData<TVariant>(rng, randomSize);

        TVector<TUtf8> expected;
        expected.reserve(data.size());
        for (const auto& value : data) {
            expected.emplace_back(value.Name());
        }

        helper.RunNodeOverWideStream(
            expected,
            [](TSetup<false>& setup, TRuntimeNode variantBlock) {
                return setup.PgmBuilder->BlockWay(variantBlock);
            },
            data);
    });
}

Y_UNIT_TEST(StructVariant_LongNames_ChunkedOutput_Optional) {
    constexpr size_t ItemsPerIteration = 10000;
    constexpr size_t NameLength = 100;
    static_assert(NameLength * ItemsPerIteration > MaxBlockSizeInBytes, "Output must overflow a single block");

    using TVariant = TMaybe<
        NTest::TStructVariant<
            TRepeatedNameMember<'a', NameLength>,
            TRepeatedNameMember<'b', NameLength>,
            TRepeatedNameMember<'c', NameLength>>>;

    auto rng = CreateDeterministicRandomProvider(7);
    TBlockHelper helper;

    helper.WithScopedFuzzers([&] {
        size_t randomSize = rng->GenRand() % ItemsPerIteration;
        const TVector<TVariant> data = NYql::GenerateRandomData<TVariant>(rng, randomSize);

        TVector<TMaybe<TUtf8>> expected;
        expected.reserve(data.size());
        for (const auto& value : data) {
            if (value) {
                expected.emplace_back(value->Name());
            } else {
                expected.emplace_back(Nothing());
            }
        }

        helper.RunNodeOverWideStream(
            expected,
            [](TSetup<false>& setup, TRuntimeNode variantBlock) {
                return setup.PgmBuilder->BlockWay(variantBlock);
            },
            data);
    });
}

Y_UNIT_TEST(TupleVariant_Scalar) {
    using TVariant = std::variant<ui32, ui64>;

    TBlockHelper().RunNodeOverWideStream(
        ui32{1},
        [](TSetup<false>& setup, TRuntimeNode variantBlock) {
            return setup.PgmBuilder->BlockWay(variantBlock);
        },
        TVariant{ui64{42}});
}

Y_UNIT_TEST(StructVariant_Scalar) {
    using TMemberAlpha = NTest::TStructMember<"alpha", ui32>;
    using TMemberBeta = NTest::TStructMember<"beta_member_longer_than_sixteen_bytes", ui64>;
    using TVariant = NTest::TStructVariant<TMemberAlpha, TMemberBeta>;

    TBlockHelper().RunNodeOverWideStream(
        TUtf8(TString("beta_member_longer_than_sixteen_bytes")),
        [](TSetup<false>& setup, TRuntimeNode variantBlock) {
            return setup.PgmBuilder->BlockWay(variantBlock);
        },
        TVariant{TMemberBeta{ui64{7}}});
}

} // Y_UNIT_TEST_SUITE(TMiniKQLBlockWayTest)

} // namespace NKikimr::NMiniKQL
