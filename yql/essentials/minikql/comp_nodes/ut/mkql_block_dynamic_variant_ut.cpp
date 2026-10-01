#include <yql/essentials/minikql/comp_nodes/mkql_block_dynamic_variant.h>

#include <yql/essentials/minikql/comp_nodes/ut/mkql_block_test_helper.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/utils/random_data_generator/random_data_generator.h>

#include <library/cpp/random_provider/random_provider.h>

#include <variant>

namespace NKikimr::NMiniKQL {

using namespace NTest;

namespace {

constexpr size_t LargeStringLength = MaxBlockSizeInBytes / 10;

// All alternatives of a dynamic variant must share the exact same payload type, so a
// tuple-backed variant with N alternatives is naturally modelled as a std::variant with
// the same C++ type repeated N times (indexed access, not type-based, so duplicates are fine).
using TVar1 = std::variant<ui32>;
using TVar3 = std::variant<ui32, ui32, ui32>;

using TXMember = NTest::TStructMember<"x", ui32>;
using TYMember = NTest::TStructMember<"y", ui32>;
using TOnlyMember = NTest::TStructMember<"only", ui32>;
using TVarXY = TStructVariant<TXMember, TYMember>;
using TVarOnly = TStructVariant<TOnlyMember>;

// Alternatives whose shared payload is itself Optional<ui32>. This exercises the
// distinct code path where a resolved (non-null result) row can still carry a null
// inner payload, as opposed to the result-level Optional<Variant<...>> nullability.
using TTupleVariantOptionalPayload = std::variant<TMaybe<ui32>, TMaybe<ui32>, TMaybe<ui32>>;

using TXMemberOptionalPayload = NTest::TStructMember<"x", TMaybe<ui32>>;
using TYMemberOptionalPayload = NTest::TStructMember<"y", TMaybe<ui32>>;
using TStructVariantOptionalPayload = TStructVariant<TXMemberOptionalPayload, TYMemberOptionalPayload>;

// Payload that is itself a variant (dense union). The dynamic variant then nests a dense union inside a
// dense union: the outer alternatives all share the same inner-variant payload type.
using TInnerTupleVariant = std::variant<ui32, ui32>;
using TNestedTupleVariant = std::variant<TInnerTupleVariant, TInnerTupleVariant, TInnerTupleVariant>;

using TInnerAMember = NTest::TStructMember<"a", ui32>;
using TInnerBMember = NTest::TStructMember<"b", ui32>;
using TInnerStructVariant = TStructVariant<TInnerAMember, TInnerBMember>;
using TInnerStructMember = NTest::TStructMember<"inner", TInnerStructVariant>;
using TOtherStructMember = NTest::TStructMember<"other", TInnerStructVariant>;
using TNestedStructVariant = TStructVariant<TInnerStructMember, TOtherStructMember>;

template <typename TVariant, typename T, typename U, typename V>
void TestDynamicVariant(const T& payload, const U& index, const V& expected) {
    TBlockHelper().TestKernelFuzzied(payload, index, expected,
                                     [](TSetup<false>& setup, TRuntimeNode payloadValue, TRuntimeNode indexValue) {
                                         auto& pb = *setup.PgmBuilder;
                                         return pb.BlockDynamicVariant(payloadValue, indexValue, ConvertToMinikqlType<TVariant>(pb));
                                     });
}

template <typename TVariant, typename T, typename U, typename V>
void TestDynamicVariantItemRoundtrip(const T& payload, const U& index, const V& expected) {
    TBlockHelper().TestKernelFuzzied(payload, index, expected,
                                     [](TSetup<false>& setup, TRuntimeNode payloadValue, TRuntimeNode indexValue) {
                                         auto& pb = *setup.PgmBuilder;
                                         auto variant = pb.BlockDynamicVariant(payloadValue, indexValue, ConvertToMinikqlType<TVariant>(pb));
                                         return pb.BlockVariantItem(variant);
                                     });
}

template <typename TVariant, typename T, typename U, typename V>
void TestDynamicVariantWayRoundtrip(const T& payload, const U& index, const V& expected) {
    TBlockHelper().TestKernelFuzzied(payload, index, expected,
                                     [](TSetup<false>& setup, TRuntimeNode payloadValue, TRuntimeNode indexValue) {
                                         auto& pb = *setup.PgmBuilder;
                                         auto variant = pb.BlockDynamicVariant(payloadValue, indexValue, ConvertToMinikqlType<TVariant>(pb));
                                         return pb.BlockWay(variant);
                                     });
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockDynamicVariantTest) {

Y_UNIT_TEST(TupleVariant_VectorPayload_VectorIndex_AllAlternatives) {
    TVector<ui32> payload = {10U, 20U, 30U};
    TVector<ui32> index = {0U, 1U, 2U};
    TVector<TMaybe<TVar3>> expected = {
        TMaybe<TVar3>{TVar3{std::in_place_index<0>, 10U}},
        TMaybe<TVar3>{TVar3{std::in_place_index<1>, 20U}},
        TMaybe<TVar3>{TVar3{std::in_place_index<2>, 30U}},
    };
    TestDynamicVariant<TVar3>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariant_VectorPayload_ScalarIndex) {
    TVector<ui32> payload = {10U, 20U, 30U};
    ui32 index = 1U;
    TVector<TMaybe<TVar3>> expected = {
        TMaybe<TVar3>{TVar3{std::in_place_index<1>, 10U}},
        TMaybe<TVar3>{TVar3{std::in_place_index<1>, 20U}},
        TMaybe<TVar3>{TVar3{std::in_place_index<1>, 30U}},
    };
    TestDynamicVariant<TVar3>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariant_ScalarPayload_VectorIndex) {
    ui32 payload = 99U;
    TVector<ui32> index = {0U, 1U, 2U};
    TVector<TMaybe<TVar3>> expected = {
        TMaybe<TVar3>{TVar3{std::in_place_index<0>, 99U}},
        TMaybe<TVar3>{TVar3{std::in_place_index<1>, 99U}},
        TMaybe<TVar3>{TVar3{std::in_place_index<2>, 99U}},
    };
    TestDynamicVariant<TVar3>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariant_ScalarPayload_ScalarIndex) {
    ui32 payload = 7U;
    ui32 index = 2U;
    TMaybe<TVar3> expected = TMaybe<TVar3>{TVar3{std::in_place_index<2>, 7U}};
    TestDynamicVariant<TVar3>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariant_OutOfRangeIndex_YieldsNull) {
    TVector<ui32> payload = {1U, 2U, 3U};
    TVector<ui32> index = {0U, 99U, 2U};
    TVector<TMaybe<TVar3>> expected = {
        TMaybe<TVar3>{TVar3{std::in_place_index<0>, 1U}},
        Nothing(),
        TMaybe<TVar3>{TVar3{std::in_place_index<2>, 3U}},
    };
    TestDynamicVariant<TVar3>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariant_NullIndex_YieldsNull) {
    TVector<ui32> payload = {1U, 2U, 3U};
    TVector<TMaybe<ui32>> index = {TMaybe<ui32>{0U}, Nothing(), TMaybe<ui32>{2U}};
    TVector<TMaybe<TVar3>> expected = {
        TMaybe<TVar3>{TVar3{std::in_place_index<0>, 1U}},
        Nothing(),
        TMaybe<TVar3>{TVar3{std::in_place_index<2>, 3U}},
    };
    TestDynamicVariant<TVar3>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariant_SingleAlternative) {
    TVector<ui32> payload = {5U, 6U, 7U};
    TVector<ui32> index = {0U, 0U, 1U}; // 1 is out of range for a single-alternative variant
    TVector<TMaybe<TVar1>> expected = {
        TMaybe<TVar1>{TVar1{std::in_place_index<0>, 5U}},
        TMaybe<TVar1>{TVar1{std::in_place_index<0>, 6U}},
        Nothing(),
    };
    TestDynamicVariant<TVar1>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariant_RoundtripViaVariantItem) {
    TVector<ui32> payload = {10U, 20U, 30U, 40U};
    TVector<ui32> index = {0U, 1U, 2U, 0U};
    TVector<TMaybe<ui32>> expected = {10U, 20U, 30U, 40U};
    TestDynamicVariantItemRoundtrip<TVar3>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariant_RoundtripViaVariantItem_WithInvalidIndex) {
    TVector<ui32> payload = {10U, 20U, 30U};
    TVector<ui32> index = {0U, 99U, 2U};
    TVector<TMaybe<ui32>> expected = {10U, Nothing(), 30U};
    TestDynamicVariantItemRoundtrip<TVar3>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariant_RoundtripViaWay) {
    TVector<ui32> payload = {1U, 2U, 3U};
    TVector<ui32> index = {0U, 1U, 2U};
    TVector<TMaybe<ui32>> expected = {0U, 1U, 2U};
    TestDynamicVariantWayRoundtrip<TVar3>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariant_RoundtripViaWay_WithNullIndex) {
    TVector<ui32> payload = {1U, 2U, 3U};
    TVector<TMaybe<ui32>> index = {TMaybe<ui32>{0U}, Nothing(), TMaybe<ui32>{2U}};
    TVector<TMaybe<ui32>> expected = {0U, Nothing(), 2U};
    TestDynamicVariantWayRoundtrip<TVar3>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariantOptionalPayload_VectorPayload_VectorIndex_AllFilledPayload) {
    TVector<TMaybe<ui32>> payload = {TMaybe<ui32>{10U}, TMaybe<ui32>{20U}, TMaybe<ui32>{30U}};
    TVector<ui32> index = {0U, 1U, 2U};
    TVector<TMaybe<TTupleVariantOptionalPayload>> expected = {
        TMaybe<TTupleVariantOptionalPayload>{TTupleVariantOptionalPayload{std::in_place_index<0>, TMaybe<ui32>{10U}}},
        TMaybe<TTupleVariantOptionalPayload>{TTupleVariantOptionalPayload{std::in_place_index<1>, TMaybe<ui32>{20U}}},
        TMaybe<TTupleVariantOptionalPayload>{TTupleVariantOptionalPayload{std::in_place_index<2>, TMaybe<ui32>{30U}}},
    };
    TestDynamicVariant<TTupleVariantOptionalPayload>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariantOptionalPayload_VectorPayload_VectorIndex_MixedNullPayload_WithOutOfRangeIndex) {
    TVector<TMaybe<ui32>> payload = {TMaybe<ui32>{10U}, Nothing(), TMaybe<ui32>{30U}};
    TVector<ui32> index = {0U, 99U, 2U};
    TVector<TMaybe<TTupleVariantOptionalPayload>> expected = {
        TMaybe<TTupleVariantOptionalPayload>{TTupleVariantOptionalPayload{std::in_place_index<0>, TMaybe<ui32>{10U}}},
        Nothing(),
        TMaybe<TTupleVariantOptionalPayload>{TTupleVariantOptionalPayload{std::in_place_index<2>, TMaybe<ui32>{30U}}},
    };
    TestDynamicVariant<TTupleVariantOptionalPayload>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariantOptionalPayload_VectorPayload_ScalarIndex_Valid) {
    TVector<TMaybe<ui32>> payload = {TMaybe<ui32>{10U}, Nothing(), TMaybe<ui32>{30U}};
    ui32 index = 1U;
    TVector<TMaybe<TTupleVariantOptionalPayload>> expected = {
        TMaybe<TTupleVariantOptionalPayload>{TTupleVariantOptionalPayload{std::in_place_index<1>, TMaybe<ui32>{10U}}},
        TMaybe<TTupleVariantOptionalPayload>{TTupleVariantOptionalPayload{std::in_place_index<1>, Nothing()}},
        TMaybe<TTupleVariantOptionalPayload>{TTupleVariantOptionalPayload{std::in_place_index<1>, TMaybe<ui32>{30U}}},
    };
    TestDynamicVariant<TTupleVariantOptionalPayload>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariantOptionalPayload_VectorPayload_ScalarIndex_NullIndex) {
    TVector<TMaybe<ui32>> payload = {TMaybe<ui32>{10U}, TMaybe<ui32>{20U}, TMaybe<ui32>{30U}};
    TMaybe<ui32> index = Nothing();
    TVector<TMaybe<TTupleVariantOptionalPayload>> expected = {Nothing(), Nothing(), Nothing()};
    TestDynamicVariant<TTupleVariantOptionalPayload>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariantOptionalPayload_ScalarPayload_VectorIndex_Valid) {
    TMaybe<ui32> payload = TMaybe<ui32>{99U};
    TVector<ui32> index = {0U, 1U, 2U};
    TVector<TMaybe<TTupleVariantOptionalPayload>> expected = {
        TMaybe<TTupleVariantOptionalPayload>{TTupleVariantOptionalPayload{std::in_place_index<0>, TMaybe<ui32>{99U}}},
        TMaybe<TTupleVariantOptionalPayload>{TTupleVariantOptionalPayload{std::in_place_index<1>, TMaybe<ui32>{99U}}},
        TMaybe<TTupleVariantOptionalPayload>{TTupleVariantOptionalPayload{std::in_place_index<2>, TMaybe<ui32>{99U}}},
    };
    TestDynamicVariant<TTupleVariantOptionalPayload>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariantOptionalPayload_ScalarPayload_VectorIndex_NullInnerPayload_WithOutOfRangeIndex) {
    TMaybe<ui32> payload = Nothing();
    TVector<ui32> index = {0U, 99U, 2U};
    TVector<TMaybe<TTupleVariantOptionalPayload>> expected = {
        TMaybe<TTupleVariantOptionalPayload>{TTupleVariantOptionalPayload{std::in_place_index<0>, Nothing()}},
        Nothing(),
        TMaybe<TTupleVariantOptionalPayload>{TTupleVariantOptionalPayload{std::in_place_index<2>, Nothing()}},
    };
    TestDynamicVariant<TTupleVariantOptionalPayload>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariantOptionalPayload_ScalarPayload_ScalarIndex_NullInnerPayload) {
    TMaybe<ui32> payload = Nothing();
    ui32 index = 2U;
    TMaybe<TTupleVariantOptionalPayload> expected = TMaybe<TTupleVariantOptionalPayload>{TTupleVariantOptionalPayload{std::in_place_index<2>, Nothing()}};
    TestDynamicVariant<TTupleVariantOptionalPayload>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariantOptionalPayload_ScalarPayload_ScalarIndex_OutOfRangeIndex) {
    TMaybe<ui32> payload = TMaybe<ui32>{7U};
    ui32 index = 99U;
    TMaybe<TTupleVariantOptionalPayload> expected = Nothing();
    TestDynamicVariant<TTupleVariantOptionalPayload>(payload, index, expected);
}

Y_UNIT_TEST(StructVariant_VectorPayload_VectorIndex_AllAlternatives) {
    TVector<ui32> payload = {10U, 20U, 30U};
    TVector<TUtf8> index = {TUtf8{"x"}, TUtf8{"y"}, TUtf8{"x"}};
    TVector<TMaybe<TVarXY>> expected = {
        TMaybe<TVarXY>{TVarXY(TXMember{10U})},
        TMaybe<TVarXY>{TVarXY(TYMember{20U})},
        TMaybe<TVarXY>{TVarXY(TXMember{30U})},
    };
    TestDynamicVariant<TVarXY>(payload, index, expected);
}

Y_UNIT_TEST(StructVariant_VectorPayload_ScalarIndex) {
    TVector<ui32> payload = {10U, 20U, 30U};
    TUtf8 index{"y"};
    TVector<TMaybe<TVarXY>> expected = {
        TMaybe<TVarXY>{TVarXY(TYMember{10U})},
        TMaybe<TVarXY>{TVarXY(TYMember{20U})},
        TMaybe<TVarXY>{TVarXY(TYMember{30U})},
    };
    TestDynamicVariant<TVarXY>(payload, index, expected);
}

Y_UNIT_TEST(StructVariant_ScalarPayload_VectorIndex) {
    ui32 payload = 42U;
    TVector<TUtf8> index = {TUtf8{"x"}, TUtf8{"y"}};
    TVector<TMaybe<TVarXY>> expected = {
        TMaybe<TVarXY>{TVarXY(TXMember{42U})},
        TMaybe<TVarXY>{TVarXY(TYMember{42U})},
    };
    TestDynamicVariant<TVarXY>(payload, index, expected);
}

Y_UNIT_TEST(StructVariant_ScalarPayload_ScalarIndex) {
    ui32 payload = 5U;
    TUtf8 index{"x"};
    TMaybe<TVarXY> expected = TMaybe<TVarXY>{TVarXY(TXMember{5U})};
    TestDynamicVariant<TVarXY>(payload, index, expected);
}

Y_UNIT_TEST(StructVariant_UnknownMember_YieldsNull) {
    TVector<ui32> payload = {1U, 2U, 3U};
    TVector<TUtf8> index = {TUtf8{"x"}, TUtf8{"unknown"}, TUtf8{"y"}};
    TVector<TMaybe<TVarXY>> expected = {
        TMaybe<TVarXY>{TVarXY(TXMember{1U})},
        Nothing(),
        TMaybe<TVarXY>{TVarXY(TYMember{3U})},
    };
    TestDynamicVariant<TVarXY>(payload, index, expected);
}

Y_UNIT_TEST(StructVariant_NullIndex_YieldsNull) {
    TVector<ui32> payload = {1U, 2U, 3U};
    TVector<TMaybe<TUtf8>> index = {TMaybe<TUtf8>{TUtf8{"x"}}, Nothing(), TMaybe<TUtf8>{TUtf8{"y"}}};
    TVector<TMaybe<TVarXY>> expected = {
        TMaybe<TVarXY>{TVarXY(TXMember{1U})},
        Nothing(),
        TMaybe<TVarXY>{TVarXY(TYMember{3U})},
    };
    TestDynamicVariant<TVarXY>(payload, index, expected);
}

Y_UNIT_TEST(StructVariant_SingleAlternative) {
    TVector<ui32> payload = {1U, 2U};
    TVector<TUtf8> index = {TUtf8{"only"}, TUtf8{"missing"}};
    TVector<TMaybe<TVarOnly>> expected = {
        TMaybe<TVarOnly>{TVarOnly(TOnlyMember{1U})},
        Nothing(),
    };
    TestDynamicVariant<TVarOnly>(payload, index, expected);
}

Y_UNIT_TEST(StructVariant_RoundtripViaVariantItem) {
    TVector<ui32> payload = {10U, 20U, 30U};
    TVector<TUtf8> index = {TUtf8{"x"}, TUtf8{"y"}, TUtf8{"x"}};
    TVector<TMaybe<ui32>> expected = {10U, 20U, 30U};
    TestDynamicVariantItemRoundtrip<TVarXY>(payload, index, expected);
}

Y_UNIT_TEST(StructVariant_RoundtripViaWay) {
    TVector<ui32> payload = {10U, 20U, 30U};
    TVector<TUtf8> index = {TUtf8{"x"}, TUtf8{"y"}, TUtf8{"x"}};
    TVector<TMaybe<TUtf8>> expected = {TUtf8{"x"}, TUtf8{"y"}, TUtf8{"x"}};
    TestDynamicVariantWayRoundtrip<TVarXY>(payload, index, expected);
}

Y_UNIT_TEST(StructVariantOptionalPayload_VectorPayload_VectorIndex_AllFilledPayload) {
    TVector<TMaybe<ui32>> payload = {TMaybe<ui32>{10U}, TMaybe<ui32>{20U}, TMaybe<ui32>{30U}};
    TVector<TUtf8> index = {TUtf8{"x"}, TUtf8{"y"}, TUtf8{"x"}};
    TVector<TMaybe<TStructVariantOptionalPayload>> expected = {
        TMaybe<TStructVariantOptionalPayload>{TStructVariantOptionalPayload(TXMemberOptionalPayload{TMaybe<ui32>{10U}})},
        TMaybe<TStructVariantOptionalPayload>{TStructVariantOptionalPayload(TYMemberOptionalPayload{TMaybe<ui32>{20U}})},
        TMaybe<TStructVariantOptionalPayload>{TStructVariantOptionalPayload(TXMemberOptionalPayload{TMaybe<ui32>{30U}})},
    };
    TestDynamicVariant<TStructVariantOptionalPayload>(payload, index, expected);
}

Y_UNIT_TEST(StructVariantOptionalPayload_VectorPayload_VectorIndex_MixedNullPayload_WithUnknownMember) {
    TVector<TMaybe<ui32>> payload = {TMaybe<ui32>{1U}, Nothing(), TMaybe<ui32>{3U}};
    TVector<TUtf8> index = {TUtf8{"x"}, TUtf8{"unknown"}, TUtf8{"y"}};
    TVector<TMaybe<TStructVariantOptionalPayload>> expected = {
        TMaybe<TStructVariantOptionalPayload>{TStructVariantOptionalPayload(TXMemberOptionalPayload{TMaybe<ui32>{1U}})},
        Nothing(),
        TMaybe<TStructVariantOptionalPayload>{TStructVariantOptionalPayload(TYMemberOptionalPayload{TMaybe<ui32>{3U}})},
    };
    TestDynamicVariant<TStructVariantOptionalPayload>(payload, index, expected);
}

Y_UNIT_TEST(StructVariantOptionalPayload_VectorPayload_ScalarIndex_Valid) {
    TVector<TMaybe<ui32>> payload = {TMaybe<ui32>{10U}, Nothing(), TMaybe<ui32>{30U}};
    TUtf8 index{"y"};
    TVector<TMaybe<TStructVariantOptionalPayload>> expected = {
        TMaybe<TStructVariantOptionalPayload>{TStructVariantOptionalPayload(TYMemberOptionalPayload{TMaybe<ui32>{10U}})},
        TMaybe<TStructVariantOptionalPayload>{TStructVariantOptionalPayload(TYMemberOptionalPayload{Nothing()})},
        TMaybe<TStructVariantOptionalPayload>{TStructVariantOptionalPayload(TYMemberOptionalPayload{TMaybe<ui32>{30U}})},
    };
    TestDynamicVariant<TStructVariantOptionalPayload>(payload, index, expected);
}

Y_UNIT_TEST(StructVariantOptionalPayload_VectorPayload_ScalarIndex_NullIndex) {
    TVector<TMaybe<ui32>> payload = {TMaybe<ui32>{10U}, TMaybe<ui32>{20U}, TMaybe<ui32>{30U}};
    TMaybe<TUtf8> index = Nothing();
    TVector<TMaybe<TStructVariantOptionalPayload>> expected = {Nothing(), Nothing(), Nothing()};
    TestDynamicVariant<TStructVariantOptionalPayload>(payload, index, expected);
}

Y_UNIT_TEST(StructVariantOptionalPayload_ScalarPayload_VectorIndex_Valid) {
    TMaybe<ui32> payload = TMaybe<ui32>{42U};
    TVector<TUtf8> index = {TUtf8{"x"}, TUtf8{"y"}};
    TVector<TMaybe<TStructVariantOptionalPayload>> expected = {
        TMaybe<TStructVariantOptionalPayload>{TStructVariantOptionalPayload(TXMemberOptionalPayload{TMaybe<ui32>{42U}})},
        TMaybe<TStructVariantOptionalPayload>{TStructVariantOptionalPayload(TYMemberOptionalPayload{TMaybe<ui32>{42U}})},
    };
    TestDynamicVariant<TStructVariantOptionalPayload>(payload, index, expected);
}

Y_UNIT_TEST(StructVariantOptionalPayload_ScalarPayload_VectorIndex_NullInnerPayload_WithUnknownMember) {
    TMaybe<ui32> payload = Nothing();
    TVector<TUtf8> index = {TUtf8{"x"}, TUtf8{"unknown"}, TUtf8{"y"}};
    TVector<TMaybe<TStructVariantOptionalPayload>> expected = {
        TMaybe<TStructVariantOptionalPayload>{TStructVariantOptionalPayload(TXMemberOptionalPayload{Nothing()})},
        Nothing(),
        TMaybe<TStructVariantOptionalPayload>{TStructVariantOptionalPayload(TYMemberOptionalPayload{Nothing()})},
    };
    TestDynamicVariant<TStructVariantOptionalPayload>(payload, index, expected);
}

Y_UNIT_TEST(StructVariantOptionalPayload_ScalarPayload_ScalarIndex_NullInnerPayload) {
    TMaybe<ui32> payload = Nothing();
    TUtf8 index{"y"};
    TMaybe<TStructVariantOptionalPayload> expected = TMaybe<TStructVariantOptionalPayload>{TStructVariantOptionalPayload(TYMemberOptionalPayload{Nothing()})};
    TestDynamicVariant<TStructVariantOptionalPayload>(payload, index, expected);
}

Y_UNIT_TEST(StructVariantOptionalPayload_ScalarPayload_ScalarIndex_UnknownMember) {
    TMaybe<ui32> payload = TMaybe<ui32>{5U};
    TUtf8 index{"unknown"};
    TMaybe<TStructVariantOptionalPayload> expected = Nothing();
    TestDynamicVariant<TStructVariantOptionalPayload>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariant_VectorPayload_VectorIndex_AllOutOfRange) {
    TVector<ui32> payload = {1U, 2U, 3U};
    TVector<ui32> index = {99U, 100U, 101U};
    TVector<TMaybe<TVar3>> expected = {Nothing(), Nothing(), Nothing()};
    TestDynamicVariant<TVar3>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariant_VectorPayload_VectorIndex_AllNullIndex) {
    TVector<ui32> payload = {1U, 2U, 3U};
    TVector<TMaybe<ui32>> index = {Nothing(), Nothing(), Nothing()};
    TVector<TMaybe<TVar3>> expected = {Nothing(), Nothing(), Nothing()};
    TestDynamicVariant<TVar3>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariant_VectorPayload_ScalarIndex_OutOfRange) {
    TVector<ui32> payload = {1U, 2U, 3U};
    ui32 index = 99U;
    TVector<TMaybe<TVar3>> expected = {Nothing(), Nothing(), Nothing()};
    TestDynamicVariant<TVar3>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariant_VectorPayload_VectorIndex_SingleAlternativeReused) {
    TVector<ui32> payload = {10U, 20U, 30U, 40U};
    TVector<ui32> index = {1U, 1U, 1U, 1U}; // only alternative 1 is used; 0 and 2 stay empty.
    TVector<TMaybe<TVar3>> expected = {
        TMaybe<TVar3>{TVar3{std::in_place_index<1>, 10U}},
        TMaybe<TVar3>{TVar3{std::in_place_index<1>, 20U}},
        TMaybe<TVar3>{TVar3{std::in_place_index<1>, 30U}},
        TMaybe<TVar3>{TVar3{std::in_place_index<1>, 40U}},
    };
    TestDynamicVariant<TVar3>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariant_VectorPayload_VectorIndex_InterleavedAlternativesWithNulls) {
    TVector<ui32> payload = {10U, 20U, 30U, 40U, 50U, 60U};
    TVector<TMaybe<ui32>> index = {TMaybe<ui32>{0U}, TMaybe<ui32>{2U}, Nothing(),
                                   TMaybe<ui32>{0U}, TMaybe<ui32>{99U}, TMaybe<ui32>{2U}};
    TVector<TMaybe<TVar3>> expected = {
        TMaybe<TVar3>{TVar3{std::in_place_index<0>, 10U}},
        TMaybe<TVar3>{TVar3{std::in_place_index<2>, 20U}},
        Nothing(),
        TMaybe<TVar3>{TVar3{std::in_place_index<0>, 40U}},
        Nothing(),
        TMaybe<TVar3>{TVar3{std::in_place_index<2>, 60U}},
    };
    TestDynamicVariant<TVar3>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariant_ScalarPayload_VectorIndex_RepeatedAlternativeWithNulls) {
    ui32 payload = 7U;
    TVector<TMaybe<ui32>> index = {TMaybe<ui32>{1U}, Nothing(), TMaybe<ui32>{1U}, TMaybe<ui32>{99U}, TMaybe<ui32>{1U}};
    TVector<TMaybe<TVar3>> expected = {
        TMaybe<TVar3>{TVar3{std::in_place_index<1>, 7U}},
        Nothing(),
        TMaybe<TVar3>{TVar3{std::in_place_index<1>, 7U}},
        Nothing(),
        TMaybe<TVar3>{TVar3{std::in_place_index<1>, 7U}},
    };
    TestDynamicVariant<TVar3>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariantOptionalPayload_VectorPayload_VectorIndex_NothingFilled) {
    TVector<TMaybe<ui32>> payload = {Nothing(), Nothing(), Nothing()};
    TVector<ui32> index = {0U, 1U, 2U};
    TVector<TMaybe<TTupleVariantOptionalPayload>> expected = {
        TMaybe<TTupleVariantOptionalPayload>{TTupleVariantOptionalPayload{std::in_place_index<0>, Nothing()}},
        TMaybe<TTupleVariantOptionalPayload>{TTupleVariantOptionalPayload{std::in_place_index<1>, Nothing()}},
        TMaybe<TTupleVariantOptionalPayload>{TTupleVariantOptionalPayload{std::in_place_index<2>, Nothing()}},
    };
    TestDynamicVariant<TTupleVariantOptionalPayload>(payload, index, expected);
}

Y_UNIT_TEST(StructVariant_VectorPayload_VectorIndex_AllUnknownMembers) {
    TVector<ui32> payload = {1U, 2U, 3U};
    TVector<TUtf8> index = {TUtf8{"a"}, TUtf8{"b"}, TUtf8{"c"}};
    TVector<TMaybe<TVarXY>> expected = {Nothing(), Nothing(), Nothing()};
    TestDynamicVariant<TVarXY>(payload, index, expected);
}

Y_UNIT_TEST(StructVariant_VectorPayload_VectorIndex_SingleAlternativeReused) {
    TVector<ui32> payload = {10U, 20U, 30U};
    TVector<TUtf8> index = {TUtf8{"y"}, TUtf8{"y"}, TUtf8{"y"}}; // only "y" (alt 1) used; "x" child stays empty.
    TVector<TMaybe<TVarXY>> expected = {
        TMaybe<TVarXY>{TVarXY(TYMember{10U})},
        TMaybe<TVarXY>{TVarXY(TYMember{20U})},
        TMaybe<TVarXY>{TVarXY(TYMember{30U})},
    };
    TestDynamicVariant<TVarXY>(payload, index, expected);
}

Y_UNIT_TEST(StructVariantOptionalPayload_VectorPayload_VectorIndex_NothingFilled) {
    TVector<TMaybe<ui32>> payload = {Nothing(), Nothing(), Nothing()};
    TVector<TUtf8> index = {TUtf8{"x"}, TUtf8{"y"}, TUtf8{"x"}};
    TVector<TMaybe<TStructVariantOptionalPayload>> expected = {
        TMaybe<TStructVariantOptionalPayload>{TStructVariantOptionalPayload(TXMemberOptionalPayload{Nothing()})},
        TMaybe<TStructVariantOptionalPayload>{TStructVariantOptionalPayload(TYMemberOptionalPayload{Nothing()})},
        TMaybe<TStructVariantOptionalPayload>{TStructVariantOptionalPayload(TXMemberOptionalPayload{Nothing()})},
    };
    TestDynamicVariant<TStructVariantOptionalPayload>(payload, index, expected);
}

Y_UNIT_TEST(NestedTupleVariant_VectorPayload_VectorIndex_AllAlternatives) {
    TVector<TInnerTupleVariant> payload = {
        TInnerTupleVariant{std::in_place_index<0>, 10U},
        TInnerTupleVariant{std::in_place_index<1>, 20U},
        TInnerTupleVariant{std::in_place_index<0>, 30U},
    };
    TVector<ui32> index = {0U, 1U, 2U};
    TVector<TMaybe<TNestedTupleVariant>> expected = {
        TMaybe<TNestedTupleVariant>{TNestedTupleVariant{std::in_place_index<0>, TInnerTupleVariant{std::in_place_index<0>, 10U}}},
        TMaybe<TNestedTupleVariant>{TNestedTupleVariant{std::in_place_index<1>, TInnerTupleVariant{std::in_place_index<1>, 20U}}},
        TMaybe<TNestedTupleVariant>{TNestedTupleVariant{std::in_place_index<2>, TInnerTupleVariant{std::in_place_index<0>, 30U}}},
    };
    TestDynamicVariant<TNestedTupleVariant>(payload, index, expected);
}

Y_UNIT_TEST(NestedTupleVariant_VectorPayload_VectorIndex_ReusedOuterAlternativeWithNulls) {
    TVector<TInnerTupleVariant> payload = {
        TInnerTupleVariant{std::in_place_index<0>, 10U},
        TInnerTupleVariant{std::in_place_index<1>, 20U},
        TInnerTupleVariant{std::in_place_index<1>, 30U},
        TInnerTupleVariant{std::in_place_index<0>, 40U},
    };
    TVector<TMaybe<ui32>> index = {TMaybe<ui32>{0U}, Nothing(), TMaybe<ui32>{0U}, TMaybe<ui32>{99U}};
    TVector<TMaybe<TNestedTupleVariant>> expected = {
        TMaybe<TNestedTupleVariant>{TNestedTupleVariant{std::in_place_index<0>, TInnerTupleVariant{std::in_place_index<0>, 10U}}},
        Nothing(),
        TMaybe<TNestedTupleVariant>{TNestedTupleVariant{std::in_place_index<0>, TInnerTupleVariant{std::in_place_index<1>, 30U}}},
        Nothing(),
    };
    TestDynamicVariant<TNestedTupleVariant>(payload, index, expected);
}

Y_UNIT_TEST(NestedTupleVariant_ScalarPayload_VectorIndex) {
    TInnerTupleVariant payload = TInnerTupleVariant{std::in_place_index<1>, 77U};
    TVector<ui32> index = {0U, 2U, 0U};
    TVector<TMaybe<TNestedTupleVariant>> expected = {
        TMaybe<TNestedTupleVariant>{TNestedTupleVariant{std::in_place_index<0>, TInnerTupleVariant{std::in_place_index<1>, 77U}}},
        TMaybe<TNestedTupleVariant>{TNestedTupleVariant{std::in_place_index<2>, TInnerTupleVariant{std::in_place_index<1>, 77U}}},
        TMaybe<TNestedTupleVariant>{TNestedTupleVariant{std::in_place_index<0>, TInnerTupleVariant{std::in_place_index<1>, 77U}}},
    };
    TestDynamicVariant<TNestedTupleVariant>(payload, index, expected);
}

Y_UNIT_TEST(NestedTupleVariant_ScalarPayload_ScalarIndex) {
    TInnerTupleVariant payload = TInnerTupleVariant{std::in_place_index<0>, 5U};
    ui32 index = 2U;
    TMaybe<TNestedTupleVariant> expected =
        TMaybe<TNestedTupleVariant>{TNestedTupleVariant{std::in_place_index<2>, TInnerTupleVariant{std::in_place_index<0>, 5U}}};
    TestDynamicVariant<TNestedTupleVariant>(payload, index, expected);
}

Y_UNIT_TEST(NestedStructVariant_VectorPayload_VectorIndex_WithUnknownMember) {
    TVector<TInnerStructVariant> payload = {
        TInnerStructVariant(TInnerAMember{10U}),
        TInnerStructVariant(TInnerBMember{20U}),
        TInnerStructVariant(TInnerAMember{30U}),
    };
    TVector<TUtf8> index = {TUtf8{"inner"}, TUtf8{"missing"}, TUtf8{"other"}};
    TVector<TMaybe<TNestedStructVariant>> expected = {
        TMaybe<TNestedStructVariant>{TNestedStructVariant(TInnerStructMember{TInnerStructVariant(TInnerAMember{10U})})},
        Nothing(),
        TMaybe<TNestedStructVariant>{TNestedStructVariant(TOtherStructMember{TInnerStructVariant(TInnerAMember{30U})})},
    };
    TestDynamicVariant<TNestedStructVariant>(payload, index, expected);
}

Y_UNIT_TEST(TupleVariant_LargeStringPayload_WideStream) {
    using TVariant = std::variant<TString, TString>;
    auto rng = CreateDeterministicRandomProvider(202);
    TBlockHelper helper;

    helper.WithScopedFuzzers([&]() {
        constexpr size_t count = 50;
        const TVector<TString> payload = NYql::GenerateRandomData<TString>(
            rng, NYql::TGeneratorSettings<TString>{.MinSize = 0, .MaxSize = LargeStringLength}, count);
        const TVector<ui32> index = NYql::GenerateRandomData<ui32>(
            rng, NYql::TGeneratorSettings<ui32>{.Min = 0U, .Max = 2U}, count); // both codes are valid alternatives.

        TVector<TMaybe<TString>> expected;
        expected.reserve(count);
        for (const auto& value : payload) {
            expected.push_back(TMaybe<TString>{value});
        }

        helper.RunNodeOverWideStream(
            expected,
            [](TSetup<false>& setup, TRuntimeNode payloadBlock, TRuntimeNode indexBlock) {
                auto& pb = *setup.PgmBuilder;
                auto variant = pb.BlockDynamicVariant(payloadBlock, indexBlock, ConvertToMinikqlType<TVariant>(pb));
                return pb.BlockVariantItem(variant);
            },
            payload, index);
    });
}

} // Y_UNIT_TEST_SUITE(TMiniKQLBlockDynamicVariantTest)

} // namespace NKikimr::NMiniKQL
