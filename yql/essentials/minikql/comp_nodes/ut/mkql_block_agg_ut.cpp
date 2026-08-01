#include <yql/essentials/minikql/comp_nodes/ut/mkql_block_test_helper.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr::NMiniKQL {

namespace {

TType* MakeHashedGroupByReturnType(TProgramBuilder& pb, TRuntimeNode wideStream, const TVector<ui32>& keys) {
    auto items = GetWideComponents(AS_TYPE(TStreamType, wideStream.GetStaticType()));
    TVector<TType*> returnItems;
    for (ui32 key : keys) {
        auto* itemType = AS_TYPE(TBlockType, items[key])->GetItemType();
        returnItems.push_back(pb.NewBlockType(itemType, TBlockType::EShape::Many));
    }
    returnItems.push_back(items.back());
    return pb.NewStreamType(pb.NewMultiType(returnItems));
}

TRuntimeNode CombineHashedByKeys(TSetup<false>& setup, TRuntimeNode fuzzedWideStream, TVector<ui32> keys) {
    TProgramBuilder& pb = *setup.PgmBuilder;
    return pb.BlockCombineHashed(fuzzedWideStream, {}, keys, {}, MakeHashedGroupByReturnType(pb, fuzzedWideStream, keys));
}

template <typename... TExpected, typename... TInputs>
void RunHashedGroupByTest(const std::tuple<TVector<TExpected>...>& expected,
                          const std::tuple<TInputs...>& input, TVector<ui32> keys) {
    TFuzzOptions fuzzOptions;
    fuzzOptions.FuzzChunked = true;
    TBlockHelper helper(NYql::EDatumValidationMode::Expensive, fuzzOptions);
    helper.WithScopedFuzzers([&] {
        helper.RunWideStreamNode(
            expected,
            [keys](TSetup<false>& setup, TRuntimeNode fuzzedWideStream) {
                return CombineHashedByKeys(setup, fuzzedWideStream, keys);
            },
            /*unordered=*/true,
            input);
    });
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockAggTest) {

Y_UNIT_TEST(GroupByNullableStringKey) {
    TVector<TMaybe<TString>> key = {
        TMaybe<TString>{},
        TMaybe<TString>("truck"),
        TMaybe<TString>{},
        TMaybe<TString>("truck"),
    };

    TVector<TMaybe<TString>> expected = {
        TMaybe<TString>{},
        TMaybe<TString>("truck"),
    };

    RunHashedGroupByTest(std::make_tuple(expected), std::make_tuple(key), {0});
}

Y_UNIT_TEST(GroupByArrayScalarArrayKeys) {
    TVector<TMaybe<TString>> key0 = {
        TMaybe<TString>{},
        TMaybe<TString>("truck"),
        TMaybe<TString>{},
        TMaybe<TString>("truck"),
    };
    TVector<ui32> key2 = {10u, 10u, 20u, 20u};

    TVector<TMaybe<TString>> expectedKey0 = {
        TMaybe<TString>{},
        TMaybe<TString>("truck"),
        TMaybe<TString>{},
        TMaybe<TString>("truck"),
    };
    TVector<TString> expectedKey1 = {"berlin", "berlin", "berlin", "berlin"};
    TVector<ui32> expectedKey2 = {10u, 10u, 20u, 20u};

    RunHashedGroupByTest(std::make_tuple(expectedKey0, expectedKey1, expectedKey2),
                         std::make_tuple(key0, TString("berlin"), key2), {0, 1, 2});
}

Y_UNIT_TEST(GroupByAllScalarKeys) {
    TVector<TMaybe<TString>> expectedKey0 = {TMaybe<TString>("truck")};
    TVector<ui32> expectedKey1 = {42u};

    RunHashedGroupByTest(std::make_tuple(expectedKey0, expectedKey1),
                         std::make_tuple(TMaybe<TString>("truck"), 42u), {0, 1});
}

} // Y_UNIT_TEST_SUITE(TMiniKQLBlockAggTest)

} // namespace NKikimr::NMiniKQL
