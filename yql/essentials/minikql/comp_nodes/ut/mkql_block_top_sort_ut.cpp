#include "mkql_computation_node_ut.h"
#include <yql/essentials/minikql/mkql_runtime_version.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_block_test_helper.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_program_builder_test_utils.h>

#include <array>

namespace NKikimr::NMiniKQL {

namespace {

using TKeySpec = std::vector<std::pair<ui32, bool>>;

std::vector<std::pair<ui32, TRuntimeNode>> MakeKeyNodes(TProgramBuilder& pb, const TKeySpec& keys) {
    std::vector<std::pair<ui32, TRuntimeNode>> keyNodes;
    for (const auto& [index, ascending] : keys) {
        keyNodes.emplace_back(index, NTest::ConvertValueToLiteralNode(pb, ascending));
    }
    return keyNodes;
}

TRuntimeNode TopBlocksByKeys(TSetup<false>& setup, TRuntimeNode fuzzedWideStream, ui64 count, const TKeySpec& keys) {
    TProgramBuilder& pb = *setup.PgmBuilder;
    return pb.WideTopBlocks(fuzzedWideStream, NTest::ConvertValueToLiteralNode(pb, count), MakeKeyNodes(pb, keys));
}

TRuntimeNode TopSortBlocksByKeys(TSetup<false>& setup, TRuntimeNode fuzzedWideStream, ui64 count, const TKeySpec& keys) {
    TProgramBuilder& pb = *setup.PgmBuilder;
    return pb.WideTopSortBlocks(fuzzedWideStream, NTest::ConvertValueToLiteralNode(pb, count), MakeKeyNodes(pb, keys));
}

TRuntimeNode SortBlocksByKeys(TSetup<false>& setup, TRuntimeNode fuzzedWideStream, const TKeySpec& keys) {
    TProgramBuilder& pb = *setup.PgmBuilder;
    return pb.WideSortBlocks(fuzzedWideStream, MakeKeyNodes(pb, keys));
}

template <typename TStreamOp, typename... TExpected, typename... TInputs>
void RunTopOrSortTest(TStreamOp&& streamOp, bool unordered,
                      const std::tuple<TVector<TExpected>...>& expected,
                      const std::tuple<TInputs...>& input) {
    TBlockHelper helper;
    helper.WithScopedFuzzers([&] {
        helper.RunWideStreamNode(expected, streamOp, unordered, input);
    });
}

constexpr std::array<TStringBuf, 9> KeyColumnData = {
    "key one",
    "key two",
    "key two",
    "very long key one",
    "very long key two",
    "very long key two",
    "very long key two",
    "very long key two",
    "very long key two",
};
constexpr std::array<TStringBuf, 9> ValueColumnData = {
    "very long value 1",
    "very long value 2",
    "very long value 3",
    "very long value 4",
    "very long value 5",
    "very long value 6",
    "very long value 7",
    "very long value 8",
    "very long value 9",
};

TVector<TStringBuf> KeyColumn() {
    return TVector<TStringBuf>(KeyColumnData.begin(), KeyColumnData.end());
}

TVector<TStringBuf> ValueColumn() {
    return TVector<TStringBuf>(ValueColumnData.begin(), ValueColumnData.end());
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLWideTopBlocksTest) {

Y_UNIT_TEST(TopFirstKeyAscending) {
    TVector<TStringBuf> expectedKey = {"key one", "key two", "key two", "very long key one"};
    TVector<TStringBuf> expectedValue = {"very long value 1", "very long value 3", "very long value 2", "very long value 4"};

    RunTopOrSortTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return TopBlocksByKeys(setup, s, 4, {{0, true}}); },
        /*unordered=*/true,
        std::make_tuple(expectedKey, expectedValue), std::make_tuple(KeyColumn(), ValueColumn()));
}

Y_UNIT_TEST(TopSecondKeyDescending) {
    TVector<TStringBuf> expectedKey = {"very long key two", "very long key two"};
    TVector<TStringBuf> expectedValue = {"very long value 9", "very long value 8"};

    RunTopOrSortTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return TopBlocksByKeys(setup, s, 2, {{1, false}}); },
        /*unordered=*/true,
        std::make_tuple(expectedKey, expectedValue), std::make_tuple(KeyColumn(), ValueColumn()));
}

Y_UNIT_TEST(TopScalarPayloadArrayKey) {
    TVector<ui32> key = {3U, 1U, 2U};
    TString payload = "same";

    TVector<ui32> expectedKey = {1U, 2U};
    TVector<TString> expectedPayload = {"same", "same"};

    RunTopOrSortTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return TopBlocksByKeys(setup, s, 2, {{0, true}}); },
        /*unordered=*/true,
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

Y_UNIT_TEST(TopMultiKeyMixedDirections) {
    TVector<ui32> key0 = {1U, 1U, 1U, 2U};
    TVector<ui64> key1 = {30U, 10U, 20U, 5U};
    TVector<TString> payload = {"x", "y", "z", "w"};

    TVector<ui32> expectedKey0 = {1U, 1U};
    TVector<ui64> expectedKey1 = {30U, 20U};
    TVector<TString> expectedPayload = {"x", "z"};

    RunTopOrSortTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return TopBlocksByKeys(setup, s, 2, {{0, true}, {1, false}}); },
        /*unordered=*/true,
        std::make_tuple(expectedKey0, expectedKey1, expectedPayload), std::make_tuple(key0, key1, payload));
}

Y_UNIT_TEST(TopAllScalars) {
    ui32 key = 7U;
    TString payload = "solo";

    RunTopOrSortTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return TopBlocksByKeys(setup, s, 1, {{0, true}}); },
        /*unordered=*/true,
        std::make_tuple(TVector<ui32>{key}, TVector<TString>{payload}), std::make_tuple(key, payload));
}

Y_UNIT_TEST(TopDoubleOptionalPayload) {
    TVector<ui32> key = {3U, 1U, 2U};
    TVector<TMaybe<TMaybe<ui64>>> payload = {
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>(3U)),
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>()),
        TMaybe<TMaybe<ui64>>(),
    };

    TVector<ui32> expectedKey = {1U, 2U};
    TVector<TMaybe<TMaybe<ui64>>> expectedPayload = {payload[1], payload[2]};

    RunTopOrSortTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return TopBlocksByKeys(setup, s, 2, {{0, true}}); },
        /*unordered=*/true,
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

Y_UNIT_TEST(TopVoidPayload) {
    TVector<ui32> key = {3U, 1U, 2U};
    TVector<TMaybe<NTest::TSingularVoid>> payload = {
        TMaybe<NTest::TSingularVoid>(NTest::TSingularVoid()),
        TMaybe<NTest::TSingularVoid>(),
        TMaybe<NTest::TSingularVoid>(NTest::TSingularVoid()),
    };

    TVector<ui32> expectedKey = {1U, 2U};
    TVector<TMaybe<NTest::TSingularVoid>> expectedPayload = {payload[1], payload[2]};

    RunTopOrSortTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return TopBlocksByKeys(setup, s, 2, {{0, true}}); },
        /*unordered=*/true,
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

} // Y_UNIT_TEST_SUITE(TMiniKQLWideTopBlocksTest)

Y_UNIT_TEST_SUITE(TMiniKQLWideTopSortBlocksTest) {

Y_UNIT_TEST(TopSortAllArraysAscending) {
    TVector<ui32> key = {5U, 1U, 4U, 2U, 3U};
    TVector<TString> payload = {"e", "a", "d", "b", "c"};

    TVector<ui32> expectedKey = {1U, 2U, 3U};
    TVector<TString> expectedPayload = {"a", "b", "c"};

    RunTopOrSortTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return TopSortBlocksByKeys(setup, s, 3, {{0, true}}); },
        /*unordered=*/false,
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

Y_UNIT_TEST(TopSortAllArraysDescending) {
    TVector<TString> key = {"aa", "dd", "bb", "cc"};
    TVector<ui64> payload = {1U, 4U, 2U, 3U};

    TVector<TString> expectedKey = {"dd", "cc"};
    TVector<ui64> expectedPayload = {4U, 3U};

    RunTopOrSortTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return TopSortBlocksByKeys(setup, s, 2, {{0, false}}); },
        /*unordered=*/false,
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

Y_UNIT_TEST(TopSortByFirstThenSecond) {
    TVector<TStringBuf> expectedKey = {"key one", "key two", "key two", "very long key one"};
    TVector<TStringBuf> expectedValue = {"very long value 1", "very long value 3", "very long value 2", "very long value 4"};

    RunTopOrSortTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return TopSortBlocksByKeys(setup, s, 4, {{0, true}, {1, false}}); },
        /*unordered=*/false,
        std::make_tuple(expectedKey, expectedValue), std::make_tuple(KeyColumn(), ValueColumn()));
}

Y_UNIT_TEST(TopSortBySecondThenFirst) {
    TVector<TStringBuf> expectedKey = {
        "very long key two",
        "very long key two",
        "very long key two",
        "very long key two",
        "very long key two",
        "very long key one",
    };
    TVector<TStringBuf> expectedValue = {
        "very long value 9",
        "very long value 8",
        "very long value 7",
        "very long value 6",
        "very long value 5",
        "very long value 4",
    };

    RunTopOrSortTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return TopSortBlocksByKeys(setup, s, 6, {{1, false}, {0, true}}); },
        /*unordered=*/false,
        std::make_tuple(expectedKey, expectedValue), std::make_tuple(KeyColumn(), ValueColumn()));
}

Y_UNIT_TEST(TopSortScalarPayloadArrayKey) {
    TVector<ui64> key = {40U, 10U, 30U, 20U};
    ui32 payload = 9U;

    TVector<ui64> expectedKey = {10U, 20U};
    TVector<ui32> expectedPayload = {9U, 9U};

    RunTopOrSortTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return TopSortBlocksByKeys(setup, s, 2, {{0, true}}); },
        /*unordered=*/false,
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

Y_UNIT_TEST(TopSortNullablePayload) {
    TVector<ui32> key = {3U, 1U, 4U, 2U};
    TVector<TMaybe<TString>> payload = {TString("c"), TMaybe<TString>{}, TString("d"), TString("b")};

    TVector<ui32> expectedKey = {1U, 2U, 3U};
    TVector<TMaybe<TString>> expectedPayload = {TMaybe<TString>{}, TString("b"), TString("c")};

    RunTopOrSortTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return TopSortBlocksByKeys(setup, s, 3, {{0, true}}); },
        /*unordered=*/false,
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

Y_UNIT_TEST(TopSortAllScalars) {
    ui32 key = 7U;
    TString payload = "solo";

    RunTopOrSortTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return TopSortBlocksByKeys(setup, s, 1, {{0, true}}); },
        /*unordered=*/false,
        std::make_tuple(TVector<ui32>{key}, TVector<TString>{payload}), std::make_tuple(key, payload));
}

Y_UNIT_TEST(TopSortDoubleOptionalPayload) {
    TVector<ui32> key = {3U, 1U, 2U};
    TVector<TMaybe<TMaybe<ui64>>> payload = {
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>(3U)),
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>()),
        TMaybe<TMaybe<ui64>>(),
    };

    TVector<ui32> expectedKey = {1U, 2U};
    TVector<TMaybe<TMaybe<ui64>>> expectedPayload = {payload[1], payload[2]};

    RunTopOrSortTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return TopSortBlocksByKeys(setup, s, 2, {{0, true}}); },
        /*unordered=*/false,
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

Y_UNIT_TEST(TopSortVoidPayload) {
    TVector<ui32> key = {3U, 1U, 2U};
    TVector<TMaybe<NTest::TSingularVoid>> payload = {
        TMaybe<NTest::TSingularVoid>(NTest::TSingularVoid()),
        TMaybe<NTest::TSingularVoid>(),
        TMaybe<NTest::TSingularVoid>(NTest::TSingularVoid()),
    };

    TVector<ui32> expectedKey = {1U, 2U};
    TVector<TMaybe<NTest::TSingularVoid>> expectedPayload = {payload[1], payload[2]};

    RunTopOrSortTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return TopSortBlocksByKeys(setup, s, 2, {{0, true}}); },
        /*unordered=*/false,
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

} // Y_UNIT_TEST_SUITE(TMiniKQLWideTopSortBlocksTest)

Y_UNIT_TEST_SUITE(TMiniKQLWideSortBlocksTest) {

Y_UNIT_TEST(SortAscending) {
    TVector<TStringBuf> expectedKey = KeyColumn();
    TVector<TStringBuf> expectedValue = ValueColumn();

    RunTopOrSortTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return SortBlocksByKeys(setup, s, {{0, true}}); },
        /*unordered=*/false,
        std::make_tuple(expectedKey, expectedValue), std::make_tuple(KeyColumn(), ValueColumn()));
}

Y_UNIT_TEST(SortDescending) {
    TVector<ui32> key = {5U, 1U, 4U, 2U, 3U};
    TVector<TString> payload = {"e", "a", "d", "b", "c"};

    TVector<ui32> expectedKey = {5U, 4U, 3U, 2U, 1U};
    TVector<TString> expectedPayload = {"e", "d", "c", "b", "a"};

    RunTopOrSortTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return SortBlocksByKeys(setup, s, {{0, false}}); },
        /*unordered=*/false,
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

Y_UNIT_TEST(SortMultiKey) {
    TVector<ui32> key0 = {1U, 1U, 2U, 2U};
    TVector<ui64> key1 = {20U, 10U, 40U, 30U};
    TVector<TString> payload = {"a", "b", "c", "d"};

    TVector<ui32> expectedKey0 = {1U, 1U, 2U, 2U};
    TVector<ui64> expectedKey1 = {10U, 20U, 30U, 40U};
    TVector<TString> expectedPayload = {"b", "a", "d", "c"};

    RunTopOrSortTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return SortBlocksByKeys(setup, s, {{0, true}, {1, true}}); },
        /*unordered=*/false,
        std::make_tuple(expectedKey0, expectedKey1, expectedPayload), std::make_tuple(key0, key1, payload));
}

Y_UNIT_TEST(SortScalarPayloadArrayKey) {
    TVector<ui64> key = {40U, 10U, 30U, 20U};
    ui32 payload = 9U;

    TVector<ui64> expectedKey = {10U, 20U, 30U, 40U};
    TVector<ui32> expectedPayload = {9U, 9U, 9U, 9U};

    RunTopOrSortTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return SortBlocksByKeys(setup, s, {{0, true}}); },
        /*unordered=*/false,
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

Y_UNIT_TEST(SortNullablePayload) {
    TVector<ui32> key = {3U, 1U, 4U, 2U};
    TVector<TMaybe<TString>> payload = {TString("c"), TMaybe<TString>{}, TString("d"), TString("b")};

    TVector<ui32> expectedKey = {1U, 2U, 3U, 4U};
    TVector<TMaybe<TString>> expectedPayload = {TMaybe<TString>{}, TString("b"), TString("c"), TString("d")};

    RunTopOrSortTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return SortBlocksByKeys(setup, s, {{0, true}}); },
        /*unordered=*/false,
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

Y_UNIT_TEST(SortAllScalars) {
    ui32 key = 7U;
    TString payload = "solo";

    RunTopOrSortTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return SortBlocksByKeys(setup, s, {{0, true}}); },
        /*unordered=*/false,
        std::make_tuple(TVector<ui32>{key}, TVector<TString>{payload}), std::make_tuple(key, payload));
}

Y_UNIT_TEST(SortDoubleOptionalPayload) {
    TVector<ui32> key = {3U, 1U, 2U};
    TVector<TMaybe<TMaybe<ui64>>> payload = {
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>(3U)),
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>()),
        TMaybe<TMaybe<ui64>>(),
    };

    TVector<ui32> expectedKey = {1U, 2U, 3U};
    TVector<TMaybe<TMaybe<ui64>>> expectedPayload = {payload[1], payload[2], payload[0]};

    RunTopOrSortTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return SortBlocksByKeys(setup, s, {{0, true}}); },
        /*unordered=*/false,
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

Y_UNIT_TEST(SortVoidPayload) {
    TVector<ui32> key = {3U, 1U, 2U};
    TVector<TMaybe<NTest::TSingularVoid>> payload = {
        TMaybe<NTest::TSingularVoid>(NTest::TSingularVoid()),
        TMaybe<NTest::TSingularVoid>(),
        TMaybe<NTest::TSingularVoid>(NTest::TSingularVoid()),
    };

    TVector<ui32> expectedKey = {1U, 2U, 3U};
    TVector<TMaybe<NTest::TSingularVoid>> expectedPayload = {payload[1], payload[2], payload[0]};

    RunTopOrSortTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return SortBlocksByKeys(setup, s, {{0, true}}); },
        /*unordered=*/false,
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

} // Y_UNIT_TEST_SUITE(TMiniKQLWideSortBlocksTest)

} // namespace NKikimr::NMiniKQL
