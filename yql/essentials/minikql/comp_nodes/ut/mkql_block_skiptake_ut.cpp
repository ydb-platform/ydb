#include "mkql_computation_node_ut.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_block_test_helper.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_program_builder_test_utils.h>

namespace NKikimr::NMiniKQL {

namespace {

TRuntimeNode SkipBlocksBy(TSetup<false>& setup, TRuntimeNode fuzzedWideStream, ui64 count) {
    TProgramBuilder& pb = *setup.PgmBuilder;
    return pb.WideSkipBlocks(fuzzedWideStream, NTest::ConvertValueToLiteralNode(pb, count));
}

TRuntimeNode TakeBlocksBy(TSetup<false>& setup, TRuntimeNode fuzzedWideStream, ui64 count) {
    TProgramBuilder& pb = *setup.PgmBuilder;
    return pb.WideTakeBlocks(fuzzedWideStream, NTest::ConvertValueToLiteralNode(pb, count));
}

template <typename TStreamOp, typename... TExpected, typename... TInputs>
void RunSkipTakeTest(TStreamOp&& streamOp,
                     const std::tuple<TVector<TExpected>...>& expected,
                     const std::tuple<TInputs...>& input) {
    TBlockHelper helper;
    helper.WithScopedFuzzers([&] {
        helper.RunWideStreamNode(expected, streamOp, /*unordered=*/false, input);
    });
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLWideSkipBlocksTest) {

Y_UNIT_TEST(SkipPartial) {
    TVector<ui32> key = {1U, 2U, 3U, 4U, 5U};
    TVector<TString> payload = {"a", "b", "c", "d", "e"};

    TVector<ui32> expectedKey = {3U, 4U, 5U};
    TVector<TString> expectedPayload = {"c", "d", "e"};

    RunSkipTakeTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return SkipBlocksBy(setup, s, 2); },
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

Y_UNIT_TEST(SkipZeroKeepsAll) {
    TVector<ui64> key = {10U, 20U, 30U};
    TVector<ui8> payload = {1U, 2U, 3U};

    RunSkipTakeTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return SkipBlocksBy(setup, s, 0); },
        std::make_tuple(key, payload), std::make_tuple(key, payload));
}

Y_UNIT_TEST(SkipAllRowsIsEmpty) {
    TVector<ui32> key = {1U, 2U, 3U};
    TVector<TString> payload = {"x", "y", "z"};

    TVector<ui32> expectedKey;
    TVector<TString> expectedPayload;

    RunSkipTakeTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return SkipBlocksBy(setup, s, 3); },
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

Y_UNIT_TEST(SkipMixedScalarArray) {
    TVector<ui32> key = {1U, 2U, 3U};
    TString payload = "const";

    TVector<ui32> expectedKey = {2U, 3U};
    TVector<TString> expectedPayload = {"const", "const"};

    RunSkipTakeTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return SkipBlocksBy(setup, s, 1); },
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

Y_UNIT_TEST(SkipAllScalarsFullySkipped) {
    ui32 key = 7U;
    TString payload = "solo";

    TVector<ui32> expectedKey;
    TVector<TString> expectedPayload;

    RunSkipTakeTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return SkipBlocksBy(setup, s, 1); },
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

Y_UNIT_TEST(SkipDoubleOptionalPayload) {
    TVector<ui32> key = {1U, 2U, 3U, 4U};
    TVector<TMaybe<TMaybe<ui64>>> payload = {
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>(1U)),
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>()),
        TMaybe<TMaybe<ui64>>(),
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>(4U)),
    };

    TVector<ui32> expectedKey = {2U, 3U, 4U};
    TVector<TMaybe<TMaybe<ui64>>> expectedPayload = {payload[1], payload[2], payload[3]};

    RunSkipTakeTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return SkipBlocksBy(setup, s, 1); },
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

Y_UNIT_TEST(SkipVoidPayload) {
    TVector<ui32> key = {1U, 2U, 3U};
    TVector<TMaybe<NTest::TSingularVoid>> payload = {
        TMaybe<NTest::TSingularVoid>(NTest::TSingularVoid()),
        TMaybe<NTest::TSingularVoid>(),
        TMaybe<NTest::TSingularVoid>(NTest::TSingularVoid()),
    };

    TVector<ui32> expectedKey = {2U, 3U};
    TVector<TMaybe<NTest::TSingularVoid>> expectedPayload = {payload[1], payload[2]};

    RunSkipTakeTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return SkipBlocksBy(setup, s, 1); },
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

} // Y_UNIT_TEST_SUITE(TMiniKQLWideSkipBlocksTest)

Y_UNIT_TEST_SUITE(TMiniKQLWideTakeBlocksTest) {

Y_UNIT_TEST(TakePartial) {
    TVector<ui32> key = {1U, 2U, 3U, 4U, 5U};
    TVector<TString> payload = {"a", "b", "c", "d", "e"};

    TVector<ui32> expectedKey = {1U, 2U, 3U};
    TVector<TString> expectedPayload = {"a", "b", "c"};

    RunSkipTakeTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return TakeBlocksBy(setup, s, 3); },
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

Y_UNIT_TEST(TakeZeroIsEmpty) {
    TVector<ui64> key = {10U, 20U, 30U};
    TVector<ui8> payload = {1U, 2U, 3U};

    TVector<ui64> expectedKey;
    TVector<ui8> expectedPayload;

    RunSkipTakeTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return TakeBlocksBy(setup, s, 0); },
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

Y_UNIT_TEST(TakeMoreThanAvailableKeepsAll) {
    TVector<ui32> key = {1U, 2U, 3U};
    TVector<TString> payload = {"x", "y", "z"};

    RunSkipTakeTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return TakeBlocksBy(setup, s, 100); },
        std::make_tuple(key, payload), std::make_tuple(key, payload));
}

Y_UNIT_TEST(TakeMixedScalarArray) {
    TVector<ui32> key = {1U, 2U, 3U};
    TString payload = "const";

    TVector<ui32> expectedKey = {1U, 2U};
    TVector<TString> expectedPayload = {"const", "const"};

    RunSkipTakeTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return TakeBlocksBy(setup, s, 2); },
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

Y_UNIT_TEST(TakeAllScalarsKeepsSingleRow) {
    ui32 key = 7U;
    TString payload = "solo";

    TVector<ui32> expectedKey = {7U};
    TVector<TString> expectedPayload = {"solo"};

    RunSkipTakeTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return TakeBlocksBy(setup, s, 1); },
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

Y_UNIT_TEST(TakeDoubleOptionalPayload) {
    TVector<ui32> key = {1U, 2U, 3U, 4U};
    TVector<TMaybe<TMaybe<ui64>>> payload = {
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>(1U)),
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>()),
        TMaybe<TMaybe<ui64>>(),
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>(4U)),
    };

    TVector<ui32> expectedKey = {1U, 2U, 3U};
    TVector<TMaybe<TMaybe<ui64>>> expectedPayload = {payload[0], payload[1], payload[2]};

    RunSkipTakeTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return TakeBlocksBy(setup, s, 3); },
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

Y_UNIT_TEST(TakeVoidPayload) {
    TVector<ui32> key = {1U, 2U, 3U};
    TVector<TMaybe<NTest::TSingularVoid>> payload = {
        TMaybe<NTest::TSingularVoid>(NTest::TSingularVoid()),
        TMaybe<NTest::TSingularVoid>(),
        TMaybe<NTest::TSingularVoid>(NTest::TSingularVoid()),
    };

    TVector<ui32> expectedKey = {1U, 2U};
    TVector<TMaybe<NTest::TSingularVoid>> expectedPayload = {payload[0], payload[1]};

    RunSkipTakeTest(
        [](TSetup<false>& setup, TRuntimeNode s) { return TakeBlocksBy(setup, s, 2); },
        std::make_tuple(expectedKey, expectedPayload), std::make_tuple(key, payload));
}

} // Y_UNIT_TEST_SUITE(TMiniKQLWideTakeBlocksTest)

} // namespace NKikimr::NMiniKQL
