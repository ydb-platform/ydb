#include "mkql_computation_node_ut.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_block_builder.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_block_test_helper.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_program_builder_test_utils.h>

namespace NKikimr::NMiniKQL {

namespace {

TRuntimeNode CompressByBitmap(TSetup<false>& setup, TRuntimeNode fuzzedWideStream, ui32 bitmapIndex) {
    return setup.PgmBuilder->BlockCompress(fuzzedWideStream, bitmapIndex);
}

template <typename... TExpected, typename... TInputs>
void RunCompressTest(const std::tuple<TVector<TExpected>...>& expected,
                     const std::tuple<TInputs...>& input, ui32 bitmapIndex) {
    TBlockHelper helper;
    helper.WithScopedFuzzers([&] {
        helper.RunWideStreamNode(
            expected,
            [bitmapIndex](TSetup<false>& setup, TRuntimeNode fuzzedWideStream) {
                return CompressByBitmap(setup, fuzzedWideStream, bitmapIndex);
            },
            /*unordered=*/false,
            input);
    });
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockCompressTest) {

Y_UNIT_TEST(CompressBasic) {
    TVector<bool> bitmap = {false, true, false, false, true, true, false};
    TVector<ui64> value = {1, 2, 3, 4, 5, 6, 7};
    TVector<bool> tag = {true, false, true, true, false, true, true};

    TVector<ui64> expectedValue = {2, 5, 6};
    TVector<bool> expectedTag = {false, false, true};

    RunCompressTest(std::make_tuple(expectedValue, expectedTag), std::make_tuple(bitmap, value, tag), 0);
}

Y_UNIT_TEST(CompressAllScalars) {
    bool bitmap = true;
    ui32 value = 42U;
    TString tag = "solo";

    RunCompressTest(std::make_tuple(TVector<ui32>{value}, TVector<TString>{tag}),
                    std::make_tuple(bitmap, value, tag), 0);
}

Y_UNIT_TEST(CompressScalarBitmapPassAll) {
    TVector<TMaybe<ui32>> value = {5U, TMaybe<ui32>{}, 7U};
    TVector<TString> tag = {"x", "y", "z"};

    RunCompressTest(std::make_tuple(value, tag), std::make_tuple(value, true, tag), 1);
}

Y_UNIT_TEST(CompressAllScalarsWithArrayBitmap) {
    ui64 value = 42;
    TVector<bool> bitmap = {true, false, true};
    TString tag = "hello";

    TVector<ui64> expectedValue = {42, 42};
    TVector<TString> expectedTag = {"hello", "hello"};

    RunCompressTest(std::make_tuple(expectedValue, expectedTag), std::make_tuple(value, bitmap, tag), 1);
}

Y_UNIT_TEST(CompressMixedShapes) {
    TVector<TMaybe<ui32>> value = {1U, TMaybe<ui32>{}, 3U, 4U};
    TVector<bool> bitmap = {false, true, true, false};
    TString tag = "const";

    TVector<TMaybe<ui32>> expectedValue = {TMaybe<ui32>{}, 3U};
    TVector<TString> expectedTag = {"const", "const"};

    RunCompressTest(std::make_tuple(expectedValue, expectedTag), std::make_tuple(value, bitmap, tag), 1);
}

Y_UNIT_TEST(CompressNullableArrays) {
    TVector<TMaybe<TString>> value = {TMaybe<TString>{}, TMaybe<TString>("a"), TMaybe<TString>("b"), TMaybe<TString>{}};
    TVector<bool> bitmap = {true, true, false, true};
    TVector<TMaybe<ui64>> num = {TMaybe<ui64>{}, 2U, 3U, TMaybe<ui64>{}};

    TVector<TMaybe<TString>> expectedValue = {TMaybe<TString>{}, TMaybe<TString>("a"), TMaybe<TString>{}};
    TVector<TMaybe<ui64>> expectedNum = {TMaybe<ui64>{}, 2U, TMaybe<ui64>{}};

    RunCompressTest(std::make_tuple(expectedValue, expectedNum), std::make_tuple(value, bitmap, num), 1);
}

Y_UNIT_TEST(CompressNestedTupleColumn) {
    TVector<std::tuple<ui64, bool>> value = {{1, true}, {2, false}, {3, true}, {4, true}, {5, false}};
    TVector<bool> bitmap = {true, false, true, true, false};

    TVector<std::tuple<ui64, bool>> expectedValue = {{1, true}, {3, true}, {4, true}};

    RunCompressTest(std::make_tuple(expectedValue), std::make_tuple(value, bitmap), 1);
}

Y_UNIT_TEST(CompressDoubleOptionalValue) {
    TVector<TMaybe<TMaybe<ui64>>> value = {
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>(1U)),
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>()),
        TMaybe<TMaybe<ui64>>(),
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>(4U)),
    };
    TVector<bool> bitmap = {true, false, true, true};

    TVector<TMaybe<TMaybe<ui64>>> expectedValue = {value[0], value[2], value[3]};

    RunCompressTest(std::make_tuple(expectedValue), std::make_tuple(value, bitmap), 1);
}

Y_UNIT_TEST(CompressVoidValue) {
    TVector<TMaybe<NTest::TSingularVoid>> value = {
        TMaybe<NTest::TSingularVoid>(NTest::TSingularVoid()),
        TMaybe<NTest::TSingularVoid>(),
        TMaybe<NTest::TSingularVoid>(NTest::TSingularVoid()),
    };
    TVector<bool> bitmap = {false, true, true};

    TVector<TMaybe<NTest::TSingularVoid>> expectedValue = {value[1], value[2]};

    RunCompressTest(std::make_tuple(expectedValue), std::make_tuple(value, bitmap), 1);
}

} // Y_UNIT_TEST_SUITE(TMiniKQLBlockCompressTest)

} // namespace NKikimr::NMiniKQL
