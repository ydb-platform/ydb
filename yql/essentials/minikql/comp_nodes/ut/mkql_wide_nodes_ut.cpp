#include "mkql_computation_node_ut.h"
#include <yql/essentials/minikql/mkql_runtime_version.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_program_builder_test_utils.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLWideNodesTest) {
// TDOD: fixme
#if 0
    Y_UNIT_TEST_LLVM(TestWideDiscard) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = NTest::ConvertValueToLiteralNode(pb, TStringBuf("000"));
        const auto data1 = NTest::ConvertValueToLiteralNode(pb, TStringBuf("100"));
        const auto data2 = NTest::ConvertValueToLiteralNode(pb, TStringBuf("200"));
        const auto data3 = NTest::ConvertValueToLiteralNode(pb, TStringBuf("300"));
        const auto dataType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3});

        const auto pgmReturn = pb.FromFlow(pb.Discard(pb.ExpandMap(pb.ToFlow(list), [](TRuntimeNode) { return TRuntimeNode::TList(); })));
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }
#endif

Y_UNIT_TEST_LLVM(TestDiscard) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TStringBuf>{"000", "100", "200", "300"});

    const auto pgmReturn = pb.FromFlow(pb.Discard(pb.ToFlow(list)));
    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
}

Y_UNIT_TEST_LLVM(TestTakeOverSource) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.Take(pb.Source(), NTest::ConvertValueToLiteralNode(pb, ui64(666))), [&](TRuntimeNode::TList) { return pb.NewTuple({}); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetListLength(), 666ULL);
}

Y_UNIT_TEST_LLVM(TestSkipAndTake) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = pb.ListFromRange(NTest::ConvertValueToLiteralNode(pb, ui32(100)), NTest::ConvertValueToLiteralNode(pb, ui32(666)), NTest::ConvertValueToLiteralNode(pb, ui32(3)));

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.Take(pb.Skip(pb.ExpandMap(pb.ToFlow(pb.Enumerate(list)),
                                                                                [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 1U), pb.Nth(item, 0U)}; }),
                                                                   NTest::ConvertValueToLiteralNode(pb, ui64(42))), NTest::ConvertValueToLiteralNode(pb, ui64(4))),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple({items.back(), items.front()}); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<ui64, ui32>>{
                                                          {ui64(42), ui32(226)},
                                                          {ui64(43), ui32(229)},
                                                          {ui64(44), ui32(232)},
                                                          {ui64(45), ui32(235)},
                                                      });
}

Y_UNIT_TEST_LLVM(TestSkipAndTakeSingular) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    constexpr ui64 limit = 100;
    constexpr ui64 offset = 100;

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.Take(pb.Skip(pb.Source(),
                                                                   NTest::ConvertValueToLiteralNode(pb, offset)), NTest::ConvertValueToLiteralNode(pb, limit)),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto length = graph->GetValue().GetListLength();
    UNIT_ASSERT_VALUES_EQUAL(limit, length);
}

Y_UNIT_TEST_LLVM(TestDoNotCalculateSkipped) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = pb.ListFromRange(NTest::ConvertValueToLiteralNode(pb, ui64(100)), NTest::ConvertValueToLiteralNode(pb, ui64(135)), NTest::ConvertValueToLiteralNode(pb, ui64(5)));

    const auto trap = NTest::ConvertValueToLiteralNode(pb, TStringBuf("IT'S A TRAP!"));

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.Skip(pb.WideMap(pb.ExpandMap(pb.ToFlow(pb.Enumerate(list)),
                                                                                   [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 1U), pb.Nth(item, 0U)}; }),
                                                                      [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {pb.Unwrap(pb.Div(items.front(), items.back()), trap, __FILE__, __LINE__, 0)}; }),
                                                           NTest::ConvertValueToLiteralNode(pb, ui64(3))),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return items.front(); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui64>{
                                                          ui64(38),
                                                          ui64(30),
                                                          ui64(25),
                                                          ui64(21),
                                                      });
}

} // Y_UNIT_TEST_SUITE(TMiniKQLWideNodesTest)

} // namespace NMiniKQL
} // namespace NKikimr
