#include "mkql_computation_node_ut.h"
#include <yql/essentials/minikql/mkql_runtime_version.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_program_builder_test_utils.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

#include <cstring>

namespace NKikimr {
namespace NMiniKQL {

namespace {

TRuntimeNode MakeTestBlockStream(TProgramBuilder& pb) {
    auto values = TVector<std::tuple<TStringBuf, TStringBuf>>{
        {"key one", "very long value 1"},
        {"key two", "very long value 2"},
        {"key two", "very long value 3"},
        {"very long key one", "very long value 4"},
        {"very long key two", "very long value 5"},
        {"very long key two", "very long value 6"},
        {"very long key two", "very long value 7"},
        {"very long key two", "very long value 8"},
        {"very long key two", "very long value 9"},
    };

    const auto list = NTest::ConvertValueToLiteralNode(pb, values);

    const auto flow = pb.ToFlow(list);
    const auto wideFlow = pb.ExpandMap(flow,
                                       [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; });
    return pb.WideToBlocks(pb.FromFlow(wideFlow));
}

using TTestRow = std::tuple<TStringBuf, TStringBuf>;

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockTopTest) {
Y_UNIT_TEST_LLVM(TopByFirstKeyAsc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<TStringBuf, TStringBuf>>(pb);

    const auto blockStream = MakeTestBlockStream(pb);
    const auto topBlocks = pb.WideTopBlocks(blockStream,
                                            NTest::ConvertValueToLiteralNode(pb, ui64(4)),
                                            {{0U, NTest::ConvertValueToLiteralNode(pb, true)}});
    const auto topFlow = pb.ToFlow(pb.WideFromBlocks(topBlocks));
    const auto pgmReturn = pb.Collect(pb.NarrowMap(topFlow,
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTestRow>{
                                                          {TStringBuf("key one"), TStringBuf("very long value 1")},
                                                          {TStringBuf("key two"), TStringBuf("very long value 3")},
                                                          {TStringBuf("key two"), TStringBuf("very long value 2")},
                                                          {TStringBuf("very long key one"), TStringBuf("very long value 4")},
                                                      });
}

Y_UNIT_TEST_LLVM(TopByFirstKeyDesc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<TStringBuf, TStringBuf>>(pb);

    const auto blockStream = MakeTestBlockStream(pb);
    const auto topBlocks = pb.WideTopBlocks(blockStream,
                                            NTest::ConvertValueToLiteralNode(pb, ui64(6)),
                                            {{0U, NTest::ConvertValueToLiteralNode(pb, false)}});
    const auto topFlow = pb.ToFlow(pb.WideFromBlocks(topBlocks));
    const auto pgmReturn = pb.Collect(pb.NarrowMap(topFlow,
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTestRow>{
                                                          {TStringBuf("very long key two"), TStringBuf("very long value 9")},
                                                          {TStringBuf("very long key two"), TStringBuf("very long value 8")},
                                                          {TStringBuf("very long key two"), TStringBuf("very long value 6")},
                                                          {TStringBuf("very long key two"), TStringBuf("very long value 5")},
                                                          {TStringBuf("very long key two"), TStringBuf("very long value 7")},
                                                          {TStringBuf("very long key one"), TStringBuf("very long value 4")},
                                                      });
}

Y_UNIT_TEST_LLVM(TopBySecondKeyAsc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<TStringBuf, TStringBuf>>(pb);

    const auto blockStream = MakeTestBlockStream(pb);
    const auto topBlocks = pb.WideTopBlocks(blockStream,
                                            NTest::ConvertValueToLiteralNode(pb, ui64(3)),
                                            {{1U, NTest::ConvertValueToLiteralNode(pb, true)}});
    const auto topFlow = pb.ToFlow(pb.WideFromBlocks(topBlocks));
    const auto pgmReturn = pb.Collect(pb.NarrowMap(topFlow,
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTestRow>{
                                                          {TStringBuf("key one"), TStringBuf("very long value 1")},
                                                          {TStringBuf("key two"), TStringBuf("very long value 2")},
                                                          {TStringBuf("key two"), TStringBuf("very long value 3")},
                                                      });
}

Y_UNIT_TEST_LLVM(TopBySecondKeyDesc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<TStringBuf, TStringBuf>>(pb);

    const auto blockStream = MakeTestBlockStream(pb);
    const auto topBlocks = pb.WideTopBlocks(blockStream,
                                            NTest::ConvertValueToLiteralNode(pb, ui64(2)),
                                            {{1U, NTest::ConvertValueToLiteralNode(pb, false)}});
    const auto topFlow = pb.ToFlow(pb.WideFromBlocks(topBlocks));
    const auto pgmReturn = pb.Collect(pb.NarrowMap(topFlow,
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTestRow>{
                                                          {TStringBuf("very long key two"), TStringBuf("very long value 9")},
                                                          {TStringBuf("very long key two"), TStringBuf("very long value 8")},
                                                      });
}

Y_UNIT_TEST_LLVM(TopSortByFirstSecondAscDesc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<TStringBuf, TStringBuf>>(pb);

    const auto blockStream = MakeTestBlockStream(pb);
    const auto topSortBlocks = pb.WideTopSortBlocks(blockStream,
                                                    NTest::ConvertValueToLiteralNode(pb, ui64(4)),
                                                    {{0U, NTest::ConvertValueToLiteralNode(pb, true)},
                                                     {1U, NTest::ConvertValueToLiteralNode(pb, false)}});
    const auto topSortFlow = pb.ToFlow(pb.WideFromBlocks(topSortBlocks));
    const auto pgmReturn = pb.Collect(pb.NarrowMap(topSortFlow,
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTestRow>{
                                                          {TStringBuf("key one"), TStringBuf("very long value 1")},
                                                          {TStringBuf("key two"), TStringBuf("very long value 3")},
                                                          {TStringBuf("key two"), TStringBuf("very long value 2")},
                                                          {TStringBuf("very long key one"), TStringBuf("very long value 4")},
                                                      });
}

Y_UNIT_TEST_LLVM(TopSortByFirstSecondDescAsc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<TStringBuf, TStringBuf>>(pb);

    const auto blockStream = MakeTestBlockStream(pb);
    const auto topSortBlocks = pb.WideTopSortBlocks(blockStream,
                                                    NTest::ConvertValueToLiteralNode(pb, ui64(6)),
                                                    {{0U, NTest::ConvertValueToLiteralNode(pb, false)},
                                                     {1U, NTest::ConvertValueToLiteralNode(pb, true)}});
    const auto topSortFlow = pb.ToFlow(pb.WideFromBlocks(topSortBlocks));
    const auto pgmReturn = pb.Collect(pb.NarrowMap(topSortFlow,
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTestRow>{
                                                          {TStringBuf("very long key two"), TStringBuf("very long value 5")},
                                                          {TStringBuf("very long key two"), TStringBuf("very long value 6")},
                                                          {TStringBuf("very long key two"), TStringBuf("very long value 7")},
                                                          {TStringBuf("very long key two"), TStringBuf("very long value 8")},
                                                          {TStringBuf("very long key two"), TStringBuf("very long value 9")},
                                                          {TStringBuf("very long key one"), TStringBuf("very long value 4")},
                                                      });
}

Y_UNIT_TEST_LLVM(TopSortBySecondFirstAscDesc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<TStringBuf, TStringBuf>>(pb);

    const auto blockStream = MakeTestBlockStream(pb);
    const auto topSortBlocks = pb.WideTopSortBlocks(blockStream,
                                                    NTest::ConvertValueToLiteralNode(pb, ui64(4)),
                                                    {{1U, NTest::ConvertValueToLiteralNode(pb, true)},
                                                     {0U, NTest::ConvertValueToLiteralNode(pb, false)}});
    const auto topSortFlow = pb.ToFlow(pb.WideFromBlocks(topSortBlocks));
    const auto pgmReturn = pb.Collect(pb.NarrowMap(topSortFlow,
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTestRow>{
                                                          {TStringBuf("key one"), TStringBuf("very long value 1")},
                                                          {TStringBuf("key two"), TStringBuf("very long value 2")},
                                                          {TStringBuf("key two"), TStringBuf("very long value 3")},
                                                          {TStringBuf("very long key one"), TStringBuf("very long value 4")},
                                                      });
}

Y_UNIT_TEST_LLVM(TopSortBySecondFirstDescAsc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<TStringBuf, TStringBuf>>(pb);

    const auto blockStream = MakeTestBlockStream(pb);
    const auto topSortBlocks = pb.WideTopSortBlocks(blockStream,
                                                    NTest::ConvertValueToLiteralNode(pb, ui64(6)),
                                                    {{1U, NTest::ConvertValueToLiteralNode(pb, false)},
                                                     {0U, NTest::ConvertValueToLiteralNode(pb, true)}});
    const auto topSortFlow = pb.ToFlow(pb.WideFromBlocks(topSortBlocks));
    const auto pgmReturn = pb.Collect(pb.NarrowMap(topSortFlow,
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTestRow>{
                                                          {TStringBuf("very long key two"), TStringBuf("very long value 9")},
                                                          {TStringBuf("very long key two"), TStringBuf("very long value 8")},
                                                          {TStringBuf("very long key two"), TStringBuf("very long value 7")},
                                                          {TStringBuf("very long key two"), TStringBuf("very long value 6")},
                                                          {TStringBuf("very long key two"), TStringBuf("very long value 5")},
                                                          {TStringBuf("very long key one"), TStringBuf("very long value 4")},
                                                      });
}
} // Y_UNIT_TEST_SUITE(TMiniKQLBlockTopTest)

Y_UNIT_TEST_SUITE(TMiniKQLBlockSortTest) {
Y_UNIT_TEST_LLVM(SortByFirstKeyAsc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<TStringBuf, TStringBuf>>(pb);

    const auto blockStream = MakeTestBlockStream(pb);
    const auto sortBlocks = pb.WideSortBlocks(blockStream,
                                              {{0U, NTest::ConvertValueToLiteralNode(pb, true)}});
    const auto sortFlow = pb.ToFlow(pb.WideFromBlocks(sortBlocks));
    const auto pgmReturn = pb.Collect(pb.NarrowMap(sortFlow,
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTestRow>{
                                                          {TStringBuf("key one"), TStringBuf("very long value 1")},
                                                          {TStringBuf("key two"), TStringBuf("very long value 2")},
                                                          {TStringBuf("key two"), TStringBuf("very long value 3")},
                                                          {TStringBuf("very long key one"), TStringBuf("very long value 4")},
                                                          {TStringBuf("very long key two"), TStringBuf("very long value 5")},
                                                          {TStringBuf("very long key two"), TStringBuf("very long value 6")},
                                                          {TStringBuf("very long key two"), TStringBuf("very long value 7")},
                                                          {TStringBuf("very long key two"), TStringBuf("very long value 8")},
                                                          {TStringBuf("very long key two"), TStringBuf("very long value 9")},
                                                      });
}
} // Y_UNIT_TEST_SUITE(TMiniKQLBlockSortTest)

} // namespace NMiniKQL
} // namespace NKikimr
