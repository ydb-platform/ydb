#include "mkql_computation_node_ut.h"
#include <yql/essentials/minikql/mkql_runtime_version.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mock_spiller_factory_ut.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_program_builder_test_utils.h>

#include <cstring>

namespace NKikimr {
namespace NMiniKQL {

namespace {

const TVector<std::tuple<TStringBuf, TStringBuf>> LongList = {
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

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLWideTopTest) {
Y_UNIT_TEST_LLVM(TopByFirstKeyAsc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<TStringBuf, TStringBuf>>(pb);
    const auto list = NTest::ConvertValueToLiteralNode(pb, LongList);

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTop(pb.ExpandMap(pb.ToFlow(list),
                                                                           [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                              NTest::ConvertValueToLiteralNode(pb, ui64(4)), {{0U, NTest::ConvertValueToLiteralNode(pb, true)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TStringBuf, TStringBuf>>{
                                                          {"very long key one", "very long value 4"},
                                                          {"key two", "very long value 3"},
                                                          {"key two", "very long value 2"},
                                                          {"key one", "very long value 1"},
                                                      });
}

Y_UNIT_TEST_LLVM(TopByFirstKeyDesc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<TStringBuf, TStringBuf>>(pb);
    const auto list = NTest::ConvertValueToLiteralNode(pb, LongList);

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTop(pb.ExpandMap(pb.ToFlow(list),
                                                                           [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                              NTest::ConvertValueToLiteralNode(pb, ui64(6)), {{0U, NTest::ConvertValueToLiteralNode(pb, false)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TStringBuf, TStringBuf>>{
                                                          {"very long key one", "very long value 4"},
                                                          {"very long key two", "very long value 7"},
                                                          {"very long key two", "very long value 5"},
                                                          {"very long key two", "very long value 6"},
                                                          {"very long key two", "very long value 8"},
                                                          {"very long key two", "very long value 9"},
                                                      });
}

Y_UNIT_TEST_LLVM(TopBySecondKeyAsc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<TStringBuf, TStringBuf>>(pb);
    const auto list = NTest::ConvertValueToLiteralNode(pb, LongList);

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTop(pb.ExpandMap(pb.ToFlow(list),
                                                                           [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                              NTest::ConvertValueToLiteralNode(pb, ui64(3)), {{1U, NTest::ConvertValueToLiteralNode(pb, true)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TStringBuf, TStringBuf>>{
                                                          {"key two", "very long value 3"},
                                                          {"key two", "very long value 2"},
                                                          {"key one", "very long value 1"},
                                                      });
}

Y_UNIT_TEST_LLVM(TopBySecondKeyDesc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<TStringBuf, TStringBuf>>(pb);
    const auto list = NTest::ConvertValueToLiteralNode(pb, LongList);

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTop(pb.ExpandMap(pb.ToFlow(list),
                                                                           [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                              NTest::ConvertValueToLiteralNode(pb, ui64(2)), {{1U, NTest::ConvertValueToLiteralNode(pb, false)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TStringBuf, TStringBuf>>{
                                                          {"very long key two", "very long value 8"},
                                                          {"very long key two", "very long value 9"},
                                                      });
}

Y_UNIT_TEST_LLVM(TopWithLargeCount) {
    TSetup<LLVM> setup;
    setup.Alloc.SetLimit(1_MB);
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<TStringBuf, TStringBuf>>(pb);
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TStringBuf, TStringBuf>>{
                                                               {"key one", "value 1"},
                                                               {"key two", "value 2"},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTop(pb.ExpandMap(pb.ToFlow(list),
                                                                           [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                              NTest::ConvertValueToLiteralNode(pb, ui64(4000000000)), {{0U, NTest::ConvertValueToLiteralNode(pb, true)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TStringBuf, TStringBuf>>{
                                                          {"key two", "value 2"},
                                                          {"key one", "value 1"},
                                                      });
}

Y_UNIT_TEST_LLVM(TopSortByFirstSecondAscDesc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<TStringBuf, TStringBuf>>(pb);
    const auto list = NTest::ConvertValueToLiteralNode(pb, LongList);

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTopSort(pb.ExpandMap(pb.ToFlow(list),
                                                                               [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                  NTest::ConvertValueToLiteralNode(pb, ui64(4)), {{0U, NTest::ConvertValueToLiteralNode(pb, true)}, {1U, NTest::ConvertValueToLiteralNode(pb, false)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TStringBuf, TStringBuf>>{
                                                          {"key one", "very long value 1"},
                                                          {"key two", "very long value 3"},
                                                          {"key two", "very long value 2"},
                                                          {"very long key one", "very long value 4"},
                                                      });
}

Y_UNIT_TEST_LLVM(TopSortByFirstSecondDescAsc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<TStringBuf, TStringBuf>>(pb);
    const auto list = NTest::ConvertValueToLiteralNode(pb, LongList);

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTopSort(pb.ExpandMap(pb.ToFlow(list),
                                                                               [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                  NTest::ConvertValueToLiteralNode(pb, ui64(6)), {{0U, NTest::ConvertValueToLiteralNode(pb, false)}, {1U, NTest::ConvertValueToLiteralNode(pb, true)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TStringBuf, TStringBuf>>{
                                                          {"very long key two", "very long value 5"},
                                                          {"very long key two", "very long value 6"},
                                                          {"very long key two", "very long value 7"},
                                                          {"very long key two", "very long value 8"},
                                                          {"very long key two", "very long value 9"},
                                                          {"very long key one", "very long value 4"},
                                                      });
}

Y_UNIT_TEST_LLVM(TopSortBySecondFirstAscDesc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<TStringBuf, TStringBuf>>(pb);
    const auto list = NTest::ConvertValueToLiteralNode(pb, LongList);

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTopSort(pb.ExpandMap(pb.ToFlow(list),
                                                                               [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                  NTest::ConvertValueToLiteralNode(pb, ui64(4)), {{1U, NTest::ConvertValueToLiteralNode(pb, true)}, {0U, NTest::ConvertValueToLiteralNode(pb, false)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TStringBuf, TStringBuf>>{
                                                          {"key one", "very long value 1"},
                                                          {"key two", "very long value 2"},
                                                          {"key two", "very long value 3"},
                                                          {"very long key one", "very long value 4"},
                                                      });
}

Y_UNIT_TEST_LLVM(TopSortBySecondFirstDescAsc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<TStringBuf, TStringBuf>>(pb);
    const auto list = NTest::ConvertValueToLiteralNode(pb, LongList);

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTopSort(pb.ExpandMap(pb.ToFlow(list),
                                                                               [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                  NTest::ConvertValueToLiteralNode(pb, ui64(6)), {{1U, NTest::ConvertValueToLiteralNode(pb, false)}, {0U, NTest::ConvertValueToLiteralNode(pb, true)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TStringBuf, TStringBuf>>{
                                                          {"very long key two", "very long value 9"},
                                                          {"very long key two", "very long value 8"},
                                                          {"very long key two", "very long value 7"},
                                                          {"very long key two", "very long value 6"},
                                                          {"very long key two", "very long value 5"},
                                                          {"very long key one", "very long value 4"},
                                                      });
}

Y_UNIT_TEST_LLVM(TopSortLargeList) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto minusday = i64(-24LL * 60LL * 60LL * 1000000LL); // -1 Day
    const auto step = pb.NewDataLiteral<NUdf::EDataSlot::Interval>(NUdf::TStringRef((const char*)&minusday, sizeof(minusday)));
    const auto list = pb.ListFromRange(pb.NewTzDataLiteral<NUdf::TTzDate>(30000u, 42u), pb.NewTzDataLiteral<NUdf::TTzDate>(10000u, 42u), step);

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTopSort(pb.ExpandMap(pb.ToFlow(pb.Enumerate(list)),
                                                                               [&](TRuntimeNode item) -> TRuntimeNode::TList {
                                                                                   const auto utf = pb.ToString<true>(pb.Nth(item, 1U));
                                                                                   const auto day = pb.StrictFromString(pb.Substring(utf, NTest::ConvertValueToLiteralNode(pb, ui32(8)), NTest::ConvertValueToLiteralNode(pb, ui32(2))), NTest::ConvertToMinikqlType<ui8>(pb));
                                                                                   return {pb.Nth(item, 0U), utf, day, pb.Nth(item, 1U)};
                                                                               }),
                                                                  NTest::ConvertValueToLiteralNode(pb, ui64(7)), {{2U, NTest::ConvertValueToLiteralNode(pb, true)}, {0U, NTest::ConvertValueToLiteralNode(pb, false)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return items[1]; }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TStringBuf>{
                                                          "1997-06-01,Africa/Mbabane",
                                                          "1997-07-01,Africa/Mbabane",
                                                          "1997-08-01,Africa/Mbabane",
                                                          "1997-09-01,Africa/Mbabane",
                                                          "1997-10-01,Africa/Mbabane",
                                                          "1997-11-01,Africa/Mbabane",
                                                          "1997-12-01,Africa/Mbabane",
                                                      });
}

Y_UNIT_TEST_LLVM(TopSortLargeCount) {
    TSetup<LLVM> setup;
    setup.Alloc.SetLimit(1_MB);
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<TStringBuf, TStringBuf>>(pb);
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TStringBuf, TStringBuf>>{
                                                               {"key one", "value 1"},
                                                               {"key two", "value 2"},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTopSort(pb.ExpandMap(pb.ToFlow(list),
                                                                               [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                  NTest::ConvertValueToLiteralNode(pb, ui64(4000000000)), {{0U, NTest::ConvertValueToLiteralNode(pb, true)}, {1U, NTest::ConvertValueToLiteralNode(pb, false)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TStringBuf, TStringBuf>>{
                                                          {"key one", "value 1"},
                                                          {"key two", "value 2"},
                                                      });
}
} // Y_UNIT_TEST_SUITE(TMiniKQLWideTopTest)

Y_UNIT_TEST_SUITE(TMiniKQLWideSortTest) {
Y_UNIT_TEST_LLVM(SortByFirstKeyAsc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<TStringBuf, TStringBuf>>(pb);
    const auto list = NTest::ConvertValueToLiteralNode(pb, LongList);

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideSort(pb.ExpandMap(pb.ToFlow(list),
                                                                            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                               {{0U, NTest::ConvertValueToLiteralNode(pb, true)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TStringBuf, TStringBuf>>{
                                                          {"key one", "very long value 1"},
                                                          {"key two", "very long value 3"},
                                                          {"key two", "very long value 2"},
                                                          {"very long key one", "very long value 4"},
                                                          {"very long key two", "very long value 9"},
                                                          {"very long key two", "very long value 8"},
                                                          {"very long key two", "very long value 7"},
                                                          {"very long key two", "very long value 6"},
                                                          {"very long key two", "very long value 5"},
                                                      });
}

Y_UNIT_TEST_LLVM(SortByExtDateTypeAsc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto extDateType = pb.NewDataType(NUdf::TDataType<NUdf::TDate32>::Id);
    const auto stringType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
    const auto tupleType = pb.NewTupleType({extDateType, stringType});

    i32 extDate1 = NUdf::MIN_DATE32;
    i32 extDate2 = NUdf::MAX_DATE32;
    i32 extDate3 = 1000;
    i32 extDate4 = -1000;
    i32 extDate5 = 0;

    const auto extDate1Val = pb.NewDataLiteral<NUdf::EDataSlot::Date32>(
        NUdf::TStringRef((const char*)&extDate1, sizeof(extDate1)));
    const auto extDate2Val = pb.NewDataLiteral<NUdf::EDataSlot::Date32>(
        NUdf::TStringRef((const char*)&extDate2, sizeof(extDate2)));
    const auto extDate3Val = pb.NewDataLiteral<NUdf::EDataSlot::Date32>(
        NUdf::TStringRef((const char*)&extDate3, sizeof(extDate3)));
    const auto extDate4Val = pb.NewDataLiteral<NUdf::EDataSlot::Date32>(
        NUdf::TStringRef((const char*)&extDate4, sizeof(extDate4)));
    const auto extDate5Val = pb.NewDataLiteral<NUdf::EDataSlot::Date32>(
        NUdf::TStringRef((const char*)&extDate5, sizeof(extDate5)));

    const auto list = pb.NewList(tupleType, {
                                                pb.NewTuple(tupleType, {extDate1Val, NTest::ConvertValueToLiteralNode(pb, TStringBuf("value1"))}),
                                                pb.NewTuple(tupleType, {extDate2Val, NTest::ConvertValueToLiteralNode(pb, TStringBuf("value2"))}),
                                                pb.NewTuple(tupleType, {extDate3Val, NTest::ConvertValueToLiteralNode(pb, TStringBuf("value3"))}),
                                                pb.NewTuple(tupleType, {extDate4Val, NTest::ConvertValueToLiteralNode(pb, TStringBuf("value4"))}),
                                                pb.NewTuple(tupleType, {extDate5Val, NTest::ConvertValueToLiteralNode(pb, TStringBuf("value5"))}),
                                            });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideSort(pb.ExpandMap(pb.ToFlow(list),
                                                                            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                               {{0U, NTest::ConvertValueToLiteralNode(pb, true)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<i32, TStringBuf>>{
                                                          {extDate1, "value1"},
                                                          {extDate4, "value4"},
                                                          {extDate5, "value5"},
                                                          {extDate3, "value3"},
                                                          {extDate2, "value2"},
                                                      });
}

Y_UNIT_TEST_LLVM_SPILLING(SortWithSpillingMultiKey) {
    TSetup<LLVM, SPILLING> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto ui64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto tupleType = pb.NewTupleType({ui64Type, ui64Type});

    const ui64 N = 3000;
    TRuntimeNode::TList items;
    items.reserve(N);
    for (ui64 i = N; i >= 1; --i) {
        items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(i), pb.NewDataLiteral<ui64>(i * 100)}));
    }

    const auto list = pb.NewList(tupleType, items);

    const auto pgmReturn = pb.FromFlow(pb.NarrowMap(pb.WideSort(pb.ExpandMap(pb.ToFlow(list),
                                                                             [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                {{0U, pb.NewDataLiteral<bool>(true)}, {1U, pb.NewDataLiteral<bool>(false)}}),
                                                    [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    if (SPILLING) {
        setup.RenameCallable(pgmReturn, "WideSort", "WideSortWithSpilling");
    }

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto spillerFactory = std::make_shared<TMockSpillerFactory>();
    if (SPILLING) {
        graph->GetContext().SpillerFactory = spillerFactory;
    }
    const auto streamVal = graph->GetValue();
    NUdf::TUnboxedValue item;

    for (ui64 i = 1; i <= N; ++i) {
        NUdf::EFetchStatus status;
        do {
            status = streamVal.Fetch(item);
            UNIT_ASSERT_C(status != NUdf::EFetchStatus::Finish, TStringBuilder() << "Lost row " << i);
        } while (status == NUdf::EFetchStatus::Yield);
        UNIT_ASSERT_VALUES_EQUAL_C(item.GetElement(0).Get<ui64>(), i,
                                   TStringBuilder() << "Row " << i << ": expected key=" << i << " got=" << item.GetElement(0).Get<ui64>());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).Get<ui64>(), i * 100);
    }
    UNIT_ASSERT_EQUAL(streamVal.Fetch(item), NUdf::EFetchStatus::Finish);

    if (SPILLING) {
        const auto& spillers = spillerFactory->GetCreatedSpillers();
        UNIT_ASSERT_C(!spillers.empty(), "Expected spiller to be created");
        const auto mockSpiller = std::dynamic_pointer_cast<TMockSpiller>(spillers.front());
        UNIT_ASSERT_C(mockSpiller != nullptr, "Expected TMockSpiller");
        UNIT_ASSERT_C(mockSpiller->GetTotalSpilled() > 0, "Expected data to be spilled");

        const size_t expectedPuts = (N + 1023) / 1024;
        const auto totalPuts = mockSpiller->GetPutSizes().size();
        UNIT_ASSERT_C(totalPuts == expectedPuts, "All the data is expected to be spilled");
    }
}

} // Y_UNIT_TEST_SUITE(TMiniKQLWideSortTest)

} // namespace NMiniKQL
} // namespace NKikimr
