#include "mkql_computation_node_ut.h"
#include <yql/essentials/minikql/mkql_runtime_version.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mock_spiller_factory_ut.h>

#include <cstring>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLWideTopTest) {
Y_UNIT_TEST_LLVM(TopByFirstKeyAsc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
    const auto tupleType = pb.NewTupleType({dataType, dataType});

    const auto keyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("key one");
    const auto keyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("key two");

    const auto longKeyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key one");
    const auto longKeyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key two");

    const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 1");
    const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 2");
    const auto value3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 3");
    const auto value4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 4");
    const auto value5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 5");
    const auto value6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 6");
    const auto value7 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 7");
    const auto value8 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 8");
    const auto value9 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 9");

    const auto data1 = pb.NewTuple(tupleType, {keyOne, value1});

    const auto data2 = pb.NewTuple(tupleType, {keyTwo, value2});
    const auto data3 = pb.NewTuple(tupleType, {keyTwo, value3});

    const auto data4 = pb.NewTuple(tupleType, {longKeyOne, value4});

    const auto data5 = pb.NewTuple(tupleType, {longKeyTwo, value5});
    const auto data6 = pb.NewTuple(tupleType, {longKeyTwo, value6});
    const auto data7 = pb.NewTuple(tupleType, {longKeyTwo, value7});
    const auto data8 = pb.NewTuple(tupleType, {longKeyTwo, value8});
    const auto data9 = pb.NewTuple(tupleType, {longKeyTwo, value9});

    const auto list = pb.NewList(tupleType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTop(pb.ExpandMap(pb.ToFlow(list),
                                                                           [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                              pb.NewDataLiteral<ui64>(4ULL), {{0U, pb.NewDataLiteral<bool>(true)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key one");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 4");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 3");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 2");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "key one");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 1");
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TopByFirstKeyDesc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
    const auto tupleType = pb.NewTupleType({dataType, dataType});

    const auto keyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("key one");
    const auto keyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("key two");

    const auto longKeyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key one");
    const auto longKeyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key two");

    const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 1");
    const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 2");
    const auto value3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 3");
    const auto value4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 4");
    const auto value5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 5");
    const auto value6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 6");
    const auto value7 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 7");
    const auto value8 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 8");
    const auto value9 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 9");

    const auto data1 = pb.NewTuple(tupleType, {keyOne, value1});

    const auto data2 = pb.NewTuple(tupleType, {keyTwo, value2});
    const auto data3 = pb.NewTuple(tupleType, {keyTwo, value3});

    const auto data4 = pb.NewTuple(tupleType, {longKeyOne, value4});

    const auto data5 = pb.NewTuple(tupleType, {longKeyTwo, value5});
    const auto data6 = pb.NewTuple(tupleType, {longKeyTwo, value6});
    const auto data7 = pb.NewTuple(tupleType, {longKeyTwo, value7});
    const auto data8 = pb.NewTuple(tupleType, {longKeyTwo, value8});
    const auto data9 = pb.NewTuple(tupleType, {longKeyTwo, value9});

    const auto list = pb.NewList(tupleType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTop(pb.ExpandMap(pb.ToFlow(list),
                                                                           [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                              pb.NewDataLiteral<ui64>(6ULL), {{0U, pb.NewDataLiteral<bool>(false)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key one");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 4");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 7");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 5");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 6");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 8");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 9");
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TopBySecondKeyAsc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
    const auto tupleType = pb.NewTupleType({dataType, dataType});

    const auto keyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("key one");
    const auto keyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("key two");

    const auto longKeyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key one");
    const auto longKeyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key two");

    const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 1");
    const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 2");
    const auto value3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 3");
    const auto value4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 4");
    const auto value5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 5");
    const auto value6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 6");
    const auto value7 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 7");
    const auto value8 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 8");
    const auto value9 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 9");

    const auto data1 = pb.NewTuple(tupleType, {keyOne, value1});

    const auto data2 = pb.NewTuple(tupleType, {keyTwo, value2});
    const auto data3 = pb.NewTuple(tupleType, {keyTwo, value3});

    const auto data4 = pb.NewTuple(tupleType, {longKeyOne, value4});

    const auto data5 = pb.NewTuple(tupleType, {longKeyTwo, value5});
    const auto data6 = pb.NewTuple(tupleType, {longKeyTwo, value6});
    const auto data7 = pb.NewTuple(tupleType, {longKeyTwo, value7});
    const auto data8 = pb.NewTuple(tupleType, {longKeyTwo, value8});
    const auto data9 = pb.NewTuple(tupleType, {longKeyTwo, value9});

    const auto list = pb.NewList(tupleType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTop(pb.ExpandMap(pb.ToFlow(list),
                                                                           [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                              pb.NewDataLiteral<ui64>(3ULL), {{1U, pb.NewDataLiteral<bool>(true)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 3");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 2");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "key one");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 1");
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TopBySecondKeyDesc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
    const auto tupleType = pb.NewTupleType({dataType, dataType});

    const auto keyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("key one");
    const auto keyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("key two");

    const auto longKeyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key one");
    const auto longKeyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key two");

    const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 1");
    const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 2");
    const auto value3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 3");
    const auto value4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 4");
    const auto value5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 5");
    const auto value6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 6");
    const auto value7 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 7");
    const auto value8 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 8");
    const auto value9 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 9");

    const auto data1 = pb.NewTuple(tupleType, {keyOne, value1});

    const auto data2 = pb.NewTuple(tupleType, {keyTwo, value2});
    const auto data3 = pb.NewTuple(tupleType, {keyTwo, value3});

    const auto data4 = pb.NewTuple(tupleType, {longKeyOne, value4});

    const auto data5 = pb.NewTuple(tupleType, {longKeyTwo, value5});
    const auto data6 = pb.NewTuple(tupleType, {longKeyTwo, value6});
    const auto data7 = pb.NewTuple(tupleType, {longKeyTwo, value7});
    const auto data8 = pb.NewTuple(tupleType, {longKeyTwo, value8});
    const auto data9 = pb.NewTuple(tupleType, {longKeyTwo, value9});

    const auto list = pb.NewList(tupleType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTop(pb.ExpandMap(pb.ToFlow(list),
                                                                           [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                              pb.NewDataLiteral<ui64>(2ULL), {{1U, pb.NewDataLiteral<bool>(false)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 8");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 9");
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TopWithLargeCount) {
    TSetup<LLVM> setup;
    setup.Alloc.SetLimit(1_MB);
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
    const auto tupleType = pb.NewTupleType({dataType, dataType});

    const auto keyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("key one");
    const auto keyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("key two");

    const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 1");
    const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 2");

    const auto data1 = pb.NewTuple(tupleType, {keyOne, value1});
    const auto data2 = pb.NewTuple(tupleType, {keyTwo, value2});

    const auto list = pb.NewList(tupleType, {data1, data2});

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTop(pb.ExpandMap(pb.ToFlow(list),
                                                                           [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                              pb.NewDataLiteral<ui64>(4000000000ULL), {{0U, pb.NewDataLiteral<bool>(true)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "value 2");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "key one");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "value 1");
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TopSortByFirstSecondAscDesc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
    const auto tupleType = pb.NewTupleType({dataType, dataType});

    const auto keyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("key one");
    const auto keyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("key two");

    const auto longKeyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key one");
    const auto longKeyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key two");

    const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 1");
    const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 2");
    const auto value3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 3");
    const auto value4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 4");
    const auto value5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 5");
    const auto value6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 6");
    const auto value7 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 7");
    const auto value8 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 8");
    const auto value9 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 9");

    const auto data1 = pb.NewTuple(tupleType, {keyOne, value1});

    const auto data2 = pb.NewTuple(tupleType, {keyTwo, value2});
    const auto data3 = pb.NewTuple(tupleType, {keyTwo, value3});

    const auto data4 = pb.NewTuple(tupleType, {longKeyOne, value4});

    const auto data5 = pb.NewTuple(tupleType, {longKeyTwo, value5});
    const auto data6 = pb.NewTuple(tupleType, {longKeyTwo, value6});
    const auto data7 = pb.NewTuple(tupleType, {longKeyTwo, value7});
    const auto data8 = pb.NewTuple(tupleType, {longKeyTwo, value8});
    const auto data9 = pb.NewTuple(tupleType, {longKeyTwo, value9});

    const auto list = pb.NewList(tupleType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTopSort(pb.ExpandMap(pb.ToFlow(list),
                                                                               [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                  pb.NewDataLiteral<ui64>(4ULL), {{0U, pb.NewDataLiteral<bool>(true)}, {1U, pb.NewDataLiteral<bool>(false)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "key one");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 1");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 3");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 2");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key one");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 4");
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TopSortByFirstSecondDescAsc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
    const auto tupleType = pb.NewTupleType({dataType, dataType});

    const auto keyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("key one");
    const auto keyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("key two");

    const auto longKeyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key one");
    const auto longKeyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key two");

    const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 1");
    const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 2");
    const auto value3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 3");
    const auto value4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 4");
    const auto value5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 5");
    const auto value6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 6");
    const auto value7 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 7");
    const auto value8 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 8");
    const auto value9 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 9");

    const auto data1 = pb.NewTuple(tupleType, {keyOne, value1});

    const auto data2 = pb.NewTuple(tupleType, {keyTwo, value2});
    const auto data3 = pb.NewTuple(tupleType, {keyTwo, value3});

    const auto data4 = pb.NewTuple(tupleType, {longKeyOne, value4});

    const auto data5 = pb.NewTuple(tupleType, {longKeyTwo, value5});
    const auto data6 = pb.NewTuple(tupleType, {longKeyTwo, value6});
    const auto data7 = pb.NewTuple(tupleType, {longKeyTwo, value7});
    const auto data8 = pb.NewTuple(tupleType, {longKeyTwo, value8});
    const auto data9 = pb.NewTuple(tupleType, {longKeyTwo, value9});

    const auto list = pb.NewList(tupleType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTopSort(pb.ExpandMap(pb.ToFlow(list),
                                                                               [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                  pb.NewDataLiteral<ui64>(6ULL), {{0U, pb.NewDataLiteral<bool>(false)}, {1U, pb.NewDataLiteral<bool>(true)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 5");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 6");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 7");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 8");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 9");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key one");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 4");
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TopSortBySecondFirstAscDesc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
    const auto tupleType = pb.NewTupleType({dataType, dataType});

    const auto keyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("key one");
    const auto keyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("key two");

    const auto longKeyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key one");
    const auto longKeyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key two");

    const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 1");
    const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 2");
    const auto value3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 3");
    const auto value4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 4");
    const auto value5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 5");
    const auto value6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 6");
    const auto value7 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 7");
    const auto value8 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 8");
    const auto value9 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 9");

    const auto data1 = pb.NewTuple(tupleType, {keyOne, value1});

    const auto data2 = pb.NewTuple(tupleType, {keyTwo, value2});
    const auto data3 = pb.NewTuple(tupleType, {keyTwo, value3});

    const auto data4 = pb.NewTuple(tupleType, {longKeyOne, value4});

    const auto data5 = pb.NewTuple(tupleType, {longKeyTwo, value5});
    const auto data6 = pb.NewTuple(tupleType, {longKeyTwo, value6});
    const auto data7 = pb.NewTuple(tupleType, {longKeyTwo, value7});
    const auto data8 = pb.NewTuple(tupleType, {longKeyTwo, value8});
    const auto data9 = pb.NewTuple(tupleType, {longKeyTwo, value9});

    const auto list = pb.NewList(tupleType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTopSort(pb.ExpandMap(pb.ToFlow(list),
                                                                               [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                  pb.NewDataLiteral<ui64>(4ULL), {{1U, pb.NewDataLiteral<bool>(true)}, {0U, pb.NewDataLiteral<bool>(false)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "key one");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 1");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 2");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 3");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key one");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 4");
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TopSortBySecondFirstDescAsc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
    const auto tupleType = pb.NewTupleType({dataType, dataType});

    const auto keyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("key one");
    const auto keyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("key two");

    const auto longKeyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key one");
    const auto longKeyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key two");

    const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 1");
    const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 2");
    const auto value3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 3");
    const auto value4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 4");
    const auto value5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 5");
    const auto value6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 6");
    const auto value7 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 7");
    const auto value8 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 8");
    const auto value9 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 9");

    const auto data1 = pb.NewTuple(tupleType, {keyOne, value1});

    const auto data2 = pb.NewTuple(tupleType, {keyTwo, value2});
    const auto data3 = pb.NewTuple(tupleType, {keyTwo, value3});

    const auto data4 = pb.NewTuple(tupleType, {longKeyOne, value4});

    const auto data5 = pb.NewTuple(tupleType, {longKeyTwo, value5});
    const auto data6 = pb.NewTuple(tupleType, {longKeyTwo, value6});
    const auto data7 = pb.NewTuple(tupleType, {longKeyTwo, value7});
    const auto data8 = pb.NewTuple(tupleType, {longKeyTwo, value8});
    const auto data9 = pb.NewTuple(tupleType, {longKeyTwo, value9});

    const auto list = pb.NewList(tupleType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTopSort(pb.ExpandMap(pb.ToFlow(list),
                                                                               [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                  pb.NewDataLiteral<ui64>(6ULL), {{1U, pb.NewDataLiteral<bool>(false)}, {0U, pb.NewDataLiteral<bool>(true)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 9");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 8");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 7");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 6");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 5");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key one");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 4");
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
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
                                                                                   const auto day = pb.StrictFromString(pb.Substring(utf, pb.NewDataLiteral<ui32>(8U), pb.NewDataLiteral<ui32>(2U)), pb.NewDataType(NUdf::EDataSlot::Uint8));
                                                                                   return {pb.Nth(item, 0U), utf, day, pb.Nth(item, 1U)};
                                                                               }),
                                                                  pb.NewDataLiteral<ui64>(7ULL), {{2U, pb.NewDataLiteral<bool>(true)}, {0U, pb.NewDataLiteral<bool>(false)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return items[1]; }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "1997-06-01,Africa/Mbabane");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "1997-07-01,Africa/Mbabane");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "1997-08-01,Africa/Mbabane");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "1997-09-01,Africa/Mbabane");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "1997-10-01,Africa/Mbabane");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "1997-11-01,Africa/Mbabane");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "1997-12-01,Africa/Mbabane");
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TopSortLargeCount) {
    TSetup<LLVM> setup;
    setup.Alloc.SetLimit(1_MB);
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
    const auto tupleType = pb.NewTupleType({dataType, dataType});

    const auto keyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("key one");
    const auto keyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("key two");

    const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 1");
    const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 2");

    const auto data1 = pb.NewTuple(tupleType, {keyOne, value1});
    const auto data2 = pb.NewTuple(tupleType, {keyTwo, value2});

    const auto list = pb.NewList(tupleType, {data1, data2});

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTopSort(pb.ExpandMap(pb.ToFlow(list),
                                                                               [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                  pb.NewDataLiteral<ui64>(4000000000ULL), {{0U, pb.NewDataLiteral<bool>(true)}, {1U, pb.NewDataLiteral<bool>(false)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "key one");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "value 1");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "value 2");
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}
} // Y_UNIT_TEST_SUITE(TMiniKQLWideTopTest)

Y_UNIT_TEST_SUITE(TMiniKQLWideSortTest) {
Y_UNIT_TEST_LLVM(SortByFirstKeyAsc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    std::cout << "MISHA TEST" << std::endl;

    const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
    const auto tupleType = pb.NewTupleType({dataType, dataType});

    const auto keyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("key one");
    const auto keyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("key two");

    const auto longKeyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key one");
    const auto longKeyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key two");

    const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 1");
    const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 2");
    const auto value3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 3");
    const auto value4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 4");
    const auto value5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 5");
    const auto value6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 6");
    const auto value7 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 7");
    const auto value8 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 8");
    const auto value9 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 9");

    const auto data1 = pb.NewTuple(tupleType, {keyOne, value1});

    const auto data2 = pb.NewTuple(tupleType, {keyTwo, value2});
    const auto data3 = pb.NewTuple(tupleType, {keyTwo, value3});

    const auto data4 = pb.NewTuple(tupleType, {longKeyOne, value4});

    const auto data5 = pb.NewTuple(tupleType, {longKeyTwo, value5});
    const auto data6 = pb.NewTuple(tupleType, {longKeyTwo, value6});
    const auto data7 = pb.NewTuple(tupleType, {longKeyTwo, value7});
    const auto data8 = pb.NewTuple(tupleType, {longKeyTwo, value8});
    const auto data9 = pb.NewTuple(tupleType, {longKeyTwo, value9});

    const auto list = pb.NewList(tupleType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideSort(pb.ExpandMap(pb.ToFlow(list),
                                                                            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                               {{0U, pb.NewDataLiteral<bool>(true)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "key one");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 1");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 3");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 2");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key one");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 4");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 9");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 8");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 7");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 6");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "very long key two");
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "very long value 5");
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
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

    const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value1");
    const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value2");
    const auto value3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value3");
    const auto value4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value4");
    const auto value5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value5");

    const auto data1 = pb.NewTuple(tupleType, {extDate1Val, value1});
    const auto data2 = pb.NewTuple(tupleType, {extDate2Val, value2});
    const auto data3 = pb.NewTuple(tupleType, {extDate3Val, value3});
    const auto data4 = pb.NewTuple(tupleType, {extDate4Val, value4});
    const auto data5 = pb.NewTuple(tupleType, {extDate5Val, value5});

    const auto list = pb.NewList(tupleType, {data1, data2, data3, data4, data5});

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideSort(pb.ExpandMap(pb.ToFlow(list),
                                                                            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                               {{0U, pb.NewDataLiteral<bool>(true)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;

    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_EQUAL(item.GetElement(0).Get<i32>(), extDate1);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "value1");

    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_EQUAL(item.GetElement(0).Get<i32>(), extDate4);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "value4");

    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_EQUAL(item.GetElement(0).Get<i32>(), extDate5);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "value5");

    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_EQUAL(item.GetElement(0).Get<i32>(), extDate3);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "value3");

    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_EQUAL(item.GetElement(0).Get<i32>(), extDate2);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "value2");

    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}
} // Y_UNIT_TEST_SUITE(TMiniKQLWideSortTest)

Y_UNIT_TEST_SUITE(TMiniKQLWideSortSpillingTest) {
Y_UNIT_TEST_LLVM_SPILLING(SortByFirstKeyAsc) {
    TSetup<LLVM, SPILLING> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    std::cout << "MISHA OUT" << std::endl;
    std::cerr << "MISHA ERR" << std::endl;

    const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
    const auto tupleType = pb.NewTupleType({dataType, dataType});

    const auto keyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("key one");
    const auto keyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("key two");

    const auto longKeyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key one");
    const auto longKeyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key two");

    const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 1");
    const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 2");
    const auto value3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 3");
    const auto value4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 4");
    const auto value5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 5");
    const auto value6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 6");
    const auto value7 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 7");
    const auto value8 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 8");
    const auto value9 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 9");

    const auto data1 = pb.NewTuple(tupleType, {keyOne, value1});

    const auto data2 = pb.NewTuple(tupleType, {keyTwo, value2});
    const auto data3 = pb.NewTuple(tupleType, {keyTwo, value3});

    const auto data4 = pb.NewTuple(tupleType, {longKeyOne, value4});

    const auto data5 = pb.NewTuple(tupleType, {longKeyTwo, value5});
    const auto data6 = pb.NewTuple(tupleType, {longKeyTwo, value6});
    const auto data7 = pb.NewTuple(tupleType, {longKeyTwo, value7});
    const auto data8 = pb.NewTuple(tupleType, {longKeyTwo, value8});
    const auto data9 = pb.NewTuple(tupleType, {longKeyTwo, value9});

    const auto list = pb.NewList(tupleType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

    const auto pgmReturn = pb.FromFlow(pb.NarrowMap(pb.WideSort(pb.ExpandMap(pb.ToFlow(list),
                                                                             [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                {{0U, pb.NewDataLiteral<bool>(true)}}),
                                                    [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    if (SPILLING) {
        graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
    }

    const auto streamVal = graph->GetValue();
    NUdf::TUnboxedValue item;
    NUdf::EFetchStatus fetchStatus;

    std::vector<std::pair<TString, TString>> expected = {
        {"key one", "very long value 1"},
        {"key two", "very long value 3"},
        {"key two", "very long value 2"},
        {"very long key one", "very long value 4"},
        {"very long key two", "very long value 9"},
        {"very long key two", "very long value 8"},
        {"very long key two", "very long value 7"},
        {"very long key two", "very long value 6"},
        {"very long key two", "very long value 5"},
    };

    size_t idx = 0;
    while (idx < expected.size()) {
        fetchStatus = streamVal.Fetch(item);
        UNIT_ASSERT_UNEQUAL(fetchStatus, NUdf::EFetchStatus::Finish);
        if (fetchStatus == NUdf::EFetchStatus::Yield) {
            continue;
        }
        const auto el0 = item.GetElement(0);
        const auto el1 = item.GetElement(1);
        UNIT_ASSERT_VALUES_EQUAL(TString(el0.AsStringRef()), expected[idx].first);
        UNIT_ASSERT_VALUES_EQUAL(TString(el1.AsStringRef()), expected[idx].second);
        ++idx;
    }
    fetchStatus = streamVal.Fetch(item);
    UNIT_ASSERT_EQUAL(fetchStatus, NUdf::EFetchStatus::Finish);
}

Y_UNIT_TEST_LLVM_SPILLING(SortByUi64Asc) {
    TSetup<LLVM, SPILLING> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto ui64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto strType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
    const auto tupleType = pb.NewTupleType({ui64Type, strType});

    std::vector<TRuntimeNode> items;
    std::vector<std::pair<ui64, TString>> expected;
    for (ui64 i = 0; i < 50; ++i) {
        auto val = 50 - i;
        auto str = TString("value_") + ToString(val);
        items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(val), pb.NewDataLiteral<NUdf::EDataSlot::String>(str)}));
        expected.push_back({val, str});
    }
    std::sort(expected.begin(), expected.end());

    const auto list = pb.NewList(tupleType, items);

    const auto pgmReturn = pb.FromFlow(pb.NarrowMap(pb.WideSort(pb.ExpandMap(pb.ToFlow(list),
                                                                             [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                {{0U, pb.NewDataLiteral<bool>(true)}}),
                                                    [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    if (SPILLING) {
        graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
    }

    const auto streamVal = graph->GetValue();
    NUdf::TUnboxedValue item;
    NUdf::EFetchStatus fetchStatus;

    size_t idx = 0;
    while (idx < expected.size()) {
        fetchStatus = streamVal.Fetch(item);
        UNIT_ASSERT_UNEQUAL(fetchStatus, NUdf::EFetchStatus::Finish);
        if (fetchStatus == NUdf::EFetchStatus::Yield) {
            continue;
        }
        const auto el0 = item.GetElement(0);
        const auto el1 = item.GetElement(1);
        UNIT_ASSERT_VALUES_EQUAL(el0.Get<ui64>(), expected[idx].first);
        UNIT_ASSERT_VALUES_EQUAL(TString(el1.AsStringRef()), expected[idx].second);
        ++idx;
    }
    fetchStatus = streamVal.Fetch(item);
    UNIT_ASSERT_EQUAL(fetchStatus, NUdf::EFetchStatus::Finish);
}
} // Y_UNIT_TEST_SUITE(TMiniKQLWideSortSpillingTest)

} // namespace NMiniKQL
} // namespace NKikimr
