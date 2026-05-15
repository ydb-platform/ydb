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

Y_UNIT_TEST_LLVM_SPILLING(SortWithSpillingBasic) {
    TSetup<LLVM, SPILLING> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto ui64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto strType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
    const auto tupleType = pb.NewTupleType({ui64Type, strType});

    // Create 9 tuples with keys in non-sorted order
    TRuntimeNode::TList items;
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(5), pb.NewDataLiteral<NUdf::EDataSlot::String>("five")}));
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(3), pb.NewDataLiteral<NUdf::EDataSlot::String>("three")}));
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(8), pb.NewDataLiteral<NUdf::EDataSlot::String>("eight")}));
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(1), pb.NewDataLiteral<NUdf::EDataSlot::String>("one")}));
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(9), pb.NewDataLiteral<NUdf::EDataSlot::String>("nine")}));
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(2), pb.NewDataLiteral<NUdf::EDataSlot::String>("two")}));
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(7), pb.NewDataLiteral<NUdf::EDataSlot::String>("seven")}));
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(4), pb.NewDataLiteral<NUdf::EDataSlot::String>("four")}));
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(6), pb.NewDataLiteral<NUdf::EDataSlot::String>("six")}));

    const auto list = pb.NewList(tupleType, items);

    const auto pgmReturn = pb.FromFlow(pb.NarrowMap(pb.WideSort(pb.ExpandMap(pb.ToFlow(list),
                                                                            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                               {{0U, pb.NewDataLiteral<bool>(true)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    if (SPILLING) {
        setup.RenameCallable(pgmReturn, "WideSort", "WideSortWithSpilling");
    }

    const auto spillerFactory = std::make_shared<TMockSpillerFactory>();
    const auto graph = setup.BuildGraph(pgmReturn);
    if (SPILLING) {
        graph->GetContext().SpillerFactory = spillerFactory;
    }
    const auto streamVal = graph->GetValue();
    NUdf::TUnboxedValue item;

    // Expect sorted ascending by first key: 1,2,3,4,5,6,7,8,9
    const NUdf::TStringRef expectedValues[] = {
        NUdf::TStringRef::Of("one"), NUdf::TStringRef::Of("two"), NUdf::TStringRef::Of("three"),
        NUdf::TStringRef::Of("four"), NUdf::TStringRef::Of("five"), NUdf::TStringRef::Of("six"),
        NUdf::TStringRef::Of("seven"), NUdf::TStringRef::Of("eight"), NUdf::TStringRef::Of("nine")
    };
    for (ui64 i = 1; i <= 9; ++i) {
        NUdf::EFetchStatus status;
        do {
            status = streamVal.Fetch(item);
            UNIT_ASSERT_C(status != NUdf::EFetchStatus::Finish, TStringBuilder() << "Unexpected end at row " << i);
        } while (status == NUdf::EFetchStatus::Yield);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<ui64>(), i);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), expectedValues[i - 1]);
    }
    UNIT_ASSERT_EQUAL(streamVal.Fetch(item), NUdf::EFetchStatus::Finish);

    if (SPILLING) {
        const auto& spillers = spillerFactory->GetCreatedSpillers();
        UNIT_ASSERT_C(!spillers.empty(), "Expected spiller to be created");
        const auto mockSpiller = std::dynamic_pointer_cast<TMockSpiller>(spillers.front());
        UNIT_ASSERT_C(mockSpiller != nullptr, "Expected TMockSpiller");
        UNIT_ASSERT_C(mockSpiller->GetTotalSpilled() > 0, "Expected data to be spilled");
    }
}

Y_UNIT_TEST_LLVM_SPILLING(SortWithSpillingDescending) {
    TSetup<LLVM, SPILLING> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto ui64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto strType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
    const auto tupleType = pb.NewTupleType({ui64Type, strType});

    TRuntimeNode::TList items;
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(5), pb.NewDataLiteral<NUdf::EDataSlot::String>("five")}));
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(3), pb.NewDataLiteral<NUdf::EDataSlot::String>("three")}));
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(8), pb.NewDataLiteral<NUdf::EDataSlot::String>("eight")}));
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(1), pb.NewDataLiteral<NUdf::EDataSlot::String>("one")}));
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(9), pb.NewDataLiteral<NUdf::EDataSlot::String>("nine")}));
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(2), pb.NewDataLiteral<NUdf::EDataSlot::String>("two")}));
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(7), pb.NewDataLiteral<NUdf::EDataSlot::String>("seven")}));
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(4), pb.NewDataLiteral<NUdf::EDataSlot::String>("four")}));
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(6), pb.NewDataLiteral<NUdf::EDataSlot::String>("six")}));

    const auto list = pb.NewList(tupleType, items);

    const auto pgmReturn = pb.FromFlow(pb.NarrowMap(pb.WideSort(pb.ExpandMap(pb.ToFlow(list),
                                                                            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                               {{0U, pb.NewDataLiteral<bool>(false)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    if (SPILLING) {
        setup.RenameCallable(pgmReturn, "WideSort", "WideSortWithSpilling");
    }

    const auto spillerFactory = std::make_shared<TMockSpillerFactory>();
    const auto graph = setup.BuildGraph(pgmReturn);
    if (SPILLING) {
        graph->GetContext().SpillerFactory = spillerFactory;
    }
    const auto streamVal = graph->GetValue();
    NUdf::TUnboxedValue item;

    // Expect sorted descending: 9,8,7,6,5,4,3,2,1
    const NUdf::TStringRef expectedValues[] = {
        NUdf::TStringRef::Of("nine"), NUdf::TStringRef::Of("eight"), NUdf::TStringRef::Of("seven"),
        NUdf::TStringRef::Of("six"), NUdf::TStringRef::Of("five"), NUdf::TStringRef::Of("four"),
        NUdf::TStringRef::Of("three"), NUdf::TStringRef::Of("two"), NUdf::TStringRef::Of("one")
    };
    for (ui64 i = 0; i < 9; ++i) {
        NUdf::EFetchStatus status;
        do {
            status = streamVal.Fetch(item);
            UNIT_ASSERT_C(status != NUdf::EFetchStatus::Finish, TStringBuilder() << "Unexpected end at row " << i);
        } while (status == NUdf::EFetchStatus::Yield);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<ui64>(), 9 - i);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), expectedValues[i]);
    }
    UNIT_ASSERT_EQUAL(streamVal.Fetch(item), NUdf::EFetchStatus::Finish);

    if (SPILLING) {
        const auto& spillers = spillerFactory->GetCreatedSpillers();
        UNIT_ASSERT_C(!spillers.empty(), "Expected spiller to be created");
        const auto mockSpiller = std::dynamic_pointer_cast<TMockSpiller>(spillers.front());
        UNIT_ASSERT_C(mockSpiller != nullptr, "Expected TMockSpiller");
        UNIT_ASSERT_C(mockSpiller->GetTotalSpilled() > 0, "Expected data to be spilled");
    }
}

Y_UNIT_TEST_LLVM_SPILLING(SortWithSpillingManyRowsTriggersMerge) {
    // This test generates enough rows to create > MaxMergeWidth (10) spilled runs.
    // With MinSpillBatchRows=2 and yellow zone always on (SPILLING=true),
    // every 2 rows trigger a spill. 30 rows => 15 spills => triggers multi-level merge.
    TSetup<LLVM, SPILLING> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto ui64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto tupleType = pb.NewTupleType({ui64Type, ui64Type});

    constexpr ui64 N = 30;
    TRuntimeNode::TList items;
    // Insert in reverse order to make sorting non-trivial
    for (ui64 i = N; i >= 1; --i) {
        items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(i), pb.NewDataLiteral<ui64>(i * 100)}));
    }

    const auto list = pb.NewList(tupleType, items);

    const auto pgmReturn = pb.FromFlow(pb.NarrowMap(pb.WideSort(pb.ExpandMap(pb.ToFlow(list),
                                                                            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                               {{0U, pb.NewDataLiteral<bool>(true)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    if (SPILLING) {
        setup.RenameCallable(pgmReturn, "WideSort", "WideSortWithSpilling");
    }

    const auto spillerFactory = std::make_shared<TMockSpillerFactory>();
    const auto graph = setup.BuildGraph(pgmReturn);
    if (SPILLING) {
        graph->GetContext().SpillerFactory = spillerFactory;
    }
    const auto streamVal = graph->GetValue();
    NUdf::TUnboxedValue item;

    // Verify all N rows come out in ascending order
    for (ui64 i = 1; i <= N; ++i) {
        NUdf::EFetchStatus status;
        do {
            status = streamVal.Fetch(item);
            UNIT_ASSERT_C(status != NUdf::EFetchStatus::Finish, TStringBuilder() << "Expected row " << i << " but got end of stream");
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

        // With 30 rows and MinSpillBatchRows=2, we get 15 initial spill batches.
        // When spilled runs reach MaxMergeWidth (10), the two smallest are merged.
        // Merge reads 2 runs and writes the merged result back as additional Put(s).
        // So total Put count must be strictly greater than 15 (the initial spills),
        // proving that merge actually wrote data back to the spiller.
        const size_t initialSpillBatches = N / 2; // MinSpillBatchRows = 2
        const auto totalPuts = mockSpiller->GetPutSizes().size();
        UNIT_ASSERT_C(totalPuts > initialSpillBatches,
            TStringBuilder() << "Expected merge to produce additional Put operations beyond "
                             << initialSpillBatches << " initial spills, but got only " << totalPuts
                             << " total Puts. Merge did not happen.");
    }
}

Y_UNIT_TEST_LLVM_SPILLING(SortWithSpillingMultiKey) {
    // Test multi-key sort with spilling: sort by (key1 ASC, key2 DESC)
    TSetup<LLVM, SPILLING> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto strType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
    const auto ui64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto tupleType = pb.NewTupleType({strType, ui64Type});

    TRuntimeNode::TList items;
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<NUdf::EDataSlot::String>("b"), pb.NewDataLiteral<ui64>(1)}));
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<NUdf::EDataSlot::String>("a"), pb.NewDataLiteral<ui64>(3)}));
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<NUdf::EDataSlot::String>("b"), pb.NewDataLiteral<ui64>(3)}));
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<NUdf::EDataSlot::String>("a"), pb.NewDataLiteral<ui64>(1)}));
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<NUdf::EDataSlot::String>("c"), pb.NewDataLiteral<ui64>(2)}));
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<NUdf::EDataSlot::String>("a"), pb.NewDataLiteral<ui64>(2)}));
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<NUdf::EDataSlot::String>("b"), pb.NewDataLiteral<ui64>(2)}));
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<NUdf::EDataSlot::String>("c"), pb.NewDataLiteral<ui64>(1)}));
    items.push_back(pb.NewTuple(tupleType, {pb.NewDataLiteral<NUdf::EDataSlot::String>("c"), pb.NewDataLiteral<ui64>(3)}));

    const auto list = pb.NewList(tupleType, items);

    // Sort by key0 ASC, key1 DESC
    const auto pgmReturn = pb.FromFlow(pb.NarrowMap(pb.WideSort(pb.ExpandMap(pb.ToFlow(list),
                                                                            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                               {{0U, pb.NewDataLiteral<bool>(true)}, {1U, pb.NewDataLiteral<bool>(false)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    if (SPILLING) {
        setup.RenameCallable(pgmReturn, "WideSort", "WideSortWithSpilling");
    }

    const auto spillerFactory = std::make_shared<TMockSpillerFactory>();
    const auto graph = setup.BuildGraph(pgmReturn);
    if (SPILLING) {
        graph->GetContext().SpillerFactory = spillerFactory;
    }
    const auto streamVal = graph->GetValue();
    NUdf::TUnboxedValue item;

    // Expected order: (a,3), (a,2), (a,1), (b,3), (b,2), (b,1), (c,3), (c,2), (c,1)
    const NUdf::TStringRef expectedKeys[] = {
        NUdf::TStringRef::Of("a"), NUdf::TStringRef::Of("a"), NUdf::TStringRef::Of("a"),
        NUdf::TStringRef::Of("b"), NUdf::TStringRef::Of("b"), NUdf::TStringRef::Of("b"),
        NUdf::TStringRef::Of("c"), NUdf::TStringRef::Of("c"), NUdf::TStringRef::Of("c")
    };
    const ui64 expectedVals[] = {3, 2, 1, 3, 2, 1, 3, 2, 1};
    for (size_t i = 0; i < 9; ++i) {
        NUdf::EFetchStatus status;
        do {
            status = streamVal.Fetch(item);
            UNIT_ASSERT_C(status != NUdf::EFetchStatus::Finish, TStringBuilder() << "Expected row " << i);
        } while (status == NUdf::EFetchStatus::Yield);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), expectedKeys[i]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).Get<ui64>(), expectedVals[i]);
    }
    UNIT_ASSERT_EQUAL(streamVal.Fetch(item), NUdf::EFetchStatus::Finish);

    if (SPILLING) {
        const auto& spillers = spillerFactory->GetCreatedSpillers();
        UNIT_ASSERT_C(!spillers.empty(), "Expected spiller to be created");
        const auto mockSpiller = std::dynamic_pointer_cast<TMockSpiller>(spillers.front());
        UNIT_ASSERT_C(mockSpiller != nullptr, "Expected TMockSpiller");
        UNIT_ASSERT_C(mockSpiller->GetTotalSpilled() > 0, "Expected data to be spilled");
    }
}

Y_UNIT_TEST_LLVM_SPILLING(SortWithSpillingEmpty) {
    // Edge case: empty input with spilling enabled
    TSetup<LLVM, SPILLING> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto ui64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto tupleType = pb.NewTupleType({ui64Type});

    const auto list = pb.NewList(tupleType, {});

    const auto pgmReturn = pb.FromFlow(pb.NarrowMap(pb.WideSort(pb.ExpandMap(pb.ToFlow(list),
                                                                            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U)}; }),
                                                               {{0U, pb.NewDataLiteral<bool>(true)}}),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(tupleType, items); }));

    if (SPILLING) {
        setup.RenameCallable(pgmReturn, "WideSort", "WideSortWithSpilling");
    }

    const auto spillerFactory = std::make_shared<TMockSpillerFactory>();
    const auto graph = setup.BuildGraph(pgmReturn);
    if (SPILLING) {
        graph->GetContext().SpillerFactory = spillerFactory;
    }
    const auto streamVal = graph->GetValue();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT_EQUAL(streamVal.Fetch(item), NUdf::EFetchStatus::Finish);

    if (SPILLING) {
        // Empty input: spiller may or may not be created, but no data should be spilled
        const auto& spillers = spillerFactory->GetCreatedSpillers();
        if (!spillers.empty()) {
            const auto mockSpiller = std::dynamic_pointer_cast<TMockSpiller>(spillers.front());
            UNIT_ASSERT_C(mockSpiller != nullptr, "Expected TMockSpiller");
            UNIT_ASSERT_VALUES_EQUAL(mockSpiller->GetTotalSpilled(), 0u);
        }
    }
}

} // Y_UNIT_TEST_SUITE(TMiniKQLWideSortTest)

} // namespace NMiniKQL
} // namespace NKikimr
