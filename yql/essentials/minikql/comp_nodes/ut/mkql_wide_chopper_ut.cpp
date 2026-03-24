#include "mkql_computation_node_ut.h"
#include <yql/essentials/minikql/mkql_runtime_version.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLWideChopperTest) {
Y_UNIT_TEST_LLVM(TestConcatKeyToItems) {
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

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideChopper(pb.ExpandMap(pb.ToFlow(list),
                                                                               [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                  [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {pb.Substring(items.front(), pb.Sub(pb.Size(items.front()), pb.NewDataLiteral<ui32>(4U)), pb.NewDataLiteral<ui32>(4U))}; },
                                                                  [&](TRuntimeNode::TList keys, TRuntimeNode::TList items) { return pb.AggrNotEquals(keys.front(), items.front()); },
                                                                  [&](TRuntimeNode::TList keys, TRuntimeNode input) { return pb.WideMap(input, [&](TRuntimeNode::TList items) -> TRuntimeNode::TList {
                                                                                                                          return {pb.AggrConcat(items.back(), keys.front())};
                                                                                                                      }); }),
                                                   [&](TRuntimeNode::TList items) { return items.front(); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "very long value 1 one");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "very long value 2 two");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "very long value 3 two");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "very long value 4 one");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "very long value 5 two");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "very long value 6 two");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "very long value 7 two");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "very long value 8 two");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "very long value 9 two");
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TestCollectKeysOnly) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
    const auto tupleType = pb.NewTupleType({dataType, dataType});

    const auto keyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("key one");
    const auto keyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("key two");

    const auto longKeyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key one");
    const auto longKeyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key two");

    const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 1");
    const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 2");
    const auto value3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 3");
    const auto value4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 4");
    const auto value5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 5");
    const auto value6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 6");
    const auto value7 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 7");
    const auto value8 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 8");
    const auto value9 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 9");

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

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideChopper(pb.ExpandMap(pb.ToFlow(list),
                                                                               [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                  [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; },
                                                                  [&](TRuntimeNode::TList keys, TRuntimeNode::TList items) { return pb.AggrNotEquals(keys.front(), items.front()); },
                                                                  [&](TRuntimeNode::TList keys, TRuntimeNode) { return pb.ExpandMap(pb.ToFlow(pb.NewOptional(keys.front())), [&](TRuntimeNode item) -> TRuntimeNode::TList {
                                                                                                                    return {item};
                                                                                                                }); }),
                                                   [&](TRuntimeNode::TList items) { return items.front(); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "key one");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "key two");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "very long key one");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "very long key two");
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TestGetPart) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
    const auto tupleType = pb.NewTupleType({dataType, dataType});

    const auto keyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("key one");
    const auto keyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("key two");

    const auto longKeyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key one");
    const auto longKeyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key two");

    const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 1");
    const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 2");
    const auto value3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 3");
    const auto value4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 4");
    const auto value5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 5");
    const auto value6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 6");
    const auto value7 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 7");
    const auto value8 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 8");
    const auto value9 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 9");

    const auto data1 = pb.NewTuple(tupleType, {keyOne, value1});

    const auto data2 = pb.NewTuple(tupleType, {keyTwo, value2});
    const auto data3 = pb.NewTuple(tupleType, {keyTwo, value3});

    const auto data4 = pb.NewTuple(tupleType, {longKeyOne, value4});
    const auto data5 = pb.NewTuple(tupleType, {longKeyOne, value5});
    const auto data6 = pb.NewTuple(tupleType, {longKeyOne, value6});
    const auto data7 = pb.NewTuple(tupleType, {longKeyOne, value7});
    const auto data8 = pb.NewTuple(tupleType, {longKeyOne, value8});

    const auto data9 = pb.NewTuple(tupleType, {longKeyTwo, value9});

    const auto list = pb.NewList(tupleType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideChopper(pb.ExpandMap(pb.ToFlow(list),
                                                                               [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                  [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; },
                                                                  [&](TRuntimeNode::TList keys, TRuntimeNode::TList items) { return pb.AggrNotEquals(keys.front(), items.front()); },
                                                                  [&](TRuntimeNode::TList, TRuntimeNode input) { return pb.Take(pb.Skip(input, pb.NewDataLiteral<ui64>(1ULL)), pb.NewDataLiteral<ui64>(3ULL)); }),
                                                   [&](TRuntimeNode::TList items) { return items.back(); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "value 3");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "value 5");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "value 6");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "value 7");
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TestSwitchByBoolFieldAndDontUseKey) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
    const auto boolType = pb.NewDataType(NUdf::TDataType<bool>::Id);
    const auto tupleType = pb.NewTupleType({pb.NewOptionalType(dataType), dataType, boolType});

    const auto key0 = pb.NewEmptyOptional(pb.NewOptionalType(dataType));
    const auto key1 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("one"));
    const auto key2 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("two"));

    const auto trueVal = pb.NewDataLiteral<bool>(true);
    const auto falseVal = pb.NewDataLiteral<bool>(false);

    const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 1");
    const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 2");
    const auto value3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 3");
    const auto value4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 4");
    const auto value5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 5");
    const auto value6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 6");
    const auto value7 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 7");
    const auto value8 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 8");
    const auto value9 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 9");

    const auto data1 = pb.NewTuple(tupleType, {key0, value1, trueVal});
    const auto data2 = pb.NewTuple(tupleType, {key1, value2, falseVal});
    const auto data3 = pb.NewTuple(tupleType, {key2, value3, falseVal});
    const auto data4 = pb.NewTuple(tupleType, {key0, value4, trueVal});
    const auto data5 = pb.NewTuple(tupleType, {key1, value5, falseVal});
    const auto data6 = pb.NewTuple(tupleType, {key2, value6, falseVal});
    const auto data7 = pb.NewTuple(tupleType, {key0, value7, falseVal});
    const auto data8 = pb.NewTuple(tupleType, {key1, value8, falseVal});
    const auto data9 = pb.NewTuple(tupleType, {key2, value9, trueVal});

    const auto list = pb.NewList(tupleType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

    const auto landmine = pb.NewDataLiteral<NUdf::EDataSlot::String>("ACHTUNG MINEN!");

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideChopper(pb.ExpandMap(pb.ToFlow(list),
                                                                               [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                                  [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {pb.Unwrap(items.front(), landmine, __FILE__, __LINE__, 0)}; },
                                                                  [&](TRuntimeNode::TList, TRuntimeNode::TList items) { return items.back(); },
                                                                  [&](TRuntimeNode::TList, TRuntimeNode input) { return pb.Take(input, pb.NewDataLiteral<ui64>(2ULL)); }),
                                                   [&](TRuntimeNode::TList items) { return items[1U]; }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "value 1");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "value 2");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "value 4");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "value 5");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "value 9");
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TestCollectKeysIfPresent) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
    const auto boolType = pb.NewDataType(NUdf::TDataType<bool>::Id);
    const auto tupleType = pb.NewTupleType({pb.NewOptionalType(dataType), dataType, boolType});

    const auto key0 = pb.NewEmptyOptional(pb.NewOptionalType(dataType));
    const auto key1 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("one"));
    const auto key2 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("two"));

    const auto trueVal = pb.NewDataLiteral<bool>(true);
    const auto falseVal = pb.NewDataLiteral<bool>(false);

    const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 1");
    const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 2");
    const auto value3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 3");
    const auto value4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 4");
    const auto value5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 5");
    const auto value6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 6");
    const auto value7 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 7");
    const auto value8 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 8");
    const auto value9 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 9");

    const auto data1 = pb.NewTuple(tupleType, {key1, value1, trueVal});
    const auto data2 = pb.NewTuple(tupleType, {key1, value2, falseVal});
    const auto data3 = pb.NewTuple(tupleType, {key1, value3, falseVal});
    const auto data4 = pb.NewTuple(tupleType, {key0, value4, trueVal});
    const auto data5 = pb.NewTuple(tupleType, {key0, value5, falseVal});
    const auto data6 = pb.NewTuple(tupleType, {key2, value6, falseVal});
    const auto data7 = pb.NewTuple(tupleType, {key0, value7, falseVal});
    const auto data8 = pb.NewTuple(tupleType, {key0, value8, falseVal});
    const auto data9 = pb.NewTuple(tupleType, {key0, value9, trueVal});

    const auto list = pb.NewList(tupleType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideChopper(pb.ExpandMap(pb.ToFlow(list),
                                                                               [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                                  [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; },
                                                                  [&](TRuntimeNode::TList keys, TRuntimeNode::TList items) { return pb.AggrNotEquals(keys.front(), items.front()); },
                                                                  [&](TRuntimeNode::TList keys, TRuntimeNode part) { return pb.IfPresent(keys,
                                                                                                                                         [&](TRuntimeNode::TList keys) { return pb.ExpandMap(pb.ToFlow(pb.NewList(dataType, keys)), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }); },
                                                                                                                                         pb.WideMap(part, [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[1U]}; })); }),
                                                   [&](TRuntimeNode::TList items) { return items.front(); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "one");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "value 4");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "value 5");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "two");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "value 7");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "value 8");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "value 9");
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TestConditionalByKeyPart) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
    const auto boolType = pb.NewDataType(NUdf::TDataType<bool>::Id);
    const auto tupleType = pb.NewTupleType({pb.NewOptionalType(dataType), dataType, boolType});

    const auto key0 = pb.NewEmptyOptional(pb.NewOptionalType(dataType));
    const auto key1 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("one"));
    const auto key2 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("two"));

    const auto trueVal = pb.NewDataLiteral<bool>(true);
    const auto falseVal = pb.NewDataLiteral<bool>(false);

    const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 1");
    const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 2");
    const auto value3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 3");
    const auto value4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 4");
    const auto value5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 5");
    const auto value6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 6");
    const auto value7 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 7");
    const auto value8 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 8");
    const auto value9 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 9");

    const auto data1 = pb.NewTuple(tupleType, {key1, value1, trueVal});
    const auto data2 = pb.NewTuple(tupleType, {key1, value2, trueVal});
    const auto data3 = pb.NewTuple(tupleType, {key1, value3, falseVal});
    const auto data4 = pb.NewTuple(tupleType, {key1, value4, falseVal});
    const auto data5 = pb.NewTuple(tupleType, {key2, value5, falseVal});
    const auto data6 = pb.NewTuple(tupleType, {key2, value6, falseVal});
    const auto data7 = pb.NewTuple(tupleType, {key2, value7, trueVal});
    const auto data8 = pb.NewTuple(tupleType, {key0, value8, trueVal});
    const auto data9 = pb.NewTuple(tupleType, {key0, value9, falseVal});

    const auto list = pb.NewList(tupleType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideChopper(pb.ExpandMap(pb.ToFlow(list),
                                                                               [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                                  [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front(), items.back()}; },
                                                                  [&](TRuntimeNode::TList keys, TRuntimeNode::TList items) { return pb.Or({pb.AggrNotEquals(keys.front(), items.front()), pb.AggrNotEquals(keys.back(), items.back())}); },
                                                                  [&](TRuntimeNode::TList keys, TRuntimeNode part) { return pb.If(keys.back(),
                                                                                                                                  pb.ExpandMap(pb.ToFlow(keys.front()), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }),
                                                                                                                                  pb.WideMap(part, [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[1U]}; })); }),
                                                   [&](TRuntimeNode::TList items) { return items.front(); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "one");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "value 3");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "value 4");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "value 5");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "value 6");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "two");
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "value 9");
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TestThinAllLambdas) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = pb.NewTupleType({});

    const auto data = pb.NewTuple({});

    const auto list = pb.NewList(tupleType, {data, data, data, data});

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideChopper(pb.ExpandMap(pb.ToFlow(list),
                                                                               [](TRuntimeNode) -> TRuntimeNode::TList { return {}; }),
                                                                  [](TRuntimeNode::TList items) { return items; },
                                                                  [&](TRuntimeNode::TList, TRuntimeNode::TList) { return pb.NewDataLiteral<bool>(true); },
                                                                  [&](TRuntimeNode::TList, TRuntimeNode input) { return pb.WideMap(input, [](TRuntimeNode::TList items) { return items; }); }),
                                                   [&](TRuntimeNode::TList) { return pb.NewTuple({}); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

class TTestProxyFlowWrapper: public TStatefulWideFlowCodegeneratorNode<TTestProxyFlowWrapper> {
    using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TTestProxyFlowWrapper>;

public:
    TTestProxyFlowWrapper(TComputationMutables& mutables,
                          IComputationWideFlowNode* flow,
                          size_t width)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed)
        , Flow_(flow)
        , Width_(width)
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state,
                             TComputationContext& ctx,
                             NUdf::TUnboxedValue* const* output) const {
        return GetState(state, ctx)->ProxyFetch(ctx, output);
    }

#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();
        // Call GetState.
        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto state = EmitFunctionCall<&TTestProxyFlowWrapper::GetState>(ptrType, {self, statePtr, ctx.Ctx}, ctx, block);

        // Initialize temporary storage.
        const auto statusType = Type::getInt32Ty(context);
        const auto indexType = Type::getInt64Ty(context);
        const auto valueType = Type::getInt128Ty(context);
        const auto arrayValueType = ArrayType::get(valueType, Width_);
        const auto ptrValueType = PointerType::getUnqual(valueType);
        const auto arrayPtrValueType = ArrayType::get(ptrValueType, Width_);

        const auto values = new AllocaInst(arrayValueType, 0, "storage", block);
        const auto output = new AllocaInst(arrayPtrValueType, 0, "output", block);
        for (size_t idx = 0; idx < Width_; idx++) {
            const auto storagePtr = GetElementPtrInst::CreateInBounds(arrayValueType, values, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, idx)}, "storagePtr", block);
            const auto outputPtr = GetElementPtrInst::CreateInBounds(arrayPtrValueType, output, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, idx)}, "outputPtr", block);
            new StoreInst(storagePtr, outputPtr, block);
        }

        // Call ProxyFetch.
        const auto result = EmitFunctionCall<&TTestProxyFlowState::ProxyFetch>(statusType, {state, ctx.Ctx, output}, ctx, block);

        // Return the result.
        ICodegeneratorInlineWideNode::TGettersList getters(Width_);
        for (size_t idx = 0; idx < getters.size(); idx++) {
            getters[idx] = [idx, values, arrayValueType, indexType, valueType](const TCodegenContext& ctx, BasicBlock*& block) {
                const auto valuePtr = GetElementPtrInst::CreateInBounds(arrayValueType, values, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, idx)}, "valuePtr", block);
                const auto value = new LoadInst(valueType, valuePtr, "value", block);
                // XXX: As a result of <ProxyFetch> call, a vector
                // of unboxed values is obtained. Hence, if these
                // values are not released here, no code later
                // will do it, leading to UV payload leakage.
                ValueRelease(EValueRepresentation::Any, value, ctx, block);
                return value;
            };
        }
        return {result, getters};
    }
#endif

private:
    class TTestProxyFlowState: public TComputationValue<TTestProxyFlowState> {
    public:
        TTestProxyFlowState(TMemoryUsageInfo* memInfo, IComputationWideFlowNode* flow)
            : TComputationValue(memInfo)
            , Flow_(flow)
        {
        }
        EFetchResult ProxyFetch(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) {
            return Flow_->FetchValues(ctx, output);
        }

    private:
        IComputationWideFlowNode* const Flow_;
    };

    TTestProxyFlowState* GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsInvalid()) {
            state = ctx.HolderFactory.Create<TTestProxyFlowState>(Flow_);
        }
        return static_cast<TTestProxyFlowState*>(state.AsBoxed().Get());
    }

    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    IComputationWideFlowNode* const Flow_;
    const size_t Width_;
};

IComputationNode* WrapTestProxyFlow(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    const auto wideComponents = GetWideComponents(AS_TYPE(TFlowType, callable.GetInput(0U).GetStaticType()));
    const auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    return new TTestProxyFlowWrapper(ctx.Mutables, wideFlow, wideComponents.size());
}

constexpr std::string_view TestProxyFlowName = "TestProxyFlow";

TComputationNodeFactory GetNodeFactory() {
    return [](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == TestProxyFlowName) {
            return WrapTestProxyFlow(callable, ctx);
        }
        return GetBuiltinFactory()(callable, ctx);
    };
}

template <bool LLVM>
TRuntimeNode MakeTestProxyFlow(TSetup<LLVM>& setup, TRuntimeNode inputWideFlow) {
    TCallableBuilder callableBuilder(*setup.Env, TestProxyFlowName, inputWideFlow.GetStaticType());
    callableBuilder.Add(inputWideFlow);
    return TRuntimeNode(callableBuilder.Build(), false);
}

Y_UNIT_TEST_LLVM(TestCodegenWithProxyFlow) {
    TSetup<LLVM> setup(GetNodeFactory());
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
    const auto tupleType = pb.NewTupleType({dataType, dataType});

    const auto key = pb.NewDataLiteral<NUdf::EDataSlot::String>("key one");
    const auto value = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 1");
    const auto list = pb.NewList(tupleType, {pb.NewTuple(tupleType, {key, value})});

    const auto wideFlow = pb.ExpandMap(pb.ToFlow(list), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; });
    const auto wideChoppedFlow = pb.WideChopper(wideFlow,
                                                [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return items; },
                                                [&](TRuntimeNode::TList, TRuntimeNode::TList) { return pb.NewDataLiteral<bool>(true); },
                                                [&](TRuntimeNode::TList keys, TRuntimeNode input) { return pb.ToFlow(pb.FromFlow(pb.WideMap(input, [&](TRuntimeNode::TList items) -> TRuntimeNode::TList {
                                                                                                        return {pb.AggrConcat(pb.AggrConcat(keys.front(), pb.NewDataLiteral<NUdf::EDataSlot::String>(": ")), items.back())};
                                                                                                    }))); });
    const auto wideProxyFlow = MakeTestProxyFlow(setup, wideChoppedFlow);
    const auto root = pb.Collect(pb.NarrowMap(wideProxyFlow, [&](TRuntimeNode::TList items) { return items.front(); }));

    const auto graph = setup.BuildGraph(root);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNBOXED_VALUE_STR_EQUAL(item, "key one: very long value 1");
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

} // Y_UNIT_TEST_SUITE(TMiniKQLWideChopperTest)

} // namespace NMiniKQL
} // namespace NKikimr
