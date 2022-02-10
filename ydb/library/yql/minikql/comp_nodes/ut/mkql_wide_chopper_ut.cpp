#include "mkql_computation_node_ut.h"
#include <ydb/library/yql/minikql/mkql_runtime_version.h>

namespace NKikimr {
namespace NMiniKQL {
#if !defined(MKQL_RUNTIME_VERSION) || MKQL_RUNTIME_VERSION >= 18u
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
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList {
                return {pb.Substring(items.front(), pb.Sub(pb.Size(items.front()), pb.NewDataLiteral<ui32>(4U)), pb.NewDataLiteral<ui32>(4U))};
            },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList items) {
                return pb.AggrNotEquals(keys.front(), items.front());
            },
            [&](TRuntimeNode::TList keys, TRuntimeNode input) {
                return pb.WideMap(input, [&](TRuntimeNode::TList items) -> TRuntimeNode::TList {
                    return {pb.AggrConcat(items.back(), keys.front())};
                });
            }),
            [&](TRuntimeNode::TList items)  { return items.front(); }
        ));

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
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList {
                return {items.front()};
            },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList items) {
                return pb.AggrNotEquals(keys.front(), items.front());
            },
            [&](TRuntimeNode::TList keys, TRuntimeNode) {
                return pb.ExpandMap(pb.ToFlow(pb.NewOptional(keys.front())), [&](TRuntimeNode item) -> TRuntimeNode::TList {
                    return {item};
                } );
            }),
            [&](TRuntimeNode::TList items)  { return items.front(); }
        ));

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
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList {
                return {items.front()};
            },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList items) {
                return pb.AggrNotEquals(keys.front(), items.front());
            },
            [&](TRuntimeNode::TList, TRuntimeNode input) {
                return pb.Take(pb.Skip(input, pb.NewDataLiteral<ui64>(1ULL)), pb.NewDataLiteral<ui64>(3ULL));
            }),
            [&](TRuntimeNode::TList items)  { return items.back(); }
        ));

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
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList {
                return {pb.Unwrap(items.front(), landmine, __FILE__, __LINE__, 0)};
            },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) {
                return items.back();
            },
            [&](TRuntimeNode::TList, TRuntimeNode input) {
                return pb.Take(input, pb.NewDataLiteral<ui64>(2ULL));
            }),
            [&](TRuntimeNode::TList items)  { return items[1U]; }
        ));

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
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList {
                return {items.front()};
            },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList items) {
                return pb.AggrNotEquals(keys.front(), items.front());
            },
            [&](TRuntimeNode::TList keys, TRuntimeNode part) {
                return pb.IfPresent(keys,
                    [&](TRuntimeNode::TList keys) { return pb.ExpandMap(pb.ToFlow(pb.NewList(dataType, keys)), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; } ); },
                    pb.WideMap(part, [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[1U]}; } ));
            }),
            [&](TRuntimeNode::TList items)  { return items.front(); }
        ));

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
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList {
                return {items.front(), items.back()};
            },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList items) {
                return pb.Or({pb.AggrNotEquals(keys.front(), items.front()), pb.AggrNotEquals(keys.back(), items.back())});
            },
            [&](TRuntimeNode::TList keys, TRuntimeNode part) {
                return pb.If(keys.back(),
                    pb.ExpandMap(pb.ToFlow(keys.front()), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; } ),
                    pb.WideMap(part, [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[1U]}; } ));
            }),
            [&](TRuntimeNode::TList items)  { return items.front(); }
        ));

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
            [&](TRuntimeNode::TList)  { return pb.NewTuple({}); }
        ));

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
}
#endif
}
}

