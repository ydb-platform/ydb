#include "mkql_computation_node_ut.h"
#include <ydb/library/yql/minikql/mkql_runtime_version.h>

namespace NKikimr {
namespace NMiniKQL {
#if !defined(MKQL_RUNTIME_VERSION) || MKQL_RUNTIME_VERSION >= 18u
Y_UNIT_TEST_SUITE(TMiniKQLWideCondense1Test) {
    Y_UNIT_TEST_LLVM(TestConcatItemsToKey) {
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

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideCondense1(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList {
                return {items.front(), pb.AggrConcat(pb.AggrConcat(items.front(), pb.NewDataLiteral<NUdf::EDataSlot::String>(": ")), items.back())};
            },
            [&](TRuntimeNode::TList items, TRuntimeNode::TList state) {
                return pb.AggrNotEquals(items.front(), state.front());
            },
            [&](TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {state.front(), pb.AggrConcat(pb.AggrConcat(state.back(), pb.NewDataLiteral<NUdf::EDataSlot::String>(", ")), items.back())};
            }),
            [&](TRuntimeNode::TList items)  { return items.back(); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "key one: very long value 1");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "key two: very long value 2, very long value 3");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "very long key one: very long value 4");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "very long key two: very long value 5, very long value 6, very long value 7, very long value 8, very long value 9");
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

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideCondense1(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Unwrap(pb.Nth(item, 0U), landmine, __FILE__, __LINE__, 0), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList {
                return {items[1U]};
            },
            [&](TRuntimeNode::TList items, TRuntimeNode::TList) {
                return items.back();
            },
            [&](TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {pb.AggrConcat(pb.AggrConcat(state.front(), pb.NewDataLiteral<NUdf::EDataSlot::String>("; ")), items[1U])};
            }),
            [&](TRuntimeNode::TList items)  { return items.front(); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "value 1; value 2; value 3");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "value 4; value 5; value 6; value 7; value 8");
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

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideCondense1(pb.ExpandMap(pb.ToFlow(list),
            [](TRuntimeNode) -> TRuntimeNode::TList { return {}; }),
            [](TRuntimeNode::TList items) { return items; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList) { return pb.NewDataLiteral<bool>(true); },
            [](TRuntimeNode::TList, TRuntimeNode::TList state) { return state;}),
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

