#include "mkql_computation_node_ut.h"
#include <ydb/library/yql/minikql/mkql_runtime_version.h>

namespace NKikimr {
namespace NMiniKQL {
#if !defined(MKQL_RUNTIME_VERSION) || MKQL_RUNTIME_VERSION >= 18u
Y_UNIT_TEST_SUITE(TMiniKQLWideMapTest) {
    Y_UNIT_TEST_LLVM(TestSimpleSwap) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(dataType)});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple({items[2U], items[1U], items[0U] }); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -1);
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -2);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 2);
        UNIT_ASSERT(!item.GetElement(2));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -3);
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 4);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 4);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestThinLambda) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType)});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4))});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideMap(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode) -> TRuntimeNode::TList { return {}; }),
            [&](TRuntimeNode::TList items) { return items; }),
            [&](TRuntimeNode::TList) -> TRuntimeNode { return pb.NewTuple({}); }
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

    Y_UNIT_TEST_LLVM(TestWideMap) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(dataType)});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideMap(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {pb.AggrMin(items[0], items[1]), pb.AggrMax(items[1], items[2]), pb.AggrAdd(items[0], items[2])}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 3);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -3);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 4);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 4);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 4);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestDotCalculateUnusedField) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewOptional(pb.NewDataLiteral<i32>(0)), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-4))});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto landmine = pb.NewDataLiteral<NUdf::EDataSlot::String>("ACHTUNG MINEN!");

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideMap(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {pb.Mul(items.front(), items.back()), pb.Unwrap(items[1], landmine, __FILE__, __LINE__, 0), pb.Add(items.front(), items.back())}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple({items.back(), items.front()}); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 0);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 0);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 0);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -9);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 0);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -16);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestPasstroughtFieldsAsIs) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewOptional(pb.NewDataLiteral<i32>(-5)), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-4)), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewOptional(pb.NewDataLiteral<i32>(-7)), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-4))});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideMap(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front(), pb.Minus(items[1u]), items.back()}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 5);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 4);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 3);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 7);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 4);
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -4);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestPasstroughtFieldSplitAsIs) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewOptional(pb.NewDataLiteral<i32>(-5)), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-4)), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewOptional(pb.NewDataLiteral<i32>(-7)), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-4))});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideMap(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[1U], pb.Mul(items.front(), items.back()), items[1U]}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -5);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -5);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -4);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -4);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -7);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -9);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -7);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -16);
        UNIT_ASSERT(!item.GetElement(2));
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFieldBothWayPasstroughtAndArg) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewOptional(pb.NewDataLiteral<i32>(-5)), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-4)), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewOptional(pb.NewDataLiteral<i32>(-7)), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-4))});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideMap(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[1U], pb.Sub(items.front(), items.back()), pb.Minus(items[1U])}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -5);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 5);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -4);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 4);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -7);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 6);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 7);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 8);
        UNIT_ASSERT(!item.GetElement(2));
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestPasstroughtFieldSplitAndFirstUnused) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewOptional(pb.NewDataLiteral<i32>(-5)), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-4)), pb.NewOptional(pb.NewDataLiteral<i32>(-6))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewOptional(pb.NewDataLiteral<i32>(-7)), pb.NewOptional(pb.NewDataLiteral<i32>(-9))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideMap(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[1U], pb.AggrAdd(items.front(), items.back()), items[1U]}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple({items[1], items[2]}); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -2);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -5);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -4);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -6);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -7);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 3);
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }
}
#endif
}
}
