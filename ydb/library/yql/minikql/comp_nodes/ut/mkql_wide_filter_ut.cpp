#include "mkql_computation_node_ut.h"
#include <ydb/library/yql/minikql/mkql_runtime_version.h>

namespace NKikimr {
namespace NMiniKQL {
#if !defined(MKQL_RUNTIME_VERSION) || MKQL_RUNTIME_VERSION >= 18u
Y_UNIT_TEST_SUITE(TMiniKQLWideFilterTest) {
    Y_UNIT_TEST_LLVM(TestPredicateExpression) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});

        const auto list = pb.NewList(tupleType, {data1, data2, data3});

        const auto pgmReturn = pb.FromFlow(
            pb.NarrowMap(
                pb.WideFilter(
                    pb.ExpandMap(
                        pb.ToFlow(list),
                        [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }
                    ),
                    [&](TRuntimeNode::TList items) -> TRuntimeNode {
                        const auto v = pb.If(
                            pb.Exists(items.front()),
                            pb.NewOptional(pb.NewDataLiteral<bool>(true)),
                            pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<bool>::Id)
                        );
                        return pb.Coalesce(v, pb.NewDataLiteral<bool>(false));
                    }
                ),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }
            )
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        NUdf::TUnboxedValue value = graph->GetValue();

        NUdf::TUnboxedValue v;
        UNIT_ASSERT_VALUES_EQUAL(value.Fetch(v), NUdf::EFetchStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(v.GetElement(0).template Get<i32>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(v.GetElement(1).template Get<i32>(), -2);

        UNIT_ASSERT_VALUES_EQUAL(value.Fetch(v), NUdf::EFetchStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(v.GetElement(0).template Get<i32>(), 3);
        UNIT_ASSERT_VALUES_EQUAL(v.GetElement(1).template Get<i32>(), -3);

        UNIT_ASSERT_VALUES_EQUAL(value.Fetch(v), NUdf::EFetchStatus::Finish);
    }

    Y_UNIT_TEST_LLVM(TestCheckedFieldPasstrought) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(dataType)});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideFilter(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.Exists(items[1U]); }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 4);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 4);
        UNIT_ASSERT(!item.GetElement(2));
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestCheckedFieldUnusedAfter) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(dataType)});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideFilter(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.Exists(items[1U]); }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple({items.back(), items.front()}); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -2);
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 4);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestDotCalculateUnusedField) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(dataType)});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto landmine = pb.NewDataLiteral<NUdf::EDataSlot::String>("ACHTUNG MINEN!");

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideFilter(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Unwrap(pb.Nth(item, 2U), landmine, __FILE__, __LINE__, 0)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.Exists(items[1U]); }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return items.front(); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 4);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestWithLimitCheckedFieldUsedAfter) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(9)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(7)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(6)), pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(dataType)});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideFilter(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            pb.NewDataLiteral<ui64>(2ULL), [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.Exists(items.front()); }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.AggrAdd(items.back(), items.front()); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 8);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 4);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestWithLimitCheckedFieldUnusedAfter) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(dataType)});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideFilter(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            pb.NewDataLiteral<ui64>(2ULL), [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.Exists(items.front()); }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return items.back(); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -3);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestTakeWhile) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(dataType)});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTakeWhile(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.Exists(items.front()); }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -1);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestTakeWhileInclusive) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(dataType)});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideTakeWhileInclusive(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.Exists(items.front()); }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -2);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestSkipWhile) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(dataType)});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideSkipWhile(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.AggrGreater(items.back(), pb.NewOptional(pb.NewDataLiteral<i32>(-3))); }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 3);
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 4);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 4);
        UNIT_ASSERT(!item.GetElement(2));
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestSkipWhileInclusive) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(dataType)});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideSkipWhileInclusive(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.AggrGreater(items.back(), pb.NewOptional(pb.NewDataLiteral<i32>(-3))); }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 4);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 4);
        UNIT_ASSERT(!item.GetElement(2));
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFilterByBooleanField) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, pb.NewDataType(NUdf::TDataType<bool>::Id), dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewDataLiteral(true), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType), pb.NewDataLiteral(false), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewDataLiteral(true), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewDataLiteral(false), pb.NewEmptyOptional(dataType)});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideFilter(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return items[1U]; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple({items.back(), items.front()}); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), +1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -3);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), +3);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }
}
#endif
}
}
