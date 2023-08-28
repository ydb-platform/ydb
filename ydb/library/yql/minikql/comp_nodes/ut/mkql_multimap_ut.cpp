#include "mkql_computation_node_ut.h"
#include <ydb/library/yql/minikql/mkql_runtime_version.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLMultiMapTest) {
    Y_UNIT_TEST_LLVM(TestOverList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(1);
        const auto data2 = pb.NewDataLiteral<ui32>(2);
        const auto data3 = pb.NewDataLiteral<ui32>(3);
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto list = pb.NewList(dataType, {data1, data2, data3});
        const auto pgmReturn = pb.MultiMap(list,
            [&](TRuntimeNode item) {
                return TRuntimeNode::TList{pb.Add(item, data1), item, pb.Mul(item, data2)};
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 6);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestOverLazyList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(1);
        const auto data2 = pb.NewDataLiteral<ui32>(2);
        const auto data3 = pb.NewDataLiteral<ui32>(3);
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto list = pb.NewList(dataType, {data1, data2, data3});
        const auto pgmReturn = pb.MultiMap(pb.LazyList(list),
            [&](TRuntimeNode item) {
                return TRuntimeNode::TList{pb.Add(item, data1), item, pb.Mul(item, data2)};
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 6);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestOverFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(1);
        const auto data2 = pb.NewDataLiteral<ui32>(2);
        const auto data3 = pb.NewDataLiteral<ui32>(3);
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto list = pb.NewList(dataType, {data1, data2, data3});
        const auto pgmReturn = pb.Collect(pb.MultiMap(pb.ToFlow(list),
            [&](TRuntimeNode item) {
                return TRuntimeNode::TList{pb.Add(item, data1), item, pb.Mul(item, data2)};
        }));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 6);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }
#if !defined(MKQL_RUNTIME_VERSION) || MKQL_RUNTIME_VERSION >= 18u
    Y_UNIT_TEST_LLVM(TestFlattenByNarrow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});

        const auto list = pb.NewList(tupleType, {data1, data2, data3});

        const auto pgmReturn = pb.Collect(pb.NarrowMultiMap(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[2U], items[1U], items[0U] }; }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 3);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }
#endif
}

}
}
