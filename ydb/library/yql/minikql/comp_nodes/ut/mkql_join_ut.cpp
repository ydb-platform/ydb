#include "mkql_computation_node_ut.h"
#include <ydb/library/yql/minikql/mkql_runtime_version.h> 

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLCommonJoinCoreTupleTest) {
    Y_UNIT_TEST_LLVM(Inner) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto indexType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto optionalType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({optionalType, optionalType, optionalType, indexType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(optionalType), pb.NewOptional(pb.NewDataLiteral<i32>(-1)), pb.NewDataLiteral<ui32>(0)});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(optionalType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2)), pb.NewDataLiteral<ui32>(0)});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(optionalType), pb.NewOptional(pb.NewDataLiteral<i32>(-3)), pb.NewDataLiteral<ui32>(1)});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(optionalType), pb.NewDataLiteral<ui32>(1)});

        const auto list = pb.NewList(tupleType, {data1, data3, data2, data4});

        const auto outputType = pb.NewFlowType(pb.NewTupleType({optionalType, optionalType}));
        const auto pgmReturn = pb.Collect(pb.CommonJoinCore(pb.ToFlow(list), EJoinKind::Inner, {0U, 0U}, {1U, 1U}, {}, {2U}, 0ULL, std::nullopt, EAnyJoinSettings::None, 3U, outputType));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 4);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(InnerOrderLeftFirst) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto indexType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto optionalType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({optionalType, optionalType, optionalType, indexType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(optionalType), pb.NewOptional(pb.NewDataLiteral<i32>(-1)), pb.NewDataLiteral<ui32>(0)});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(optionalType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2)), pb.NewDataLiteral<ui32>(0)});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(optionalType), pb.NewOptional(pb.NewDataLiteral<i32>(-3)), pb.NewDataLiteral<ui32>(1)});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(optionalType), pb.NewDataLiteral<ui32>(1)});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto outputType = pb.NewFlowType(pb.NewTupleType({optionalType, optionalType}));
        const auto pgmReturn = pb.Collect(pb.CommonJoinCore(pb.ToFlow(list), EJoinKind::Inner, {0U, 0U}, {1U, 1U}, {}, {2U}, 0ULL, {0U}, EAnyJoinSettings::None, 3U, outputType));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 4);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(InnerOrderRightFirst) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto indexType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto optionalType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({optionalType, optionalType, optionalType, indexType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(optionalType), pb.NewOptional(pb.NewDataLiteral<i32>(-1)), pb.NewDataLiteral<ui32>(0)});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(optionalType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2)), pb.NewDataLiteral<ui32>(0)});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(optionalType), pb.NewOptional(pb.NewDataLiteral<i32>(-3)), pb.NewDataLiteral<ui32>(1)});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(optionalType), pb.NewDataLiteral<ui32>(1)});

        const auto list = pb.NewList(tupleType, {data3, data4, data1, data2});

        const auto outputType = pb.NewFlowType(pb.NewTupleType({optionalType, optionalType}));
        const auto pgmReturn = pb.Collect(pb.CommonJoinCore(pb.ToFlow(list), EJoinKind::Inner, {0U, 0U}, {1U, 1U}, {}, {2U}, 0ULL, {1U}, EAnyJoinSettings::None, 3U, outputType));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 4);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }
}
#if !defined(MKQL_RUNTIME_VERSION) || MKQL_RUNTIME_VERSION >= 18u
Y_UNIT_TEST_SUITE(TMiniKQLCommonJoinCoreWideTest) {
    Y_UNIT_TEST_LLVM(Inner) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto indexType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto optionalType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({optionalType, optionalType, optionalType, indexType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(optionalType), pb.NewOptional(pb.NewDataLiteral<i32>(-1)), pb.NewDataLiteral<ui32>(0)});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(optionalType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2)), pb.NewDataLiteral<ui32>(0)});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(optionalType), pb.NewOptional(pb.NewDataLiteral<i32>(-3)), pb.NewDataLiteral<ui32>(1)});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(optionalType), pb.NewDataLiteral<ui32>(1)});

        const auto list = pb.NewList(tupleType, {data1, data3, data2, data4});

        const auto outputType = pb.NewFlowType(pb.NewTupleType({optionalType, optionalType}));
        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.CommonJoinCore(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U), pb.Nth(item, 3U)}; }),
            EJoinKind::Inner, {0U, 0U}, {1U, 1U}, {}, {2U}, 0ULL, std::nullopt, EAnyJoinSettings::None, 3U, outputType),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 4);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(InnerOrderLeftFirst) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto indexType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto optionalType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({optionalType, optionalType, optionalType, indexType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(optionalType), pb.NewOptional(pb.NewDataLiteral<i32>(-1)), pb.NewDataLiteral<ui32>(0)});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(optionalType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2)), pb.NewDataLiteral<ui32>(0)});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(optionalType), pb.NewOptional(pb.NewDataLiteral<i32>(-3)), pb.NewDataLiteral<ui32>(1)});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(optionalType), pb.NewDataLiteral<ui32>(1)});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto outputType = pb.NewFlowType(pb.NewTupleType({optionalType, optionalType}));
        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.CommonJoinCore(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U), pb.Nth(item, 3U)}; }),
            EJoinKind::Inner, {0U, 0U}, {1U, 1U}, {}, {2U}, 0ULL, {0U}, EAnyJoinSettings::None, 3U, outputType),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 4);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(InnerOrderRightFirst) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto indexType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto optionalType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({optionalType, optionalType, optionalType, indexType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(optionalType), pb.NewOptional(pb.NewDataLiteral<i32>(-1)), pb.NewDataLiteral<ui32>(0)});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(optionalType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2)), pb.NewDataLiteral<ui32>(0)});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(optionalType), pb.NewOptional(pb.NewDataLiteral<i32>(-3)), pb.NewDataLiteral<ui32>(1)});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(optionalType), pb.NewDataLiteral<ui32>(1)});

        const auto list = pb.NewList(tupleType, {data3, data4, data1, data2});

        const auto outputType = pb.NewFlowType(pb.NewTupleType({optionalType, optionalType}));
        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.CommonJoinCore(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U), pb.Nth(item, 3U)}; }),
            EJoinKind::Inner, {0U, 0U}, {1U, 1U}, {}, {2U}, 0ULL, {1U}, EAnyJoinSettings::None, 3U, outputType),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 4);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }
}
#endif
}
}

