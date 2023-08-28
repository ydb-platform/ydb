#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr {
namespace NMiniKQL {

#if !defined(MKQL_RUNTIME_VERSION) || MKQL_RUNTIME_VERSION >= 36u
Y_UNIT_TEST_SUITE(TMiniKQLWideStreamTest) {

Y_UNIT_TEST_LLVM(TestSimple) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto ui64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto tupleType = pb.NewTupleType({ui64Type, ui64Type});

    const auto data1 = pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(1), pb.NewDataLiteral<ui64>(10)});
    const auto data2 = pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(2), pb.NewDataLiteral<ui64>(20)});
    const auto data3 = pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(3), pb.NewDataLiteral<ui64>(30)});

    const auto list = pb.NewList(tupleType, {data1, data2, data3});
    const auto flow = pb.ToFlow(list);

    const auto wideFlow = pb.ExpandMap(flow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
        return {pb.Nth(item, 0U), pb.Nth(item, 1U)};
    });

    const auto wideStream = pb.FromFlow(wideFlow);
    const auto newWideFlow = pb.ToFlow(wideStream);

    const auto narrowFlow = pb.NarrowMap(newWideFlow, [&](TRuntimeNode::TList items) -> TRuntimeNode {
        return pb.Sub(items[1], items[0]);
    });
    const auto pgmReturn = pb.Collect(narrowFlow);

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();

    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 9);

    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 18);

    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 27);

    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}
}

#endif

}
}
