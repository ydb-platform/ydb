#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr {
namespace NMiniKQL {
Y_UNIT_TEST_SUITE(TMiniKQLBlockCompressTest) {
Y_UNIT_TEST(CompressBasic) {
    TSetup<false> setup;
    auto& pb = *setup.PgmBuilder;

    const auto ui64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto boolType = pb.NewDataType(NUdf::TDataType<bool>::Id);
    const auto tupleType = pb.NewTupleType({boolType, ui64Type, boolType});

    const auto data1 = pb.NewTuple(tupleType, {pb.NewDataLiteral<bool>(false), pb.NewDataLiteral<ui64>(1), pb.NewDataLiteral<bool>(true)});
    const auto data2 = pb.NewTuple(tupleType, {pb.NewDataLiteral<bool>(true),  pb.NewDataLiteral<ui64>(2), pb.NewDataLiteral<bool>(false)});
    const auto data3 = pb.NewTuple(tupleType, {pb.NewDataLiteral<bool>(false), pb.NewDataLiteral<ui64>(3), pb.NewDataLiteral<bool>(true)});
    const auto data4 = pb.NewTuple(tupleType, {pb.NewDataLiteral<bool>(false), pb.NewDataLiteral<ui64>(4), pb.NewDataLiteral<bool>(true)});
    const auto data5 = pb.NewTuple(tupleType, {pb.NewDataLiteral<bool>(true),  pb.NewDataLiteral<ui64>(5), pb.NewDataLiteral<bool>(false)});
    const auto data6 = pb.NewTuple(tupleType, {pb.NewDataLiteral<bool>(true),  pb.NewDataLiteral<ui64>(6), pb.NewDataLiteral<bool>(true)});
    const auto data7 = pb.NewTuple(tupleType, {pb.NewDataLiteral<bool>(false), pb.NewDataLiteral<ui64>(7), pb.NewDataLiteral<bool>(true)});

    const auto list = pb.NewList(tupleType, {data1, data2, data3, data4, data5, data6, data7});
    const auto flow = pb.ToFlow(list);

    const auto wideFlow = pb.ExpandMap(flow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
        return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)};
    });
    const auto compressedFlow = pb.WideFromBlocks(pb.BlockCompress(pb.WideToBlocks(wideFlow), 0));
    const auto narrowFlow = pb.NarrowMap(compressedFlow, [&](TRuntimeNode::TList items) -> TRuntimeNode {
        return pb.NewTuple({items[0], items[1]});
    });

    const auto pgmReturn = pb.ForwardList(narrowFlow);

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();

    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<ui64>(), 2);
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).Get<bool>(), false);

    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<ui64>(), 5);
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).Get<bool>(), false);

    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<ui64>(), 6);
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).Get<bool>(), true);

    UNIT_ASSERT(!iterator.Next(item));
}

}

} // namespace NMiniKQL
} // namespace NKikimr
