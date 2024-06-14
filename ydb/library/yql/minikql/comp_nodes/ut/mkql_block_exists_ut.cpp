#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_block_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

void DoBlockExistsOffset(size_t length, size_t offset) {
    TSetup<false> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto ui64Type    = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto optui64Type = pb.NewOptionalType(ui64Type);
    const auto boolType   = pb.NewDataType(NUdf::TDataType<bool>::Id);

    const auto inputTupleType  = pb.NewTupleType({ui64Type, optui64Type, optui64Type, optui64Type});
    const auto outputTupleType = pb.NewTupleType({ui64Type, boolType, boolType, boolType});

    TRuntimeNode::TList input;
    TVector<bool> isNull;
    static_assert(MaxBlockSizeInBytes % 4 == 0);

    const auto drng = CreateDeterministicRandomProvider(std::time(nullptr));

    for (size_t i = 0; i < length; i++) {
        const ui64 randomValue = drng->GenRand();
        const auto maybeNull = (randomValue % 2)
            ? pb.NewOptional(pb.NewDataLiteral<ui64>(randomValue / 2))
            : pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui64>::Id);

        const auto inputTuple = pb.NewTuple(inputTupleType, {
            pb.NewDataLiteral<ui64>(i),
            maybeNull,
            pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui64>::Id),
            pb.NewOptional(pb.NewDataLiteral<ui64>(i))
        });

        input.push_back(inputTuple);
        isNull.push_back((randomValue % 2) != 0);
    }

    const auto list = pb.NewList(inputTupleType, std::move(input));

    auto node = pb.ToFlow(list);
    node = pb.ExpandMap(node, [&](TRuntimeNode item) -> TRuntimeNode::TList {
        return {
            pb.Nth(item, 0),
            pb.Nth(item, 1),
            pb.Nth(item, 2),
            pb.Nth(item, 3)
        };
    });
    node = pb.WideToBlocks(node);
    if (offset > 0) {
        node = pb.WideSkipBlocks(node, pb.NewDataLiteral<ui64>(offset));
    }
    node = pb.WideMap(node, [&](TRuntimeNode::TList items) -> TRuntimeNode::TList {
        return {
            items[0],
            pb.BlockExists(items[1]),
            pb.BlockExists(items[2]),
            pb.BlockExists(items[3]),
            items[4],
        };
    });
    node = pb.WideFromBlocks(node);
    node = pb.NarrowMap(node, [&](TRuntimeNode::TList items) -> TRuntimeNode {
        return pb.NewTuple(outputTupleType, {items[0], items[1], items[2], items[3]});
    });

    const auto pgmReturn = pb.Collect(node);
    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();

    for (size_t i = 0; i < length; i++) {
        if (i < offset) {
            continue;
        }

        NUdf::TUnboxedValue outputTuple;
        UNIT_ASSERT(iterator.Next(outputTuple));
        const ui32 key = outputTuple.GetElement(0).Get<ui64>();
        const bool maybeNull = outputTuple.GetElement(1).Get<bool>();
        const bool alwaysNull = outputTuple.GetElement(2).Get<bool>();
        const bool neverNull = outputTuple.GetElement(3).Get<bool>();

        UNIT_ASSERT_VALUES_EQUAL(key, i);
        UNIT_ASSERT_VALUES_EQUAL(maybeNull, isNull[i]);
        UNIT_ASSERT_VALUES_EQUAL(alwaysNull, false);
        UNIT_ASSERT_VALUES_EQUAL(neverNull, true);
    }

    NUdf::TUnboxedValue outputTuple;
    UNIT_ASSERT(!iterator.Next(outputTuple));
    UNIT_ASSERT(!iterator.Next(outputTuple));
}

} //namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockExistsTest) {

Y_UNIT_TEST(ExistsWithOffset) {
    const size_t length = 20;
    for (size_t offset = 0; offset < length; offset++) {
        DoBlockExistsOffset(length, offset);
    }
}

} // Y_UNIT_TEST_SUITE

} // namespace NMiniKQL
} // namespace NKikimr
