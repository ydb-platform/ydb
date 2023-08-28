#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_block_builder.h>

#include <util/random/random.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

void DoNestedTuplesCompressTest(bool useRandom, bool doFilter) {
    TSetup<false> setup;
    auto& pb = *setup.PgmBuilder;

    const auto ui64Type   = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto boolType   = pb.NewDataType(NUdf::TDataType<bool>::Id);
    const auto utf8Type   = pb.NewDataType(NUdf::EDataSlot::Utf8);

    const auto innerTupleType = pb.NewTupleType({ui64Type, boolType, utf8Type});
    const auto outerTupleType = pb.NewTupleType({ui64Type, innerTupleType, utf8Type});
    const auto finalTupleType = pb.NewTupleType({ui64Type, outerTupleType, boolType});

    const auto resultTupleType = pb.NewTupleType({ui64Type, outerTupleType});

    TVector<TRuntimeNode> items;
    static_assert(MaxBlockSizeInBytes % 4 == 0);
    constexpr size_t fixedStrSize = MaxBlockSizeInBytes / 4;

    if (useRandom) {
        SetRandomSeed(0);
    }

    for (size_t i = 0; i < 95; ++i) {
        std::string str;
        bool filterValue;
        if (useRandom) {
            size_t len = RandomNumber<size_t>(2 * MaxBlockSizeInBytes);
            str.reserve(len);
            for (size_t i = 0; i < len; ++i) {
                str.push_back((char)RandomNumber<ui8>(128));
            }
            if (doFilter) {
                filterValue = RandomNumber<ui8>() & 1;
            } else {
                filterValue = true;
            }
        } else {
            str = std::string(fixedStrSize, ' ' + i);
            if (doFilter) {
                filterValue = (i % 4) < 2;
            } else {
                filterValue = true;
            }
        }

        auto innerTuple = pb.NewTuple(innerTupleType, {
            pb.NewDataLiteral<ui64>(i),
            pb.NewDataLiteral<bool>(i % 2),
            pb.NewDataLiteral<NUdf::EDataSlot::Utf8>((i % 2) ? str : std::string()),
            });
        auto outerTuple = pb.NewTuple(outerTupleType, {
            pb.NewDataLiteral<ui64>(i),
            innerTuple,
            pb.NewDataLiteral<NUdf::EDataSlot::Utf8>((i % 2) ? std::string() : str),
            });

        auto finalTuple = pb.NewTuple(finalTupleType, {
            pb.NewDataLiteral<ui64>(i),
            outerTuple,
            pb.NewDataLiteral<bool>(filterValue),
            });
        items.push_back(finalTuple);
    }

    const auto list = pb.NewList(finalTupleType, std::move(items));

    auto node = pb.ToFlow(list);
    node = pb.ExpandMap(node, [&](TRuntimeNode item) -> TRuntimeNode::TList {
        return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)};
    });
    node = pb.WideToBlocks(node);

    node = pb.BlockExpandChunked(node);
    node = pb.WideSkipBlocks(node, pb.NewDataLiteral<ui64>(19));
    node = pb.BlockCompress(node, 2);
    node = pb.WideFromBlocks(node);

    node = pb.NarrowMap(node, [&](TRuntimeNode::TList items) -> TRuntimeNode {
        return pb.NewTuple(resultTupleType, {items[0], items[1]});
    });

    const auto pgmReturn = pb.ForwardList(node);
    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();

    if (useRandom) {
        SetRandomSeed(0);
    }

    for (size_t i = 0; i < 95; ++i) {
        std::string str;
        bool filterValue;
        if (useRandom) {
            size_t len = RandomNumber<size_t>(2 * MaxBlockSizeInBytes);
            str.reserve(len);
            for (size_t i = 0; i < len; ++i) {
                str.push_back((char)RandomNumber<ui8>(128));
            }
            if (doFilter) {
                filterValue = RandomNumber<ui8>() & 1;
            } else {
                filterValue = true;
            }
        } else {
            str = std::string(fixedStrSize, ' ' + i);
            if (doFilter) {
                filterValue = (i % 4) < 2;
            } else {
                filterValue = true;
            }
        }

        if (i < 19 || !filterValue) {
            continue;
        }

        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        ui64 topNum = item.GetElement(0).Get<ui64>();
        const auto& outer = item.GetElement(1);

        ui64 num = outer.GetElement(0).Get<ui64>();
        const auto& inner = outer.GetElement(1);

        auto outerStrVal = outer.GetElement(2);
        std::string_view outerStr = outerStrVal.AsStringRef();

        ui64 innerNum = inner.GetElement(0).Get<ui64>();
        bool innerBool = inner.GetElement(1).Get<bool>();
        auto innerStrVal = inner.GetElement(2);

        std::string_view innerStr = innerStrVal.AsStringRef();

        UNIT_ASSERT_VALUES_EQUAL(num, i);
        UNIT_ASSERT_VALUES_EQUAL(topNum, i);
        UNIT_ASSERT_VALUES_EQUAL(innerNum, i);
        UNIT_ASSERT_VALUES_EQUAL(innerBool, i % 2);

        std::string expectedInner = (i % 2) ? str : std::string();
        std::string expectedOuter = (i % 2) ? std::string() : str;

        UNIT_ASSERT(innerStr == expectedInner);
        UNIT_ASSERT(outerStr == expectedOuter);
    }

    NUdf::TUnboxedValue item;
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

} //namespace

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

Y_UNIT_TEST(CompressNestedTuples) {
    DoNestedTuplesCompressTest(false, true);
    DoNestedTuplesCompressTest(false, false);
}

Y_UNIT_TEST(CompressNestedTuplesWithRandom) {
    DoNestedTuplesCompressTest(true, true);
    DoNestedTuplesCompressTest(true, false);
}

}

} // namespace NMiniKQL
} // namespace NKikimr
