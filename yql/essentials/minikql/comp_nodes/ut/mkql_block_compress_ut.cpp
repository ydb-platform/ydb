#include "mkql_computation_node_ut.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_block_builder.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_program_builder_test_utils.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

#include <util/random/random.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <bool UseRandom, bool DoFilter, bool LLVM>
void DoNestedTuplesCompressTest() {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto resultTupleType = NTest::ConvertToMinikqlType<
        std::tuple<ui64, std::tuple<ui64, std::tuple<ui64, bool, NTest::TUtf8>, NTest::TUtf8>>>(pb);

    TRuntimeNode::TList items;
    static_assert(MaxBlockSizeInBytes % 4 == 0);
    constexpr size_t fixedStrSize = MaxBlockSizeInBytes / 4;

    if constexpr (UseRandom) {
        SetRandomSeed(0);
    }

    for (size_t i = 0; i < 95; ++i) {
        std::string str;
        bool filterValue;
        if constexpr (UseRandom) {
            size_t len = RandomNumber<size_t>(2 * MaxBlockSizeInBytes);
            str.reserve(len);
            for (size_t i = 0; i < len; ++i) {
                str.push_back((char)RandomNumber<ui8>(128));
            }
            if constexpr (DoFilter) {
                filterValue = RandomNumber<ui8>() & 1;
            } else {
                filterValue = true;
            }
        } else {
            str = std::string(fixedStrSize, ' ' + i);
            if constexpr (DoFilter) {
                filterValue = (i % 4) < 2;
            } else {
                filterValue = true;
            }
        }

        const auto finalTuple = NTest::ConvertValueToLiteralNode(pb,
                                                                 std::tuple<ui64, std::tuple<ui64, std::tuple<ui64, bool, NTest::TUtf8>, NTest::TUtf8>, bool>{
                                                                     i,
                                                                     {i, {i, bool(i % 2), NTest::TUtf8{TStringBuf((i % 2) ? str : std::string())}},
                                                                      NTest::TUtf8{TStringBuf((i % 2) ? std::string() : str)}},
                                                                     filterValue});
        items.push_back(finalTuple);
    }

    const auto list = pb.NewList(items.front().GetStaticType(), std::move(items));

    auto node = pb.ToFlow(list);
    node = pb.ExpandMap(node, [&](TRuntimeNode item) -> TRuntimeNode::TList {
        return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)};
    });
    node = pb.WideToBlocks(pb.FromFlow(node));

    node = pb.BlockExpandChunked(node);
    node = pb.WideSkipBlocks(node, NTest::ConvertValueToLiteralNode(pb, ui64(19)));
    node = pb.BlockCompress(node, 2);
    node = pb.ToFlow(pb.WideFromBlocks(node));

    node = pb.NarrowMap(node, [&](TRuntimeNode::TList items) -> TRuntimeNode {
        return pb.NewTuple(resultTupleType, {items[0], items[1]});
    });

    const auto pgmReturn = pb.Collect(node);
    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();

    if constexpr (UseRandom) {
        SetRandomSeed(0);
    }

    for (size_t i = 0; i < 95; ++i) {
        std::string str;
        bool filterValue;
        if constexpr (UseRandom) {
            size_t len = RandomNumber<size_t>(2 * MaxBlockSizeInBytes);
            str.reserve(len);
            for (size_t i = 0; i < len; ++i) {
                str.push_back((char)RandomNumber<ui8>(128));
            }
            if constexpr (DoFilter) {
                filterValue = RandomNumber<ui8>() & 1;
            } else {
                filterValue = true;
            }
        } else {
            str = std::string(fixedStrSize, ' ' + i);
            if constexpr (DoFilter) {
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
        TStringBuf outerStr = outerStrVal.AsStringRef();

        ui64 innerNum = inner.GetElement(0).Get<ui64>();
        bool innerBool = inner.GetElement(1).Get<bool>();
        auto innerStrVal = inner.GetElement(2);

        TStringBuf innerStr = innerStrVal.AsStringRef();

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

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockCompressTest) {
Y_UNIT_TEST_LLVM(CompressBasic) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<std::tuple<bool, ui64, bool>>{
                                                           {false, ui64(1), true},
                                                           {true, ui64(2), false},
                                                           {false, ui64(3), true},
                                                           {false, ui64(4), true},
                                                           {true, ui64(5), false},
                                                           {true, ui64(6), true},
                                                           {false, ui64(7), true}});
    const auto flow = pb.ToFlow(list);

    const auto wideFlow = pb.ExpandMap(flow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
        return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)};
    });
    const auto uncompressedBlocks = pb.WideToBlocks(pb.FromFlow(wideFlow));
    const auto compressedBlocks = pb.BlockCompress(uncompressedBlocks, 0);
    const auto compressedFlow = pb.ToFlow(pb.WideFromBlocks(compressedBlocks));
    const auto narrowFlow = pb.NarrowMap(compressedFlow, [&](TRuntimeNode::TList items) -> TRuntimeNode {
        return pb.NewTuple({items[0], items[1]});
    });

    const auto pgmReturn = pb.Collect(narrowFlow);

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto res = graph->GetValue();

    AssertUnboxedValueElementEqual(res,
                                   TVector<std::tuple<ui64, bool>>{{ui64(2), false}, {ui64(5), false}, {ui64(6), true}});
}

Y_UNIT_TEST_LLVM(CompressNestedTuples) {
    DoNestedTuplesCompressTest<false, false, LLVM>();
}

Y_UNIT_TEST_LLVM(CompressNestedTuplesWithFilter) {
    DoNestedTuplesCompressTest<false, true, LLVM>();
}

Y_UNIT_TEST_LLVM(CompressNestedTuplesWithRandom) {
    DoNestedTuplesCompressTest<true, false, LLVM>();
}

Y_UNIT_TEST_LLVM(CompressNestedTuplesWithRandomWithFilter) {
    DoNestedTuplesCompressTest<true, true, LLVM>();
}

} // Y_UNIT_TEST_SUITE(TMiniKQLBlockCompressTest)

} // namespace NMiniKQL
} // namespace NKikimr
