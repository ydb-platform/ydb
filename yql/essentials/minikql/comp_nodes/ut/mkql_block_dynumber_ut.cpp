#include "mkql_block_serializer_test_utils.h"
#include "mkql_block_test_helper.h"
#include "mkql_computation_node_ut.h"
#include "mkql_program_builder_test_utils.h"

#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/computation/mkql_value_builder.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>
#include <yql/essentials/public/udf/arrow/block_builder.h>

namespace NKikimr::NMiniKQL {

namespace {

constexpr TStringBuf DyNumber1 = "0";
constexpr TStringBuf DyNumber2 = "-123.45e3";
constexpr TStringBuf DyNumber3 = "150e2";

template <typename T>
void TestIdentityKernel(T input, T expected) {
    TBlockHelper().TestKernelFuzzied(input, expected,
                                     [](TSetup<false>&, TRuntimeNode node) { return node; });
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockDyNumberTest) {

Y_UNIT_TEST(DyNumberKernel) {
    TestIdentityKernel(
        TVector<NTest::TTestDyNumber>{
            NTest::TTestDyNumber{DyNumber1},
            NTest::TTestDyNumber{DyNumber2},
            NTest::TTestDyNumber{DyNumber3},
        },
        TVector<NTest::TTestDyNumber>{
            NTest::TTestDyNumber{DyNumber1},
            NTest::TTestDyNumber{DyNumber2},
            NTest::TTestDyNumber{DyNumber3},
        });
}

Y_UNIT_TEST(DyNumberKernelWithNulls) {
    TestIdentityKernel(
        TVector<TMaybe<NTest::TTestDyNumber>>{
            Nothing(),
            NTest::TTestDyNumber{DyNumber1},
            NTest::TTestDyNumber{DyNumber2},
            Nothing(),
        },
        TVector<TMaybe<NTest::TTestDyNumber>>{
            Nothing(),
            NTest::TTestDyNumber{DyNumber1},
            NTest::TTestDyNumber{DyNumber2},
            Nothing(),
        });
}

Y_UNIT_TEST(DyNumberSerializerRoundtrip) {
    TBlockHelper helper;

    const TVector<NTest::TTestDyNumber> data = {
        NTest::TTestDyNumber{DyNumber1},
        NTest::TTestDyNumber{DyNumber2},
        NTest::TTestDyNumber{DyNumber3},
    };
    auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
    Y_UNUSED(graph);
    auto arrayData = TArrowBlock::From(value).GetDatum().array();
    auto restored = DoSerializerRoundtrip(arrayData, itemType, blockType);

    auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
    const TVector<NTest::TTestDyNumber> expected = {
        NTest::TTestDyNumber{DyNumber1},
        NTest::TTestDyNumber{DyNumber2},
        NTest::TTestDyNumber{DyNumber3},
    };

    for (size_t i = 0; i < expected.size(); ++i) {
        NYql::NUdf::AssertUnboxedValueElementEqual(reader->GetItem(*restored, i), expected[i]);
    }
}

Y_UNIT_TEST(DyNumberComparator) {
    TBlockHelper helper;

    const TVector<NTest::TTestDyNumber> data = {
        NTest::TTestDyNumber{DyNumber2},
        NTest::TTestDyNumber{DyNumber3},
        NTest::TTestDyNumber{DyNumber2},
    };
    auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
    Y_UNUSED(graph, blockType);
    auto arrayData = TArrowBlock::From(value).GetDatum().array();

    auto reader0 = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
    auto reader1 = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
    auto reader2 = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
    const auto itemSmaller = reader0->GetItem(*arrayData, 0);
    const auto itemLarger = reader1->GetItem(*arrayData, 1);
    const auto itemEqual = reader2->GetItem(*arrayData, 2);

    auto comparator = TBlockTypeHelper().MakeComparator(itemType);
    UNIT_ASSERT(comparator->Less(itemSmaller, itemLarger));
    UNIT_ASSERT(!comparator->Equals(itemSmaller, itemLarger));
    UNIT_ASSERT(comparator->Equals(itemSmaller, itemEqual));
}

Y_UNIT_TEST(DyNumberHasher) {
    TBlockHelper helper;

    const TVector<NTest::TTestDyNumber> data = {
        NTest::TTestDyNumber{DyNumber1},
        NTest::TTestDyNumber{DyNumber2},
    };
    auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
    Y_UNUSED(graph, blockType);
    auto arrayData = TArrowBlock::From(value).GetDatum().array();

    auto reader0 = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
    auto reader1 = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
    const auto item0 = reader0->GetItem(*arrayData, 0);
    const auto item1 = reader1->GetItem(*arrayData, 1);

    auto hasher = TBlockTypeHelper().MakeHasher(itemType);
    UNIT_ASSERT_VALUES_EQUAL(hasher->Hash(item0), hasher->Hash(item0));
    UNIT_ASSERT_VALUES_UNEQUAL(hasher->Hash(item0), hasher->Hash(item1));
}

Y_UNIT_TEST(DyNumberBlockItemConverter) {
    TBlockHelper helper;

    const TVector<NTest::TTestDyNumber> data = {
        NTest::TTestDyNumber{DyNumber1},
        NTest::TTestDyNumber{DyNumber2},
        NTest::TTestDyNumber{DyNumber3},
    };
    auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
    Y_UNUSED(blockType);
    auto arrayData = TArrowBlock::From(value).GetDatum().array();

    const THolderFactory& holderFactory = graph->GetHolderFactory();
    TDefaultValueBuilder valueBuilder(holderFactory);
    auto converter = MakeBlockItemConverter(TTypeInfoHelper(), itemType, valueBuilder.GetPgBuilder());
    auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);

    for (size_t i = 0; i < data.size(); ++i) {
        const TBlockItem blockItem = reader->GetItem(*arrayData, i);
        NUdf::TUnboxedValue fromBlock(converter->MakeValue(blockItem, holderFactory));
        NYql::NUdf::AssertUnboxedValueElementEqual(fromBlock, data[i]);

        NUdf::TUnboxedValue roundTripped(converter->MakeValue(converter->MakeItem(fromBlock), holderFactory));
        NYql::NUdf::AssertUnboxedValueElementEqual(roundTripped, data[i]);
    }
}

} // Y_UNIT_TEST_SUITE(TMiniKQLBlockDyNumberTest)

} // namespace NKikimr::NMiniKQL
