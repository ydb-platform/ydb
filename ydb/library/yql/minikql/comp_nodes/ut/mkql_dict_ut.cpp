#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLDictRelatedNodesTest) {
    Y_UNIT_TEST_LLVM(TestDictLength) {
        TSetup<LLVM> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

        const auto key1 = pgmBuilder.NewDataLiteral<ui32>(1);
        const auto key2 = pgmBuilder.NewDataLiteral<ui32>(2);
        const auto key3 = pgmBuilder.NewDataLiteral<ui32>(2);
        const auto payload1 = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("A");
        const auto payload2 = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("B");
        const auto payload3 = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("C");
        TVector<std::pair<TRuntimeNode, TRuntimeNode>> dictItems;
        dictItems.push_back(std::make_pair(key1, payload1));
        dictItems.push_back(std::make_pair(key2, payload2));
        dictItems.push_back(std::make_pair(key3, payload3));
        const auto dictType = pgmBuilder.NewDictType(pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id),
            pgmBuilder.NewDataType(NUdf::TDataType<char*>::Id), false);
        const auto dict = pgmBuilder.NewDict(dictType, dictItems);
        const auto pgmReturn = pgmBuilder.Length(dict);

        const auto graph = setup.BuildGraph(pgmReturn);
        UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().template Get<ui64>(), 2);
    }

    Y_UNIT_TEST_LLVM(TestDictContains) {
        TSetup<LLVM> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

        const auto key1 = pgmBuilder.NewDataLiteral<ui32>(1);
        const auto key2 = pgmBuilder.NewDataLiteral<ui32>(2);
        const auto key3 = pgmBuilder.NewDataLiteral<ui32>(2);
        const auto missingKey = pgmBuilder.NewDataLiteral<ui32>(3);
        const auto payload1 = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("A");
        const auto payload2 = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("B");
        const auto payload3 = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("C");
        TVector<std::pair<TRuntimeNode, TRuntimeNode>> dictItems;
        dictItems.push_back(std::make_pair(key1, payload1));
        dictItems.push_back(std::make_pair(key2, payload2));
        dictItems.push_back(std::make_pair(key3, payload3));
        const auto dictType = pgmBuilder.NewDictType(pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id),
            pgmBuilder.NewDataType(NUdf::TDataType<char*>::Id), false);
        const auto dict = pgmBuilder.NewDict(dictType, dictItems);
        const auto keys = pgmBuilder.NewList(pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id), {key1, key2, missingKey});

        const auto pgmReturn = pgmBuilder.Map(keys,
        [&](TRuntimeNode key) {
            return pgmBuilder.Contains(dict, key);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestDictLookup) {
        TSetup<LLVM> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

        const auto key1 = pgmBuilder.NewDataLiteral<ui32>(1);
        const auto key2 = pgmBuilder.NewDataLiteral<ui32>(2);
        const auto key3 = pgmBuilder.NewDataLiteral<ui32>(2);
        const auto missingKey = pgmBuilder.NewDataLiteral<ui32>(3);
        const auto payload1 = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("A");
        const auto payload2 = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("B");
        const auto payload3 = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("C");
        TVector<std::pair<TRuntimeNode, TRuntimeNode>> dictItems;
        dictItems.push_back(std::make_pair(key1, payload1));
        dictItems.push_back(std::make_pair(key2, payload2));
        dictItems.push_back(std::make_pair(key3, payload3));
        const auto dictType = pgmBuilder.NewDictType(pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id),
            pgmBuilder.NewDataType(NUdf::TDataType<char*>::Id), false);
        const auto dict = pgmBuilder.NewDict(dictType, dictItems);
        const auto keys = pgmBuilder.NewList(pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id), {key1, key2, missingKey});

        const auto pgmReturn = pgmBuilder.Map(keys,
        [&](TRuntimeNode key) {
            return pgmBuilder.Lookup(dict, key);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item);
        UNBOXED_VALUE_STR_EQUAL(item, "A");
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item);
        UNBOXED_VALUE_STR_EQUAL(item, "B");
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    template<bool Multi>
    TRuntimeNode PrepareTestDict(TProgramBuilder& pgmBuilder, TRuntimeNode(TProgramBuilder::* factory)(TRuntimeNode list, bool multi,
        const TProgramBuilder::TUnaryLambda& keySelector,
        const TProgramBuilder::TUnaryLambda& payloadSelector, bool isCompact, ui64 itemsCountHint)) {
        const auto key1 = pgmBuilder.NewDataLiteral<ui32>(1);
        const auto key2 = pgmBuilder.NewDataLiteral<ui32>(2);
        const auto key3 = pgmBuilder.NewDataLiteral<ui32>(2);
        const auto key4 = pgmBuilder.NewDataLiteral<ui32>(5);
        const auto key5 = pgmBuilder.NewDataLiteral<ui32>(7);
        const auto payload1 = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("A");
        const auto payload2 = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("B");
        const auto payload3 = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("C");
        const auto payload4 = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("D");
        const auto payload5 = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("E");
        auto structType = pgmBuilder.NewStructType(pgmBuilder.NewEmptyStructType(), "Key", pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id));
        structType = pgmBuilder.NewStructType(structType, "Payload", pgmBuilder.NewDataType(NUdf::TDataType<char*>::Id));
        const auto list = pgmBuilder.NewList(structType, {
            pgmBuilder.AddMember(pgmBuilder.AddMember(pgmBuilder.NewEmptyStruct(), "Key", key3), "Payload", payload3),
            pgmBuilder.AddMember(pgmBuilder.AddMember(pgmBuilder.NewEmptyStruct(), "Key", key1), "Payload", payload1),
            pgmBuilder.AddMember(pgmBuilder.AddMember(pgmBuilder.NewEmptyStruct(), "Key", key5), "Payload", payload5),
            pgmBuilder.AddMember(pgmBuilder.AddMember(pgmBuilder.NewEmptyStruct(), "Key", key4), "Payload", payload4),
            pgmBuilder.AddMember(pgmBuilder.AddMember(pgmBuilder.NewEmptyStruct(), "Key", key2), "Payload", payload2)
        });
        const auto dict = (pgmBuilder.*factory)(list, Multi,
            [&](TRuntimeNode item) {
            return pgmBuilder.Member(item, "Key");
        },
            [&](TRuntimeNode item) {
            return pgmBuilder.Member(item, "Payload");
        }, false, 0);
        return dict;
    }

    template<bool LLVM>
    void TestConvertedDictContains(TRuntimeNode(TProgramBuilder::* factory)(TRuntimeNode list, bool multi,
        const TProgramBuilder::TUnaryLambda& keySelector,
        const TProgramBuilder::TUnaryLambda& payloadSelector, bool isCompact, ui64 itemsCountHint)) {

        TSetup<LLVM> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        const auto dict = PrepareTestDict<false>(pgmBuilder, factory);

        const auto key1 = pgmBuilder.NewDataLiteral<ui32>(1);
        const auto key2 = pgmBuilder.NewDataLiteral<ui32>(2);
        const auto missingKey = pgmBuilder.NewDataLiteral<ui32>(42);
        const auto keys = pgmBuilder.NewList(pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id), {key1, key2, missingKey});

        const auto pgmReturn = pgmBuilder.Map(keys,
        [&](TRuntimeNode key) {
            return pgmBuilder.Contains(dict, key);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    template<bool LLVM>
    void TestConvertedDictLookup(TRuntimeNode(TProgramBuilder::* factory)(TRuntimeNode list, bool multi,
        const TProgramBuilder::TUnaryLambda& keySelector,
        const TProgramBuilder::TUnaryLambda& payloadSelector, bool isCompact, ui64 itemsCountHint)) {

        TSetup<LLVM> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        const auto dict = PrepareTestDict<false>(pgmBuilder, factory);

        const auto key1 = pgmBuilder.NewDataLiteral<ui32>(1);
        const auto key2 = pgmBuilder.NewDataLiteral<ui32>(2);
        const auto missingKey = pgmBuilder.NewDataLiteral<ui32>(18);
        const auto keys = pgmBuilder.NewList(pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id), {key1, key2, missingKey});

        const auto pgmReturn = pgmBuilder.Map(keys,
        [&](TRuntimeNode key) {
            return pgmBuilder.Lookup(dict, key);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item);
        UNBOXED_VALUE_STR_EQUAL(item, "A");
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item);
        UNBOXED_VALUE_STR_EQUAL(item, "C");
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestSortedDictContains) {
        TestConvertedDictContains<LLVM>(&TProgramBuilder::ToSortedDict);
    }

    Y_UNIT_TEST_LLVM(TestSortedDictLookup) {
        TestConvertedDictLookup<LLVM>(&TProgramBuilder::ToSortedDict);
    }

    Y_UNIT_TEST_LLVM(TestHashedDictContains) {
        TestConvertedDictContains<LLVM>(&TProgramBuilder::ToHashedDict);
    }

    Y_UNIT_TEST_LLVM(TestHashedDictLookup) {
        TestConvertedDictLookup<LLVM>(&TProgramBuilder::ToHashedDict);
    }

    template<bool LLVM, bool SortBeforeCompare>
    void TestDictItemsImpl(TRuntimeNode(TProgramBuilder::* factory)(TRuntimeNode list, bool multi,
        const TProgramBuilder::TUnaryLambda& keySelector,
        const TProgramBuilder::TUnaryLambda& payloadSelector, bool isCompact, ui64 itemsCountHint)) {
        TSetup<LLVM> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        const auto dict = PrepareTestDict<false>(pgmBuilder, factory);
        const auto pgmReturn = pgmBuilder.DictItems(dict);
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();

        std::vector<std::pair<ui32, TString>> items;
        for (NUdf::TUnboxedValue item;  iterator.Next(item);) {
            const auto& pay = item.GetElement(1);
            items.emplace_back(item.GetElement(0).template Get<ui32>(), pay.AsStringRef());
        }

        if (SortBeforeCompare) {
            std::sort(items.begin(), items.end(), [](const std::pair<ui32, TString>& left, const std::pair<ui32, TString>& right) {
                return left.first < right.first;
            });
        }

        UNIT_ASSERT_VALUES_EQUAL(items.size(), 4U);
        UNIT_ASSERT_VALUES_EQUAL(items[0].first, 1);
        UNIT_ASSERT_VALUES_EQUAL(items[0].second, "A");
        UNIT_ASSERT_VALUES_EQUAL(items[1].first, 2);
        UNIT_ASSERT_VALUES_EQUAL(items[1].second, "C");
        UNIT_ASSERT_VALUES_EQUAL(items[2].first, 5);
        UNIT_ASSERT_VALUES_EQUAL(items[2].second, "D");
        UNIT_ASSERT_VALUES_EQUAL(items[3].first, 7);
        UNIT_ASSERT_VALUES_EQUAL(items[3].second, "E");
    }

    template<bool LLVM, bool SortBeforeCompare>
    void TestDictKeysImpl(TRuntimeNode(TProgramBuilder::* factory)(TRuntimeNode list, bool multi,
        const TProgramBuilder::TUnaryLambda& keySelector,
        const TProgramBuilder::TUnaryLambda& payloadSelector, bool isCompact, ui64 itemsCountHint)) {
        TSetup<LLVM> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        const auto dict = PrepareTestDict<false>(pgmBuilder, factory);
        const auto pgmReturn = pgmBuilder.DictKeys(dict);
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();

        std::vector<ui32> items;
        for (NUdf::TUnboxedValue item;  iterator.Next(item);) {
            items.emplace_back(item.template Get<ui32>());
        }

        if (SortBeforeCompare) {
            std::sort(items.begin(), items.end());
        }

        UNIT_ASSERT_VALUES_EQUAL(items.size(), 4U);
        UNIT_ASSERT_VALUES_EQUAL(items[0], 1);
        UNIT_ASSERT_VALUES_EQUAL(items[1], 2);
        UNIT_ASSERT_VALUES_EQUAL(items[2], 5);
        UNIT_ASSERT_VALUES_EQUAL(items[3], 7);
    }

    template<bool LLVM, bool SortBeforeCompare>
    void TestDictPayloadsImpl(TRuntimeNode(TProgramBuilder::* factory)(TRuntimeNode list, bool multi,
        const TProgramBuilder::TUnaryLambda& keySelector,
        const TProgramBuilder::TUnaryLambda& payloadSelector, bool isCompact, ui64 itemsCountHint)) {
        TSetup<LLVM> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        const auto dict = PrepareTestDict<false>(pgmBuilder, factory);
        const auto pgmReturn = pgmBuilder.DictPayloads(dict);
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();

        std::vector<TString> items;
        for (NUdf::TUnboxedValue item;  iterator.Next(item);) {
            items.emplace_back(item.AsStringRef());
        }

        if (SortBeforeCompare) {
            std::sort(items.begin(), items.end());
        }

        UNIT_ASSERT_VALUES_EQUAL(items.size(), 4U);
        UNIT_ASSERT_VALUES_EQUAL(items[0], "A");
        UNIT_ASSERT_VALUES_EQUAL(items[1], "C");
        UNIT_ASSERT_VALUES_EQUAL(items[2], "D");
        UNIT_ASSERT_VALUES_EQUAL(items[3], "E");
    }

    Y_UNIT_TEST_LLVM(TestSortedDictItems) {
        TestDictItemsImpl<LLVM, false>(&TProgramBuilder::ToSortedDict);
    }

    Y_UNIT_TEST_LLVM(TestHashedDictItems) {
        TestDictItemsImpl<LLVM, true>(&TProgramBuilder::ToHashedDict);
    }

    Y_UNIT_TEST_LLVM(TestSortedDictKeys) {
        TestDictKeysImpl<LLVM, false>(&TProgramBuilder::ToSortedDict);
    }

    Y_UNIT_TEST_LLVM(TestHashedDictKeys) {
        TestDictKeysImpl<LLVM, true>(&TProgramBuilder::ToHashedDict);
    }

    Y_UNIT_TEST_LLVM(TestSortedPayloadsKeys) {
        TestDictPayloadsImpl<LLVM, false>(&TProgramBuilder::ToSortedDict);
    }

    Y_UNIT_TEST_LLVM(TestHashedPayloadsKeys) {
        TestDictPayloadsImpl<LLVM, true>(&TProgramBuilder::ToHashedDict);
    }

    template<bool LLVM>
    void TestConvertedMultiDictLookup(TRuntimeNode(TProgramBuilder::* factory)(TRuntimeNode list, bool multi,
        const TProgramBuilder::TUnaryLambda& keySelector,
        const TProgramBuilder::TUnaryLambda& payloadSelector, bool isCompact, ui64 itemsCountHint)) {

        TSetup<LLVM> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        const auto dict = PrepareTestDict<true>(pgmBuilder, factory);
        const auto key1 = pgmBuilder.NewDataLiteral<ui32>(1);
        const auto key2 = pgmBuilder.NewDataLiteral<ui32>(2);
        const auto missingKey = pgmBuilder.NewDataLiteral<ui32>(3);
        const auto keys = pgmBuilder.NewList(pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id), {key1, key2, missingKey});
        const auto pgmReturn = pgmBuilder.Map(keys,
            [&](TRuntimeNode key) {
                return pgmBuilder.Lookup(dict, key);
            });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item, item2;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item);
        auto iter2 = item.GetListIterator();
        UNIT_ASSERT(iter2.Next(item2));
        UNBOXED_VALUE_STR_EQUAL(item2, "A");
        UNIT_ASSERT(!iter2.Next(item2));
        UNIT_ASSERT(!iter2.Next(item2));

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item);
        iter2 = item.GetListIterator();
        UNIT_ASSERT(iter2.Next(item2));
        UNBOXED_VALUE_STR_EQUAL(item2, "C");
        UNIT_ASSERT(iter2.Next(item2));
        UNBOXED_VALUE_STR_EQUAL(item2, "B");
        UNIT_ASSERT(!iter2.Next(item2));
        UNIT_ASSERT(!iter2.Next(item2));

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestSortedMultiDictLookup) {
        TestConvertedMultiDictLookup<LLVM>(&TProgramBuilder::ToSortedDict);
    }

    Y_UNIT_TEST_LLVM(TestHashedMultiDictLookup) {
        TestConvertedMultiDictLookup<LLVM>(&TProgramBuilder::ToHashedDict);
    }
}

}
}
