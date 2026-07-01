#include "mkql_computation_node_ut.h"
#include "mkql_program_builder_test_utils.h"

#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLDictRelatedNodesTest) {
Y_UNIT_TEST_LLVM(TestDictLength) {
    TSetup<LLVM> setup;
    TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

    const auto key1 = NTest::ConvertValueToLiteralNode(pgmBuilder, ui32(1));
    const auto key2 = NTest::ConvertValueToLiteralNode(pgmBuilder, ui32(2));
    const auto payload1 = NTest::ConvertValueToLiteralNode(pgmBuilder, TStringBuf("A"));
    const auto payload2 = NTest::ConvertValueToLiteralNode(pgmBuilder, TStringBuf("B"));
    const auto payload3 = NTest::ConvertValueToLiteralNode(pgmBuilder, TStringBuf("C"));
    const auto dictType = pgmBuilder.NewDictType(pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id),
                                                 pgmBuilder.NewDataType(NUdf::TDataType<char*>::Id), false);
    const auto dict = pgmBuilder.NewDict(dictType, {{key1, payload1}, {key2, payload2}, {key2, payload3}});
    const auto pgmReturn = pgmBuilder.Length(dict);

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), ui64(2));
}

Y_UNIT_TEST_LLVM(TestDictContains) {
    TSetup<LLVM> setup;
    TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

    const auto key1 = NTest::ConvertValueToLiteralNode(pgmBuilder, ui32(1));
    const auto key2 = NTest::ConvertValueToLiteralNode(pgmBuilder, ui32(2));
    const auto payload1 = NTest::ConvertValueToLiteralNode(pgmBuilder, TStringBuf("A"));
    const auto payload2 = NTest::ConvertValueToLiteralNode(pgmBuilder, TStringBuf("B"));
    const auto payload3 = NTest::ConvertValueToLiteralNode(pgmBuilder, TStringBuf("C"));
    const auto dictType = pgmBuilder.NewDictType(pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id),
                                                 pgmBuilder.NewDataType(NUdf::TDataType<char*>::Id), false);
    const auto dict = pgmBuilder.NewDict(dictType, {{key1, payload1}, {key2, payload2}, {key2, payload3}});
    const auto keys = NTest::ConvertValueToLiteralNode(pgmBuilder, TVector<ui32>{1, 2, 3});

    const auto pgmReturn = pgmBuilder.Map(keys,
                                          [&](TRuntimeNode key) {
                                              return pgmBuilder.Contains(dict, key);
                                          });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<bool>{true, true, false});
}

Y_UNIT_TEST_LLVM(TestDictLookup) {
    TSetup<LLVM> setup;
    TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

    const auto key1 = NTest::ConvertValueToLiteralNode(pgmBuilder, ui32(1));
    const auto key2 = NTest::ConvertValueToLiteralNode(pgmBuilder, ui32(2));
    const auto payload1 = NTest::ConvertValueToLiteralNode(pgmBuilder, TStringBuf("A"));
    const auto payload2 = NTest::ConvertValueToLiteralNode(pgmBuilder, TStringBuf("B"));
    const auto payload3 = NTest::ConvertValueToLiteralNode(pgmBuilder, TStringBuf("C"));
    const auto dictType = pgmBuilder.NewDictType(pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id),
                                                 pgmBuilder.NewDataType(NUdf::TDataType<char*>::Id), false);
    const auto dict = pgmBuilder.NewDict(dictType, {{key1, payload1}, {key2, payload2}, {key2, payload3}});
    const auto keys = NTest::ConvertValueToLiteralNode(pgmBuilder, TVector<ui32>{1, 2, 3});

    const auto pgmReturn = pgmBuilder.Map(keys,
                                          [&](TRuntimeNode key) {
                                              return pgmBuilder.Lookup(dict, key);
                                          });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<TString>>{"A", "B", {}});
}

template <bool Multi>
TRuntimeNode PrepareTestDict(TProgramBuilder& pgmBuilder, TRuntimeNode (TProgramBuilder::*factory)(TRuntimeNode list, bool multi,
                                                                                                   const TProgramBuilder::TUnaryLambda& keySelector,
                                                                                                   const TProgramBuilder::TUnaryLambda& payloadSelector, bool isCompact, ui64 itemsCountHint)) {
    using TKeyPayloadRow = NTest::TStructType<NTest::TStructMember<"Key", ui32>,
                                              NTest::TStructMember<"Payload", TStringBuf>>;
    const auto list = NTest::ConvertValueToLiteralNode(pgmBuilder, TVector<TKeyPayloadRow>{
                                                                       {{{ui32(2)}, {"C"}}},
                                                                       {{{ui32(1)}, {"A"}}},
                                                                       {{{ui32(7)}, {"E"}}},
                                                                       {{{ui32(5)}, {"D"}}},
                                                                       {{{ui32(2)}, {"B"}}},
                                                                   });
    const auto dict = (pgmBuilder.*factory)(list, Multi,
                                            [&](TRuntimeNode item) { return pgmBuilder.Member(item, "Key"); },
                                            [&](TRuntimeNode item) { return pgmBuilder.Member(item, "Payload"); }, false, 0);
    return dict;
}

template <bool LLVM>
void TestConvertedDictContains(TRuntimeNode (TProgramBuilder::*factory)(TRuntimeNode list, bool multi,
                                                                        const TProgramBuilder::TUnaryLambda& keySelector,
                                                                        const TProgramBuilder::TUnaryLambda& payloadSelector, bool isCompact, ui64 itemsCountHint)) {
    TSetup<LLVM> setup;
    TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
    const auto dict = PrepareTestDict<false>(pgmBuilder, factory);

    const auto keys = NTest::ConvertValueToLiteralNode(pgmBuilder, TVector<ui32>{1, 2, 42});

    const auto pgmReturn = pgmBuilder.Map(keys,
                                          [&](TRuntimeNode key) {
                                              return pgmBuilder.Contains(dict, key);
                                          });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<bool>{true, true, false});
}

template <bool LLVM>
void TestConvertedDictLookup(TRuntimeNode (TProgramBuilder::*factory)(TRuntimeNode list, bool multi,
                                                                      const TProgramBuilder::TUnaryLambda& keySelector,
                                                                      const TProgramBuilder::TUnaryLambda& payloadSelector, bool isCompact, ui64 itemsCountHint)) {
    TSetup<LLVM> setup;
    TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
    const auto dict = PrepareTestDict<false>(pgmBuilder, factory);

    const auto keys = NTest::ConvertValueToLiteralNode(pgmBuilder, TVector<ui32>{1, 2, 18});

    const auto pgmReturn = pgmBuilder.Map(keys,
                                          [&](TRuntimeNode key) {
                                              return pgmBuilder.Lookup(dict, key);
                                          });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<TStringBuf>>{"A", "C", {}});
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

template <bool LLVM, bool SortBeforeCompare>
void TestDictItemsImpl(TRuntimeNode (TProgramBuilder::*factory)(TRuntimeNode list, bool multi,
                                                                const TProgramBuilder::TUnaryLambda& keySelector,
                                                                const TProgramBuilder::TUnaryLambda& payloadSelector, bool isCompact, ui64 itemsCountHint)) {
    TSetup<LLVM> setup;
    TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
    const auto dict = PrepareTestDict<false>(pgmBuilder, factory);
    const auto pgmReturn = pgmBuilder.DictItems(dict);
    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();

    std::vector<std::pair<ui32, TString>> items;
    for (NUdf::TUnboxedValue item; iterator.Next(item);) {
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

template <bool LLVM, bool SortBeforeCompare>
void TestDictKeysImpl(TRuntimeNode (TProgramBuilder::*factory)(TRuntimeNode list, bool multi,
                                                               const TProgramBuilder::TUnaryLambda& keySelector,
                                                               const TProgramBuilder::TUnaryLambda& payloadSelector, bool isCompact, ui64 itemsCountHint)) {
    TSetup<LLVM> setup;
    TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
    const auto dict = PrepareTestDict<false>(pgmBuilder, factory);
    const auto pgmReturn = pgmBuilder.DictKeys(dict);
    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();

    std::vector<ui32> items;
    for (NUdf::TUnboxedValue item; iterator.Next(item);) {
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

template <bool LLVM, bool SortBeforeCompare>
void TestDictPayloadsImpl(TRuntimeNode (TProgramBuilder::*factory)(TRuntimeNode list, bool multi,
                                                                   const TProgramBuilder::TUnaryLambda& keySelector,
                                                                   const TProgramBuilder::TUnaryLambda& payloadSelector, bool isCompact, ui64 itemsCountHint)) {
    TSetup<LLVM> setup;
    TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
    const auto dict = PrepareTestDict<false>(pgmBuilder, factory);
    const auto pgmReturn = pgmBuilder.DictPayloads(dict);
    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();

    std::vector<TString> items;
    for (NUdf::TUnboxedValue item; iterator.Next(item);) {
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

template <bool LLVM>
void TestConvertedMultiDictLookup(TRuntimeNode (TProgramBuilder::*factory)(TRuntimeNode list, bool multi,
                                                                           const TProgramBuilder::TUnaryLambda& keySelector,
                                                                           const TProgramBuilder::TUnaryLambda& payloadSelector, bool isCompact, ui64 itemsCountHint)) {
    TSetup<LLVM> setup;
    TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
    const auto dict = PrepareTestDict<true>(pgmBuilder, factory);
    const auto keys = NTest::ConvertValueToLiteralNode(pgmBuilder, TVector<ui32>{1, 2, 3});
    const auto pgmReturn = pgmBuilder.Map(keys,
                                          [&](TRuntimeNode key) {
                                              return pgmBuilder.Lookup(dict, key);
                                          });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<TVector<TStringBuf>>>{
                                                          TVector<TStringBuf>{"A"},
                                                          TVector<TStringBuf>{"C", "B"},
                                                          {}});
}

Y_UNIT_TEST_LLVM(TestSortedMultiDictLookup) {
    TestConvertedMultiDictLookup<LLVM>(&TProgramBuilder::ToSortedDict);
}

Y_UNIT_TEST_LLVM(TestHashedMultiDictLookup) {
    TestConvertedMultiDictLookup<LLVM>(&TProgramBuilder::ToHashedDict);
}
} // Y_UNIT_TEST_SUITE(TMiniKQLDictRelatedNodesTest)

} // namespace NMiniKQL
} // namespace NKikimr
