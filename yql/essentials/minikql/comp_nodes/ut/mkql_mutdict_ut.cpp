#include "mkql_computation_node_ut.h"
#include <yql/essentials/minikql/mkql_string_util.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLMutDicttest) {
    TRuntimeNode MakeTestDict(TProgramBuilder& pb) {
        const auto dictType = pb.NewDictType(
            pb.NewDataType(NUdf::EDataSlot::String),
            pb.NewDataType(NUdf::EDataSlot::Int32),
            false);
        const auto dict = pb.NewDict(dictType, {
            { pb.NewDataLiteral<NUdf::EDataSlot::String>("foo"), pb.NewDataLiteral<i32>(1)},
            { pb.NewDataLiteral<NUdf::EDataSlot::String>("bar"), pb.NewDataLiteral<i32>(2)}});

        return dict;
    }

    void CheckExpectedDict(const NUdf::TUnboxedValue& dict, const THashMap<TString, i32>& expected) {
        UNIT_ASSERT_VALUES_EQUAL(dict.GetDictLength(), expected.size());
        THashMap<TString, i32> d;
        const auto it = dict.GetDictIterator();
        NUdf::TUnboxedValue key, value;
        for (ui32 i = 0; i < expected.size(); ++i) {
            UNIT_ASSERT(it.NextPair(key, value));
            d.emplace(TString(key.AsStringRef()), value.template Get<i32>());
        }

        UNIT_ASSERT(!it.NextPair(key, value));
        UNIT_ASSERT(!it.NextPair(key, value));
        UNIT_ASSERT_VALUES_EQUAL(d, expected);
    }

    Y_UNIT_TEST_LLVM(TestCreateEmpty) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto res = pb.NewResourceType("mdict");
        const auto mdictType = pb.NewLinearType(res, false);
        const auto dictType = pb.NewDictType(
            pb.NewDataType(NUdf::EDataSlot::String),
            pb.NewDataType(NUdf::EDataSlot::Int32),
            false);
        const auto pgmReturn = pb.MutDictCreate(dictType, mdictType, {pb.NewDataLiteral(ui32(1))});
        const auto graph = setup.BuildGraph(pgmReturn);
        UNIT_ASSERT(graph->GetValue().IsBoxed());
    }

    Y_UNIT_TEST_LLVM(TestCreateCopy) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto res = pb.NewResourceType("mdict");
        const auto mdictType = pb.NewLinearType(res, false);
        const auto dict = MakeTestDict(pb);
        const auto pgmReturn = pb.ToMutDict(dict, mdictType, {pb.NewDataLiteral(ui32(1))});
        const auto graph = setup.BuildGraph(pgmReturn);
        UNIT_ASSERT(graph->GetValue().IsBoxed());
    }

    Y_UNIT_TEST_LLVM(TestCreateCopyAndBack) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto res = pb.NewResourceType("mdict");
        const auto mdictType = pb.NewLinearType(res, false);
        const auto dict = MakeTestDict(pb);
        const auto pgmReturn = pb.FromMutDict(dict.GetStaticType(), pb.ToMutDict(dict, mdictType, {pb.NewDataLiteral(ui32(1))}));
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto value = graph->GetValue();
        UNIT_ASSERT_VALUES_EQUAL(value.GetDictLength(), 2);
        UNIT_ASSERT(value.HasDictItems());
        NUdf::TUnboxedValue fooStr = MakeString("foo");
        NUdf::TUnboxedValue barStr = MakeString("bar");
        NUdf::TUnboxedValue bazStr = MakeString("baz");
        UNIT_ASSERT(value.Contains(fooStr));
        UNIT_ASSERT(value.Contains(barStr));
        UNIT_ASSERT(!value.Contains(bazStr));
        const auto fooLookup = value.Lookup(fooStr);
        UNIT_ASSERT_VALUES_EQUAL(fooLookup.template Get<i32>(), 1);
        const auto barLookup = value.Lookup(barStr);
        UNIT_ASSERT_VALUES_EQUAL(barLookup.template Get<i32>(), 2);
        const auto bazLookup = value.Lookup(bazStr);
        UNIT_ASSERT(!bazLookup);

        NUdf::TUnboxedValue key1, key2, value1, value2, tmp;
        const auto keyIt = value.GetKeysIterator();
        UNIT_ASSERT(keyIt.Next(key1));
        UNIT_ASSERT(keyIt.Next(key2));
        UNIT_ASSERT(!keyIt.Next(tmp));
        const auto keysSet = THashSet<TString>{TString(key1.AsStringRef()),TString(key2.AsStringRef())};
        const auto expectedKeysSet = THashSet<TString>{"foo", "bar"};
        UNIT_ASSERT_VALUES_EQUAL(keysSet, expectedKeysSet);

        const auto payloadIt = value.GetPayloadsIterator();
        UNIT_ASSERT(payloadIt.Next(value1));
        UNIT_ASSERT(payloadIt.Next(value2));
        UNIT_ASSERT(!payloadIt.Next(tmp));
        const auto payloadsSet = THashSet<i32>{value1.template Get<i32>(),value2.template Get<i32>()};
        const auto expectedPayloadSet = THashSet<i32>{1, 2};
        UNIT_ASSERT_VALUES_EQUAL(payloadsSet, expectedPayloadSet);

        CheckExpectedDict(value, {{"foo", 1}, {"bar", 2}});
    }

    Y_UNIT_TEST_LLVM(TestInsert) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto res = pb.NewResourceType("mdict");
        const auto mdictType = pb.NewLinearType(res, false);
        const auto dictType = pb.NewDictType(
            pb.NewDataType(NUdf::EDataSlot::String),
            pb.NewDataType(NUdf::EDataSlot::Int32),
            false);
        auto mdict = pb.MutDictCreate(dictType, mdictType, {pb.NewDataLiteral(ui32(1))});
        mdict = pb.MutDictInsert(dictType, mdict, pb.NewDataLiteral<NUdf::EDataSlot::String>("foo"), pb.NewDataLiteral<i32>(1));
        const auto pgmReturn = pb.FromMutDict(dictType, mdict);
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto value = graph->GetValue();
        CheckExpectedDict(value, {{"foo", 1}});
    }

    Y_UNIT_TEST_LLVM(TestInsertTwice) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto res = pb.NewResourceType("mdict");
        const auto mdictType = pb.NewLinearType(res, false);
        const auto dictType = pb.NewDictType(
            pb.NewDataType(NUdf::EDataSlot::String),
            pb.NewDataType(NUdf::EDataSlot::Int32),
            false);
        auto mdict = pb.MutDictCreate(dictType, mdictType, {pb.NewDataLiteral(ui32(1))});
        mdict = pb.MutDictInsert(dictType, mdict, pb.NewDataLiteral<NUdf::EDataSlot::String>("foo"), pb.NewDataLiteral<i32>(1));
        mdict = pb.MutDictInsert(dictType, mdict, pb.NewDataLiteral<NUdf::EDataSlot::String>("foo"), pb.NewDataLiteral<i32>(2));
        const auto pgmReturn = pb.FromMutDict(dictType, mdict);
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto value = graph->GetValue();
        CheckExpectedDict(value, {{"foo", 1}});
    }

    Y_UNIT_TEST_LLVM(TestUpsert) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto res = pb.NewResourceType("mdict");
        const auto mdictType = pb.NewLinearType(res, false);
        const auto dictType = pb.NewDictType(
            pb.NewDataType(NUdf::EDataSlot::String),
            pb.NewDataType(NUdf::EDataSlot::Int32),
            false);
        auto mdict = pb.MutDictCreate(dictType, mdictType, {pb.NewDataLiteral(ui32(1))});
        mdict = pb.MutDictUpsert(dictType, mdict, pb.NewDataLiteral<NUdf::EDataSlot::String>("foo"), pb.NewDataLiteral<i32>(1));
        const auto pgmReturn = pb.FromMutDict(dictType, mdict);
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto value = graph->GetValue();
        CheckExpectedDict(value, {{"foo", 1}});
    }

    Y_UNIT_TEST_LLVM(TestUpsertTwice) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto res = pb.NewResourceType("mdict");
        const auto mdictType = pb.NewLinearType(res, false);
        const auto dictType = pb.NewDictType(
            pb.NewDataType(NUdf::EDataSlot::String),
            pb.NewDataType(NUdf::EDataSlot::Int32),
            false);
        auto mdict = pb.MutDictCreate(dictType, mdictType, {pb.NewDataLiteral(ui32(1))});
        mdict = pb.MutDictUpsert(dictType, mdict, pb.NewDataLiteral<NUdf::EDataSlot::String>("foo"), pb.NewDataLiteral<i32>(1));
        mdict = pb.MutDictUpsert(dictType, mdict, pb.NewDataLiteral<NUdf::EDataSlot::String>("foo"), pb.NewDataLiteral<i32>(2));
        const auto pgmReturn = pb.FromMutDict(dictType, mdict);
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto value = graph->GetValue();
        CheckExpectedDict(value, {{"foo", 2}});
    }

    Y_UNIT_TEST_LLVM(TestUpdateMissing) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto res = pb.NewResourceType("mdict");
        const auto mdictType = pb.NewLinearType(res, false);
        const auto dictType = pb.NewDictType(
            pb.NewDataType(NUdf::EDataSlot::String),
            pb.NewDataType(NUdf::EDataSlot::Int32),
            false);
        auto mdict = pb.MutDictCreate(dictType, mdictType, {pb.NewDataLiteral(ui32(1))});
        mdict = pb.MutDictUpdate(dictType, mdict, pb.NewDataLiteral<NUdf::EDataSlot::String>("foo"), pb.NewDataLiteral<i32>(1));
        const auto pgmReturn = pb.FromMutDict(dictType, mdict);
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto value = graph->GetValue();
        CheckExpectedDict(value, {});
    }

    Y_UNIT_TEST_LLVM(TestUpdateExisting) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto res = pb.NewResourceType("mdict");
        const auto mdictType = pb.NewLinearType(res, false);
        const auto dict = MakeTestDict(pb);
        const auto dictType = dict.GetStaticType();
        auto mdict = pb.ToMutDict(dict, mdictType, {pb.NewDataLiteral(ui32(1))});
        mdict = pb.MutDictUpdate(dictType, mdict, pb.NewDataLiteral<NUdf::EDataSlot::String>("foo"), pb.NewDataLiteral<i32>(3));
        auto pgmReturn = pb.FromMutDict(dictType, mdict);
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto value = graph->GetValue();
        CheckExpectedDict(value, {{"foo", 3},{"bar", 2}});
    }

    Y_UNIT_TEST_LLVM(TestRemoveMissing) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto res = pb.NewResourceType("mdict");
        const auto mdictType = pb.NewLinearType(res, false);
        const auto dict = MakeTestDict(pb);
        const auto dictType = dict.GetStaticType();
        auto mdict = pb.ToMutDict(dict, mdictType, {pb.NewDataLiteral(ui32(1))});
        mdict = pb.MutDictRemove(dictType, mdict, pb.NewDataLiteral<NUdf::EDataSlot::String>("baz"));
        const auto pgmReturn = pb.FromMutDict(dictType, mdict);
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto value = graph->GetValue();
        CheckExpectedDict(value, {{"foo", 1},{"bar", 2}});
    }

    Y_UNIT_TEST_LLVM(TestRemoveExisting) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto res = pb.NewResourceType("mdict");
        const auto mdictType = pb.NewLinearType(res, false);
        const auto dict = MakeTestDict(pb);
        const auto dictType = dict.GetStaticType();
        auto mdict = pb.ToMutDict(dict, mdictType, {pb.NewDataLiteral(ui32(1))});
        mdict = pb.MutDictRemove(dictType, mdict, pb.NewDataLiteral<NUdf::EDataSlot::String>("foo"));
        const auto pgmReturn = pb.FromMutDict(dictType, mdict);
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto value = graph->GetValue();
        CheckExpectedDict(value, {{"bar", 2}});
    }

    Y_UNIT_TEST_LLVM(TestPopMissing) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto res = pb.NewResourceType("mdict");
        const auto mdictType = pb.NewLinearType(res, false);
        const auto dict = MakeTestDict(pb);
        const auto dictType = dict.GetStaticType();
        auto mdict = pb.ToMutDict(dict, mdictType, {pb.NewDataLiteral(ui32(1))});
        const auto popTuple = pb.MutDictPop(dictType, mdict, pb.NewDataLiteral<NUdf::EDataSlot::String>("baz"));
        mdict = pb.Nth(popTuple, 0);
        const auto popRes = pb.Nth(popTuple, 1);
        const auto pgmReturn = pb.NewTuple({pb.FromMutDict(dictType, mdict), popRes});
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto valuePair = graph->GetValue();
        const auto value = valuePair.GetElement(0);
        const auto popValue = valuePair.GetElement(1);
        UNIT_ASSERT(!popValue);
        CheckExpectedDict(value, {{"foo", 1},{"bar", 2}});
    }

    Y_UNIT_TEST_LLVM(TestPopExisting) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto res = pb.NewResourceType("mdict");
        const auto mdictType = pb.NewLinearType(res, false);
        const auto dict = MakeTestDict(pb);
        const auto dictType = dict.GetStaticType();
        auto mdict = pb.ToMutDict(dict, mdictType, {pb.NewDataLiteral(ui32(1))});
        const auto popTuple = pb.MutDictPop(dictType, mdict, pb.NewDataLiteral<NUdf::EDataSlot::String>("foo"));
        mdict = pb.Nth(popTuple, 0);
        const auto popRes = pb.Nth(popTuple, 1);
        const auto pgmReturn = pb.NewTuple({pb.FromMutDict(dictType, mdict), popRes});
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto valuePair = graph->GetValue();
        const auto value = valuePair.GetElement(0);
        const auto popValue = valuePair.GetElement(1);
        UNIT_ASSERT(popValue);
        UNIT_ASSERT_VALUES_EQUAL(popValue.template Get<i32>(), 1);
        CheckExpectedDict(value, {{"bar", 2}});
    }

    Y_UNIT_TEST_LLVM(TestContainsMissing) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto res = pb.NewResourceType("mdict");
        const auto mdictType = pb.NewLinearType(res, false);
        const auto dict = MakeTestDict(pb);
        const auto dictType = dict.GetStaticType();
        auto mdict = pb.ToMutDict(dict, mdictType, {pb.NewDataLiteral(ui32(1))});
        const auto containsTuple = pb.MutDictContains(dictType, mdict, pb.NewDataLiteral<NUdf::EDataSlot::String>("baz"));
        mdict = pb.Nth(containsTuple, 0);
        const auto containsRes = pb.Nth(containsTuple, 1);
        const auto pgmReturn = pb.NewTuple({pb.FromMutDict(dictType, mdict), containsRes});
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto valuePair = graph->GetValue();
        const auto value = valuePair.GetElement(0);
        const auto containsValue = valuePair.GetElement(1);
        UNIT_ASSERT(!containsValue.template Get<bool>());
        UNIT_ASSERT_VALUES_EQUAL(value.GetDictLength(), 2);
    }

    Y_UNIT_TEST_LLVM(TestContainsExisting) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto res = pb.NewResourceType("mdict");
        const auto mdictType = pb.NewLinearType(res, false);
        const auto dict = MakeTestDict(pb);
        const auto dictType = dict.GetStaticType();
        auto mdict = pb.ToMutDict(dict, mdictType, {pb.NewDataLiteral(ui32(1))});
        const auto containsTuple = pb.MutDictContains(dictType, mdict, pb.NewDataLiteral<NUdf::EDataSlot::String>("foo"));
        mdict = pb.Nth(containsTuple, 0);
        const auto containsRes = pb.Nth(containsTuple, 1);
        const auto pgmReturn = pb.NewTuple({pb.FromMutDict(dictType, mdict), containsRes});
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto valuePair = graph->GetValue();
        const auto value = valuePair.GetElement(0);
        const auto containsValue = valuePair.GetElement(1);
        UNIT_ASSERT(containsValue.template Get<bool>());
        UNIT_ASSERT_VALUES_EQUAL(value.GetDictLength(), 2);
    }

    Y_UNIT_TEST_LLVM(TestLookupMissing) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto res = pb.NewResourceType("mdict");
        const auto mdictType = pb.NewLinearType(res, false);
        const auto dict = MakeTestDict(pb);
        const auto dictType = dict.GetStaticType();
        auto mdict = pb.ToMutDict(dict, mdictType, {pb.NewDataLiteral(ui32(1))});
        const auto lookupTuple = pb.MutDictLookup(dictType, mdict, pb.NewDataLiteral<NUdf::EDataSlot::String>("baz"));
        mdict = pb.Nth(lookupTuple, 0);
        const auto lookupRes = pb.Nth(lookupTuple, 1);
        const auto pgmReturn = pb.NewTuple({pb.FromMutDict(dictType, mdict), lookupRes});
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto valuePair = graph->GetValue();
        const auto value = valuePair.GetElement(0);
        const auto lookupValue = valuePair.GetElement(1);
        UNIT_ASSERT(!lookupValue);
        UNIT_ASSERT_VALUES_EQUAL(value.GetDictLength(), 2);
    }

    Y_UNIT_TEST_LLVM(TestLookupExisting) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto res = pb.NewResourceType("mdict");
        const auto mdictType = pb.NewLinearType(res, false);
        const auto dict = MakeTestDict(pb);
        const auto dictType = dict.GetStaticType();
        auto mdict = pb.ToMutDict(dict, mdictType, {pb.NewDataLiteral(ui32(1))});
        const auto lookupTuple = pb.MutDictLookup(dictType, mdict, pb.NewDataLiteral<NUdf::EDataSlot::String>("foo"));
        mdict = pb.Nth(lookupTuple, 0);
        const auto lookupRes = pb.Nth(lookupTuple, 1);
        const auto pgmReturn = pb.NewTuple({pb.FromMutDict(dictType, mdict), lookupRes});
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto valuePair = graph->GetValue();
        const auto value = valuePair.GetElement(0);
        const auto lookupValue = valuePair.GetElement(1);
        UNIT_ASSERT(lookupValue);
        UNIT_ASSERT_VALUES_EQUAL(lookupValue.template Get<i32>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(value.GetDictLength(), 2);
    }

    Y_UNIT_TEST_LLVM(TestHasItems) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto res = pb.NewResourceType("mdict");
        const auto mdictType = pb.NewLinearType(res, false);
        const auto dict = MakeTestDict(pb);
        const auto dictType = dict.GetStaticType();
        auto mdict = pb.ToMutDict(dict, mdictType, {pb.NewDataLiteral(ui32(1))});
        const auto hasTuple = pb.MutDictHasItems(dictType, mdict);
        mdict = pb.Nth(hasTuple, 0);
        const auto hasRes = pb.Nth(hasTuple, 1);
        const auto pgmReturn = pb.NewTuple({pb.FromMutDict(dictType, mdict), hasRes});
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto valuePair = graph->GetValue();
        const auto value = valuePair.GetElement(0);
        const auto hasValue = valuePair.GetElement(1);
        UNIT_ASSERT(hasValue.template Get<bool>());
        UNIT_ASSERT_VALUES_EQUAL(value.GetDictLength(), 2);
    }

    Y_UNIT_TEST_LLVM(TestLength) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto res = pb.NewResourceType("mdict");
        const auto mdictType = pb.NewLinearType(res, false);
        const auto dict = MakeTestDict(pb);
        const auto dictType = dict.GetStaticType();
        auto mdict = pb.ToMutDict(dict, mdictType, {pb.NewDataLiteral(ui32(1))});
        const auto lenTuple = pb.MutDictLength(dictType, mdict);
        mdict = pb.Nth(lenTuple, 0);
        const auto lenRes = pb.Nth(lenTuple, 1);
        const auto pgmReturn = pb.NewTuple({pb.FromMutDict(dictType, mdict), lenRes});
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto valuePair = graph->GetValue();
        const auto value = valuePair.GetElement(0);
        const auto lenValue = valuePair.GetElement(1);
        UNIT_ASSERT_VALUES_EQUAL(lenValue.template Get<ui64>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(value.GetDictLength(), 2);
    }

    Y_UNIT_TEST_LLVM(TestItems) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto res = pb.NewResourceType("mdict");
        const auto mdictType = pb.NewLinearType(res, false);
        const auto dict = MakeTestDict(pb);
        const auto dictType = dict.GetStaticType();
        auto mdict = pb.ToMutDict(dict, mdictType, {pb.NewDataLiteral(ui32(1))});
        const auto listTuple = pb.MutDictItems(dictType, mdict);
        mdict = pb.Nth(listTuple, 0);
        const auto listRes = pb.Nth(listTuple, 1);
        const auto pgmReturn = pb.NewTuple({pb.FromMutDict(dictType, mdict), listRes});
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto valuePair = graph->GetValue();
        const auto value = valuePair.GetElement(0);
        const auto listValue = valuePair.GetElement(1);
        UNIT_ASSERT_VALUES_EQUAL(listValue.GetListLength(),2);
        UNIT_ASSERT_VALUES_EQUAL(value.GetDictLength(), 2);
        THashMap<TString, i32> itemsSet;
        NUdf::TUnboxedValue x;
        for (auto it = listValue.GetListIterator(); it.Next(x);) {
            const NUdf::TUnboxedValue& elem0 = x.GetElement(0);
            itemsSet.emplace(TString(elem0.AsStringRef()), x.GetElement(1).template Get<i32>());
        }

        auto expectedItemsSet = THashMap<TString, i32>{{"foo", 1}, {"bar", 2}};
        UNIT_ASSERT_VALUES_EQUAL(itemsSet, expectedItemsSet);
    }

    Y_UNIT_TEST_LLVM(TestKeys) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto res = pb.NewResourceType("mdict");
        const auto mdictType = pb.NewLinearType(res, false);
        const auto dict = MakeTestDict(pb);
        const auto dictType = dict.GetStaticType();
        auto mdict = pb.ToMutDict(dict, mdictType, {pb.NewDataLiteral(ui32(1))});
        const auto listTuple = pb.MutDictKeys(dictType, mdict);
        mdict = pb.Nth(listTuple, 0);
        const auto listRes = pb.Nth(listTuple, 1);
        const auto pgmReturn = pb.NewTuple({pb.FromMutDict(dictType, mdict), listRes});
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto valuePair = graph->GetValue();
        const auto value = valuePair.GetElement(0);
        const auto listValue = valuePair.GetElement(1);
        UNIT_ASSERT_VALUES_EQUAL(listValue.GetListLength(),2);
        UNIT_ASSERT_VALUES_EQUAL(value.GetDictLength(), 2);
        THashSet<TString> keysSet;
        NUdf::TUnboxedValue x;
        for (auto it = listValue.GetListIterator(); it.Next(x);) {
            keysSet.emplace(TString(x.AsStringRef()));
        }

        auto expectedKeysSet = THashSet<TString>{"foo", "bar"};
        UNIT_ASSERT_VALUES_EQUAL(keysSet, expectedKeysSet);
    }

    Y_UNIT_TEST_LLVM(TestPayloads) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto res = pb.NewResourceType("mdict");
        const auto mdictType = pb.NewLinearType(res, false);
        const auto dict = MakeTestDict(pb);
        const auto dictType = dict.GetStaticType();
        auto mdict = pb.ToMutDict(dict, mdictType, {pb.NewDataLiteral(ui32(1))});
        const auto listTuple = pb.MutDictPayloads(dictType, mdict);
        mdict = pb.Nth(listTuple, 0);
        const auto listRes = pb.Nth(listTuple, 1);
        const auto pgmReturn = pb.NewTuple({pb.FromMutDict(dictType, mdict), listRes});
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto valuePair = graph->GetValue();
        const auto value = valuePair.GetElement(0);
        const auto listValue = valuePair.GetElement(1);
        UNIT_ASSERT_VALUES_EQUAL(listValue.GetListLength(),2);
        UNIT_ASSERT_VALUES_EQUAL(value.GetDictLength(), 2);
        THashSet<i32> payloadsSet;
        NUdf::TUnboxedValue x;
        for (auto it = listValue.GetListIterator(); it.Next(x);) {
            payloadsSet.emplace(x.template Get<i32>());
        }

        auto expectedPayloadsSet = THashSet<i32>{1, 2};
        UNIT_ASSERT_VALUES_EQUAL(payloadsSet, expectedPayloadsSet);
    }
}

}
}
