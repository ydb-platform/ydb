#include "mkql_computation_node_ut.h"
#include <yql/essentials/minikql/mkql_string_util.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLMutDicttest) {
template <bool IsSet>
using TTestDictStorage = std::conditional_t<IsSet, THashSet<TString>, THashMap<TString, i32>>;

template <bool IsSet>
TType* MakeTestDictType(TProgramBuilder& pb) {
    return pb.NewDictType(
        pb.NewDataType(NUdf::EDataSlot::String),
        IsSet ? pb.NewVoidType() : pb.NewDataType(NUdf::EDataSlot::Int32),
        false);
}

template <bool IsSet>
TRuntimeNode MakeTestDict(TProgramBuilder& pb) {
    const auto dictType = pb.NewDictType(
        pb.NewDataType(NUdf::EDataSlot::String),
        IsSet ? pb.NewVoidType() : pb.NewDataType(NUdf::EDataSlot::Int32),
        false);
    const auto dict = pb.NewDict(dictType, {{pb.NewDataLiteral<NUdf::EDataSlot::String>("foo"), IsSet ? pb.NewVoid() : pb.NewDataLiteral<i32>(1)},
                                            {pb.NewDataLiteral<NUdf::EDataSlot::String>("bar"), IsSet ? pb.NewVoid() : pb.NewDataLiteral<i32>(2)}});

    return dict;
}

template <bool IsSet>
TTestDictStorage<IsSet> AdaptValues(const TTestDictStorage<false>& d) {
    if constexpr (IsSet) {
        TTestDictStorage<IsSet> ret;
        for (const auto& [k, v] : d) {
            ret.emplace(k);
        }

        return ret;
    } else {
        return d;
    }
}

template <bool IsSet>
void CheckExpectedDict(const NUdf::TUnboxedValue& dict, const TTestDictStorage<false>& expected) {
    UNIT_ASSERT_VALUES_EQUAL(dict.GetDictLength(), expected.size());
    TTestDictStorage<IsSet> d;
    const auto it = dict.GetDictIterator();
    NUdf::TUnboxedValue key, value;
    for (ui32 i = 0; i < expected.size(); ++i) {
        UNIT_ASSERT(it.NextPair(key, value));
        if constexpr (IsSet) {
            d.emplace(TString(key.AsStringRef()));
        } else {
            d.emplace(TString(key.AsStringRef()), value.template Get<i32>());
        }
    }

    UNIT_ASSERT(!it.NextPair(key, value));
    UNIT_ASSERT(!it.NextPair(key, value));
    UNIT_ASSERT_VALUES_EQUAL(d, AdaptValues<IsSet>(expected));
}

#define Y_UNIT_TEST_LLVM_SET(N) Y_UNIT_TEST_QUAD(N, LLVM, IsSet)

Y_UNIT_TEST_LLVM_SET(TestCreateEmpty) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto res = pb.NewResourceType("mdict");
    const auto mdictType = pb.NewLinearType(res, false);
    const auto dictType = MakeTestDictType<IsSet>(pb);
    const auto pgmReturn = pb.MutDictCreate(dictType, mdictType, {pb.NewDataLiteral(ui32(1))});
    const auto graph = setup.BuildGraph(pgmReturn);
    UNIT_ASSERT(graph->GetValue().IsBoxed());
}

Y_UNIT_TEST_LLVM_SET(TestCreateCopy) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto res = pb.NewResourceType("mdict");
    const auto mdictType = pb.NewLinearType(res, false);
    const auto dict = MakeTestDict<IsSet>(pb);
    const auto pgmReturn = pb.ToMutDict(dict, mdictType, {pb.NewDataLiteral(ui32(1))});
    const auto graph = setup.BuildGraph(pgmReturn);
    UNIT_ASSERT(graph->GetValue().IsBoxed());
}

Y_UNIT_TEST_LLVM_SET(TestCreateCopyAndBack) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto res = pb.NewResourceType("mdict");
    const auto mdictType = pb.NewLinearType(res, false);
    const auto dict = MakeTestDict<IsSet>(pb);
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
    UNIT_ASSERT(fooLookup);
    if constexpr (!IsSet) {
        UNIT_ASSERT_VALUES_EQUAL(fooLookup.template Get<i32>(), 1);
    }

    const auto barLookup = value.Lookup(barStr);
    UNIT_ASSERT(barLookup);
    if constexpr (!IsSet) {
        UNIT_ASSERT_VALUES_EQUAL(barLookup.template Get<i32>(), 2);
    }

    const auto bazLookup = value.Lookup(bazStr);
    UNIT_ASSERT(!bazLookup);

    NUdf::TUnboxedValue key1, key2, value1, value2, tmp;
    const auto keyIt = value.GetKeysIterator();
    UNIT_ASSERT(keyIt.Next(key1));
    UNIT_ASSERT(keyIt.Next(key2));
    UNIT_ASSERT(!keyIt.Next(tmp));
    const auto keysSet = THashSet<TString>{TString(key1.AsStringRef()), TString(key2.AsStringRef())};
    const auto expectedKeysSet = THashSet<TString>{"foo", "bar"};
    UNIT_ASSERT_VALUES_EQUAL(keysSet, expectedKeysSet);

    const auto payloadIt = value.GetPayloadsIterator();
    UNIT_ASSERT(payloadIt.Next(value1));
    UNIT_ASSERT(payloadIt.Next(value2));
    UNIT_ASSERT(!payloadIt.Next(tmp));
    if constexpr (IsSet) {
        UNIT_ASSERT(value1.IsEmbedded());
        UNIT_ASSERT(value2.IsEmbedded());
    } else {
        const auto payloadsSet = THashSet<i32>{value1.template Get<i32>(), value2.template Get<i32>()};
        const auto expectedPayloadSet = THashSet<i32>{1, 2};
        UNIT_ASSERT_VALUES_EQUAL(payloadsSet, expectedPayloadSet);
    }

    CheckExpectedDict<IsSet>(value, {{"foo", 1}, {"bar", 2}});
}

Y_UNIT_TEST_LLVM_SET(TestInsert) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto res = pb.NewResourceType("mdict");
    const auto mdictType = pb.NewLinearType(res, false);
    const auto dictType = MakeTestDictType<IsSet>(pb);
    auto mdict = pb.MutDictCreate(dictType, mdictType, {pb.NewDataLiteral(ui32(1))});
    mdict = pb.MutDictInsert(dictType, mdict,
                             pb.NewDataLiteral<NUdf::EDataSlot::String>("foo"),
                             IsSet ? pb.NewVoid() : pb.NewDataLiteral<i32>(1));
    const auto pgmReturn = pb.FromMutDict(dictType, mdict);
    const auto graph = setup.BuildGraph(pgmReturn);
    const auto value = graph->GetValue();
    CheckExpectedDict<IsSet>(value, {{"foo", 1}});
}

Y_UNIT_TEST_LLVM_SET(TestInsertTwice) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto res = pb.NewResourceType("mdict");
    const auto mdictType = pb.NewLinearType(res, false);
    const auto dictType = MakeTestDictType<IsSet>(pb);
    auto mdict = pb.MutDictCreate(dictType, mdictType, {pb.NewDataLiteral(ui32(1))});
    mdict = pb.MutDictInsert(dictType, mdict,
                             pb.NewDataLiteral<NUdf::EDataSlot::String>("foo"),
                             IsSet ? pb.NewVoid() : pb.NewDataLiteral<i32>(1));
    mdict = pb.MutDictInsert(dictType, mdict,
                             pb.NewDataLiteral<NUdf::EDataSlot::String>("foo"),
                             IsSet ? pb.NewVoid() : pb.NewDataLiteral<i32>(2));
    const auto pgmReturn = pb.FromMutDict(dictType, mdict);
    const auto graph = setup.BuildGraph(pgmReturn);
    const auto value = graph->GetValue();
    CheckExpectedDict<IsSet>(value, {{"foo", 1}});
}

Y_UNIT_TEST_LLVM_SET(TestUpsert) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto res = pb.NewResourceType("mdict");
    const auto mdictType = pb.NewLinearType(res, false);
    const auto dictType = MakeTestDictType<IsSet>(pb);
    auto mdict = pb.MutDictCreate(dictType, mdictType, {pb.NewDataLiteral(ui32(1))});
    mdict = pb.MutDictUpsert(dictType, mdict,
                             pb.NewDataLiteral<NUdf::EDataSlot::String>("foo"),
                             IsSet ? pb.NewVoid() : pb.NewDataLiteral<i32>(1));
    const auto pgmReturn = pb.FromMutDict(dictType, mdict);
    const auto graph = setup.BuildGraph(pgmReturn);
    const auto value = graph->GetValue();
    CheckExpectedDict<IsSet>(value, {{"foo", 1}});
}

Y_UNIT_TEST_LLVM_SET(TestUpsertTwice) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto res = pb.NewResourceType("mdict");
    const auto mdictType = pb.NewLinearType(res, false);
    const auto dictType = MakeTestDictType<IsSet>(pb);
    auto mdict = pb.MutDictCreate(dictType, mdictType, {pb.NewDataLiteral(ui32(1))});
    mdict = pb.MutDictUpsert(dictType, mdict,
                             pb.NewDataLiteral<NUdf::EDataSlot::String>("foo"),
                             IsSet ? pb.NewVoid() : pb.NewDataLiteral<i32>(1));
    mdict = pb.MutDictUpsert(dictType, mdict,
                             pb.NewDataLiteral<NUdf::EDataSlot::String>("foo"),
                             IsSet ? pb.NewVoid() : pb.NewDataLiteral<i32>(2));
    const auto pgmReturn = pb.FromMutDict(dictType, mdict);
    const auto graph = setup.BuildGraph(pgmReturn);
    const auto value = graph->GetValue();
    CheckExpectedDict<IsSet>(value, {{"foo", 2}});
}

Y_UNIT_TEST_LLVM_SET(TestUpdateMissing) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto res = pb.NewResourceType("mdict");
    const auto mdictType = pb.NewLinearType(res, false);
    const auto dictType = MakeTestDictType<IsSet>(pb);
    auto mdict = pb.MutDictCreate(dictType, mdictType, {pb.NewDataLiteral(ui32(1))});
    mdict = pb.MutDictUpdate(dictType, mdict,
                             pb.NewDataLiteral<NUdf::EDataSlot::String>("foo"),
                             IsSet ? pb.NewVoid() : pb.NewDataLiteral<i32>(1));
    const auto pgmReturn = pb.FromMutDict(dictType, mdict);
    const auto graph = setup.BuildGraph(pgmReturn);
    const auto value = graph->GetValue();
    CheckExpectedDict<IsSet>(value, {});
}

Y_UNIT_TEST_LLVM_SET(TestUpdateExisting) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto res = pb.NewResourceType("mdict");
    const auto mdictType = pb.NewLinearType(res, false);
    const auto dict = MakeTestDict<IsSet>(pb);
    const auto dictType = dict.GetStaticType();
    auto mdict = pb.ToMutDict(dict, mdictType, {pb.NewDataLiteral(ui32(1))});
    mdict = pb.MutDictUpdate(dictType, mdict,
                             pb.NewDataLiteral<NUdf::EDataSlot::String>("foo"),
                             IsSet ? pb.NewVoid() : pb.NewDataLiteral<i32>(3));
    auto pgmReturn = pb.FromMutDict(dictType, mdict);
    const auto graph = setup.BuildGraph(pgmReturn);
    const auto value = graph->GetValue();
    CheckExpectedDict<IsSet>(value, {{"foo", 3}, {"bar", 2}});
}

Y_UNIT_TEST_LLVM_SET(TestRemoveMissing) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto res = pb.NewResourceType("mdict");
    const auto mdictType = pb.NewLinearType(res, false);
    const auto dict = MakeTestDict<IsSet>(pb);
    const auto dictType = dict.GetStaticType();
    auto mdict = pb.ToMutDict(dict, mdictType, {pb.NewDataLiteral(ui32(1))});
    mdict = pb.MutDictRemove(dictType, mdict, pb.NewDataLiteral<NUdf::EDataSlot::String>("baz"));
    const auto pgmReturn = pb.FromMutDict(dictType, mdict);
    const auto graph = setup.BuildGraph(pgmReturn);
    const auto value = graph->GetValue();
    CheckExpectedDict<IsSet>(value, {{"foo", 1}, {"bar", 2}});
}

Y_UNIT_TEST_LLVM_SET(TestRemoveExisting) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto res = pb.NewResourceType("mdict");
    const auto mdictType = pb.NewLinearType(res, false);
    const auto dict = MakeTestDict<IsSet>(pb);
    const auto dictType = dict.GetStaticType();
    auto mdict = pb.ToMutDict(dict, mdictType, {pb.NewDataLiteral(ui32(1))});
    mdict = pb.MutDictRemove(dictType, mdict, pb.NewDataLiteral<NUdf::EDataSlot::String>("foo"));
    const auto pgmReturn = pb.FromMutDict(dictType, mdict);
    const auto graph = setup.BuildGraph(pgmReturn);
    const auto value = graph->GetValue();
    CheckExpectedDict<IsSet>(value, {{"bar", 2}});
}

Y_UNIT_TEST_LLVM_SET(TestPopMissing) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto res = pb.NewResourceType("mdict");
    const auto mdictType = pb.NewLinearType(res, false);
    const auto dict = MakeTestDict<IsSet>(pb);
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
    CheckExpectedDict<IsSet>(value, {{"foo", 1}, {"bar", 2}});
}

Y_UNIT_TEST_LLVM_SET(TestPopExisting) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto res = pb.NewResourceType("mdict");
    const auto mdictType = pb.NewLinearType(res, false);
    const auto dict = MakeTestDict<IsSet>(pb);
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
    UNIT_ASSERT(popValue.IsEmbedded());
    if constexpr (!IsSet) {
        UNIT_ASSERT_VALUES_EQUAL(popValue.template Get<i32>(), 1);
    }

    CheckExpectedDict<IsSet>(value, {{"bar", 2}});
}

Y_UNIT_TEST_LLVM_SET(TestContainsMissing) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto res = pb.NewResourceType("mdict");
    const auto mdictType = pb.NewLinearType(res, false);
    const auto dict = MakeTestDict<IsSet>(pb);
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

Y_UNIT_TEST_LLVM_SET(TestContainsExisting) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto res = pb.NewResourceType("mdict");
    const auto mdictType = pb.NewLinearType(res, false);
    const auto dict = MakeTestDict<IsSet>(pb);
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

Y_UNIT_TEST_LLVM_SET(TestLookupMissing) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto res = pb.NewResourceType("mdict");
    const auto mdictType = pb.NewLinearType(res, false);
    const auto dict = MakeTestDict<IsSet>(pb);
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

Y_UNIT_TEST_LLVM_SET(TestLookupExisting) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto res = pb.NewResourceType("mdict");
    const auto mdictType = pb.NewLinearType(res, false);
    const auto dict = MakeTestDict<IsSet>(pb);
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
    if constexpr (IsSet) {
        UNIT_ASSERT(lookupValue.IsEmbedded());
    } else {
        UNIT_ASSERT_VALUES_EQUAL(lookupValue.template Get<i32>(), 1);
    }

    UNIT_ASSERT_VALUES_EQUAL(value.GetDictLength(), 2);
}

Y_UNIT_TEST_LLVM_SET(TestHasItems) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto res = pb.NewResourceType("mdict");
    const auto mdictType = pb.NewLinearType(res, false);
    const auto dict = MakeTestDict<IsSet>(pb);
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

Y_UNIT_TEST_LLVM_SET(TestLength) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto res = pb.NewResourceType("mdict");
    const auto mdictType = pb.NewLinearType(res, false);
    const auto dict = MakeTestDict<IsSet>(pb);
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

Y_UNIT_TEST_LLVM_SET(TestItems) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto res = pb.NewResourceType("mdict");
    const auto mdictType = pb.NewLinearType(res, false);
    const auto dict = MakeTestDict<IsSet>(pb);
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
    UNIT_ASSERT_VALUES_EQUAL(listValue.GetListLength(), 2);
    UNIT_ASSERT_VALUES_EQUAL(value.GetDictLength(), 2);

    TTestDictStorage<IsSet> itemsSet;
    NUdf::TUnboxedValue x;
    for (auto it = listValue.GetListIterator(); it.Next(x);) {
        const NUdf::TUnboxedValue& elem0 = x.GetElement(0);
        if constexpr (IsSet) {
            itemsSet.emplace(TString(elem0.AsStringRef()));
        } else {
            itemsSet.emplace(TString(elem0.AsStringRef()), x.GetElement(1).template Get<i32>());
        }
    }

    auto expectedItemsSet = THashMap<TString, i32>{{"foo", 1}, {"bar", 2}};
    UNIT_ASSERT_VALUES_EQUAL(itemsSet, AdaptValues<IsSet>(expectedItemsSet));
}

Y_UNIT_TEST_LLVM_SET(TestKeys) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto res = pb.NewResourceType("mdict");
    const auto mdictType = pb.NewLinearType(res, false);
    const auto dict = MakeTestDict<IsSet>(pb);
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
    UNIT_ASSERT_VALUES_EQUAL(listValue.GetListLength(), 2);
    UNIT_ASSERT_VALUES_EQUAL(value.GetDictLength(), 2);
    THashSet<TString> keysSet;
    NUdf::TUnboxedValue x;
    for (auto it = listValue.GetListIterator(); it.Next(x);) {
        keysSet.emplace(TString(x.AsStringRef()));
    }

    auto expectedKeysSet = THashSet<TString>{"foo", "bar"};
    UNIT_ASSERT_VALUES_EQUAL(keysSet, expectedKeysSet);
}

Y_UNIT_TEST_LLVM_SET(TestPayloads) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto res = pb.NewResourceType("mdict");
    const auto mdictType = pb.NewLinearType(res, false);
    const auto dict = MakeTestDict<IsSet>(pb);
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
    UNIT_ASSERT_VALUES_EQUAL(listValue.GetListLength(), 2);
    UNIT_ASSERT_VALUES_EQUAL(value.GetDictLength(), 2);

    if constexpr (IsSet) {
        NUdf::TUnboxedValue x;
        ui32 count = 0;
        for (auto it = listValue.GetListIterator(); it.Next(x); ++count) {
            UNIT_ASSERT(x.IsEmbedded());
        }

        UNIT_ASSERT_VALUES_EQUAL(count, 2);
    } else {
        THashSet<i32> payloadsSet;
        NUdf::TUnboxedValue x;
        for (auto it = listValue.GetListIterator(); it.Next(x);) {
            payloadsSet.emplace(x.template Get<i32>());
        }

        auto expectedPayloadsSet = THashSet<i32>{1, 2};
        UNIT_ASSERT_VALUES_EQUAL(payloadsSet, expectedPayloadsSet);
    }
}
} // Y_UNIT_TEST_SUITE(TMiniKQLMutDicttest)

} // namespace NMiniKQL
} // namespace NKikimr
