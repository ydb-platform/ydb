#include "mkql_computation_node_ut.h"
#include "mkql_program_builder_test_utils.h"

#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLVariantTest) {
Y_UNIT_TEST_LLVM(TestGuessTuple) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TVar = std::variant<ui32, TStringBuf>;
    const auto var1 = NTest::ConvertValueToLiteralNode(pb, TVar{ui32(1)});
    const auto var2 = NTest::ConvertValueToLiteralNode(pb, TVar{TStringBuf("abc")});
    std::vector<TRuntimeNode> tupleItems;
    tupleItems.push_back(pb.Guess(var1, 0));
    tupleItems.push_back(pb.Guess(var1, 1));
    tupleItems.push_back(pb.Guess(var2, 0));
    tupleItems.push_back(pb.Guess(var2, 1));
    const auto pgmReturn = pb.NewTuple(tupleItems);

    const auto graph = setup.BuildGraph(pgmReturn);
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(),
                                               std::make_tuple(TMaybe<ui32>{1}, TMaybe<TString>{}, TMaybe<ui32>{}, TMaybe<TString>{"abc"}));
}

Y_UNIT_TEST_LLVM(TestGuessTupleOpt) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TVar = std::variant<ui32, TStringBuf>;
    const auto var1 = NTest::ConvertValueToLiteralNode(pb, TVar{ui32(1)});
    const auto var2 = NTest::ConvertValueToLiteralNode(pb, TVar{TStringBuf("abc")});
    const auto jvar1 = pb.NewOptional(var1);
    const auto jvar2 = pb.NewOptional(var2);
    const auto nothing = NTest::ConvertValueToLiteralNode(pb, TMaybe<TVar>{});

    std::vector<TRuntimeNode> tupleItems;
    tupleItems.push_back(pb.Guess(jvar1, 0));
    tupleItems.push_back(pb.Guess(jvar1, 1));
    tupleItems.push_back(pb.Guess(jvar2, 0));
    tupleItems.push_back(pb.Guess(jvar2, 1));
    tupleItems.push_back(pb.Guess(nothing, 0));
    tupleItems.push_back(pb.Guess(nothing, 1));
    const auto pgmReturn = pb.NewTuple(tupleItems);

    const auto graph = setup.BuildGraph(pgmReturn);
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(),
                                               std::make_tuple(TMaybe<ui32>{1}, TMaybe<TString>{}, TMaybe<ui32>{}, TMaybe<TString>{"abc"},
                                                               TMaybe<ui32>{}, TMaybe<TString>{}));
}

Y_UNIT_TEST_LLVM(TestGuessStruct) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data1 = pb.NewDataLiteral<ui32>(1);
    const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("abc");
    const auto structType = pb.NewStructType({{"x", pb.NewDataType(NUdf::TDataType<ui32>::Id)}, {"y", pb.NewDataType(NUdf::TDataType<char*>::Id)}});
    const auto varType = pb.NewVariantType(structType);
    const auto var1 = pb.NewVariant(data1, "x", varType);
    const auto var2 = pb.NewVariant(data2, "y", varType);
    std::vector<TRuntimeNode> tupleItems;
    tupleItems.push_back(pb.Guess(var1, "x"));
    tupleItems.push_back(pb.Guess(var1, "y"));
    tupleItems.push_back(pb.Guess(var2, "x"));
    tupleItems.push_back(pb.Guess(var2, "y"));
    const auto pgmReturn = pb.NewTuple(tupleItems);

    const auto graph = setup.BuildGraph(pgmReturn);
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(),
                                               std::make_tuple(TMaybe<ui32>{1}, TMaybe<TString>{}, TMaybe<ui32>{}, TMaybe<TString>{"abc"}));
}

Y_UNIT_TEST_LLVM(TestGuessStructOpt) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data1 = pb.NewDataLiteral<ui32>(1);
    const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("abc");
    const auto structType = pb.NewStructType({{"x", pb.NewDataType(NUdf::TDataType<ui32>::Id)}, {"y", pb.NewDataType(NUdf::TDataType<char*>::Id)}});
    const auto varType = pb.NewVariantType(structType);
    const auto var1 = pb.NewVariant(data1, "x", varType);
    const auto var2 = pb.NewVariant(data2, "y", varType);
    const auto jvar1 = pb.NewOptional(var1);
    const auto jvar2 = pb.NewOptional(var2);
    const auto nothing = pb.NewEmptyOptional(pb.NewOptionalType(varType));

    std::vector<TRuntimeNode> tupleItems;
    tupleItems.push_back(pb.Guess(jvar1, "x"));
    tupleItems.push_back(pb.Guess(jvar1, "y"));
    tupleItems.push_back(pb.Guess(jvar2, "x"));
    tupleItems.push_back(pb.Guess(jvar2, "y"));
    tupleItems.push_back(pb.Guess(nothing, "x"));
    tupleItems.push_back(pb.Guess(nothing, "y"));
    const auto pgmReturn = pb.NewTuple(tupleItems);

    const auto graph = setup.BuildGraph(pgmReturn);
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(),
                                               std::make_tuple(TMaybe<ui32>{1}, TMaybe<TString>{}, TMaybe<ui32>{}, TMaybe<TString>{"abc"},
                                                               TMaybe<ui32>{}, TMaybe<TString>{}));
}

Y_UNIT_TEST_LLVM(TestVisitAllTuple) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto at = NTest::ConvertValueToLiteralNode(pb, TStringBuf("@"));
    using TItem = std::variant<ui32, TStringBuf>;
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TItem>{
                                                               TItem{ui32(1)},
                                                               TItem{TStringBuf("abc")},
                                                           });
    const auto pgmReturn = pb.Map(list, [&](TRuntimeNode item) {
        return pb.VisitAll(item, [&](ui32 index, TRuntimeNode item) {
            if (!index) {
                return pb.Concat(at, pb.ToString(item));
            } else {
                return pb.Concat(at, item);
            }
        });
    });

    const auto graph = setup.BuildGraph(pgmReturn);
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TString>{"@1", "@abc"});
}

Y_UNIT_TEST_LLVM(TestVisitAllStruct) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto at = pb.NewDataLiteral<NUdf::EDataSlot::String>("@");
    const auto data1 = pb.NewDataLiteral<ui32>(1);
    const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("abc");
    const auto structType = pb.NewStructType({{"x", pb.NewDataType(NUdf::TDataType<ui32>::Id)}, {"y", pb.NewDataType(NUdf::TDataType<char*>::Id)}});
    const auto varType = pb.NewVariantType(structType);
    const auto var1 = pb.NewVariant(data1, "x", varType);
    const auto var2 = pb.NewVariant(data2, "y", varType);
    const auto list = pb.NewList(varType, {var1, var2});

    const auto xIndex = AS_TYPE(TStructType, structType)->GetMemberIndex("x");
    const auto pgmReturn = pb.Map(list, [&](TRuntimeNode item) {
        return pb.VisitAll(item, [&](ui32 index, TRuntimeNode item) {
            if (xIndex == index) {
                return pb.Concat(at, pb.ToString(item));
            } else {
                return pb.Concat(at, item);
            }
        });
    });

    const auto graph = setup.BuildGraph(pgmReturn);
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TString>{"@1", "@abc"});
}

Y_UNIT_TEST_LLVM(TestVisitAllTupleFlow) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto at = pb.NewDataLiteral<NUdf::EDataSlot::Utf8>("@");
    const auto data1 = pb.NewDataLiteral<ui64>(3ULL);
    const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::Utf8>("abc");
    const auto tupleType = pb.NewTupleType({pb.NewDataType(NUdf::TDataType<ui64>::Id), pb.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id)});
    const auto varType = pb.NewVariantType(tupleType);
    const auto var1 = pb.NewVariant(data1, 0, varType);
    const auto var2 = pb.NewVariant(data2, 1, varType);
    const auto list = pb.NewList(varType, {var1, var2});
    const auto pgmReturn = pb.FromFlow(pb.FlatMap(pb.ToFlow(list), [&](TRuntimeNode item) {
        return pb.VisitAll(item, [&](ui32 index, TRuntimeNode item) {
            if (!index) {
                return pb.ToFlow(pb.Replicate(at, item, __FILE__, __LINE__, 0U));
            } else {
                return pb.ToFlow(pb.NewOptional(item));
            }
        });
    }));

    const auto graph = setup.BuildGraph(pgmReturn);
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(),
                                               NYql::NUdf::TUnboxedValueComparatorStreamView<TString>(TVector<TString>{"@", "@", "@", "abc"}));
}

Y_UNIT_TEST_LLVM(TestVisitAllStructFlow) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto at = pb.NewDataLiteral<NUdf::EDataSlot::Utf8>("@");
    const auto data1 = pb.NewDataLiteral<ui64>(4ULL);
    const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::Utf8>("abc");
    const auto structType = pb.NewStructType({{"x", pb.NewDataType(NUdf::TDataType<ui64>::Id)}, {"y", pb.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id)}});
    const auto varType = pb.NewVariantType(structType);
    const auto var1 = pb.NewVariant(data1, "x", varType);
    const auto var2 = pb.NewVariant(data2, "y", varType);
    const auto list = pb.NewList(varType, {var2, var1, var2});

    const auto xIndex = AS_TYPE(TStructType, structType)->GetMemberIndex("x");
    const auto pgmReturn = pb.FromFlow(pb.FlatMap(pb.ToFlow(list), [&](TRuntimeNode item) {
        return pb.VisitAll(item, [&](ui32 index, TRuntimeNode item) {
            if (xIndex == index) {
                return pb.ToFlow(pb.Replicate(at, item, __FILE__, __LINE__, 0U));
            } else {
                return pb.ToFlow(pb.NewOptional(item));
            }
        });
    }));

    const auto graph = setup.BuildGraph(pgmReturn);
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(),
                                               NYql::NUdf::TUnboxedValueComparatorStreamView<TString>(TVector<TString>{"abc", "@", "@", "@", "@", "abc"}));
}

Y_UNIT_TEST_LLVM(TestVisitAllStructWideFlow) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto at = pb.NewDataLiteral<NUdf::EDataSlot::Utf8>("@");
    const auto data1 = pb.NewDataLiteral<ui64>(4ULL);
    const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::Utf8>("abc");
    const auto structType = pb.NewStructType({{"x", pb.NewDataType(NUdf::TDataType<ui64>::Id)}, {"y", pb.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id)}});
    const auto varType = pb.NewVariantType(structType);
    const auto var1 = pb.NewVariant(data1, "x", varType);
    const auto var2 = pb.NewVariant(data2, "y", varType);
    const auto list = pb.NewList(varType, {var2, var1, var2});

    const auto xIndex = AS_TYPE(TStructType, structType)->GetMemberIndex("x");
    const auto pgmReturn = pb.FromFlow(pb.NarrowMap(pb.FlatMap(pb.ToFlow(list), [&](TRuntimeNode item) {
        return pb.VisitAll(item, [&](ui32 index, TRuntimeNode item) {
            if (xIndex == index) {
                return pb.ExpandMap(pb.ToFlow(pb.Replicate(at, item, __FILE__, __LINE__, 0U)), [&](TRuntimeNode x) -> TRuntimeNode::TList { return {x, pb.NewDataLiteral(i32(index))}; });
            } else {
                return pb.ExpandMap(pb.ToFlow(pb.NewOptional(item)), [&](TRuntimeNode x) -> TRuntimeNode::TList { return {x, pb.NewDataLiteral(-i32(index))}; });
            }
        });
    }),
                                                    [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }));

    using TRow = std::tuple<TString, i32>;
    const auto graph = setup.BuildGraph(pgmReturn);
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(),
                                               NYql::NUdf::TUnboxedValueComparatorStreamView<TRow>(TVector<TRow>{
                                                   {"abc", i32(-1)},
                                                   {"@", i32(0)},
                                                   {"@", i32(0)},
                                                   {"@", i32(0)},
                                                   {"@", i32(0)},
                                                   {"abc", i32(-1)},
                                               }));
}

Y_UNIT_TEST_LLVM(TestWayTuple) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TItem = std::variant<ui32, TStringBuf>;
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TItem>{
                                                               TItem{TStringBuf("abc")},
                                                               TItem{ui32(1)},
                                                           });
    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) { return pb.Way(item); });

    const auto graph = setup.BuildGraph(pgmReturn);
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{1U, 0U});
}

Y_UNIT_TEST_LLVM(TestWayTupleOpt) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TItem = std::variant<ui32, TStringBuf>;
    const auto var1 = NTest::ConvertValueToLiteralNode(pb, TItem{ui32(1)});
    const auto var2 = NTest::ConvertValueToLiteralNode(pb, TItem{TStringBuf("abc")});
    const auto jvar1 = pb.NewOptional(var1);
    const auto jvar2 = pb.NewOptional(var2);
    const auto nothing = NTest::ConvertValueToLiteralNode(pb, TMaybe<TItem>{});
    const auto list = pb.NewList(nothing.GetStaticType(), {jvar2, nothing, jvar1});
    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) { return pb.Way(item); });

    const auto graph = setup.BuildGraph(pgmReturn);
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(),
                                               TVector<TMaybe<ui32>>{TMaybe<ui32>{1U}, TMaybe<ui32>{}, TMaybe<ui32>{0U}});
}

Y_UNIT_TEST_LLVM(TestWayStruct) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data1 = pb.NewDataLiteral<ui32>(1);
    const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("abc");
    const auto structType = pb.NewStructType({{"x", pb.NewDataType(NUdf::TDataType<ui32>::Id)}, {"y", pb.NewDataType(NUdf::TDataType<char*>::Id)}});
    const auto varType = pb.NewVariantType(structType);
    const auto var1 = pb.NewVariant(data1, "x", varType);
    const auto var2 = pb.NewVariant(data2, "y", varType);
    const auto list = pb.NewList(varType, {var2, var1});
    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) { return pb.Way(item); });

    const auto graph = setup.BuildGraph(pgmReturn);
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TString>{"y", "x"});
}

Y_UNIT_TEST_LLVM(TestWayStructOpt) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data1 = pb.NewDataLiteral<ui32>(1);
    const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("abc");
    const auto structType = pb.NewStructType({{"x", pb.NewDataType(NUdf::TDataType<ui32>::Id)}, {"y", pb.NewDataType(NUdf::TDataType<char*>::Id)}});
    const auto varType = pb.NewVariantType(structType);
    const auto var1 = pb.NewVariant(data1, "x", varType);
    const auto var2 = pb.NewVariant(data2, "y", varType);
    const auto jvar1 = pb.NewOptional(var1);
    const auto jvar2 = pb.NewOptional(var2);
    const auto optType = pb.NewOptionalType(varType);
    const auto nothing = pb.NewEmptyOptional(optType);
    const auto list = pb.NewList(optType, {jvar2, nothing, jvar1});
    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) { return pb.Way(item); });

    const auto graph = setup.BuildGraph(pgmReturn);
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(),
                                               TVector<TMaybe<TString>>{TMaybe<TString>{"y"}, TMaybe<TString>{}, TMaybe<TString>{"x"}});
}

Y_UNIT_TEST_LLVM(TestItemInMap) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TItem = std::variant<i32, TStringBuf, bool>;
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TItem>{
                                                               TItem{i32(77)},
                                                               TItem{TStringBuf("abc")},
                                                               TItem{false},
                                                               TItem{true},
                                                               TItem{TStringBuf("DEF")},
                                                               TItem{i32(-1267)},
                                                           });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.VariantItem(item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 77);
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(TStringBuf(item.AsStringRef()), "abc");
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(TStringBuf(item.AsStringRef()), "DEF");
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -1267);
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TestGuessInMap) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TItem = std::variant<i32, TStringBuf, bool>;
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TItem>{
                                                               TItem{i32(77)},
                                                               TItem{TStringBuf("abc")},
                                                               TItem{false},
                                                               TItem{true},
                                                               TItem{TStringBuf("DEF")},
                                                               TItem{i32(-1267)},
                                                           });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.NewTuple({pb.Guess(item, 0), pb.Guess(item, 2)});
                                  });

    using TRow = std::tuple<TMaybe<i32>, TMaybe<bool>>;
    const auto graph = setup.BuildGraph(pgmReturn);
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TRow>{
                                                                      {TMaybe<i32>{77}, TMaybe<bool>{}},
                                                                      {TMaybe<i32>{}, TMaybe<bool>{}},
                                                                      {TMaybe<i32>{}, TMaybe<bool>{false}},
                                                                      {TMaybe<i32>{}, TMaybe<bool>{true}},
                                                                      {TMaybe<i32>{}, TMaybe<bool>{}},
                                                                      {TMaybe<i32>{-1267}, TMaybe<bool>{}},
                                                                  });
}

Y_UNIT_TEST(TestDynamicVariantTuple) {
    TSetup<false> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const TStringBuf str1 = "VERY LONG STRING!!!";
    const TStringBuf str2 = "SHORT";
    const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>(str1);
    const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>(str2);
    const auto data3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("");
    const auto dataType = pb.NewDataType(NUdf::TDataType<char*>::Id);
    const auto tupleType = pb.NewTupleType({dataType, dataType});
    const auto varType = pb.NewVariantType(tupleType);
    const auto var1 = pb.DynamicVariant(data1, pb.NewDataLiteral<ui32>(0), varType);
    const auto var2 = pb.DynamicVariant(data2, pb.NewDataLiteral<ui32>(1), varType);
    const auto var3 = pb.DynamicVariant(data3, pb.NewDataLiteral<ui32>(2), varType);
    const auto list = pb.AsList({var1, var2, var3});
    const auto pgmReturn = list;

    using TItem = TMaybe<std::variant<TString, TString>>;
    const auto graph = setup.BuildGraph(pgmReturn);
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TItem>{
                                                                      TItem{std::variant<TString, TString>{std::in_place_index<0>, TString(str1)}},
                                                                      TItem{std::variant<TString, TString>{std::in_place_index<1>, TString(str2)}},
                                                                      TItem{},
                                                                  });
}

Y_UNIT_TEST(TestDynamicVariantStruct) {
    TSetup<false> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data1 = pb.NewDataLiteral<ui32>(10);
    const auto data2 = pb.NewDataLiteral<ui32>(20);
    const auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
    const auto structType = pb.NewStructType({{"x", dataType}, {"y", dataType}});
    const auto varType = pb.NewVariantType(structType);
    const auto var1 = pb.DynamicVariant(data1, pb.NewDataLiteral<NUdf::EDataSlot::Utf8>("x"), varType);
    const auto var2 = pb.DynamicVariant(data2, pb.NewDataLiteral<NUdf::EDataSlot::Utf8>("z"), varType);
    const auto list = pb.AsList({var1, var2});
    const auto pgmReturn = list;

    using TItem = TMaybe<std::variant<ui32, ui32>>;
    const auto graph = setup.BuildGraph(pgmReturn);
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TItem>{
                                                                      TItem{std::variant<ui32, ui32>{std::in_place_index<0>, ui32(10)}},
                                                                      TItem{},
                                                                  });
}

Y_UNIT_TEST(TestDynamicVariantStructWithNullIndex) {
    TSetup<false> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data1 = pb.NewDataLiteral<ui32>(10);
    const auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
    const auto structType = pb.NewStructType({{"x", dataType}, {"y", dataType}});
    const auto varType = pb.NewVariantType(structType);
    const auto var1 = pb.DynamicVariant(data1, pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<NUdf::TUtf8>::Id), varType);
    const auto list = pb.AsList({var1});
    const auto pgmReturn = list;

    const auto graph = setup.BuildGraph(pgmReturn);
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(),
                                               TVector<TMaybe<std::variant<ui32, ui32>>>{TMaybe<std::variant<ui32, ui32>>{}});
}
} // Y_UNIT_TEST_SUITE(TMiniKQLVariantTest)

} // namespace NMiniKQL
} // namespace NKikimr
