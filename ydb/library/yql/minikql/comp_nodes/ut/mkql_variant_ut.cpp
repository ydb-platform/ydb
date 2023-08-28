#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLVariantTest) {
    Y_UNIT_TEST_LLVM(TestGuessTuple) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(1);
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("abc");
        const auto tupleType = pb.NewTupleType({pb.NewDataType(NUdf::TDataType<ui32>::Id), pb.NewDataType(NUdf::TDataType<char*>::Id)});
        const auto varType = pb.NewVariantType(tupleType);
        const auto var1 = pb.NewVariant(data1, 0, varType);
        const auto var2 = pb.NewVariant(data2, 1, varType);
        std::vector<TRuntimeNode> tupleItems;
        tupleItems.push_back(pb.Guess(var1, 0));
        tupleItems.push_back(pb.Guess(var1, 1));
        tupleItems.push_back(pb.Guess(var2, 0));
        tupleItems.push_back(pb.Guess(var2, 1));
        const auto pgmReturn = pb.NewTuple(tupleItems);

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto res = graph->GetValue();
        UNIT_ASSERT(res.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(res.GetElement(0).template Get<ui32>(), 1);
        UNIT_ASSERT(!res.GetElement(1));
        UNIT_ASSERT(!res.GetElement(2));
        UNIT_ASSERT(res.GetElement(3));
        UNBOXED_VALUE_STR_EQUAL(res.GetElement(3), "abc");
    }

    Y_UNIT_TEST_LLVM(TestGuessTupleOpt) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(1);
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("abc");
        const auto tupleType = pb.NewTupleType({pb.NewDataType(NUdf::TDataType<ui32>::Id), pb.NewDataType(NUdf::TDataType<char*>::Id)});
        const auto varType = pb.NewVariantType(tupleType);
        const auto var1 = pb.NewVariant(data1, 0, varType);
        const auto var2 = pb.NewVariant(data2, 1, varType);
        const auto jvar1 = pb.NewOptional(var1);
        const auto jvar2 = pb.NewOptional(var2);
        const auto nothing = pb.NewEmptyOptional(pb.NewOptionalType(varType));

        std::vector<TRuntimeNode> tupleItems;
        tupleItems.push_back(pb.Guess(jvar1, 0));
        tupleItems.push_back(pb.Guess(jvar1, 1));
        tupleItems.push_back(pb.Guess(jvar2, 0));
        tupleItems.push_back(pb.Guess(jvar2, 1));
        tupleItems.push_back(pb.Guess(nothing, 0));
        tupleItems.push_back(pb.Guess(nothing, 1));
        const auto pgmReturn = pb.NewTuple(tupleItems);

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto res = graph->GetValue();
        UNIT_ASSERT(res.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(res.GetElement(0).template Get<ui32>(), 1);
        UNIT_ASSERT(!res.GetElement(1));
        UNIT_ASSERT(!res.GetElement(2));
        UNIT_ASSERT(res.GetElement(3));
        UNBOXED_VALUE_STR_EQUAL(res.GetElement(3), "abc");
        UNIT_ASSERT(!res.GetElement(4));
        UNIT_ASSERT(!res.GetElement(5));
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
        const auto res = graph->GetValue();
        UNIT_ASSERT(res.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(res.GetElement(0).template Get<ui32>(), 1);
        UNIT_ASSERT(!res.GetElement(1));
        UNIT_ASSERT(!res.GetElement(2));
        UNIT_ASSERT(res.GetElement(3));
        UNBOXED_VALUE_STR_EQUAL(res.GetElement(3), "abc");
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
        const auto res = graph->GetValue();
        UNIT_ASSERT(res.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(res.GetElement(0).template Get<ui32>(), 1);
        UNIT_ASSERT(!res.GetElement(1));
        UNIT_ASSERT(!res.GetElement(2));
        UNIT_ASSERT(res.GetElement(3));
        UNBOXED_VALUE_STR_EQUAL(res.GetElement(3), "abc");
        UNIT_ASSERT(!res.GetElement(4));
        UNIT_ASSERT(!res.GetElement(5));
    }

    Y_UNIT_TEST_LLVM(TestVisitAllTuple) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(1);
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("abc");
        const auto at = pb.NewDataLiteral<NUdf::EDataSlot::String>("@");
        const auto tupleType = pb.NewTupleType({pb.NewDataType(NUdf::TDataType<ui32>::Id), pb.NewDataType(NUdf::TDataType<char*>::Id)});
        const auto varType = pb.NewVariantType(tupleType);
        const auto var1 = pb.NewVariant(data1, 0, varType);
        const auto var2 = pb.NewVariant(data2, 1, varType);
        const auto list = pb.NewList(varType, {var1, var2});
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
        const auto res = graph->GetValue();
        UNBOXED_VALUE_STR_EQUAL(res.GetElement(0), "@1");
        UNBOXED_VALUE_STR_EQUAL(res.GetElement(1), "@abc");
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
        const auto res = graph->GetValue();
        UNBOXED_VALUE_STR_EQUAL(res.GetElement(0), "@1");
        UNBOXED_VALUE_STR_EQUAL(res.GetElement(1), "@abc");
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
        const auto res = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_EQUAL(res.Fetch(item), NUdf::EFetchStatus::Ok);
        UNBOXED_VALUE_STR_EQUAL(item, "@");
        UNIT_ASSERT_EQUAL(res.Fetch(item), NUdf::EFetchStatus::Ok);
        UNBOXED_VALUE_STR_EQUAL(item, "@");
        UNIT_ASSERT_EQUAL(res.Fetch(item), NUdf::EFetchStatus::Ok);
        UNBOXED_VALUE_STR_EQUAL(item, "@");
        UNIT_ASSERT_EQUAL(res.Fetch(item), NUdf::EFetchStatus::Ok);
        UNBOXED_VALUE_STR_EQUAL(item, "abc");
        UNIT_ASSERT_EQUAL(res.Fetch(item), NUdf::EFetchStatus::Finish);
        UNIT_ASSERT_EQUAL(res.Fetch(item), NUdf::EFetchStatus::Finish);
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
        const auto res = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_EQUAL(res.Fetch(item), NUdf::EFetchStatus::Ok);
        UNBOXED_VALUE_STR_EQUAL(item, "abc");
        UNIT_ASSERT_EQUAL(res.Fetch(item), NUdf::EFetchStatus::Ok);
        UNBOXED_VALUE_STR_EQUAL(item, "@");
        UNIT_ASSERT_EQUAL(res.Fetch(item), NUdf::EFetchStatus::Ok);
        UNBOXED_VALUE_STR_EQUAL(item, "@");
        UNIT_ASSERT_EQUAL(res.Fetch(item), NUdf::EFetchStatus::Ok);
        UNBOXED_VALUE_STR_EQUAL(item, "@");
        UNIT_ASSERT_EQUAL(res.Fetch(item), NUdf::EFetchStatus::Ok);
        UNBOXED_VALUE_STR_EQUAL(item, "@");
        UNIT_ASSERT_EQUAL(res.Fetch(item), NUdf::EFetchStatus::Ok);
        UNBOXED_VALUE_STR_EQUAL(item, "abc");
        UNIT_ASSERT_EQUAL(res.Fetch(item), NUdf::EFetchStatus::Finish);
        UNIT_ASSERT_EQUAL(res.Fetch(item), NUdf::EFetchStatus::Finish);
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
            [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto res = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_EQUAL(res.Fetch(item), NUdf::EFetchStatus::Ok);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "abc");
        UNIT_ASSERT_EQUAL(item.GetElement(1).Get<i32>(), -1);
        UNIT_ASSERT_EQUAL(res.Fetch(item), NUdf::EFetchStatus::Ok);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "@");
        UNIT_ASSERT_EQUAL(item.GetElement(1).Get<i32>(), 0);
        UNIT_ASSERT_EQUAL(res.Fetch(item), NUdf::EFetchStatus::Ok);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "@");
        UNIT_ASSERT_EQUAL(item.GetElement(1).Get<i32>(), 0);
        UNIT_ASSERT_EQUAL(res.Fetch(item), NUdf::EFetchStatus::Ok);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "@");
        UNIT_ASSERT_EQUAL(item.GetElement(1).Get<i32>(), 0);
        UNIT_ASSERT_EQUAL(res.Fetch(item), NUdf::EFetchStatus::Ok);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "@");
        UNIT_ASSERT_EQUAL(item.GetElement(1).Get<i32>(), 0);
        UNIT_ASSERT_EQUAL(res.Fetch(item), NUdf::EFetchStatus::Ok);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "abc");
        UNIT_ASSERT_EQUAL(item.GetElement(1).Get<i32>(), -1);
        UNIT_ASSERT_EQUAL(res.Fetch(item), NUdf::EFetchStatus::Finish);
        UNIT_ASSERT_EQUAL(res.Fetch(item), NUdf::EFetchStatus::Finish);
    }

    Y_UNIT_TEST_LLVM(TestWayTuple) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(1);
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("abc");
        const auto tupleType = pb.NewTupleType({pb.NewDataType(NUdf::TDataType<ui32>::Id), pb.NewDataType(NUdf::TDataType<char*>::Id)});
        const auto varType = pb.NewVariantType(tupleType);
        const auto var1 = pb.NewVariant(data1, 0, varType);
        const auto var2 = pb.NewVariant(data2, 1, varType);
        const auto list = pb.NewList(varType, {var2, var1});
        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) { return pb.Way(item); }
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 1U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0U);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestWayTupleOpt) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(1);
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("abc");
        const auto tupleType = pb.NewTupleType({pb.NewDataType(NUdf::TDataType<ui32>::Id), pb.NewDataType(NUdf::TDataType<char*>::Id)});
        const auto varType = pb.NewVariantType(tupleType);
        const auto var1 = pb.NewVariant(data1, 0, varType);
        const auto var2 = pb.NewVariant(data2, 1, varType);
        const auto jvar1 = pb.NewOptional(var1);
        const auto jvar2 = pb.NewOptional(var2);
        const auto optType = pb.NewOptionalType(varType);
        const auto nothing = pb.NewEmptyOptional(optType);
        const auto list = pb.NewList(optType, {jvar2, nothing, jvar1});
        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) { return pb.Way(item); }
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 1U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0U);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
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
            [&](TRuntimeNode item) { return pb.Way(item); }
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "y");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "x");
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
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
            [&](TRuntimeNode item) { return pb.Way(item); }
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "y");
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "x");
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestItemInMap) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto varType = pb.NewVariantType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<i32>::Id), pb.NewDataType(NUdf::TDataType<char*>::Id), pb.NewDataType(NUdf::TDataType<bool>::Id)}));

        const auto data0 = pb.NewVariant(pb.NewDataLiteral<i32>(77), 0, varType);
        const auto data1 = pb.NewVariant(pb.NewDataLiteral<NUdf::EDataSlot::String>("abc"), 1, varType);
        const auto data2 = pb.NewVariant(pb.NewDataLiteral<bool>(false), 2, varType);
        const auto data3 = pb.NewVariant(pb.NewDataLiteral<bool>(true), 2, varType);
        const auto data4 = pb.NewVariant(pb.NewDataLiteral<NUdf::EDataSlot::String>("DEF"), 1, varType);
        const auto data5 = pb.NewVariant(pb.NewDataLiteral<i32>(-1267), 0, varType);
        const auto list = pb.NewList(varType, {data0, data1, data2, data3, data4, data5});

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

        const auto varType = pb.NewVariantType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<i32>::Id), pb.NewDataType(NUdf::TDataType<char*>::Id), pb.NewDataType(NUdf::TDataType<bool>::Id)}));

        const auto data0 = pb.NewVariant(pb.NewDataLiteral<i32>(77), 0, varType);
        const auto data1 = pb.NewVariant(pb.NewDataLiteral<NUdf::EDataSlot::String>("abc"), 1, varType);
        const auto data2 = pb.NewVariant(pb.NewDataLiteral<bool>(false), 2, varType);
        const auto data3 = pb.NewVariant(pb.NewDataLiteral<bool>(true), 2, varType);
        const auto data4 = pb.NewVariant(pb.NewDataLiteral<NUdf::EDataSlot::String>("DEF"), 1, varType);
        const auto data5 = pb.NewVariant(pb.NewDataLiteral<i32>(-1267), 0, varType);
        const auto list = pb.NewList(varType, {data0, data1, data2, data3, data4, data5});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return  pb.NewTuple({pb.Guess(item, 0), pb.Guess(item, 2)});
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 77);
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<bool>(), false);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -1267);
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }
}

}
}
