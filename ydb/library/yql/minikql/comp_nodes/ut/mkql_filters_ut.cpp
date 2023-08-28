#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/mkql_runtime_version.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLFiltersTest) {
    Y_UNIT_TEST_LLVM(TestSkipNullMembers) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto structType = pb.NewStructType({{"Key", dataType}, {"Payload", dataType}});

        const auto data1 = pb.NewStruct(structType, {{"Key", pb.NewOptional(pb.NewDataLiteral<i32>(1))}, {"Payload", pb.NewEmptyOptional(dataType)}});
        const auto data2 = pb.NewStruct(structType, {{"Key", pb.NewEmptyOptional(dataType)}, {"Payload", pb.NewOptional(pb.NewDataLiteral<i32>(2))}});
        const auto data3 = pb.NewStruct(structType, {{"Key", pb.NewOptional(pb.NewDataLiteral<i32>(3))}, {"Payload", pb.NewEmptyOptional(dataType)}});

        const auto list = pb.NewList(structType, {data1, data2, data3});

        const auto pgmReturn = pb.SkipNullMembers(list, {"Payload"});

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 2);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFilterNullMembers) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto structType = pb.NewStructType({{"Key", dataType}, {"Payload", dataType}});

        const auto data1 = pb.NewStruct(structType, {{"Key", pb.NewOptional(pb.NewDataLiteral<i32>(1))}, {"Payload", pb.NewEmptyOptional(dataType)}});
        const auto data2 = pb.NewStruct(structType, {{"Key", pb.NewEmptyOptional(dataType)}, {"Payload", pb.NewOptional(pb.NewDataLiteral<i32>(2))}});
        const auto data3 = pb.NewStruct(structType, {{"Key", pb.NewOptional(pb.NewDataLiteral<i32>(3))}, {"Payload", pb.NewEmptyOptional(dataType)}});

        const auto list = pb.NewList(structType, {data1, data2, data3});

        const auto pgmReturn = pb.FilterNullMembers(list, {"Payload"});

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 2);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFilterNullMembersMultiOptional) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id)));
        const auto structType = pb.NewStructType({{"Key", dataType}, {"Payload", dataType}});
        const auto justNothing = pb.NewOptional(pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<i32>::Id));

        const auto data1 = pb.NewStruct(structType, {{"Key", pb.NewOptional(pb.NewOptional(pb.NewDataLiteral<i32>(1)))}, {"Payload", pb.NewEmptyOptional(dataType)}});
        const auto data2 = pb.NewStruct(structType, {{"Key", pb.NewEmptyOptional(dataType)}, {"Payload", pb.NewOptional(pb.NewOptional(pb.NewDataLiteral<i32>(2)))}});
        const auto data3 = pb.NewStruct(structType, {{"Key", justNothing}, {"Payload", justNothing}});

        const auto list = pb.NewList(structType, {data1, data2, data3});

        const auto pgmReturn = pb.FilterNullMembers(list, {"Payload"});

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0));
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestSkipNullMembersOverFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto structType = pb.NewStructType({{"Key", dataType}, {"Payload", dataType}});

        const auto data1 = pb.NewStruct(structType, {{"Key", pb.NewOptional(pb.NewDataLiteral<i32>(1))}, {"Payload", pb.NewEmptyOptional(dataType)}});
        const auto data2 = pb.NewStruct(structType, {{"Key", pb.NewEmptyOptional(dataType)}, {"Payload", pb.NewOptional(pb.NewDataLiteral<i32>(2))}});
        const auto data3 = pb.NewStruct(structType, {{"Key", pb.NewOptional(pb.NewDataLiteral<i32>(3))}, {"Payload", pb.NewEmptyOptional(dataType)}});

        const auto list = pb.NewList(structType, {data1, data2, data3});

        const auto pgmReturn = pb.FromFlow(pb.SkipNullMembers(pb.ToFlow(list), {"Payload"}));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestFilterNullMembersOverFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto structType = pb.NewStructType({{"Key", dataType}, {"Payload", dataType}});

        const auto data1 = pb.NewStruct(structType, {{"Key", pb.NewOptional(pb.NewDataLiteral<i32>(1))}, {"Payload", pb.NewEmptyOptional(dataType)}});
        const auto data2 = pb.NewStruct(structType, {{"Key", pb.NewEmptyOptional(dataType)}, {"Payload", pb.NewOptional(pb.NewDataLiteral<i32>(2))}});
        const auto data3 = pb.NewStruct(structType, {{"Key", pb.NewOptional(pb.NewDataLiteral<i32>(3))}, {"Payload", pb.NewEmptyOptional(dataType)}});

        const auto list = pb.NewList(structType, {data1, data2, data3});

        const auto pgmReturn = pb.FromFlow(pb.FilterNullMembers(pb.ToFlow(list), {"Payload"}));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestFilterNullMembersMultiOptionalOverFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id)));
        const auto structType = pb.NewStructType({{"Key", dataType}, {"Payload", dataType}});
        const auto justNothing = pb.NewOptional(pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<i32>::Id));

        const auto data1 = pb.NewStruct(structType, {{"Key", pb.NewOptional(pb.NewOptional(pb.NewDataLiteral<i32>(1)))}, {"Payload", pb.NewEmptyOptional(dataType)}});
        const auto data2 = pb.NewStruct(structType, {{"Key", pb.NewEmptyOptional(dataType)}, {"Payload", pb.NewOptional(pb.NewOptional(pb.NewDataLiteral<i32>(2)))}});
        const auto data3 = pb.NewStruct(structType, {{"Key", justNothing}, {"Payload", justNothing}});

        const auto list = pb.NewList(structType, {data1, data2, data3});

        const auto pgmReturn = pb.FromFlow(pb.FilterNullMembers(pb.ToFlow(list), {"Payload"}));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(item.GetElement(0));
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestSkipNullElements) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(dataType)});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.SkipNullElements(list, {1U, 2U});

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -2);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFilterNullElements) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(dataType)});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.FilterNullElements(list, {1U, 2U});

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -2);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFilterNullElementsMultiOptional) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id)));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});
        const auto justNothing = pb.NewOptional(pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<i32>::Id));

        const auto data1 = pb.NewTuple(tupleType, {justNothing, pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewOptional(pb.NewDataLiteral<i32>(-1)))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewOptional(pb.NewDataLiteral<i32>(2))), justNothing});
        const auto data3 = pb.NewTuple(tupleType, {justNothing, pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewOptional(pb.NewDataLiteral<i32>(-3)))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewOptional(pb.NewDataLiteral<i32>(4))), justNothing, pb.NewOptional(pb.NewOptional(pb.NewDataLiteral<i32>(-4)))});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.FilterNullElements(list, {1U, 2U});

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 2);
        UNIT_ASSERT(!item.GetElement(2));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 4);
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -4);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestSkipNullElementsOverOverFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(dataType)});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.FromFlow(pb.SkipNullElements(pb.ToFlow(list), {1U, 2U}));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -2);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestFilterNullElementsOverOverFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(dataType)});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.FromFlow(pb.FilterNullElements(pb.ToFlow(list), {1U, 2U}));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -2);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestFilterNullElementsOverMultiOptionalOverFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id)));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});
        const auto justNothing = pb.NewOptional(pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<i32>::Id));

        const auto data1 = pb.NewTuple(tupleType, {justNothing, pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewOptional(pb.NewDataLiteral<i32>(-1)))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewOptional(pb.NewDataLiteral<i32>(2))), justNothing});
        const auto data3 = pb.NewTuple(tupleType, {justNothing, pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewOptional(pb.NewDataLiteral<i32>(-3)))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewOptional(pb.NewDataLiteral<i32>(4))), justNothing, pb.NewOptional(pb.NewOptional(pb.NewDataLiteral<i32>(-4)))});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.FromFlow(pb.FilterNullElements(pb.ToFlow(list), {1U, 2U}));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 2);
        UNIT_ASSERT(!item.GetElement(2));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 4);
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -4);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestFilterOverList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data2 = pb.NewDataLiteral<ui32>(2);
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto optionalType = pb.NewOptionalType(dataType);
        const auto list = pb.NewList(optionalType, {pb.NewEmptyOptional(optionalType), pb.NewOptional(data2)});

        const auto pgmReturn = pb.Filter(list,
            [&](TRuntimeNode item) {
            return pb.Exists(item);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item);
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFilterOverStream) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<ui32>(0);
        const auto data1 = pb.NewDataLiteral<ui32>(1);
        const auto data2 = pb.NewDataLiteral<ui32>(2);
        const auto data3 = pb.NewDataLiteral<ui32>(3);
        const auto data4 = pb.NewDataLiteral<ui32>(4);
        const auto data5 = pb.NewDataLiteral<ui32>(5);
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4, data5});
        const auto pgmReturn = pb.Filter(pb.Iterator(list, {}),
            [&](TRuntimeNode item) {
            return pb.Greater(pb.Unwrap(pb.Mod(item, data3), pb.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0), data1);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 5);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestFilterOverFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<ui32>(0);
        const auto data1 = pb.NewDataLiteral<ui32>(1);
        const auto data2 = pb.NewDataLiteral<ui32>(2);
        const auto data3 = pb.NewDataLiteral<ui32>(3);
        const auto data4 = pb.NewDataLiteral<ui32>(4);
        const auto data5 = pb.NewDataLiteral<ui32>(5);
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4, data5});
        const auto pgmReturn = pb.FromFlow(pb.Filter(pb.ToFlow(list),
            [&](TRuntimeNode item) {
            return pb.Greater(pb.Unwrap(pb.Mod(item, data3), pb.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0), data1);
        }));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 5);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestFilterOverListLazy) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<ui32>::Id));
        const auto data = pb.NewEmptyOptional(dataType);
        const auto data2 = pb.NewOptional(pb.NewDataLiteral<ui32>(2U));
        const auto list = pb.NewList(dataType, {data, data2});

        const auto pgmReturn = pb.Filter(pb.LazyList(list),
            [&](TRuntimeNode item) { return pb.Exists(item); }
        );

        const auto graph = setup.BuildGraph(pgmReturn);

        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item);
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFilterByString) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto optionalType = pb.NewOptionalType(dataType);
        const auto data0 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("000"));
        const auto data1 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("100"));
        const auto data2 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("200"));
        const auto data3 = pb.NewEmptyOptional(optionalType);
        const auto list = pb.NewList(optionalType, {data0, data1, data2, data3, data1, data0});

        const auto pgmReturn = pb.Filter(list,
            [&](TRuntimeNode item) {
            return pb.Coalesce(pb.Equals(item, data1), pb.NewDataLiteral<bool>(false));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto result = graph->GetValue();
        UNIT_ASSERT_VALUES_EQUAL(result.GetListLength(), 2ULL);
        UNBOXED_VALUE_STR_EQUAL(result.GetElement(0U), "100");
        UNBOXED_VALUE_STR_EQUAL(result.GetElement(1U), "100");
    }

    Y_UNIT_TEST_LLVM(TestSkipWhile) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<NUdf::EDataSlot::String>("000");
        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("100");
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("200");
        const auto data3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("300");
        const auto dataType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3});

        const auto pgmReturn = pb.SkipWhile(list,
            [&](TRuntimeNode item) {
            return pb.NotEquals(item, data2);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "200");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "300");
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestTakeWhile) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<NUdf::EDataSlot::String>("000");
        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("100");
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("200");
        const auto data3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("300");
        const auto dataType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3});

        const auto pgmReturn = pb.TakeWhile(list,
            [&](TRuntimeNode item) {
            return pb.NotEquals(item, data2);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "000");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "100");
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestSkipWhileOverFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<NUdf::EDataSlot::String>("000");
        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("100");
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("200");
        const auto data3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("300");
        const auto dataType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3});

        const auto pgmReturn = pb.FromFlow(pb.SkipWhile(pb.ToFlow(list),
            [&](TRuntimeNode item) {
            return pb.NotEquals(item, data2);
        }));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNBOXED_VALUE_STR_EQUAL(item, "200");
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNBOXED_VALUE_STR_EQUAL(item, "300");
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestTakeWhileOverFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<NUdf::EDataSlot::String>("000");
        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("100");
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("200");
        const auto data3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("300");
        const auto dataType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3});

        const auto pgmReturn = pb.FromFlow(pb.TakeWhile(pb.ToFlow(list),
            [&](TRuntimeNode item) {
            return pb.NotEquals(item, data2);
        }));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNBOXED_VALUE_STR_EQUAL(item, "000");
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNBOXED_VALUE_STR_EQUAL(item, "100");
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestSkipWhileInclusive) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<NUdf::EDataSlot::String>("000");
        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("100");
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("200");
        const auto data3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("300");
        const auto dataType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3});

        const auto pgmReturn = pb.SkipWhileInclusive(list,
            [&](TRuntimeNode item) {
            return pb.NotEquals(item, data2);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "300");
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestTakeWhileInclusive) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<NUdf::EDataSlot::String>("000");
        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("100");
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("200");
        const auto data3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("300");
        const auto dataType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3});

        const auto pgmReturn = pb.TakeWhileInclusive(list,
            [&](TRuntimeNode item) {
            return pb.NotEquals(item, data2);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "000");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "100");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "200");
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestSkipWhileInclusiveOnEmptyList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto list = pb.NewList(dataType, {});

        const auto pgmReturn = pb.SkipWhileInclusive(list,
            [&](TRuntimeNode item) {
            return pb.NotEquals(item, pb.NewDataLiteral<NUdf::EDataSlot::String>("000"));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestTakeWhileInclusiveOnEmptyList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto list = pb.NewList(dataType, {});

        const auto pgmReturn = pb.TakeWhileInclusive(list,
            [&](TRuntimeNode item) {
            return pb.NotEquals(item, pb.NewDataLiteral<NUdf::EDataSlot::String>("000"));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestSkipWhileInclusiveOverFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<NUdf::EDataSlot::String>("000");
        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("100");
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("200");
        const auto data3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("300");
        const auto dataType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3});

        const auto pgmReturn = pb.FromFlow(pb.SkipWhileInclusive(pb.ToFlow(list),
            [&](TRuntimeNode item) {
            return pb.NotEquals(item, data2);
        }));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNBOXED_VALUE_STR_EQUAL(item, "300");
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestTakeWhileInclusiveOverFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<NUdf::EDataSlot::String>("000");
        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("100");
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("200");
        const auto data3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("300");
        const auto dataType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3});

        const auto pgmReturn = pb.FromFlow(pb.TakeWhileInclusive(pb.ToFlow(list),
            [&](TRuntimeNode item) {
            return pb.NotEquals(item, data2);
        }));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNBOXED_VALUE_STR_EQUAL(item, "000");
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNBOXED_VALUE_STR_EQUAL(item, "100");
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNBOXED_VALUE_STR_EQUAL(item, "200");
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestDateToStringCompleteCheck) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list = pb.ListFromRange(pb.NewDataLiteral<ui16>(0U), pb.NewDataLiteral<ui16>(NUdf::MAX_DATE), pb.NewDataLiteral<ui16>(1U));
        const auto dateType = pb.NewDataType(NUdf::EDataSlot::Date, true);
        const auto pgmReturn = pb.Not(pb.HasItems(pb.Filter(list,
            [&](TRuntimeNode item) {
                const auto date = pb.ToIntegral(item, dateType);
                const auto utf8 = pb.ToString<true>(date);
                return pb.AggrNotEquals(date, pb.FromString(utf8, dateType));
            }
        )));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto result = graph->GetValue();
        UNIT_ASSERT(result.template Get<bool>());
    }

    Y_UNIT_TEST_LLVM(TestTzDateToStringCompleteCheck) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list = pb.ListFromRange(pb.NewDataLiteral<ui16>(0U), pb.NewDataLiteral<ui16>(NUdf::MAX_DATE), pb.NewDataLiteral<ui16>(1U));
        const auto dateType = pb.NewDataType(NUdf::EDataSlot::Date, true);
        const auto dateTypeTz = pb.NewDataType(NUdf::EDataSlot::TzDate, true);
        const auto canada = pb.NewDataLiteral<ui16>(375U);
        const auto europe = pb.NewDataLiteral<ui16>(459U);
        const auto pgmReturn = pb.Not(pb.HasItems(pb.Filter(list,
            [&](TRuntimeNode item) {
                const auto date = pb.Unwrap(pb.ToIntegral(item, dateType), pb.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0);
                const auto date1 = pb.Unwrap(pb.FromString(pb.ToString<true>(pb.AddTimezone(date, canada)), dateTypeTz), pb.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0);
                const auto date2 = pb.Unwrap(pb.FromString(pb.ToString<true>(pb.AddTimezone(date, europe)), dateTypeTz), pb.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0);
                return pb.Or({pb.NotEquals(date, date1), pb.NotEquals(date, date2)});
            }
        )));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto result = graph->GetValue();
        UNIT_ASSERT(result.template Get<bool>());
    }

    Y_UNIT_TEST_LLVM(TestInt16ToFloatCompleteCheck) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list = pb.ListFromRange(pb.NewDataLiteral<i16>(std::numeric_limits<i16>::min()), pb.NewDataLiteral<i16>(std::numeric_limits<i16>::max()), pb.NewDataLiteral<i16>(1));
        const auto type = pb.NewDataType(NUdf::EDataSlot::Float);
        const auto pgmReturn = pb.Not(pb.HasItems(pb.Filter(list,
            [&](TRuntimeNode item) {
                return pb.NotEquals(item, pb.Convert(item, type));
            }
        )));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto result = graph->GetValue();
        UNIT_ASSERT(result.template Get<bool>());
    }

    Y_UNIT_TEST_LLVM(TestUint16ToFloatCompleteCheck) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list = pb.ListFromRange(pb.NewDataLiteral<ui16>(std::numeric_limits<ui16>::min()), pb.NewDataLiteral<ui16>(std::numeric_limits<ui16>::max()), pb.NewDataLiteral<ui16>(1U));
        const auto type = pb.NewDataType(NUdf::EDataSlot::Float);
        const auto pgmReturn = pb.Not(pb.HasItems(pb.Filter(list,
            [&](TRuntimeNode item) {
                return pb.NotEquals(item, pb.Convert(item, type));
            }
        )));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto result = graph->GetValue();
        UNIT_ASSERT(result.template Get<bool>());
    }

    Y_UNIT_TEST_LLVM(TestDateToDatetimeCompleteCheck) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list = pb.ListFromRange(pb.NewDataLiteral<ui16>(0U), pb.NewDataLiteral<ui16>(NUdf::MAX_DATE), pb.NewDataLiteral<ui16>(1U));
        const auto dateType = pb.NewDataType(NUdf::EDataSlot::Date, true);
        const auto datetimeType = pb.NewDataType(NUdf::EDataSlot::Datetime, true);
        const auto pgmReturn = pb.Not(pb.HasItems(pb.Filter(list,
            [&](TRuntimeNode item) {
                const auto date = pb.ToIntegral(item, dateType);
                return pb.Coalesce(pb.NotEquals(date, pb.Convert(date, datetimeType)), pb.NewDataLiteral(false));
            }
        )));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto result = graph->GetValue();
        UNIT_ASSERT(result.template Get<bool>());
    }

    Y_UNIT_TEST_LLVM(TestTzDateToDatetimeCompleteCheck) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list = pb.ListFromRange(pb.NewDataLiteral<ui16>(0U), pb.NewDataLiteral<ui16>(NUdf::MAX_DATE), pb.NewDataLiteral<ui16>(1U));
        const auto dateType = pb.NewDataType(NUdf::EDataSlot::Date, true);
        const auto datetimeType = pb.NewDataType(NUdf::EDataSlot::Datetime);
        const auto canada = pb.NewDataLiteral<ui16>(375U);
        const auto europe = pb.NewDataLiteral<ui16>(459U);
        const auto pgmReturn = pb.Not(pb.HasItems(pb.Filter(list,
            [&](TRuntimeNode item) {
                const auto date = pb.ToIntegral(item, dateType);
                const auto date1 = pb.Unwrap(pb.AddTimezone(date, canada), pb.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0);
                const auto date2 = pb.Unwrap(pb.AddTimezone(date, europe), pb.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0);
                return pb.Or({pb.NotEquals(date1, pb.Cast(date1, datetimeType)), pb.NotEquals(date2, pb.Cast(date2, datetimeType))});
            }
        )));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto result = graph->GetValue();
        UNIT_ASSERT(result.template Get<bool>());
    }

    Y_UNIT_TEST_LLVM(TestDateAddTimezoneAndCastOrderCompleteCheck) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list = pb.ListFromRange(pb.NewDataLiteral<ui16>(0U), pb.NewDataLiteral<ui16>(NUdf::MAX_DATE), pb.NewDataLiteral<ui16>(1U));
        const auto dateType = pb.NewDataType(NUdf::EDataSlot::Date, true);
        const auto datetimeType = pb.NewDataType(NUdf::EDataSlot::Datetime);
        const auto datetimeTypeTz = pb.NewDataType(NUdf::EDataSlot::TzDatetime);
        const auto canada = pb.NewDataLiteral<ui16>(375U);
        const auto europe = pb.NewDataLiteral<ui16>(459U);
        const auto pgmReturn = pb.Not(pb.HasItems(pb.Filter(list,
            [&](TRuntimeNode item) {
                const auto date = pb.Unwrap(pb.ToIntegral(item, dateType), pb.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0);
                const auto date1_1 = pb.Cast(pb.Unwrap(pb.AddTimezone(date, canada), pb.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0), datetimeTypeTz);
                const auto date1_2 = pb.Unwrap(pb.AddTimezone(pb.Cast(date, datetimeType), canada), pb.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0);
                const auto date2_1 = pb.Cast(pb.Unwrap(pb.AddTimezone(date, europe), pb.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0), datetimeTypeTz);
                const auto date2_2 = pb.Unwrap(pb.AddTimezone(pb.Cast(date, datetimeType), europe), pb.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0);
                return pb.Or({pb.NotEquals(date1_1, date1_2), pb.NotEquals(date2_1, date2_2), pb.NotEquals(date1_1, date2_2), pb.NotEquals(date2_1, date1_2)});
            }
        )));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto result = graph->GetValue();
        UNIT_ASSERT(result.template Get<bool>());
    }

    Y_UNIT_TEST_LLVM(TestFilterWithLimitOverList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<ui32>(0);
        const auto data1 = pb.NewDataLiteral<ui32>(1);
        const auto data2 = pb.NewDataLiteral<ui32>(2);
        const auto data3 = pb.NewDataLiteral<ui32>(3);
        const auto data4 = pb.NewDataLiteral<ui32>(4);
        const auto data5 = pb.NewDataLiteral<ui32>(5);
        const auto data6 = pb.NewDataLiteral<ui32>(6);
        const auto limit = pb.NewDataLiteral<ui64>(3ULL);
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto optionalType = pb.NewOptionalType(dataType);
        const auto list = pb.NewList(optionalType, {
            pb.NewOptional(data0),
            pb.NewOptional(data1),
            pb.NewOptional(data2),
            pb.NewOptional(data3),
            pb.NewOptional(data4),
            pb.NewOptional(data5),
            pb.NewOptional(data3),
            pb.NewOptional(data4),
            pb.NewOptional(data6),
            pb.NewEmptyOptional(optionalType),
            pb.NewEmptyOptional(optionalType)
        });

        const auto pgmReturn = pb.Filter(list, limit,
            [&](TRuntimeNode item) {
            return pb.AggrEquals(pb.Unwrap(pb.Mod(item, data2), pb.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0), data0);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 4);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFilterWithLimitOverEmptyList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto limit = pb.NewDataLiteral<ui64>(3ULL);
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto optionalType = pb.NewOptionalType(dataType);
        const auto list = pb.NewEmptyList(optionalType);

        const auto pgmReturn = pb.Filter(list, limit,
            [&](TRuntimeNode) {
            return pb.NewDataLiteral<bool>(true);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFilterWithLimitOverLazyList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto start = pb.NewDataLiteral<ui64>(0ULL);
        const auto stop = pb.NewDataLiteral<ui64>(1000000000000ULL);
        const auto step = pb.NewDataLiteral<ui64>(1ULL);

        const auto limit = pb.NewDataLiteral<ui64>(7ULL);
        const auto list = pb.ListFromRange(start, stop, step);

        const auto pgmReturn = pb.Filter(list, limit,
            [&](TRuntimeNode item) {
            return pb.AggrEquals(pb.CountBits(item), pb.NewDataLiteral<ui64>(3ULL));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 7ULL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 11ULL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 13ULL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 14ULL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 19ULL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 21ULL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 22ULL);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFilterWithSmallLimitGetTailOfLargeList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto start = pb.NewDataLiteral<ui64>(0ULL);
        const auto stop = pb.NewDataLiteral<ui64>(100100LL);
        const auto step = pb.NewDataLiteral<ui64>(1ULL);

        const auto limit = pb.NewDataLiteral<ui64>(3ULL);
        const auto list = pb.ListFromRange(start, stop, step);

        const auto pgmReturn = pb.Filter(pb.Collect(list), limit,
            [&](TRuntimeNode item) {
            return pb.AggrGreaterOrEqual(item, pb.NewDataLiteral<ui64>(100000LL));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 100000LL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 100001LL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 100002LL);

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFilterWithLimitOverEnumerate) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto start = pb.NewDataLiteral<i64>(0LL);
        const auto stop = pb.NewDataLiteral<i64>(-1000000000000LL);
        const auto step = pb.NewDataLiteral<i64>(-1LL);

        const auto estart = pb.NewDataLiteral<ui64>(42ULL);
        const auto estep = pb.NewDataLiteral<ui64>(3ULL);

        const auto limit = pb.NewDataLiteral<ui64>(5ULL);
        const auto list = pb.Enumerate(pb.ListFromRange(start, stop, step), estart, estep);

        const auto pgmReturn = pb.Filter(list, limit,
            [&](TRuntimeNode item) {
            return pb.AggrEquals(pb.CountBits(pb.Add(pb.Nth(item, 0), pb.Nth(item, 1))), pb.NewDataLiteral<i64>(4LL));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0U).template Get<ui64>(), 48ULL);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1U).template Get<i64>(), -2LL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0U).template Get<ui64>(), 60ULL);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1U).template Get<i64>(), -6LL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0U).template Get<ui64>(), 66ULL);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1U).template Get<i64>(), -8LL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0U).template Get<ui64>(), 69ULL);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1U).template Get<i64>(), -9LL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0U).template Get<ui64>(), 96ULL);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1U).template Get<i64>(), -18LL);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFilterWithLimitOverStream) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto type = pb.NewStreamType(pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<ui32>::Id)));
        const auto stream = pb.ChainMap(pb.SourceOf(type), pb.NewDataLiteral<ui64>(0ULL),
            [&](TRuntimeNode, TRuntimeNode state) -> TRuntimeNodePair {
                return {state, pb.Increment(state)};
            }
        );

        const auto limit = pb.NewDataLiteral<ui64>(7ULL);
        const auto pgmReturn = pb.Filter(stream, limit,
            [&](TRuntimeNode item) {
            return pb.AggrEquals(pb.CountBits(item), pb.NewDataLiteral<ui64>(3ULL));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 7ULL);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 11ULL);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 13ULL);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 14ULL);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 19ULL);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 21ULL);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 22ULL);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestFilterWithLimitOverFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto type = pb.NewFlowType(pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<ui32>::Id)));
        const auto flow = pb.ChainMap(pb.SourceOf(type), pb.NewDataLiteral<ui64>(0ULL),
            [&](TRuntimeNode, TRuntimeNode state) -> TRuntimeNodePair {
                return {state, pb.Increment(state)};
            }
        );

        const auto limit = pb.NewDataLiteral<ui64>(7ULL);
        const auto pgmReturn = pb.FromFlow(pb.Filter(flow, limit,
            [&](TRuntimeNode item) {
            return pb.AggrEquals(pb.CountBits(item), pb.NewDataLiteral<ui64>(3ULL));
        }));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 7ULL);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 11ULL);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 13ULL);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 14ULL);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 19ULL);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 21ULL);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 22ULL);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }
}

}
}
