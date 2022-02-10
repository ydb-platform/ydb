#include "mkql_computation_node_ut.h"
#include <ydb/library/yql/minikql/mkql_runtime_version.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLFlatMapTest) {
    Y_UNIT_TEST_LLVM(TestOverListAndPartialLists) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(1);
        const auto data2 = pb.NewDataLiteral<ui32>(2);
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto list = pb.NewList(dataType, {data1, data2});
        const auto pgmReturn = pb.FlatMap(list,
            [&](TRuntimeNode item) {
            return pb.NewList(dataType, {pb.Add(item, data1), pb.Mul(item, data2)});
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 4);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestOverListAndStreams) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<i8>(3);
        const auto data2 = pb.NewDataLiteral<i8>(-7);
        const auto dataType = pb.NewDataType(NUdf::TDataType<i8>::Id);
        const auto list = pb.NewList(dataType, {data1, data2});
        const auto pgmReturn = pb.FlatMap(list,
            [&](TRuntimeNode item) {
            return pb.Iterator(pb.NewList(dataType, {pb.Plus(item), pb.Minus(item)}), {});
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i8>(), 3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i8>(), -3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i8>(), -7);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i8>(), 7);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestOverStreamAndPartialLists) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui16>(10);
        const auto data2 = pb.NewDataLiteral<ui16>(20);
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui16>::Id);
        const auto list = pb.NewList(dataType, {data1, data2});
        const auto pgmReturn = pb.FlatMap(pb.Iterator(list, {}),
            [&](TRuntimeNode item) {
            return pb.NewList(dataType, {pb.Sub(item, data1), pb.Unwrap(pb.Div(item, data2), pb.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0)});
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 0);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 0);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 10);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestOverFlowAndPartialLists) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui16>(10);
        const auto data2 = pb.NewDataLiteral<ui16>(20);
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui16>::Id);
        const auto list = pb.NewList(dataType, {data1, data2});
        const auto pgmReturn = pb.FromFlow(pb.FlatMap(pb.ToFlow(pb.Iterator(list, {})),
            [&](TRuntimeNode item) {
            return pb.NewList(dataType, {pb.Sub(item, data1), pb.Unwrap(pb.Div(item, data2), pb.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0)});
        }));

        const auto& graph = setup.BuildGraph(pgmReturn);
        const NUdf::TUnboxedValue& iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 0);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 0);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 10);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestOverStreamAndStreams) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data = pb.NewDataLiteral<i32>(-100);
        const auto data0 = pb.NewDataLiteral<i32>(0);
        const auto data1 = pb.NewDataLiteral<i32>(3);
        const auto data2 = pb.NewDataLiteral<i32>(7);
        const auto dataType = pb.NewDataType(NUdf::TDataType<i32>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2});
        const auto pgmReturn = pb.FlatMap(pb.Iterator(list, {}),
            [&](TRuntimeNode item) {
            return pb.Iterator(pb.NewList(pb.NewOptionalType(dataType),
                {pb.Mod(data, item), pb.Div(data, item)}), {});
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -1);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -33);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -2);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -14);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestOverFlowAndStreams) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data = pb.NewDataLiteral<i32>(-100);
        const auto data0 = pb.NewDataLiteral<i32>(0);
        const auto data1 = pb.NewDataLiteral<i32>(3);
        const auto data2 = pb.NewDataLiteral<i32>(7);
        const auto dataType = pb.NewDataType(NUdf::TDataType<i32>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2});
        const auto pgmReturn = pb.FromFlow(pb.FlatMap(pb.ToFlow(list),
            [&](TRuntimeNode item) {
            return pb.Iterator(pb.NewList(pb.NewOptionalType(dataType),
                {pb.Mod(data, item), pb.Div(data, item)}), {});
        }));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -1);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -33);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -2);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -14);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestOverFlowAndFlows) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data = pb.NewDataLiteral<i32>(-100);
        const auto data0 = pb.NewDataLiteral<i32>(0);
        const auto data1 = pb.NewDataLiteral<i32>(3);
        const auto data2 = pb.NewDataLiteral<i32>(7);
        const auto dataType = pb.NewDataType(NUdf::TDataType<i32>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2});
        const auto pgmReturn = pb.FromFlow(pb.FlatMap(pb.ToFlow(list),
            [&](TRuntimeNode item) {
            return pb.ToFlow(pb.NewList(pb.NewOptionalType(dataType),
                {pb.Mod(data, item), pb.Div(data, item)}));
        }));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -1);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -33);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -2);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -14);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestOverListAndFlows) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data = pb.NewDataLiteral<i32>(-100);
        const auto data0 = pb.NewDataLiteral<i32>(0);
        const auto data1 = pb.NewDataLiteral<i32>(3);
        const auto data2 = pb.NewDataLiteral<i32>(7);
        const auto dataType = pb.NewDataType(NUdf::TDataType<i32>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2});
        const auto pgmReturn = pb.FromFlow(pb.FlatMap(list,
            [&](TRuntimeNode item) {
            return pb.ToFlow(pb.NewList(pb.NewOptionalType(dataType),
                {pb.Mod(data, item), pb.Div(data, item)}));
        }));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -1);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -33);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -2);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -14);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestOverFlowAndIndependentFlows) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data = pb.NewDataLiteral<i32>(-100);
        const auto data0 = pb.NewDataLiteral<i32>(0);
        const auto data1 = pb.NewDataLiteral<i32>(3);
        const auto data2 = pb.NewDataLiteral<i32>(7);
        const auto dataType = pb.NewDataType(NUdf::TDataType<i32>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2});
        const auto pgmReturn = pb.FromFlow(pb.FlatMap(pb.ToFlow(list),
            [&](TRuntimeNode) {
            return pb.Map(pb.ToFlow(pb.NewList(dataType, {data, data})), [&](TRuntimeNode it) { return pb.Abs(it); });
        }));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 100);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 100);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 100);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 100);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 100);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 100);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestOverListAndIndependentFlows) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data = pb.NewDataLiteral<i32>(-100);
        const auto data0 = pb.NewDataLiteral<i32>(0);
        const auto data1 = pb.NewDataLiteral<i32>(3);
        const auto data2 = pb.NewDataLiteral<i32>(7);
        const auto dataType = pb.NewDataType(NUdf::TDataType<i32>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2});
        const auto pgmReturn = pb.FromFlow(pb.FlatMap(list,
            [&](TRuntimeNode) {
            return pb.Map(pb.ToFlow(pb.NewList(dataType, {data, data})), [&](TRuntimeNode it) { return pb.Minus(it); });
        }));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 100);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 100);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 100);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 100);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 100);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 100);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestOverFlowAndPartialOptionals) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data = pb.NewDataLiteral<i64>(-100);
        const auto data0 = pb.NewDataLiteral<i64>(0);
        const auto data1 = pb.NewDataLiteral<i64>(3);
        const auto data2 = pb.NewDataLiteral<i64>(7);
        const auto dataType = pb.NewDataType(NUdf::TDataType<i64>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2});
        const auto pgmReturn = pb.FromFlow(pb.FlatMap(pb.ToFlow(pb.Iterator(list, {})),
            [&](TRuntimeNode item) {
            return pb.Div(data, item);
        }));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i64>(), -33);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i64>(), -14);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestOverStreamAndPartialOptionals) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data = pb.NewDataLiteral<i64>(-100);
        const auto data0 = pb.NewDataLiteral<i64>(0);
        const auto data1 = pb.NewDataLiteral<i64>(3);
        const auto data2 = pb.NewDataLiteral<i64>(7);
        const auto dataType = pb.NewDataType(NUdf::TDataType<i64>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2});
        const auto pgmReturn = pb.FlatMap(pb.Iterator(list, {}),
            [&](TRuntimeNode item) {
            return pb.Div(data, item);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i64>(), -33);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i64>(), -14);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestOverListAndPartialOptionals) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<ui32>(0);
        const auto data1 = pb.NewDataLiteral<ui32>(1);
        const auto data2 = pb.NewDataLiteral<ui32>(2);
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2});
        const auto pgmReturn = pb.FlatMap(list,
            [&](TRuntimeNode item) {
            return pb.Div(data2, item);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 1);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestOverListAndDoubleOptionals) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<ui32>(0);
        const auto data1 = pb.NewDataLiteral<ui32>(1);
        const auto data2 = pb.NewDataLiteral<ui32>(2);
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2});
        const auto pgmReturn = pb.FlatMap(list,
            [&](TRuntimeNode item) {
            return pb.NewOptional(pb.Div(data2, item));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 1);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestOverOptionalAndPartialOptionals) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data2 = pb.NewDataLiteral<ui32>(2);
        const auto list = pb.NewOptional(data2);
        const auto pgmReturn = pb.FlatMap(list,
            [&](TRuntimeNode item) {
            return pb.Div(item, data2);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto value = graph->GetValue();
        UNIT_ASSERT(value);
        UNIT_ASSERT_VALUES_EQUAL(value.template Get<ui32>(), 1);
    }

    Y_UNIT_TEST_LLVM(TestOverOptionalAndPartialLists) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(1);
        const auto data2 = pb.NewDataLiteral<ui32>(2);
        const auto list = pb.NewOptional(data2);
        const auto pgmReturn = pb.FlatMap(list,
            [&](TRuntimeNode item) {
            return pb.Append(pb.AsList(item), data1);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 1);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestOverListAndPartialListsLazy) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(1U);
        const auto data2 = pb.NewDataLiteral<ui32>(2U);
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto list = pb.NewList(dataType, {data1, data2});

        const auto pgmReturn = pb.FlatMap(pb.LazyList(list),
            [&](TRuntimeNode item) {
            return pb.NewList(dataType, {pb.Add(item, data1), pb.Mul(item, data2)});
        });


        const auto graph = setup.BuildGraph(pgmReturn);

        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 4);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestOverListAndPartialOptionalsLazy) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<ui32>(0U);
        const auto data2 = pb.NewDataLiteral<ui32>(2U);
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto list = pb.NewList(dataType, {data0, data2});

        const auto pgmReturn = pb.FlatMap(pb.LazyList(list),
            [&](TRuntimeNode item) { return pb.Div(data2, item); }
        );

        const auto graph = setup.BuildGraph(pgmReturn);

        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 1);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }
#if !defined(MKQL_RUNTIME_VERSION) || MKQL_RUNTIME_VERSION >= 18u
    Y_UNIT_TEST_LLVM(TestNarrowWithList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});

        const auto list = pb.NewList(tupleType, {data1, data2, data3});

        const auto pgmReturn = pb.Collect(pb.NarrowFlatMap(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U),pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.FlatMap(pb.NewList(dataType, items), [](TRuntimeNode item){ return item; }); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -3);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestNarrowWithFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});

        const auto list = pb.NewList(tupleType, {data1, data2, data3});

        const auto pgmReturn = pb.Collect(pb.NarrowFlatMap(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U),pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.FlatMap(pb.ToFlow(pb.NewList(dataType, items)), [](TRuntimeNode item){ return item; }); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -3);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestNarrowWithIndependentFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});

        const auto list = pb.NewList(tupleType, {data, data, data});

        const auto pgmReturn = pb.Collect(pb.NarrowFlatMap(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U),pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList) { return pb.Map(
                pb.ToFlow(pb.NewList(pb.NewDataType(NUdf::TDataType<float>::Id), {pb.NewDataLiteral<float>(+1.f), pb.NewDataLiteral<float>(-1.f)})),
                [&](TRuntimeNode item) { return pb.Minus(item); }); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), -1.f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), +1.f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), -1.f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), +1.f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), -1.f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), +1.f);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestThinNarrowWithList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto tupleType = pb.NewTupleType({});

        const auto data = pb.NewTuple(tupleType, {});
        const auto list = pb.NewList(tupleType, {data, data, data});

        const auto pgmReturn = pb.Collect(pb.NarrowFlatMap(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode) -> TRuntimeNode::TList { return {}; }),
            [&](TRuntimeNode::TList) -> TRuntimeNode { return pb.Replicate(pb.NewDataLiteral<i32>(7), pb.NewDataLiteral<ui64>(3), __FILE__, __LINE__, 0); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 7);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 7);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 7);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 7);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 7);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 7);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 7);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 7);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 7);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestOverFlowAndWideFlows) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data = pb.NewDataLiteral<i32>(-100);
        const auto data0 = pb.NewDataLiteral<i32>(0);
        const auto data1 = pb.NewDataLiteral<i32>(3);
        const auto data2 = pb.NewDataLiteral<i32>(7);
        const auto dataType = pb.NewDataType(NUdf::TDataType<i32>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2});
        const auto pgmReturn = pb.FromFlow(pb.NarrowMap(pb.FlatMap(pb.ToFlow(list),
            [&](TRuntimeNode item) {
            return pb.ExpandMap(pb.ToFlow(pb.NewList(pb.NewOptionalType(dataType),
                {pb.Mod(data, item), pb.Div(data, item)})),
                [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Plus(item), pb.Minus(item)}; });
            }),
            [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), +1);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -33);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), +33);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -2);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), +2);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -14);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), +14);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestOverListAndWideFlows) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data = pb.NewDataLiteral<i32>(-100);
        const auto data0 = pb.NewDataLiteral<i32>(0);
        const auto data1 = pb.NewDataLiteral<i32>(3);
        const auto data2 = pb.NewDataLiteral<i32>(7);
        const auto dataType = pb.NewDataType(NUdf::TDataType<i32>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2});
        const auto pgmReturn = pb.FromFlow(pb.NarrowMap(pb.FlatMap(list,
            [&](TRuntimeNode item) {
            return pb.ExpandMap(pb.ToFlow(pb.NewList(pb.NewOptionalType(dataType),
                {pb.Mod(data, item), pb.Div(data, item)})),
                [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Minus(item), pb.Plus(item)}; });
            }),
            [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), +1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -1);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), +33);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -33);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), +2);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -2);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), +14);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -14);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestOverFlowAndIndependentWideFlows) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data = pb.NewDataLiteral<i32>(-100);
        const auto data0 = pb.NewDataLiteral<i32>(0);
        const auto data1 = pb.NewDataLiteral<i32>(3);
        const auto data2 = pb.NewDataLiteral<i32>(7);
        const auto dataType = pb.NewDataType(NUdf::TDataType<i32>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2});
        const auto pgmReturn = pb.FromFlow(pb.NarrowMap(pb.FlatMap(pb.ToFlow(list),
            [&](TRuntimeNode) {
            return pb.ExpandMap(pb.ToFlow(pb.NewList(dataType, {data, data})),
                [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Plus(item), pb.Minus(item)}; });
            }),
            [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -100);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), +100);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -100);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), +100);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -100);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), +100);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -100);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), +100);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -100);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), +100);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -100);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), +100);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestOverListAndIndependentWideFlows) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data = pb.NewDataLiteral<i32>(-100);
        const auto data0 = pb.NewDataLiteral<i32>(0);
        const auto data1 = pb.NewDataLiteral<i32>(3);
        const auto data2 = pb.NewDataLiteral<i32>(7);
        const auto dataType = pb.NewDataType(NUdf::TDataType<i32>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2});
        const auto pgmReturn = pb.FromFlow(pb.NarrowMap(pb.FlatMap(list,
            [&](TRuntimeNode) {
            return pb.ExpandMap(pb.ToFlow(pb.NewList(dataType, {data, data})),
                [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Minus(item), pb.Plus(item)}; });
            }),
            [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), +100);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -100);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), +100);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -100);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), +100);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -100);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), +100);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -100);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), +100);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -100);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), +100);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -100);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }
#endif
}

}
}
