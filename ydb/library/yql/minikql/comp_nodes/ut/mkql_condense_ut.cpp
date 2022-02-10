#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLCondenseNodeTest) {
    Y_UNIT_TEST_LLVM(TestSqueeze) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<double>::Id));
        const auto data1 = pb.NewOptional(pb.NewDataLiteral<double>(3.8));
        const auto data2 = pb.NewOptional(pb.NewDataLiteral<double>(-53.2));
        const auto data3 = pb.NewOptional(pb.NewDataLiteral<double>(233.8));
        const auto data4 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<double>::Id);
        const auto data0 = pb.NewOptional(pb.NewDataLiteral<double>(HUGE_VAL));
        const auto list = pb.NewList(dataType, {data4, data3, data2, data1});

        const auto pgmReturn = pb.Squeeze(pb.Iterator(list, {}), data0,
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.AggrMin(item, state);
            }
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), -53.2);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestSqueezeOnEmpty) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<double>::Id));
        const auto data0 = pb.NewOptional(pb.NewDataLiteral<double>(HUGE_VAL));
        const auto list = pb.NewEmptyList(dataType);

        const auto pgmReturn = pb.Squeeze(pb.Iterator(list, {}), data0,
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.AggrMin(item, state);
            }
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), HUGE_VAL);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestSqueeze1OverEmpty) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<i32>::Id);
        const auto list = pb.NewEmptyList(dataType);
        const auto pgmReturn = pb.Squeeze1(pb.Iterator(list, {}),
            [&](TRuntimeNode item) {
                return pb.Minus(item);
            },
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.Mul(item, state);
            }
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestSqueeze1OverSingle) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<i32>::Id);
        const auto data1 = pb.NewDataLiteral<i32>(1);
        const auto list = pb.NewList(dataType, {data1});
        const auto pgmReturn = pb.Squeeze1(pb.Iterator(list, {}),
            [&](TRuntimeNode item) {
                return pb.Minus(item);
            },
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.Mul(item, state);
            }
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -1);

        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestSqueeze1OverMany) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<i32>::Id);
        const auto data1 = pb.NewDataLiteral<i32>(1);
        const auto data2 = pb.NewDataLiteral<i32>(2);
        const auto data3 = pb.NewDataLiteral<i32>(7);
        const auto list = pb.NewList(dataType, {data1, data2, data3});
        const auto pgmReturn = pb.Squeeze1(pb.Iterator(list, {}),
            [&](TRuntimeNode item) {
                return pb.Minus(item);
            },
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.Mul(item, state);
            }
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -14);

        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestCondense) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<double>::Id));
        const auto data0 = pb.NewOptional(pb.NewDataLiteral<double>(0.0));
        const auto data1 = pb.NewOptional(pb.NewDataLiteral<double>(3.8));
        const auto data2 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<double>::Id);
        const auto data3 = pb.NewOptional(pb.NewDataLiteral<double>(-53.2));
        const auto data4 = pb.NewOptional(pb.NewDataLiteral<double>(233.8));
        const auto data5 = pb.NewOptional(pb.NewDataLiteral<double>(3.14));
        const auto data6 = pb.NewOptional(pb.NewDataLiteral<double>(-73.12));
        const auto data7 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<double>::Id);
        const auto data8 = pb.NewOptional(pb.NewDataLiteral<double>(233.8));
        const auto data9 = pb.NewOptional(pb.NewDataLiteral<double>(1221.8));
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.Condense(pb.Iterator(list, {}), data0,
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.Or({pb.Not(pb.Exists(item)), pb.Less(state, data0)});
            },
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.AggrAdd(item, state);
            }
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 3.8);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), -53.2);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 163.82);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 1455.6);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestCondense1) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<i32>::Id);
        const auto data0 = pb.NewDataLiteral<i32>(-1);
        const auto data1 = pb.NewDataLiteral<i32>(2);
        const auto data2 = pb.NewDataLiteral<i32>(0);
        const auto data3 = pb.NewDataLiteral<i32>(7);
        const auto data4 = pb.NewDataLiteral<i32>(5);
        const auto data5 = pb.NewDataLiteral<i32>(-7);
        const auto data6 = pb.NewDataLiteral<i32>(-6);
        const auto data7 = pb.NewDataLiteral<i32>(4);
        const auto data8 = pb.NewDataLiteral<i32>(8);
        const auto data9 = pb.NewDataLiteral<i32>(9);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4, data5, data6, data7, data8, data9});
        const auto pgmReturn = pb.Condense1(pb.Iterator(list, {}),
            [&](TRuntimeNode item) {
                return pb.Minus(item);
            },
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.LessOrEqual(item, state);
            },
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.Mul(item, state);
            }
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 0);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 7);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 6);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -288);

        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestCondenseOverList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<double>::Id));
        const auto data0 = pb.NewOptional(pb.NewDataLiteral<double>(0.0));
        const auto data1 = pb.NewOptional(pb.NewDataLiteral<double>(3.8));
        const auto data2 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<double>::Id);
        const auto data3 = pb.NewOptional(pb.NewDataLiteral<double>(-53.2));
        const auto data4 = pb.NewOptional(pb.NewDataLiteral<double>(233.8));
        const auto data5 = pb.NewOptional(pb.NewDataLiteral<double>(3.14));
        const auto data6 = pb.NewOptional(pb.NewDataLiteral<double>(-73.12));
        const auto data7 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<double>::Id);
        const auto data8 = pb.NewOptional(pb.NewDataLiteral<double>(233.8));
        const auto data9 = pb.NewOptional(pb.NewDataLiteral<double>(1221.8));
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.Condense(list, data0,
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.Or({pb.Not(pb.Exists(item)), pb.Less(state, data0)});
            },
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.AggrAdd(item, state);
            }
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 3.8);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), -53.2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 163.82);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 1455.6);

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestCondense1OverList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<i32>::Id);
        const auto data0 = pb.NewDataLiteral<i32>(-1);
        const auto data1 = pb.NewDataLiteral<i32>(2);
        const auto data2 = pb.NewDataLiteral<i32>(0);
        const auto data3 = pb.NewDataLiteral<i32>(7);
        const auto data4 = pb.NewDataLiteral<i32>(5);
        const auto data5 = pb.NewDataLiteral<i32>(-7);
        const auto data6 = pb.NewDataLiteral<i32>(-6);
        const auto data7 = pb.NewDataLiteral<i32>(4);
        const auto data8 = pb.NewDataLiteral<i32>(8);
        const auto data9 = pb.NewDataLiteral<i32>(9);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4, data5, data6, data7, data8, data9});
        const auto pgmReturn = pb.Condense1(list,
            [&](TRuntimeNode item) {
                return pb.Minus(item);
            },
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.LessOrEqual(item, state);
            },
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.Mul(item, state);
            }
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 7);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 6);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -288);

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestCondenseInterrupt) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<bool>::Id));
        const auto data0 = pb.NewOptional(pb.NewDataLiteral<bool>(false));
        const auto data1 = pb.NewOptional(pb.NewDataLiteral<bool>(false));
        const auto data2 = pb.NewOptional(pb.NewDataLiteral<bool>(false));
        const auto data3 = pb.NewOptional(pb.NewDataLiteral<bool>(false));
        const auto data4 = pb.NewOptional(pb.NewDataLiteral<bool>(true));
        const auto data5 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<bool>::Id);
        const auto data6 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<bool>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4, data5, data6});

        const auto pgmReturn = pb.FromFlow(pb.Condense(pb.ToFlow(list), pb.NewDataLiteral<bool>(false),
            [&](TRuntimeNode, TRuntimeNode state) {
                return pb.If(state, pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<bool>::Id), pb.NewDataLiteral<bool>(false));
            },
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.Or({pb.Unwrap(item, pb.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0), state});
            }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(item.template Get<bool>());
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestCondense1Interrupt) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<bool>::Id));
        const auto data0 = pb.NewOptional(pb.NewDataLiteral<bool>(true));
        const auto data1 = pb.NewOptional(pb.NewDataLiteral<bool>(true));
        const auto data2 = pb.NewOptional(pb.NewDataLiteral<bool>(true));
        const auto data3 = pb.NewOptional(pb.NewDataLiteral<bool>(true));
        const auto data4 = pb.NewOptional(pb.NewDataLiteral<bool>(false));
        const auto data5 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<bool>::Id);
        const auto data6 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<bool>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4, data5, data6});

        const auto pgmReturn = pb.FromFlow(pb.Condense1(pb.ToFlow(list),
            [&](TRuntimeNode item) {
                return pb.Unwrap(item, pb.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0);
            },
            [&](TRuntimeNode, TRuntimeNode state) {
                return pb.If(state, pb.NewDataLiteral<bool>(false), pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<bool>::Id));
            },
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.And({pb.Unwrap(item, pb.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0), state});
            }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(!item.template Get<bool>());
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestCondenseInterruptEndlessStream) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto pgmReturn = pb.Condense(pb.SourceOf(pb.NewStreamType(pb.NewNull().GetStaticType())), pb.NewDataLiteral<ui32>(0U),
            [&](TRuntimeNode, TRuntimeNode state) {
                return pb.If(pb.AggrLess(state, pb.NewDataLiteral<ui32>(123456U)), pb.NewDataLiteral<bool>(false), pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<bool>::Id));
            },
            [&](TRuntimeNode, TRuntimeNode state) {
                return pb.AggrAdd(pb.NewDataLiteral<ui32>(1U), state);
            }
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 123456U);

        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestCondense1InterruptEndlessFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto pgmReturn = pb.FromFlow(pb.Condense1(pb.SourceOf(pb.NewFlowType(pb.NewNull().GetStaticType())),
            [&](TRuntimeNode) {
                return pb.NewDataLiteral<ui32>(0U);
            },
            [&](TRuntimeNode, TRuntimeNode state) {
                return pb.If(pb.AggrLess(state, pb.NewDataLiteral<ui32>(123456U)), pb.NewDataLiteral<bool>(false), pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<bool>::Id));
            },
            [&](TRuntimeNode, TRuntimeNode state) {
                return pb.AggrAdd(pb.NewDataLiteral<ui32>(1U), state);
            }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 123456U);

        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }


    Y_UNIT_TEST_LLVM(TestCondenseListeralListInMap) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<NUdf::EDataSlot::String>("other");
        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("foo");
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("bar");
        const auto type = pb.NewDataType(NUdf::EDataSlot::String);
        const auto list2 = pb.NewList(type, {data1, data2});
        const auto list0 = pb.NewList(type, {data1, data2, data0});

        const auto pgmReturn = pb.Map(list0,
            [&](TRuntimeNode item) {
            return pb.Head(pb.Condense(list2,
                pb.NewDataLiteral(false),
                [&](TRuntimeNode, TRuntimeNode) { return pb.NewDataLiteral(false); },
                [&](TRuntimeNode it, TRuntimeNode state) { return pb.Or({state, pb.AggrEquals(item, it)}); }
            ));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.Get<bool>());
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.Get<bool>());
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.Get<bool>());
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestCondense1ListeralListInMap) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<NUdf::EDataSlot::String>("other");
        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("foo");
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("bar");
        const auto type = pb.NewDataType(NUdf::EDataSlot::String);
        const auto list2 = pb.NewList(type, {data1, data2});
        const auto list0 = pb.NewList(type, {data1, data2, data0});

        const auto pgmReturn = pb.Map(list0,
            [&](TRuntimeNode item) {
            return pb.Head(pb.Condense1(list2,
                [&](TRuntimeNode it) { return pb.AggrEquals(item, it); },
                [&](TRuntimeNode, TRuntimeNode) { return pb.NewDataLiteral(false); },
                [&](TRuntimeNode it, TRuntimeNode state) { return pb.Or({state, pb.AggrEquals(item, it)}); }
            ));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.Get<bool>());
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.Get<bool>());
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.Get<bool>());
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }
}

}
}
