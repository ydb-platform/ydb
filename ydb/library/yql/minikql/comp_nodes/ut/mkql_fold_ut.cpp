#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <random>
#include <ctime>
#include <algorithm>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLFoldNodeTest) {
    Y_UNIT_TEST_LLVM(TestFoldOverList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto data1 = pb.NewDataLiteral<ui32>(1);
        auto data2 = pb.NewDataLiteral<ui32>(2);
        auto data3 = pb.NewDataLiteral<ui32>(3);
        auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        auto list = pb.NewList(dataType, {data1, data2, data3});
        auto pgmReturn = pb.Fold(list, pb.NewDataLiteral<ui32>(0),
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.Add(item, state);
            });

        auto graph = setup.BuildGraph(pgmReturn);
        auto res = graph->GetValue().template Get<ui32>();
        UNIT_ASSERT_VALUES_EQUAL(res, 6);
    }

    Y_UNIT_TEST_LLVM(TestFold1OverEmptyList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        auto list = pb.NewEmptyList(dataType);
        auto data2 = pb.NewDataLiteral<ui32>(2);
        auto pgmReturn = pb.Fold1(list, [&](TRuntimeNode item) {
                return pb.Mul(item, data2);
            },
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.Add(item, state);
            });

        auto graph = setup.BuildGraph(pgmReturn);
        auto value = graph->GetValue();
        UNIT_ASSERT(!value);
    }

    Y_UNIT_TEST_LLVM(TestFold1OverSingleElementList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        auto data1 = pb.NewDataLiteral<ui32>(1);
        auto data2 = pb.NewDataLiteral<ui32>(2);
        auto list = pb.NewList(dataType, {data1});
        auto pgmReturn = pb.Fold1(list,
            [&](TRuntimeNode item) {
                return pb.Mul(item, data2);
            },
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.Add(item, state);
            });

        auto graph = setup.BuildGraph(pgmReturn);
        auto value = graph->GetValue();
        UNIT_ASSERT(value);
        UNIT_ASSERT_VALUES_EQUAL(value.template Get<ui32>(), 2);
    }

    Y_UNIT_TEST_LLVM(TestFold1OverManyElementList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        auto data1 = pb.NewDataLiteral<ui32>(1);
        auto data2 = pb.NewDataLiteral<ui32>(2);
        auto list = pb.NewList(dataType, {data1, data2});
        auto pgmReturn = pb.Fold1(list,
            [&](TRuntimeNode item) {
                return pb.Mul(item, data2);
            },
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.Add(item, state);
            });

        auto graph = setup.BuildGraph(pgmReturn);
        auto value = graph->GetValue();
        UNIT_ASSERT(value);
        UNIT_ASSERT_VALUES_EQUAL(value.template Get<ui32>(), 4);
    }

    Y_UNIT_TEST_LLVM(TestFoldWithAggrAdd) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<float>::Id));
        auto data1 = pb.NewOptional(pb.NewDataLiteral<float>(1));
        auto data2 = pb.NewOptional(pb.NewDataLiteral<float>(2));
        auto data3 = pb.NewOptional(pb.NewDataLiteral<float>(3));
        auto data4 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<float>::Id);
        auto data0 = pb.NewOptional(pb.NewDataLiteral<float>(42));
        auto list = pb.NewList(dataType, {data4, data3, data2, data1});

        auto pgmReturn = pb.Fold(list, data0,
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.AggrAdd(pb.Increment(item), pb.Decrement(state));
            });

        auto graph = setup.BuildGraph(pgmReturn);
        auto res = graph->GetValue().template Get<float>();
        UNIT_ASSERT_VALUES_EQUAL(res, 47);
    }

    Y_UNIT_TEST_LLVM(TestNestedApply) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto data1 = pb.NewDataLiteral<i32>(1);
        auto data2 = pb.NewDataLiteral<i32>(2);
        auto data3 = pb.NewDataLiteral<i32>(3);
        auto dataType = pb.NewDataType(NUdf::TDataType<i32>::Id);
        auto list = pb.NewList(dataType, {data1, data2, data3});

        const auto callType = TCallableTypeBuilder(pb.GetTypeEnvironment(), "TEST", dataType).Add(dataType).Add(dataType).Build();

        auto pgmReturn = pb.Fold(list, pb.NewDataLiteral<i32>(100),
            [&](TRuntimeNode item, TRuntimeNode state) {
            return pb.Apply(pb.Callable(callType,
                [&](const TArrayRef<const TRuntimeNode>& args) {
                    return pb.Sub(args[1], args[0]);
                }), {item, state});
        });

        auto graph = setup.BuildGraph(pgmReturn);
        auto res = graph->GetValue().template Get<i32>();
        UNIT_ASSERT_VALUES_EQUAL(res, 94);
    }

    Y_UNIT_TEST_LLVM(TestLogicalOpts) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto truth = pb.NewDataLiteral(true);
        auto falsehood = pb.NewDataLiteral(false);
        auto type = pb.NewDataType(NUdf::TDataType<bool>::Id);
        auto args = pb.NewList(type, {truth, falsehood});

        auto optTruth = pb.NewOptional(truth);
        auto optFalsehood = pb.NewOptional(falsehood);
        auto empty = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<bool>::Id);
        auto optType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<bool>::Id));
        auto opts = pb.NewList(optType, {empty, optTruth, optFalsehood});

        auto pgmReturn = pb.Fold(opts, pb.NewEmptyList(optType),
            [&](TRuntimeNode item, TRuntimeNode state) {
                const auto append = pb.Append(state, pb.Not(item));

                const auto one = pb.Fold(args, pb.NewEmptyList(optType),
                [&](TRuntimeNode item2, TRuntimeNode state2) {
                    state2 = pb.Append(state2, pb.And({item, item2}));
                    state2 = pb.Append(state2, pb.And({item2, item}));
                    state2 = pb.Append(state2, pb.Or({item, item2}));
                    state2 = pb.Append(state2, pb.Or({item2, item}));
                    state2 = pb.Append(state2, pb.Xor({item, item2}));
                    state2 = pb.Append(state2, pb.Xor({item2, item}));
                    return state2;
                });

                const auto two = pb.Fold(opts, pb.NewEmptyList(optType),
                [&](TRuntimeNode item2, TRuntimeNode state2) {
                    state2 = pb.Append(state2, pb.And({item, item2}));
                    state2 = pb.Append(state2, pb.Or({item, item2}));
                    state2 = pb.Append(state2, pb.Xor({item, item2}));
                    return state2;
                });

            return pb.Extend({append, one, two});
        });

        auto graph = setup.BuildGraph(pgmReturn);

        auto res = graph->GetValue();
        UNIT_ASSERT_VALUES_EQUAL(res.GetListLength(), 66ULL);
        auto iterator = res.GetListIterator();

        NUdf::TUnboxedValue item;

        /// empty
        // not
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        // and
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        // or
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);

        // xor
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        // and
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);

        // or
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        // xor
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        // and
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        // or
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        // xor
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        // and
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        // or
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);

        // xor
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        // and
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);

        // or
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        // xor
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);


        /// true
        // not
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);

        // and
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);

        // or
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);

        // xor
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);

        // and
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);

        // or
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);

        // xor
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);

        // and
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        // or
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);

        // xor
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        // and
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);

        // or
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);

        // xor
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);

        // and
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);

        // or
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);

        // xor
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);

        /// false
        // not
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);

        // and
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);

        // or
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);

        // xor
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);

        // and
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);

        // or
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);

        // xor
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);

        // and
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);

        // or
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        // xor
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        // and
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);

        // or
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);

        // xor
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);

        // and
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);

        // or
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);

        // xor
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFoldWithListInState) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto data1 = pb.NewDataLiteral<ui32>(1);
        auto data2 = pb.NewDataLiteral<ui32>(2);
        auto data3 = pb.NewDataLiteral<ui32>(3);
        auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        auto list = pb.NewList(dataType, {data1, data2, data3});
        auto optType = pb.NewOptionalType(dataType);
        auto empty = pb.AddMember(pb.AddMember(
            pb.NewEmptyStruct(), "Max", pb.NewEmptyOptional(optType)),
            "List", pb.NewEmptyList(dataType));

        auto pgmReturn = pb.Fold(list, empty,
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Max",
                    pb.IfPresent({pb.Member(state, "Max")},
                    [&](TRuntimeNode::TList oldMax) {
                        return pb.NewOptional(pb.Max(oldMax.front(), item));
                    }, pb.NewOptional(item))),
                    "List", pb.Append(pb.Member(state, "List"), item)
                );
            });

        auto graph = setup.BuildGraph(pgmReturn);
        auto iterator = graph->GetValue().GetElement(0).GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 3);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));

        UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetElement(1).template Get<ui32>(), 3);
    }

    Y_UNIT_TEST_LLVM(TestManyAppend) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto zeroList = pb.NewEmptyList(pb.NewDataType(NUdf::TDataType<ui32>::Id));
        zeroList = pb.Append(zeroList, pb.NewDataLiteral<ui32>(0));
        const ui32 n = 13;
        for (ui32 i = 0; i < n; ++i)
            zeroList = pb.Extend({zeroList, zeroList});

        auto state = pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Counter",
            pb.NewDataLiteral<ui32>(0)), "NewList",
            pb.NewEmptyList(pb.NewDataType(NUdf::TDataType<ui32>::Id)));

        auto fold = pb.Fold(zeroList, state,
            [&](TRuntimeNode item, TRuntimeNode state) {
                Y_UNUSED(item);
        auto oldList = pb.Member(state, "NewList");
        auto oldCounter = pb.Member(state, "Counter");
                return pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Counter",
                    pb.Add(oldCounter, pb.NewDataLiteral<ui32>(1))),
                    "NewList", pb.Append(oldList, oldCounter));
            });

        auto pgmReturn = pb.Member(fold, "NewList");

        auto graph = setup.BuildGraph(pgmReturn);
        UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetListLength(), 1 << n);

        auto iterator = graph->GetValue().GetListIterator();
        ui32 i = 0;
        for (NUdf::TUnboxedValue item; iterator.Next(item); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(i, item.template Get<ui32>());
        }
        UNIT_ASSERT(!iterator.Skip());
        UNIT_ASSERT_VALUES_EQUAL(i, 1 << n);
    }

    Y_UNIT_TEST_LLVM(TestManyPrepend) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto zeroList = pb.NewEmptyList(pb.NewDataType(NUdf::TDataType<ui32>::Id));
        zeroList = pb.Append(zeroList, pb.NewDataLiteral<ui32>(0));
        const ui32 n = 13;
        for (ui32 i = 0; i < n; ++i)
            zeroList = pb.Extend({zeroList, zeroList});

        auto state = pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Counter",
            pb.NewDataLiteral<ui32>(0)), "NewList",
            pb.NewEmptyList(pb.NewDataType(NUdf::TDataType<ui32>::Id)));

        auto fold = pb.Fold(zeroList, state,
            [&](TRuntimeNode item, TRuntimeNode state) {
            Y_UNUSED(item);
            auto oldList = pb.Member(state, "NewList");
            auto oldCounter = pb.Member(state, "Counter");
            return pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Counter",
                pb.Add(oldCounter, pb.NewDataLiteral<ui32>(1))),
                "NewList", pb.Prepend(oldCounter, oldList));
        });

        auto pgmReturn = pb.Member(fold, "NewList");

        auto graph = setup.BuildGraph(pgmReturn);
        UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetListLength(), 1 << n);

        auto iterator = graph->GetValue().GetListIterator();
        ui32 i = 1 << n;
        for (NUdf::TUnboxedValue item; iterator.Next(item);) {
            UNIT_ASSERT_VALUES_EQUAL(--i, item.template Get<ui32>());
        }
        UNIT_ASSERT(!iterator.Skip());
        UNIT_ASSERT_VALUES_EQUAL(i, 0);
    }

    Y_UNIT_TEST_LLVM(TestManyExtend) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto zeroList = pb.NewEmptyList(pb.NewDataType(NUdf::TDataType<ui32>::Id));
        zeroList = pb.Append(zeroList, pb.NewDataLiteral<ui32>(0));
        const ui32 n = 13;
        for (ui32 i = 0; i < n; ++i)
            zeroList = pb.Extend({zeroList, zeroList});

        auto state = pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Counter",
            pb.NewDataLiteral<ui32>(0)), "NewList",
            pb.NewEmptyList(pb.NewDataType(NUdf::TDataType<ui32>::Id)));

        auto fold = pb.Fold(zeroList, state,
            [&](TRuntimeNode item, TRuntimeNode state) {
            Y_UNUSED(item);
            auto oldList = pb.Member(state, "NewList");
            auto oldCounter = pb.Member(state, "Counter");
            auto oldCounterMul2 = pb.Mul(oldCounter, pb.NewDataLiteral<ui32>(2));
            auto extList = pb.NewEmptyList(pb.NewDataType(NUdf::TDataType<ui32>::Id));
            extList = pb.Append(extList, oldCounterMul2);
            extList = pb.Append(extList, pb.Increment(oldCounterMul2));
            return pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Counter",
                pb.Add(oldCounter, pb.NewDataLiteral<ui32>(1))),
                "NewList", pb.Extend({oldList, extList}));
        });

        auto pgmReturn = pb.Member(fold, "NewList");

        auto graph = setup.BuildGraph(pgmReturn);
        UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetListLength(), 1 << (n+1));

        auto iterator = graph->GetValue().GetListIterator();
        ui32 i = 0;
        for (NUdf::TUnboxedValue item; iterator.Next(item); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(i, item.template Get<ui32>());
        }
        UNIT_ASSERT(!iterator.Skip());
        UNIT_ASSERT_VALUES_EQUAL(i, 1 << (n + 1));
    }

    Y_UNIT_TEST_LLVM(TestFoldSingular) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto data1 = pb.NewDataLiteral<ui32>(1);
        auto data2 = pb.NewDataLiteral<ui32>(2);
        auto data3 = pb.NewDataLiteral<ui32>(3);
        auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        auto list = pb.NewList(dataType, {data1, data2, data3});
        auto fold1 = pb.Fold(list, pb.NewDataLiteral<ui32>(0),
            [&](TRuntimeNode item, TRuntimeNode state) {
            Y_UNUSED(state);
            return item;
        });

        auto fold2 = pb.Fold(list, pb.NewDataLiteral<ui32>(0),
            [&](TRuntimeNode item, TRuntimeNode state) {
            Y_UNUSED(item);
            return state;
        });

        auto pgmReturn = pb.NewList(dataType, {fold1, fold2});

        auto graph = setup.BuildGraph(pgmReturn);
        auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestSumListSizes) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto itemType = pb.NewDataType(NUdf::TDataType<float>::Id);
        auto item = pb.NewDataLiteral<float>(0.f);

        auto listType = pb.NewListType(itemType);

        auto data0 = pb.NewEmptyList(itemType);
        auto data1 = pb.NewList(itemType, {item});
        auto data2 = pb.NewList(itemType, {item, item, item});
        auto data3 = pb.NewList(itemType, {item, item, item, item, item});

        auto list = pb.NewList(listType, {data0, data1, data2, data3});

        auto pgmReturn = pb.Fold1(list,
            [&](TRuntimeNode item) { return pb.Length(item); },
            [&](TRuntimeNode item, TRuntimeNode state) { return pb.AggrAdd(state, pb.Length(item)); }
        );

        auto graph = setup.BuildGraph(pgmReturn);
        UNIT_ASSERT_VALUES_EQUAL(9ULL, graph->GetValue().template Get<ui64>());
    }

    Y_UNIT_TEST_LLVM(TestHasListsItems) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto itemType = pb.NewDataType(NUdf::TDataType<float>::Id);
        auto item = pb.NewDataLiteral<float>(0.f);

        auto listType = pb.NewListType(itemType);

        auto data0 = pb.NewEmptyList(itemType);
        auto data1 = pb.NewList(itemType, {item});
        auto data2 = pb.NewEmptyList(itemType);

        auto list = pb.NewList(listType, {data0, data1, data2});

        auto pgmReturn = pb.Fold(list, pb.NewOptional(pb.NewDataLiteral<bool>(false)),
            [&](TRuntimeNode item, TRuntimeNode state) { return pb.Or({state, pb.HasItems(item)}); }
        );

        auto graph = setup.BuildGraph(pgmReturn);
        UNIT_ASSERT(graph->GetValue().template Get<bool>());
    }

    Y_UNIT_TEST_LLVM(TestConcat) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto data0 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
        auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("aa");
        auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("bbb");
        auto data3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("zzzz");
        auto type = pb.NewDataType(NUdf::EDataSlot::String);
        auto list = pb.NewList(type, {data1, data2, data3});
        auto pgmReturn = pb.Fold(list, data0,
        [&](TRuntimeNode item, TRuntimeNode state) {
            return pb.Concat(state, item);
        });

        auto graph = setup.BuildGraph(pgmReturn);
        auto res = graph->GetValue();
        UNBOXED_VALUE_STR_EQUAL(res, "Xaabbbzzzz");
    }

    Y_UNIT_TEST_LLVM(TestConcatOpt) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto data0 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>(""));
        auto data1 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("very large string"));
        auto data2 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>(" + "));
        auto data3 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("small"));
        auto type = pb.NewOptionalType(pb.NewDataType(NUdf::EDataSlot::String));
        auto list = pb.NewList(type, {data1, data2, data3, data0});
        auto pgmReturn = pb.Fold(list, data0,
        [&](TRuntimeNode item, TRuntimeNode state) {
            return pb.Concat(state, item);
        });

        auto graph = setup.BuildGraph(pgmReturn);
        auto res = graph->GetValue();
        UNBOXED_VALUE_STR_EQUAL(res, "very large string + small");
    }

    Y_UNIT_TEST_LLVM(TestAggrConcat) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto type = pb.NewOptionalType(pb.NewDataType(NUdf::EDataSlot::Utf8));
        const auto data0 = pb.NewEmptyOptional(type);
        const auto data1 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::Utf8>("PREFIX:"));
        const auto data2 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::Utf8>("very large string"));
        const auto data3 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::Utf8>(":SUFFIX"));
        const auto list = pb.NewList(type, {data0, data1, data0, data2, data3, data0});

        const auto pgmReturn = pb.Fold1(list,
            [&](TRuntimeNode item) { return item; },
            [&](TRuntimeNode item, TRuntimeNode state) { return pb.AggrConcat(state, item); }
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto str = graph->GetValue();
        UNBOXED_VALUE_STR_EQUAL(str, "PREFIX:very large string:SUFFIX");
    }

    Y_UNIT_TEST_LLVM(TestLongFold) {
        for (ui32 i = 0; i < 10; ++i) {
            TSetup<LLVM> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;
            const ui32 n = 1000;

            auto firstList = pb.Replicate(pb.NewDataLiteral<ui32>(0),
                pb.NewDataLiteral<ui64>(n), "", 0, 0);

            auto secondList = pb.Replicate(firstList, pb.NewDataLiteral<ui64>(n), "", 0, 0);

            auto pgmReturn = pb.Fold(secondList, pb.NewDataLiteral<ui32>(0),
                [&](TRuntimeNode item, TRuntimeNode state) {
            auto partialSum = pb.Fold(item, pb.NewDataLiteral<ui32>(0),
                    [&](TRuntimeNode item, TRuntimeNode state) {
                        Y_UNUSED(item);
                        return pb.AggrAdd(state, pb.NewDataLiteral<ui32>(1));
                    });

                    return pb.AggrAdd(state, partialSum);
                });


            auto graph = setup.BuildGraph(pgmReturn);
            auto value = graph->GetValue().template Get<ui32>();
            UNIT_ASSERT_VALUES_EQUAL(value, n * n);
        }
    }

    Y_UNIT_TEST_LLVM(TestFoldAggrAddIntervals) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto upper = i64(+1000LL);
        const auto lower = i64(-1000LL);
        const auto part = i64(100LL);
        const auto from = pb.NewDataLiteral<NUdf::EDataSlot::Interval>(NUdf::TStringRef((const char*)&lower, sizeof(lower)));
        const auto stop = pb.NewDataLiteral<NUdf::EDataSlot::Interval>(NUdf::TStringRef((const char*)&upper, sizeof(upper)));
        const auto step = pb.NewDataLiteral<NUdf::EDataSlot::Interval>(NUdf::TStringRef((const char*)&part, sizeof(part)));
        const auto list = pb.ListFromRange(from, stop, step);

        const auto pgmReturn = pb.Fold1(pb.ListFromRange(from, stop, step),
            [&](TRuntimeNode item) { return pb.NewOptional(item); },
            [&](TRuntimeNode item, TRuntimeNode state) { return pb.AggrAdd(pb.NewOptional(item), state); }
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto value = graph->GetValue();
        UNIT_ASSERT_VALUES_EQUAL(value.template Get<i64>(), -1000LL);
    }

    Y_UNIT_TEST_LLVM(TestFoldFoldPerf) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const ui32 n = 3333U;

        const auto firstList = pb.Replicate(pb.NewDataLiteral<ui32>(1), pb.NewDataLiteral<ui64>(n), "", 0, 0);

        const auto secondList = pb.Replicate(firstList, pb.NewDataLiteral<ui64>(n), "", 0, 0);

        const auto pgmReturn = pb.Fold(secondList, pb.NewDataLiteral<ui32>(0),
            [&](TRuntimeNode item, TRuntimeNode state) {
                const auto partialSum = pb.Fold(item, pb.NewDataLiteral<ui32>(0),
                        [&](TRuntimeNode i2, TRuntimeNode state) {
                            return pb.AggrAdd(state, i2);
                        });

                return pb.AggrAdd(state, partialSum);
            });


        const auto t1 = TInstant::Now();
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto t2 = TInstant::Now();
        const auto value = graph->GetValue().template Get<ui32>();
        const auto t3 = TInstant::Now();
        Cerr << "Time is " << t3 - t1 << " (" << t2 - t1 << " + " << t3 - t2 << ")." << Endl;
        UNIT_ASSERT_VALUES_EQUAL(value, n * n);
    }

    std::vector<double> MakeSamples() {
        std::default_random_engine eng;
        std::uniform_real_distribution<double> unif(-999.0, +999.0);

        std::vector<double> samples(3333333U);

        eng.seed(std::time(nullptr));
        std::generate(samples.begin(), samples.end(), std::bind(std::move(unif), std::move(eng)));
        return samples;
    }

    static const auto Samples = MakeSamples();

    Y_UNIT_TEST_LLVM(TestSumDoubleArrayListPerf) {
        TSetup<LLVM> setup;

        const auto t = TInstant::Now();
        const double sum = std::accumulate(Samples.cbegin(), Samples.cend(), 0.0);
        const auto cppTime = TInstant::Now() - t;

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewDataType(NUdf::TDataType<double>::Id));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Fold1(pb.Collect(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) { return item; },
            [&](TRuntimeNode item, TRuntimeNode state) { return pb.AggrAdd(state, item); }
        );

        const auto t1 = TInstant::Now();
        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(Samples.size(), items));
        std::transform(Samples.cbegin(), Samples.cend(), items, &ToValue<double>);
        const auto t2 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t3 = TInstant::Now();
        Cerr << "Time is " << t3 - t1 << " (" << t2 - t1 << " + " << t3 - t2 << ") vs C++ " << cppTime << Endl;
        UNIT_ASSERT_VALUES_EQUAL(value.template Get<double>(), sum);
    }

    Y_UNIT_TEST_LLVM(TestSumDoubleLazyListPerf) {
        TSetup<LLVM> setup;

        const auto t = TInstant::Now();
        const double sum = std::accumulate(Samples.cbegin(), Samples.cend(), 0.0);
        const auto cppTime = TInstant::Now() - t;

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewDataType(NUdf::TDataType<double>::Id));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Fold1(pb.LazyList(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) { return item; },
            [&](TRuntimeNode item, TRuntimeNode state) { return pb.AggrAdd(state, item); }
        );

        const auto t1 = TInstant::Now();
        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(Samples.size(), items));
        std::transform(Samples.cbegin(), Samples.cend(), items, &ToValue<double>);
        const auto t2 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t3 = TInstant::Now();
        Cerr << "Time is " << t3 - t1 << " (" << t2 - t1 << " + " << t3 - t2 << ") vs C++ " << cppTime << Endl;
        UNIT_ASSERT_VALUES_EQUAL(value.template Get<double>(), sum);
    }

    Y_UNIT_TEST_LLVM(TestSumDoubleFilteredArrayListPerf) {
        TSetup<LLVM> setup;

        const auto t = TInstant::Now();
        const double sum = std::accumulate(Samples.cbegin(), Samples.cend(), 0.0, [](double s, double v) { return v > 0.0 ? s + v : s; });
        const auto cppTime = TInstant::Now() - t;

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewDataType(NUdf::TDataType<double>::Id));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Fold1(
            pb.Filter(pb.Collect(TRuntimeNode(list, false)),
                [&](TRuntimeNode item) { return pb.AggrGreater(item, pb.NewDataLiteral(0.0)); }
            ),
            [&](TRuntimeNode item) { return item; },
            [&](TRuntimeNode item, TRuntimeNode state) { return pb.AggrAdd(state, item); }
        );

        const auto t1 = TInstant::Now();
        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(Samples.size(), items));
        std::transform(Samples.cbegin(), Samples.cend(), items, &ToValue<double>);
        const auto t2 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t3 = TInstant::Now();
        Cerr << "Time is " << t3 - t1 << " (" << t2 - t1 << " + " << t3 - t2 << ") vs C++ " << cppTime << Endl;
        UNIT_ASSERT_VALUES_EQUAL(value.template Get<double>(), sum);
    }

    Y_UNIT_TEST_LLVM(TestSumDoubleFilteredLazyListPerf) {
        TSetup<LLVM> setup;

        const auto t = TInstant::Now();
        const double sum = std::accumulate(Samples.cbegin(), Samples.cend(), 0.0, [](double s, double v) { return v > 0.0 ? s + v : s; });
        const auto cppTime = TInstant::Now() - t;

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewDataType(NUdf::TDataType<double>::Id));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Fold1(
            pb.Filter(pb.LazyList(TRuntimeNode(list, false)),
                [&](TRuntimeNode item) { return pb.AggrGreater(item, pb.NewDataLiteral(0.0)); }
            ),
            [&](TRuntimeNode item) { return item; },
            [&](TRuntimeNode item, TRuntimeNode state) { return pb.AggrAdd(state, item); }
        );

        const auto t1 = TInstant::Now();
        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(Samples.size(), items));
        std::transform(Samples.cbegin(), Samples.cend(), items, &ToValue<double>);
        const auto t2 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t3 = TInstant::Now();
        Cerr << "Time is " << t3 - t1 << " (" << t2 - t1 << " + " << t3 - t2 << ") vs C++ " << cppTime << Endl;
        UNIT_ASSERT_VALUES_EQUAL(value.template Get<double>(), sum);
    }

    Y_UNIT_TEST_LLVM(TestMinMaxSumDoubleArrayListPerf) {
        TSetup<LLVM> setup;

        double min(Samples.front()), max(Samples.front()), sum(0.0);

        const auto t = TInstant::Now();
        for (const auto v : Samples) {
            min = std::fmin(min, v);
            max = std::fmax(max, v);
            sum += v;
        }
        const auto cppTime = TInstant::Now() - t;

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewDataType(NUdf::TDataType<double>::Id));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Fold1(pb.Collect(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) { return pb.NewTuple({item, item, item}); },
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.NewTuple({pb.AggrMin(pb.Nth(state, 0U), item), pb.AggrMax(pb.Nth(state, 1U), item), pb.AggrAdd(pb.Nth(state, 2U), item)});
            });

        const auto t1 = TInstant::Now();
        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(Samples.size(), items));
        std::transform(Samples.cbegin(), Samples.cend(), items, &ToValue<double>);
        const auto t2 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t3 = TInstant::Now();
        Cerr << "Time is " << t3 - t1 << " (" << t2 - t1 << " + " << t3 - t2 << ") vs C++ " << cppTime << Endl;
        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(0U).template Get<double>(), min);
        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(1U).template Get<double>(), max);
        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(2U).template Get<double>(), sum);
    }

    Y_UNIT_TEST_LLVM(TestMinMaxSumDoubleLazyListPerf) {
        TSetup<LLVM> setup;

        double min(Samples.front()), max(Samples.front()), sum(0.0);

        const auto t = TInstant::Now();
        for (const auto v : Samples) {
            min = std::fmin(min, v);
            max = std::fmax(max, v);
            sum += v;
        }
        const auto cppTime = TInstant::Now() - t;

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewDataType(NUdf::TDataType<double>::Id));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Fold1(pb.LazyList(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) { return pb.NewTuple({item, item, item}); },
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.NewTuple({pb.AggrMin(pb.Nth(state, 0U), item), pb.AggrMax(pb.Nth(state, 1U), item), pb.AggrAdd(pb.Nth(state, 2U), item)});
            });

        const auto t1 = TInstant::Now();
        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(Samples.size(), items));
        std::transform(Samples.cbegin(), Samples.cend(), items, &ToValue<double>);
        const auto t2 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t3 = TInstant::Now();
        Cerr << "Time is " << t3 - t1 << " (" << t2 - t1 << " + " << t3 - t2 << ") vs C++ " << cppTime << Endl;
        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(0U).template Get<double>(), min);
        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(1U).template Get<double>(), max);
        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(2U).template Get<double>(), sum);
    }

    Y_UNIT_TEST_LLVM(TestMinMaxSumDoubleFilteredArrayListPerf) {
        TSetup<LLVM> setup;

        double min(std::nan("")), max(std::nan("")), sum(0.0);

        const auto t = TInstant::Now();
        for (const auto v : Samples) {
            if (v < 0.0) {
                min = std::fmin(min, v);
                max = std::fmax(max, v);
                sum += v;
            }
        }
        const auto cppTime = TInstant::Now() - t;

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewDataType(NUdf::TDataType<double>::Id));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Fold1(
            pb.Filter(pb.Collect(TRuntimeNode(list, false)),
                [&](TRuntimeNode item) { return pb.AggrLess(item, pb.NewDataLiteral(0.0)); }
            ),
            [&](TRuntimeNode item) { return pb.NewTuple({item, item, item}); },
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.NewTuple({pb.AggrMin(pb.Nth(state, 0U), item), pb.AggrMax(pb.Nth(state, 1U), item), pb.AggrAdd(pb.Nth(state, 2U), item)});
            });

        const auto t1 = TInstant::Now();
        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(Samples.size(), items));
        std::transform(Samples.cbegin(), Samples.cend(), items, &ToValue<double>);
        const auto t2 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t3 = TInstant::Now();
        Cerr << "Time is " << t3 - t1 << " (" << t2 - t1 << " + " << t3 - t2 << ") vs C++ " << cppTime << Endl;
        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(0U).template Get<double>(), min);
        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(1U).template Get<double>(), max);
        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(2U).template Get<double>(), sum);
    }

    Y_UNIT_TEST_LLVM(TestMinMaxSumDoubleFilteredLazyListPerf) {
        TSetup<LLVM> setup;

        double min(std::nan("")), max(std::nan("")), sum(0.0);

        const auto t = TInstant::Now();
        for (const auto v : Samples) {
            if (v < 0.0) {
                min = std::fmin(min, v);
                max = std::fmax(max, v);
                sum += v;
            }
        }
        const auto cppTime = TInstant::Now() - t;

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewDataType(NUdf::TDataType<double>::Id));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Fold1(
            pb.Filter(pb.LazyList(TRuntimeNode(list, false)),
                [&](TRuntimeNode item) { return pb.AggrLess(item, pb.NewDataLiteral(0.0)); }
            ),
            [&](TRuntimeNode item) { return pb.NewTuple({item, item, item}); },
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.NewTuple({pb.AggrMin(pb.Nth(state, 0U), item), pb.AggrMax(pb.Nth(state, 1U), item), pb.AggrAdd(pb.Nth(state, 2U), item)});
            });

        const auto t1 = TInstant::Now();
        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(Samples.size(), items));
        std::transform(Samples.cbegin(), Samples.cend(), items, &ToValue<double>);
        const auto t2 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t3 = TInstant::Now();
        Cerr << "Time is " << t3 - t1 << " (" << t2 - t1 << " + " << t3 - t2 << ") vs C++ " << cppTime << Endl;
        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(0U).template Get<double>(), min);
        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(1U).template Get<double>(), max);
        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(2U).template Get<double>(), sum);
    }

    Y_UNIT_TEST_LLVM(TestAvgDoubleByTupleFoldArrayListPerf) {
        TSetup<LLVM> setup;

        const auto t = TInstant::Now();
        const double avg = std::accumulate(Samples.cbegin(), Samples.cend(), 0.0) / Samples.size();
        const auto cppTime = TInstant::Now() - t;

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewDataType(NUdf::TDataType<double>::Id));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto fold = pb.Fold(pb.Collect(TRuntimeNode(list, false)),
            pb.NewTuple({pb.NewDataLiteral(0.0), pb.NewDataLiteral<ui64>(0ULL)}),
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.NewTuple({pb.AggrAdd(pb.Nth(state, 0), item), pb.Increment(pb.Nth(state, 1))});
            });

        const auto pgmReturn = pb.Div(pb.Nth(fold, 0U), pb.Nth(fold, 1U));

        const auto t1 = TInstant::Now();
        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(Samples.size(), items));
        std::transform(Samples.cbegin(), Samples.cend(), items, &ToValue<double>);
        const auto t2 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t3 = TInstant::Now();
        Cerr << "Time is " << t3 - t1 << " (" << t2 - t1 << " + " << t3 - t2 << ") vs C++ " << cppTime << Endl;
        UNIT_ASSERT_VALUES_EQUAL(value.template Get<double>(), avg);
    }

    Y_UNIT_TEST_LLVM(TestAvgDoubleByTupleFoldLazyListPerf) {
        TSetup<LLVM> setup;

        const auto t = TInstant::Now();
        const double avg = std::accumulate(Samples.cbegin(), Samples.cend(), 0.0) / Samples.size();
        const auto cppTime = TInstant::Now() - t;

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewDataType(NUdf::TDataType<double>::Id));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto fold = pb.Fold(pb.LazyList(TRuntimeNode(list, false)),
            pb.NewTuple({pb.NewDataLiteral(0.0), pb.NewDataLiteral<ui64>(0ULL)}),
            [&](TRuntimeNode item, TRuntimeNode state) {
                return pb.NewTuple({pb.AggrAdd(pb.Nth(state, 0U), item), pb.Increment(pb.Nth(state, 1U))});
            });

        const auto pgmReturn = pb.Div(pb.Nth(fold, 0U), pb.Nth(fold, 1U));

        const auto t1 = TInstant::Now();
        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(Samples.size(), items));
        std::transform(Samples.cbegin(), Samples.cend(), items, &ToValue<double>);
        const auto t2 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t3 = TInstant::Now();
        Cerr << "Time is " << t3 - t1 << " (" << t2 - t1 << " + " << t3 - t2 << ") vs C++ " << cppTime << Endl;
        UNIT_ASSERT_VALUES_EQUAL(value.template Get<double>(), avg);
    }

    Y_UNIT_TEST_LLVM(TestAvgDoubleByCollectFoldLazyListPerf) {
        TSetup<LLVM> setup;

        const auto t = TInstant::Now();
        const double avg = std::accumulate(Samples.cbegin(), Samples.cend(), 0.0) / Samples.size();
        const auto cppTime = TInstant::Now() - t;

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewDataType(NUdf::TDataType<double>::Id));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto src = pb.Collect(pb.LazyList(TRuntimeNode(list, false)));
        const auto pgmReturn = pb.Div(
            pb.Fold(src, pb.NewDataLiteral(0.0),
                [&](TRuntimeNode item, TRuntimeNode state) { return pb.AggrAdd(state, item); }
            ),
            pb.Length(src)
        );

        const auto t1 = TInstant::Now();
        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(Samples.size(), items));
        std::transform(Samples.cbegin(), Samples.cend(), items, &ToValue<double>);
        const auto t2 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t3 = TInstant::Now();
        Cerr << "Time is " << t3 - t1 << " (" << t2 - t1 << " + " << t3 - t2 << ") vs C++ " << cppTime << Endl;
        UNIT_ASSERT_VALUES_EQUAL(value.template Get<double>(), avg);
    }
}

}
}
