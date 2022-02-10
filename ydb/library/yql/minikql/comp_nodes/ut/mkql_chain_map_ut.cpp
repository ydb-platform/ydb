#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLChainMapNodeTest) {
    Y_UNIT_TEST_LLVM(TestOverList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto dataType = pb.NewOptionalType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<i32>::Id), pb.NewDataType(NUdf::TDataType<char*>::Id)}));

        auto data0 = pb.NewEmptyOptional(dataType);

        auto data2 = pb.NewOptional(pb.NewTuple({
            pb.NewDataLiteral<i32>(7),
            pb.NewDataLiteral<NUdf::EDataSlot::String>("A")
        }));
        auto data3 = pb.NewOptional(pb.NewTuple({
            pb.NewDataLiteral<i32>(1),
            pb.NewDataLiteral<NUdf::EDataSlot::String>("D")
        }));

        auto list = pb.NewList(dataType, {data2, data0, data3});

        auto init = pb.NewTuple({
            pb.NewOptional(pb.NewDataLiteral<i32>(3)),
            pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("B"))
        });

        auto pgmReturn = pb.ChainMap(list, init,
            [&](TRuntimeNode item, TRuntimeNode state) -> TRuntimeNodePair {
                auto key = pb.Nth(item, 0);
                auto val = pb.Nth(item, 1);
                auto skey = pb.AggrAdd(pb.Nth(state, 0), key);
                auto sval = pb.AggrConcat(pb.Nth(state, 1), val);
                return {pb.NewTuple({key, val, skey, sval}), pb.NewTuple({skey, sval})};
            }
        );

        auto graph = setup.BuildGraph(pgmReturn);
        auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 7);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "A");
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 10);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(3), "BA");
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 10);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(3), "BA");
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "D");
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 11);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(3), "BAD");
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(Test1OverList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto dataType = pb.NewOptionalType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<i32>::Id), pb.NewDataType(NUdf::TDataType<char*>::Id)}));

        auto data0 = pb.NewEmptyOptional(dataType);

        auto data1 = pb.NewOptional(pb.NewTuple({
            pb.NewDataLiteral<i32>(3),
            pb.NewDataLiteral<NUdf::EDataSlot::String>("B")
        }));
        auto data2 = pb.NewOptional(pb.NewTuple({
            pb.NewDataLiteral<i32>(7),
            pb.NewDataLiteral<NUdf::EDataSlot::String>("A")
        }));
        auto data3 = pb.NewOptional(pb.NewTuple({
            pb.NewDataLiteral<i32>(1),
            pb.NewDataLiteral<NUdf::EDataSlot::String>("D")
        }));

        auto list = pb.NewList(dataType, {data1, data2, data3, data0});

        auto pgmReturn = pb.Chain1Map(list,
            [&](TRuntimeNode item) -> TRuntimeNodePair {
                auto key = pb.Nth(item, 0);
                auto val = pb.Nth(item, 1);
                return {pb.NewTuple({key, val, key, val}), pb.NewTuple({key, val})};
            },
            [&](TRuntimeNode item, TRuntimeNode state) -> TRuntimeNodePair {
                auto key = pb.Nth(item, 0);
                auto val = pb.Nth(item, 1);
                auto skey = pb.Add(pb.Nth(state, 0), key);
                auto sval = pb.Concat(pb.Nth(state, 1), val);
                return {pb.NewTuple({key, val, skey, sval}), pb.NewTuple({skey, sval})};
            }
        );

        auto graph = setup.BuildGraph(pgmReturn);
        auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 3);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "B");
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 3);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(3), "B");
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 7);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "A");
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 10);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(3), "BA");
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "D");
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 11);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(3), "BAD");
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT(!item.GetElement(2));
        UNIT_ASSERT(!item.GetElement(3));
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestOverFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto dataType = pb.NewOptionalType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<i32>::Id), pb.NewDataType(NUdf::TDataType<char*>::Id)}));

        auto data0 = pb.NewEmptyOptional(dataType);

        auto data2 = pb.NewOptional(pb.NewTuple({
            pb.NewDataLiteral<i32>(7),
            pb.NewDataLiteral<NUdf::EDataSlot::String>("A")
        }));
        auto data3 = pb.NewOptional(pb.NewTuple({
            pb.NewDataLiteral<i32>(1),
            pb.NewDataLiteral<NUdf::EDataSlot::String>("D")
        }));

        auto list = pb.NewList(dataType, {data2, data0, data3});

        auto init = pb.NewTuple({
            pb.NewOptional(pb.NewDataLiteral<i32>(3)),
            pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("B"))
        });

        auto pgmReturn = pb.FromFlow(pb.ChainMap(pb.ToFlow(list), init,
            [&](TRuntimeNode item, TRuntimeNode state) -> TRuntimeNodePair {
                auto key = pb.Nth(item, 0);
                auto val = pb.Nth(item, 1);
                auto skey = pb.AggrAdd(pb.Nth(state, 0), key);
                auto sval = pb.AggrConcat(pb.Nth(state, 1), val);
                return {pb.NewTuple({key, val, skey, sval}), pb.NewTuple({skey, sval})};
            }
        ));

        auto graph = setup.BuildGraph(pgmReturn);
        auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 7);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "A");
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 10);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(3), "BA");
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 10);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(3), "BA");
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "D");
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 11);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(3), "BAD");
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(Test1OverFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto dataType = pb.NewOptionalType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<i32>::Id), pb.NewDataType(NUdf::TDataType<char*>::Id)}));

        auto data0 = pb.NewEmptyOptional(dataType);

        auto data1 = pb.NewOptional(pb.NewTuple({
            pb.NewDataLiteral<i32>(3),
            pb.NewDataLiteral<NUdf::EDataSlot::String>("B")
        }));
        auto data2 = pb.NewOptional(pb.NewTuple({
            pb.NewDataLiteral<i32>(7),
            pb.NewDataLiteral<NUdf::EDataSlot::String>("A")
        }));
        auto data3 = pb.NewOptional(pb.NewTuple({
            pb.NewDataLiteral<i32>(1),
            pb.NewDataLiteral<NUdf::EDataSlot::String>("D")
        }));

        auto list = pb.NewList(dataType, {data1, data2, data3, data0});

        auto pgmReturn = pb.FromFlow(pb.Chain1Map(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNodePair {
                auto key = pb.Nth(item, 0);
                auto val = pb.Nth(item, 1);
                return {pb.NewTuple({key, val, key, val}), pb.NewTuple({key, val})};
            },
            [&](TRuntimeNode item, TRuntimeNode state) -> TRuntimeNodePair {
                auto key = pb.Nth(item, 0);
                auto val = pb.Nth(item, 1);
                auto skey = pb.Add(pb.Nth(state, 0), key);
                auto sval = pb.Concat(pb.Nth(state, 1), val);
                return {pb.NewTuple({key, val, skey, sval}), pb.NewTuple({skey, sval})};
            }
        ));

        auto graph = setup.BuildGraph(pgmReturn);
        auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 3);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "B");
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 3);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(3), "B");
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 7);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "A");
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 10);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(3), "BA");
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "D");
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 11);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(3), "BAD");
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT(!item.GetElement(2));
        UNIT_ASSERT(!item.GetElement(3));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }
}

}
}
