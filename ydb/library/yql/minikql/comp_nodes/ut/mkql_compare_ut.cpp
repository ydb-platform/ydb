#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/mkql_node_printer.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_list_adapter.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <cfloat>
#include <utility>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLCompareTest) {
    Y_UNIT_TEST_LLVM(SqlString) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto data0 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<char*>::Id);
        auto data1 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("010"));
        auto data2 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("020"));

        auto dataType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        auto optionalType = pb.NewOptionalType(dataType);
        auto pairType = pb.NewTupleType({optionalType, optionalType});
        auto list = pb.NewList(pairType, {
            pb.NewTuple({data0, data0}),
            pb.NewTuple({data0, data1}),
            pb.NewTuple({data0, data2}),

            pb.NewTuple({data1, data0}),
            pb.NewTuple({data1, data1}),
            pb.NewTuple({data1, data2}),

            pb.NewTuple({data2, data0}),
            pb.NewTuple({data2, data1}),
            pb.NewTuple({data2, data2})
        });

        auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.NewTuple({
                pb.Equals(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.NotEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.Less(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.LessOrEqual(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.Greater(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.GreaterOrEqual(pb.Nth(item, 0), pb.Nth(item, 1))
            });
        });

        auto graph = setup.BuildGraph(pgmReturn);
        auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0)); // ==
        UNIT_ASSERT(!item.GetElement(1)); // !=
        UNIT_ASSERT(!item.GetElement(2)); // <
        UNIT_ASSERT(!item.GetElement(3)); // <=
        UNIT_ASSERT(!item.GetElement(4)); // >
        UNIT_ASSERT(!item.GetElement(5)); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0)); // ==
        UNIT_ASSERT(!item.GetElement(1)); // !=
        UNIT_ASSERT(!item.GetElement(2)); // <
        UNIT_ASSERT(!item.GetElement(3)); // <=
        UNIT_ASSERT(!item.GetElement(4)); // >
        UNIT_ASSERT(!item.GetElement(5)); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0)); // ==
        UNIT_ASSERT(!item.GetElement(1)); // !=
        UNIT_ASSERT(!item.GetElement(2)); // <
        UNIT_ASSERT(!item.GetElement(3)); // <=
        UNIT_ASSERT(!item.GetElement(4)); // >
        UNIT_ASSERT(!item.GetElement(5)); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0)); // ==
        UNIT_ASSERT(!item.GetElement(1)); // !=
        UNIT_ASSERT(!item.GetElement(2)); // <
        UNIT_ASSERT(!item.GetElement(3)); // <=
        UNIT_ASSERT(!item.GetElement(4)); // >
        UNIT_ASSERT(!item.GetElement(5)); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(!item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(!item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0)); // ==
        UNIT_ASSERT(!item.GetElement(1)); // !=
        UNIT_ASSERT(!item.GetElement(2)); // <
        UNIT_ASSERT(!item.GetElement(3)); // <=
        UNIT_ASSERT(!item.GetElement(4)); // >
        UNIT_ASSERT(!item.GetElement(5)); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(!item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(!item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(AggrString) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto data0 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<char*>::Id);
        auto data1 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("010"));
        auto data2 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("020"));

        auto dataType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        auto optionalType = pb.NewOptionalType(dataType);
        auto pairType = pb.NewTupleType({optionalType, optionalType});
        auto list = pb.NewList(pairType, {
            pb.NewTuple({data0, data0}),
            pb.NewTuple({data0, data1}),
            pb.NewTuple({data0, data2}),

            pb.NewTuple({data1, data0}),
            pb.NewTuple({data1, data1}),
            pb.NewTuple({data1, data2}),

            pb.NewTuple({data2, data0}),
            pb.NewTuple({data2, data1}),
            pb.NewTuple({data2, data2})
        });

        auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.NewTuple({
                pb.AggrEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.AggrNotEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.AggrLess(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.AggrLessOrEqual(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.AggrGreater(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.AggrGreaterOrEqual(pb.Nth(item, 0), pb.Nth(item, 1))
            });
        });

        auto graph = setup.BuildGraph(pgmReturn);
        auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(!item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(!item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(!item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(!item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(!item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(!item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(!item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(!item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(!item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(SqlFloats) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto data1 = pb.NewDataLiteral<float>(-7.0f);
        auto data2 = pb.NewDataLiteral<float>(3.0f);
        auto data3 = pb.NewDataLiteral<float>(0.0f*HUGE_VALF);
        auto data4 = pb.NewDataLiteral<double>(-7.0);
        auto data5 = pb.NewDataLiteral<double>(3.0);
        auto data6 = pb.NewDataLiteral<double>(0.0*HUGE_VAL);

        auto pairType = pb.NewTupleType({pb.NewDataType(NUdf::TDataType<float>::Id), pb.NewDataType(NUdf::TDataType<double>::Id)});
        auto list = pb.NewList(pairType, {
            pb.NewTuple({data1, data4}),
            pb.NewTuple({data1, data5}),
            pb.NewTuple({data1, data6}),

            pb.NewTuple({data2, data4}),
            pb.NewTuple({data2, data5}),
            pb.NewTuple({data2, data6}),

            pb.NewTuple({data3, data4}),
            pb.NewTuple({data3, data5}),
            pb.NewTuple({data3, data6}),
        });

        auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.NewTuple({
                pb.Equals(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.NotEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.Less(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.LessOrEqual(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.Greater(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.GreaterOrEqual(pb.Nth(item, 0), pb.Nth(item, 1))
            });
        });

        auto graph = setup.BuildGraph(pgmReturn);
        auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(!item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(!item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(!item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(!item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(!item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(!item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(!item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(!item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(!item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(!item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(!item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(!item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(!item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(!item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(AggrFloats) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto data1 = pb.NewDataLiteral<float>(-7.0f);
        auto data2 = pb.NewDataLiteral<float>(3.0f);
        auto data3 = pb.NewDataLiteral<float>(0.0f*HUGE_VALF);

        auto pairType = pb.NewTupleType({pb.NewDataType(NUdf::TDataType<float>::Id), pb.NewDataType(NUdf::TDataType<float>::Id)});
        auto list = pb.NewList(pairType, {
            pb.NewTuple({data1, data1}),
            pb.NewTuple({data1, data2}),
            pb.NewTuple({data1, data3}),

            pb.NewTuple({data2, data1}),
            pb.NewTuple({data2, data2}),
            pb.NewTuple({data2, data3}),

            pb.NewTuple({data3, data1}),
            pb.NewTuple({data3, data2}),
            pb.NewTuple({data3, data3}),
        });

        auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.NewTuple({
                pb.AggrEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.AggrNotEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.AggrLess(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.AggrLessOrEqual(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.AggrGreater(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.AggrGreaterOrEqual(pb.Nth(item, 0), pb.Nth(item, 1))
            });
        });

        auto graph = setup.BuildGraph(pgmReturn);
        auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(!item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(!item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(!item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(!item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(!item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(!item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(!item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(!item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(!item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(SqlSigned) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto data1 = pb.NewDataLiteral<i16>(-1);
        auto data2 = pb.NewDataLiteral<i16>(+1);
        auto data3 = pb.NewDataLiteral<i32>(-1);
        auto data4 = pb.NewDataLiteral<i32>(+1);

        auto pairType = pb.NewTupleType({pb.NewDataType(NUdf::TDataType<i16>::Id), pb.NewDataType(NUdf::TDataType<i32>::Id)});
        auto list = pb.NewList(pairType, {
            pb.NewTuple({data1, data3}),
            pb.NewTuple({data1, data4}),

            pb.NewTuple({data2, data3}),
            pb.NewTuple({data2, data4})
        });

        auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.NewTuple({
                pb.Equals(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.NotEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.Less(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.LessOrEqual(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.Greater(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.GreaterOrEqual(pb.Nth(item, 0), pb.Nth(item, 1))
            });
        });

        auto graph = setup.BuildGraph(pgmReturn);
        auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(!item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(!item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(!item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(!item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(SqlSignedAndUnsigned) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto data1 = pb.NewDataLiteral<i8>(-1);
        auto data2 = pb.NewDataLiteral<i8>(127);
        auto data3 = pb.NewDataLiteral<ui8>(0xFF);
        auto data4 = pb.NewDataLiteral<ui8>(127);

        auto pairType = pb.NewTupleType({pb.NewDataType(NUdf::TDataType<i8>::Id), pb.NewDataType(NUdf::TDataType<ui8>::Id)});
        auto list = pb.NewList(pairType, {
            pb.NewTuple({data1, data3}),
            pb.NewTuple({data1, data4}),

            pb.NewTuple({data2, data3}),
            pb.NewTuple({data2, data4})
        });

        auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.NewTuple({
                pb.Equals(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.NotEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.Less(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.LessOrEqual(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.Greater(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.GreaterOrEqual(pb.Nth(item, 0), pb.Nth(item, 1))
            });
        });

        auto graph = setup.BuildGraph(pgmReturn);
        auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(!item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(!item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(!item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(!item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(SqlUnsignedAndSigned) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto data1 = pb.NewDataLiteral<ui16>(0);
        auto data2 = pb.NewDataLiteral<ui16>(0xFFFF);
        auto data3 = pb.NewDataLiteral<i64>(-1);
        auto data4 = pb.NewDataLiteral<i64>(0xFFFF);

        auto pairType = pb.NewTupleType({pb.NewDataType(NUdf::TDataType<ui16>::Id), pb.NewDataType(NUdf::TDataType<i64>::Id)});
        auto list = pb.NewList(pairType, {
            pb.NewTuple({data1, data3}),
            pb.NewTuple({data1, data4}),

            pb.NewTuple({data2, data3}),
            pb.NewTuple({data2, data4})
        });

        auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.NewTuple({
                pb.Equals(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.NotEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.Less(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.LessOrEqual(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.Greater(pb.Nth(item, 0), pb.Nth(item, 1)),
                pb.GreaterOrEqual(pb.Nth(item, 0), pb.Nth(item, 1))
            });
        });

        auto graph = setup.BuildGraph(pgmReturn);
        auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(!item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(!item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(!item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).template Get<bool>()); // ==
        UNIT_ASSERT(!item.GetElement(1).template Get<bool>()); // !=
        UNIT_ASSERT(!item.GetElement(2).template Get<bool>()); // <
        UNIT_ASSERT(item.GetElement(3).template Get<bool>()); // <=
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(item.GetElement(5).template Get<bool>()); // >=

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(SimpleSqlString) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto opt1 = pb.NewOptional(pb.NewDataLiteral<ui32>(1));
        auto opt2 = pb.NewOptional(pb.NewDataLiteral<ui32>(2));
        auto optEmpty = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        auto pgmReturn = pb.NewEmptyList(pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<bool>::Id)));

        pgmReturn = pb.Append(pgmReturn, pb.Equals(opt1, opt1));
        pgmReturn = pb.Append(pgmReturn, pb.Equals(opt1, opt2));
        pgmReturn = pb.Append(pgmReturn, pb.Equals(opt1, optEmpty));
        pgmReturn = pb.Append(pgmReturn, pb.Equals(optEmpty, opt2));
        pgmReturn = pb.Append(pgmReturn, pb.Equals(optEmpty, optEmpty));

        pgmReturn = pb.Append(pgmReturn, pb.NotEquals(opt1, opt1));
        pgmReturn = pb.Append(pgmReturn, pb.NotEquals(opt1, opt2));
        pgmReturn = pb.Append(pgmReturn, pb.NotEquals(opt1, optEmpty));
        pgmReturn = pb.Append(pgmReturn, pb.NotEquals(optEmpty, opt2));
        pgmReturn = pb.Append(pgmReturn, pb.NotEquals(optEmpty, optEmpty));

        pgmReturn = pb.Append(pgmReturn, pb.Less(opt1, opt1));
        pgmReturn = pb.Append(pgmReturn, pb.Less(opt1, opt2));
        pgmReturn = pb.Append(pgmReturn, pb.Less(opt2, opt1));
        pgmReturn = pb.Append(pgmReturn, pb.Less(opt1, optEmpty));
        pgmReturn = pb.Append(pgmReturn, pb.Less(optEmpty, opt2));
        pgmReturn = pb.Append(pgmReturn, pb.Less(optEmpty, optEmpty));

        pgmReturn = pb.Append(pgmReturn, pb.LessOrEqual(opt1, opt1));
        pgmReturn = pb.Append(pgmReturn, pb.LessOrEqual(opt1, opt2));
        pgmReturn = pb.Append(pgmReturn, pb.LessOrEqual(opt2, opt1));
        pgmReturn = pb.Append(pgmReturn, pb.LessOrEqual(opt1, optEmpty));
        pgmReturn = pb.Append(pgmReturn, pb.LessOrEqual(optEmpty, opt2));
        pgmReturn = pb.Append(pgmReturn, pb.LessOrEqual(optEmpty, optEmpty));

        pgmReturn = pb.Append(pgmReturn, pb.Greater(opt1, opt1));
        pgmReturn = pb.Append(pgmReturn, pb.Greater(opt1, opt2));
        pgmReturn = pb.Append(pgmReturn, pb.Greater(opt2, opt1));
        pgmReturn = pb.Append(pgmReturn, pb.Greater(opt1, optEmpty));
        pgmReturn = pb.Append(pgmReturn, pb.Greater(optEmpty, opt2));
        pgmReturn = pb.Append(pgmReturn, pb.Greater(optEmpty, optEmpty));

        pgmReturn = pb.Append(pgmReturn, pb.GreaterOrEqual(opt1, opt1));
        pgmReturn = pb.Append(pgmReturn, pb.GreaterOrEqual(opt1, opt2));
        pgmReturn = pb.Append(pgmReturn, pb.GreaterOrEqual(opt2, opt1));
        pgmReturn = pb.Append(pgmReturn, pb.GreaterOrEqual(opt1, optEmpty));
        pgmReturn = pb.Append(pgmReturn, pb.GreaterOrEqual(optEmpty, opt2));
        pgmReturn = pb.Append(pgmReturn, pb.GreaterOrEqual(optEmpty, optEmpty));

        auto opt1s = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("A"));
        auto opt2s = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("B"));
        auto optEmptys = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<char*>::Id);

        pgmReturn = pb.Append(pgmReturn, pb.Equals(opt1s, opt1s));
        pgmReturn = pb.Append(pgmReturn, pb.Equals(opt1s, opt2s));
        pgmReturn = pb.Append(pgmReturn, pb.Equals(opt1s, optEmptys));
        pgmReturn = pb.Append(pgmReturn, pb.Equals(optEmptys, opt2s));
        pgmReturn = pb.Append(pgmReturn, pb.Equals(optEmptys, optEmptys));

        pgmReturn = pb.Append(pgmReturn, pb.NotEquals(opt1s, opt1s));
        pgmReturn = pb.Append(pgmReturn, pb.NotEquals(opt1s, opt2s));
        pgmReturn = pb.Append(pgmReturn, pb.NotEquals(opt1s, optEmptys));
        pgmReturn = pb.Append(pgmReturn, pb.NotEquals(optEmptys, opt2s));
        pgmReturn = pb.Append(pgmReturn, pb.NotEquals(optEmptys, optEmptys));

        auto graph = setup.BuildGraph(pgmReturn);
        auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        // equals
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        // not equals
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        // less
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        // less or equal
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        // greater
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        // greater or equal
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        // equals - string
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        // not equals - string
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TzMin) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui16>(1ULL);
        const auto data2 = pb.NewDataLiteral<ui16>(7ULL);

        const auto data3 = pb.NewDataLiteral<ui32>(1ULL);
        const auto data4 = pb.NewDataLiteral<ui32>(7ULL);

        const auto zones = pb.ListFromRange(data1, data2, data1);
        const auto dates = pb.ListFromRange(data3, data4, data3);

        const auto source = pb.Map(pb.Zip({pb.Reverse(dates), zones}),
            [&](TRuntimeNode item) {
                return pb.AddTimezone(pb.ToIntegral(pb.Nth(item, 0U), pb.NewDataType(NUdf::EDataSlot::Datetime, true)), pb.Nth(item, 1U));
            });

        const auto pgmReturn = pb.ToString(pb.Unwrap(pb.Fold1(source,
            [&](TRuntimeNode item) { return item; },
            [&](TRuntimeNode item, TRuntimeNode state) { return pb.Min(item, state); }
        ), pb.NewDataLiteral<NUdf::EDataSlot::String>(""), __FILE__, __LINE__, 0));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto result = graph->GetValue();
        UNBOXED_VALUE_STR_EQUAL(result, "1970-01-01T03:00:01,Africa/Asmara");
    }

    Y_UNIT_TEST_LLVM(TzMax) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui16>(1ULL);
        const auto data2 = pb.NewDataLiteral<ui16>(7ULL);

        const auto data3 = pb.NewDataLiteral<ui32>(1ULL);
        const auto data4 = pb.NewDataLiteral<ui32>(7ULL);

        const auto zones = pb.ListFromRange(data1, data2, data1);
        const auto dates = pb.ListFromRange(data3, data4, data3);

        const auto source = pb.Map(pb.Zip({dates, pb.Reverse(zones)}),
            [&](TRuntimeNode item) {
                return pb.AddTimezone(pb.ToIntegral(pb.Nth(item, 0U), pb.NewDataType(NUdf::EDataSlot::Datetime, true)), pb.Nth(item, 1U));
            });

        const auto pgmReturn = pb.ToString(pb.Unwrap(pb.Fold1(source,
            [&](TRuntimeNode item) { return item; },
            [&](TRuntimeNode item, TRuntimeNode state) { return pb.Max(item, state); }
        ), pb.NewDataLiteral<NUdf::EDataSlot::String>(""), __FILE__, __LINE__, 0));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto result = graph->GetValue();
        UNBOXED_VALUE_STR_EQUAL(result, "1970-01-01T03:00:06,Europe/Moscow");
   }

   Y_UNIT_TEST_LLVM(TzAggrMin) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui16>(1ULL);
        const auto data2 = pb.NewDataLiteral<ui16>(7ULL);

        const auto data3 = pb.NewDataLiteral<ui32>(1ULL);
        const auto data4 = pb.NewDataLiteral<ui32>(7ULL);

        const auto zones = pb.ListFromRange(data1, data2, data1);
        const auto dates = pb.ListFromRange(data3, data4, data3);

        const auto source = pb.FlatMap(zones,
        [&](TRuntimeNode zone) {
            return pb.Map(dates,
                [&](TRuntimeNode date) {
                    return pb.AddTimezone(pb.ToIntegral(date, pb.NewDataType(NUdf::EDataSlot::Datetime, true)), zone);
                });
        });

        const auto pgmReturn = pb.ToString(pb.Unwrap(pb.Fold1(source,
            [&](TRuntimeNode item) { return item; },
            [&](TRuntimeNode item, TRuntimeNode state) { return pb.AggrMin(item, state); }
        ), pb.NewDataLiteral<NUdf::EDataSlot::String>(""), __FILE__, __LINE__, 0));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto result = graph->GetValue();
        UNBOXED_VALUE_STR_EQUAL(result, "1970-01-01T03:00:01,Europe/Moscow");
   }

   Y_UNIT_TEST_LLVM(TzAggrMax) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui16>(1ULL);
        const auto data2 = pb.NewDataLiteral<ui16>(7ULL);

        const auto data3 = pb.NewDataLiteral<ui32>(1ULL);
        const auto data4 = pb.NewDataLiteral<ui32>(7ULL);

        const auto zones = pb.ListFromRange(data1, data2, data1);
        const auto dates = pb.ListFromRange(data3, data4, data3);

        const auto source = pb.FlatMap(dates,
        [&](TRuntimeNode date) {
            return pb.Map(zones,
                [&](TRuntimeNode zone) {
                    return pb.AddTimezone(pb.ToIntegral(date, pb.NewDataType(NUdf::EDataSlot::Datetime, true)), zone);
                });
        });

        const auto pgmReturn = pb.ToString(pb.Unwrap(pb.Fold1(source,
            [&](TRuntimeNode item) { return item; },
            [&](TRuntimeNode item, TRuntimeNode state) { return pb.AggrMax(item, state); }
        ), pb.NewDataLiteral<NUdf::EDataSlot::String>(""), __FILE__, __LINE__, 0));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto result = graph->GetValue();
        UNBOXED_VALUE_STR_EQUAL(result, "1970-01-01T03:00:06,Africa/Asmara");
    }

    Y_UNIT_TEST_LLVM(TestAggrMinMaxFloats) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<float>(0.0f*HUGE_VALF);
        const auto data2 = pb.NewDataLiteral<float>(HUGE_VALF);
        const auto data3 = pb.NewDataLiteral<float>(3.14f);
        const auto data4 = pb.NewDataLiteral<float>(-2.13f);
        const auto data5 = pb.NewDataLiteral<float>(-HUGE_VALF);
        const auto dataType = pb.NewDataType(NUdf::TDataType<float>::Id);
        const auto list = pb.NewList(dataType, {data1, data2, data3, data4, data5});
        const auto pgmReturn = pb.FlatMap(list,
            [&](TRuntimeNode left) {
            return pb.Map(list,
                [&](TRuntimeNode right) {
                return pb.NewTuple({pb.AggrMin(left, right), pb.AggrMax(left, right)});
            });
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(std::isnan(item.GetElement(0).Get<float>()));
        UNIT_ASSERT(std::isnan(item.GetElement(1).Get<float>()));

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<float>(), HUGE_VALF);
        UNIT_ASSERT(std::isnan(item.GetElement(1).Get<float>()));

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<float>(), 3.14f);
        UNIT_ASSERT(std::isnan(item.GetElement(1).Get<float>()));

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<float>(), -2.13f);
        UNIT_ASSERT(std::isnan(item.GetElement(1).Get<float>()));

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<float>(), -HUGE_VALF);
        UNIT_ASSERT(std::isnan(item.GetElement(1).Get<float>()));

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<float>(), HUGE_VALF);
        UNIT_ASSERT(std::isnan(item.GetElement(1).Get<float>()));

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<float>(), HUGE_VALF);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).Get<float>(), HUGE_VALF);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<float>(), 3.14f);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).Get<float>(), HUGE_VALF);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<float>(), -2.13f);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).Get<float>(), HUGE_VALF);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<float>(), -HUGE_VALF);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).Get<float>(), HUGE_VALF);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<float>(), 3.14f);
        UNIT_ASSERT(std::isnan(item.GetElement(1).Get<float>()));

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<float>(), 3.14f);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).Get<float>(), HUGE_VALF);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<float>(), 3.14f);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).Get<float>(), 3.14f);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<float>(), -2.13f);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).Get<float>(), 3.14f);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<float>(), -HUGE_VALF);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).Get<float>(), 3.14f);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<float>(), -2.13f);
        UNIT_ASSERT(std::isnan(item.GetElement(1).Get<float>()));

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<float>(), -2.13f);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).Get<float>(), HUGE_VALF);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<float>(), -2.13f);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).Get<float>(), 3.14f);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<float>(), -2.13f);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).Get<float>(), -2.13f);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<float>(), -HUGE_VALF);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).Get<float>(), -2.13f);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<float>(), -HUGE_VALF);
        UNIT_ASSERT(std::isnan(item.GetElement(1).Get<float>()));

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<float>(), -HUGE_VALF);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).Get<float>(), HUGE_VALF);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<float>(), -HUGE_VALF);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).Get<float>(), 3.14f);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<float>(), -HUGE_VALF);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).Get<float>(), -2.13f);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).Get<float>(), -HUGE_VALF);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).Get<float>(), -HUGE_VALF);

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }
}

}
}
