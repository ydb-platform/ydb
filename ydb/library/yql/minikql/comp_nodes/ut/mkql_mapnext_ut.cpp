#include "mkql_computation_node_ut.h"
#include <ydb/library/yql/minikql/mkql_runtime_version.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLMapNextTest) {
    Y_UNIT_TEST_LLVM(OverStream) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui16>(10);
        const auto data2 = pb.NewDataLiteral<ui16>(20);
        const auto data3 = pb.NewDataLiteral<ui16>(30);

        const auto dataType = pb.NewDataType(NUdf::TDataType<ui16>::Id);
        const auto optDataType = pb.NewOptionalType(dataType);
        const auto tupleType = pb.NewTupleType({dataType, optDataType});

        const auto list = pb.NewList(dataType, {data1, data2, data3});
        const auto pgmReturn = pb.MapNext(pb.Iterator(list, {}),
            [&](TRuntimeNode item, TRuntimeNode nextItem) {
            return pb.NewTuple(tupleType, {item, nextItem});
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui16>(), 10);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<ui16>(), 20);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui16>(), 20);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<ui16>(), 30);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui16>(), 30);
        UNIT_ASSERT(!item.GetElement(1).HasValue());
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(OverSingleElementStream) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui16>(10);

        const auto dataType = pb.NewDataType(NUdf::TDataType<ui16>::Id);
        const auto optDataType = pb.NewOptionalType(dataType);
        const auto tupleType = pb.NewTupleType({dataType, optDataType});

        const auto list = pb.NewList(dataType, {data1});
        const auto pgmReturn = pb.MapNext(pb.Iterator(list, {}),
                                          [&](TRuntimeNode item, TRuntimeNode nextItem) {
            return pb.NewTuple(tupleType, {item, nextItem});
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui16>(), 10);
        UNIT_ASSERT(!item.GetElement(1).HasValue());
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(OverEmptyStream) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<ui16>::Id);
        const auto optDataType = pb.NewOptionalType(dataType);
        const auto tupleType = pb.NewTupleType({dataType, optDataType});

        const auto list = pb.NewList(dataType, {});
        const auto pgmReturn = pb.MapNext(pb.Iterator(list, {}),
                                          [&](TRuntimeNode item, TRuntimeNode nextItem) {
            return pb.NewTuple(tupleType, {item, nextItem});
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(OverFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui16>(10);
        const auto data2 = pb.NewDataLiteral<ui16>(20);
        const auto data3 = pb.NewDataLiteral<ui16>(30);

        const auto dataType = pb.NewDataType(NUdf::TDataType<ui16>::Id);
        const auto optDataType = pb.NewOptionalType(dataType);
        const auto tupleType = pb.NewTupleType({dataType, optDataType});

        const auto list = pb.NewList(dataType, {data1, data2, data3});
        const auto pgmReturn = pb.FromFlow(pb.MapNext(pb.ToFlow(pb.Iterator(list, {})),
                                          [&](TRuntimeNode item, TRuntimeNode nextItem) {
            return pb.NewTuple(tupleType, {item, nextItem});
        }));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui16>(), 10);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<ui16>(), 20);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui16>(), 20);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<ui16>(), 30);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui16>(), 30);
        UNIT_ASSERT(!item.GetElement(1).HasValue());
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(OverSingleElementFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui16>(10);

        const auto dataType = pb.NewDataType(NUdf::TDataType<ui16>::Id);
        const auto optDataType = pb.NewOptionalType(dataType);
        const auto tupleType = pb.NewTupleType({dataType, optDataType});

        const auto list = pb.NewList(dataType, {data1});
        const auto pgmReturn = pb.FromFlow(pb.MapNext(pb.ToFlow(pb.Iterator(list, {})),
                                                      [&](TRuntimeNode item, TRuntimeNode nextItem) {
            return pb.NewTuple(tupleType, {item, nextItem});
        }));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui16>(), 10);
        UNIT_ASSERT(!item.GetElement(1).HasValue());
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(OverEmptyFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<ui16>::Id);
        const auto optDataType = pb.NewOptionalType(dataType);
        const auto tupleType = pb.NewTupleType({dataType, optDataType});

        const auto list = pb.NewList(dataType, {});
        const auto pgmReturn = pb.FromFlow(pb.MapNext(pb.ToFlow(pb.Iterator(list, {})),
                                                      [&](TRuntimeNode item, TRuntimeNode nextItem) {
            return pb.NewTuple(tupleType, {item, nextItem});
        }));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }
}

}
}
