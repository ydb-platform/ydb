#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLDecimalTest) {
    Y_UNIT_TEST_LLVM(TestNanvl) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto type = pb.NewOptionalType(pb.NewDecimalType(13, 5));
        const auto data0 = pb.NewOptional(pb.NewDecimalLiteral(0, 13, 5));
        const auto data1 = pb.NewOptional(pb.NewDecimalLiteral(NYql::NDecimal::Nan(), 13, 5));
        const auto data2 = pb.NewOptional(pb.NewDecimalLiteral(+NYql::NDecimal::Inf(), 13, 5));
        const auto data3 = pb.NewOptional(pb.NewDecimalLiteral(-NYql::NDecimal::Inf(), 13, 5));
        const auto data4 = pb.NewEmptyOptional(type);
        const auto data5 = pb.NewOptional(pb.NewDecimalLiteral(-NYql::NDecimal::Nan(), 13, 5));
        const auto data = pb.NewDecimalLiteral(314159, 13, 5);

        const auto list = pb.NewList(type, {data0, data1, data2, data3, data4, data5});

        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            return pb.Nanvl(item, data);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 314159);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == +NYql::NDecimal::Inf());
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -NYql::NDecimal::Inf());
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 314159);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestToIntegral) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto type = pb.NewDecimalType(13, 1);
        const auto data0 = pb.NewDecimalLiteral(0, 13, 1);
        const auto data1 = pb.NewDecimalLiteral(NYql::NDecimal::Nan(), 13, 1);
        const auto data2 = pb.NewDecimalLiteral(+NYql::NDecimal::Inf(), 13, 1);
        const auto data3 = pb.NewDecimalLiteral(-NYql::NDecimal::Inf(), 13, 1);
        const auto data4 = pb.NewDecimalLiteral(1270, 13, 1);
        const auto data5 = pb.NewDecimalLiteral(-1280, 13, 1);
        const auto data6 = pb.NewDecimalLiteral(2550, 13, 1);
        const auto data7 = pb.NewDecimalLiteral(-2560, 13, 1);
        const auto data8 = pb.NewDecimalLiteral(2560, 13, 1);
        const auto data9 = pb.NewDecimalLiteral(-2570, 13, 1);
        const auto dataA = pb.NewDecimalLiteral(327670, 13, 1);
        const auto dataB = pb.NewDecimalLiteral(-327680, 13, 1);
        const auto dataC = pb.NewDecimalLiteral(655350, 13, 1);
        const auto dataD = pb.NewDecimalLiteral(-655360, 13, 1);
        const auto dataE = pb.NewDecimalLiteral(21474836470, 13, 1);
        const auto dataF = pb.NewDecimalLiteral(-21474836480, 13, 1);
        const auto dataG = pb.NewDecimalLiteral(21474836480, 13, 1);
        const auto dataH = pb.NewDecimalLiteral(-21474836490, 13, 1);

        const auto list = pb.NewList(type, {data0, data1, data2, data3, data4, data5, data6, data7, data8, data9, dataA, dataB, dataC, dataD, dataE, dataF, dataG, dataH});

        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            return pb.NewTuple({
                pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<i8>::Id, true)),
                pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<ui8>::Id, true)),
                pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<i16>::Id, true)),
                pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<ui16>::Id, true)),
                pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<i32>::Id, true)),
                pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<ui32>::Id, true)),
                pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<i64>::Id, true)),
                pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<ui64>::Id, true))
            });
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[0].Get<i8>(), 0);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[1].Get<ui8>(), 0);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[2].Get<i16>(), 0);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[3].Get<ui16>(), 0);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), 0);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[5].Get<ui32>(), 0);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<i64>(), 0);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[7].Get<ui64>(), 0);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT(!item.GetElements()[2]);
        UNIT_ASSERT(!item.GetElements()[3]);
        UNIT_ASSERT(!item.GetElements()[4]);
        UNIT_ASSERT(!item.GetElements()[5]);
        UNIT_ASSERT(!item.GetElements()[6]);
        UNIT_ASSERT(!item.GetElements()[7]);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT(!item.GetElements()[2]);
        UNIT_ASSERT(!item.GetElements()[3]);
        UNIT_ASSERT(!item.GetElements()[4]);
        UNIT_ASSERT(!item.GetElements()[5]);
        UNIT_ASSERT(!item.GetElements()[6]);
        UNIT_ASSERT(!item.GetElements()[7]);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT(!item.GetElements()[2]);
        UNIT_ASSERT(!item.GetElements()[3]);
        UNIT_ASSERT(!item.GetElements()[4]);
        UNIT_ASSERT(!item.GetElements()[5]);
        UNIT_ASSERT(!item.GetElements()[6]);
        UNIT_ASSERT(!item.GetElements()[7]);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[0].Get<i8>(), 127);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[1].Get<ui8>(), 127);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[2].Get<i16>(), 127);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[3].Get<ui16>(), 127);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), 127);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[5].Get<ui32>(), 127);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<i64>(), 127);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[7].Get<ui64>(), 127);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[0].Get<i8>(), -128);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[2].Get<i16>(), -128);
        UNIT_ASSERT(!item.GetElements()[3]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), -128);
        UNIT_ASSERT(!item.GetElements()[5]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<i64>(), -128);
        UNIT_ASSERT(!item.GetElements()[7]);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[1].Get<ui8>(), 255);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[2].Get<i16>(), 255);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[3].Get<ui16>(), 255);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), 255);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[5].Get<ui32>(), 255);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<i64>(), 255);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[7].Get<ui64>(), 255);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[2].Get<i16>(), -256);
        UNIT_ASSERT(!item.GetElements()[3]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), -256);
        UNIT_ASSERT(!item.GetElements()[5]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<i64>(), -256);
        UNIT_ASSERT(!item.GetElements()[7]);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[2].Get<i16>(), 256);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[3].Get<ui16>(), 256);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), 256);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[5].Get<ui32>(), 256);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<i64>(), 256);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[7].Get<ui64>(), 256);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[2].Get<i16>(), -257);
        UNIT_ASSERT(!item.GetElements()[3]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), -257);
        UNIT_ASSERT(!item.GetElements()[5]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<i64>(), -257);
        UNIT_ASSERT(!item.GetElements()[7]);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[2].Get<i16>(), 32767);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[3].Get<ui16>(), 32767);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), 32767);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[5].Get<ui32>(), 32767);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<i64>(), 32767);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[7].Get<ui64>(), 32767);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[2].Get<i16>(), -32768);
        UNIT_ASSERT(!item.GetElements()[3]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), -32768);
        UNIT_ASSERT(!item.GetElements()[5]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<i64>(), -32768);
        UNIT_ASSERT(!item.GetElements()[7]);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT(!item.GetElements()[2]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[3].Get<ui16>(), 65535);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), 65535);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[5].Get<ui32>(), 65535);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<i64>(), 65535);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[7].Get<ui64>(), 65535);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT(!item.GetElements()[2]);
        UNIT_ASSERT(!item.GetElements()[3]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), -65536);
        UNIT_ASSERT(!item.GetElements()[5]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<i64>(), -65536);
        UNIT_ASSERT(!item.GetElements()[7]);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT(!item.GetElements()[2]);
        UNIT_ASSERT(!item.GetElements()[3]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), 2147483647LL);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[5].Get<ui32>(), 2147483647LL);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<i64>(), 2147483647LL);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[7].Get<ui64>(), 2147483647LL);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT(!item.GetElements()[2]);
        UNIT_ASSERT(!item.GetElements()[3]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), -2147483648LL);
        UNIT_ASSERT(!item.GetElements()[5]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<i64>(), -2147483648LL);
        UNIT_ASSERT(!item.GetElements()[7]);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT(!item.GetElements()[2]);
        UNIT_ASSERT(!item.GetElements()[3]);
        UNIT_ASSERT(!item.GetElements()[4]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[5].Get<ui32>(), 2147483648LL);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<i64>(), 2147483648LL);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[7].Get<ui64>(), 2147483648LL);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT(!item.GetElements()[2]);
        UNIT_ASSERT(!item.GetElements()[3]);
        UNIT_ASSERT(!item.GetElements()[4]);
        UNIT_ASSERT(!item.GetElements()[5]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<i64>(), -2147483649LL);
        UNIT_ASSERT(!item.GetElements()[7]);

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestToFloat) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDecimalType(10, 3);
        const auto data0 = pb.NewDecimalLiteral(2123, 10, 3);
        const auto data1 = pb.NewDecimalLiteral(233, 10, 3);
        const auto data2 = pb.NewDecimalLiteral(0, 10, 3);
        const auto data3 = pb.NewDecimalLiteral(-3277823, 10, 3);
        const auto data4 = pb.NewDecimalLiteral(-1, 10, 3);
        const auto data5 = pb.NewDecimalLiteral(7128, 10, 3);
        const auto data6 = pb.NewDecimalLiteral(NYql::NDecimal::Nan(), 10, 3);
        const auto data7 = pb.NewDecimalLiteral(+NYql::NDecimal::Inf(), 10, 3);
        const auto data8 = pb.NewDecimalLiteral(-NYql::NDecimal::Inf(), 10, 3);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4, data5, data6, data7, data8});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.Convert(item, pb.NewDataType(NUdf::TDataType<float>::Id));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), 2.123f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), 0.233f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), 0.0f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), -3277.823f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), -0.001f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), 7.128f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(std::isnan(item.template Get<float>()));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(std::isinf(item.template Get<float>()) && item.template Get<float>() > 0.0f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(std::isinf(item.template Get<float>()) && item.template Get<float>() < 0.0f);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestToDouble) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDecimalType(10, 5);
        const auto data0 = pb.NewDecimalLiteral(2123, 10, 5);
        const auto data1 = pb.NewDecimalLiteral(233, 10, 5);
        const auto data2 = pb.NewDecimalLiteral(0, 10, 5);
        const auto data3 = pb.NewDecimalLiteral(-3277823, 10, 5);
        const auto data4 = pb.NewDecimalLiteral(-1, 10, 5);
        const auto data5 = pb.NewDecimalLiteral(7128, 10, 5);
        const auto data6 = pb.NewDecimalLiteral(NYql::NDecimal::Nan(), 10, 5);
        const auto data7 = pb.NewDecimalLiteral(+NYql::NDecimal::Inf(), 10, 5);
        const auto data8 = pb.NewDecimalLiteral(-NYql::NDecimal::Inf(), 10, 5);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4, data5, data6, data7, data8});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.Convert(item, pb.NewDataType(NUdf::TDataType<double>::Id));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 0.02123);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 0.00233);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 0.0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), -32.77823);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), -0.00001);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 0.07128);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(std::isnan(item.template Get<double>()));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(std::isinf(item.template Get<double>()) && item.template Get<double>() > 0.0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(std::isinf(item.template Get<double>()) && item.template Get<double>() < 0.0);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestDiv) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDecimalType(10, 0);
        const auto data0 = pb.NewDecimalLiteral(2, 10, 0);
        const auto data1 = pb.NewDecimalLiteral(23, 10, 0);
        const auto data2 = pb.NewDecimalLiteral(-23, 10, 0);
        const auto data3 = pb.NewDecimalLiteral(25, 10, 0);
        const auto data4 = pb.NewDecimalLiteral(-25, 10, 0);
        const auto data5 = pb.NewDecimalLiteral(1, 10, 0);
        const auto data6 = pb.NewDecimalLiteral(-1, 10, 0);
        const auto data7 = pb.NewDecimalLiteral(3, 10, 0);
        const auto data8 = pb.NewDecimalLiteral(-3, 10, 0);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4, data5, data6, data7, data8});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.DecimalDiv(item, data0);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 12);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -12);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 12);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -12);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -2);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestDivInt) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<i8>::Id);
        const auto data0 = pb.NewDecimalLiteral(-238973, 9, 3);
        const auto data1 = pb.NewDataLiteral<i8>(0);
        const auto data2 = pb.NewDataLiteral<i8>(-1);
        const auto data3 = pb.NewDataLiteral<i8>(-128);
        const auto data4 = pb.NewDataLiteral<i8>(3);
        const auto data5 = pb.NewDataLiteral<i8>(5);
        const auto data6 = pb.NewDataLiteral<i8>(-7);
        const auto data7 = pb.NewDataLiteral<i8>(13);
        const auto data8 = pb.NewDataLiteral<i8>(-19);
        const auto data9 = pb.NewDataLiteral<i8>(42);
        const auto list = pb.NewList(dataType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.DecimalDiv(data0, item);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -NYql::NDecimal::Inf());
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 238973);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 1866);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -79658);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -47795);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 34139);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -18383);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 12577);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -5690);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestMod) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDecimalType(5, 2);
        const auto data0 = pb.NewDecimalLiteral(-12323, 5, 2);
        const auto data1 = pb.NewDecimalLiteral(0, 5, 2);
        const auto data2 = pb.NewDecimalLiteral(NYql::NDecimal::Inf(), 5, 2);
        const auto data3 = pb.NewDecimalLiteral(-1, 5, 2);
        const auto data4 = pb.NewDecimalLiteral(2, 5, 2);
        const auto data5 = pb.NewDecimalLiteral(-3, 5, 2);
        const auto data6 = pb.NewDecimalLiteral(NYql::NDecimal::Nan(), 5, 2);
        const auto data7 = pb.NewDecimalLiteral(7, 5, 2);
        const auto data8 = pb.NewDecimalLiteral(-10000, 5, 2);
        const auto data9 = pb.NewDecimalLiteral(12329, 5, 2);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.DecimalMod(data0, item);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == NYql::NDecimal::Nan());
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == NYql::NDecimal::Nan());
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == NYql::NDecimal::Nan());
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -2323);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -12323);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestModInt) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<i16>::Id);
        const auto data0 = pb.NewDecimalLiteral(-743, 3, 2);
        const auto data1 = pb.NewDataLiteral<i16>(0);
        const auto data2 = pb.NewDataLiteral<i16>(1);
        const auto data3 = pb.NewDataLiteral<i16>(-2);
        const auto data4 = pb.NewDataLiteral<i16>(3);
        const auto data5 = pb.NewDataLiteral<i16>(4);
        const auto data6 = pb.NewDataLiteral<i16>(-5);
        const auto data7 = pb.NewDataLiteral<i16>(8);
        const auto data8 = pb.NewDataLiteral<i16>(10);
        const auto data9 = pb.NewDataLiteral<i16>(-10);
        const auto list = pb.NewList(dataType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.DecimalMod(data0, item);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == NYql::NDecimal::Nan());
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -43);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -143);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -143);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -343);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -243);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -743);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -743);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -743);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestMul) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDecimalType(10, 2);
        const auto data0 = pb.NewDecimalLiteral(333, 10, 2);
        const auto data1 = pb.NewDecimalLiteral(-100, 10, 2);
        const auto data2 = pb.NewDecimalLiteral(-120, 10, 2);
        const auto data3 = pb.NewDecimalLiteral(3, 10, 2);
        const auto data4 = pb.NewDecimalLiteral(77, 10, 2);
        const auto data5 = pb.NewDecimalLiteral(122, 10, 2);
        const auto data6 = pb.NewDecimalLiteral(1223, 10, 2);
        const auto data7 = pb.NewDecimalLiteral(-999, 10, 2);
        const auto data8 = pb.NewDecimalLiteral(0, 10, 2);
        const auto data9 = pb.NewDecimalLiteral(-3003003003LL, 10, 2);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.DecimalMul(item, data0);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 1109);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -333);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -400);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 10);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 256);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 406);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 4073);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -3327);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -NYql::NDecimal::Inf());
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestMulUInt) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<ui16>::Id);
        const auto data0 = pb.NewDecimalLiteral(-333, 7, 2);
        const auto data1 = pb.NewDataLiteral<ui16>(0);
        const auto data2 = pb.NewDataLiteral<ui16>(1);
        const auto data3 = pb.NewDataLiteral<ui16>(2);
        const auto data4 = pb.NewDataLiteral<ui16>(3);
        const auto data5 = pb.NewDataLiteral<ui16>(10);
        const auto data6 = pb.NewDataLiteral<ui16>(100);
        const auto data7 = pb.NewDataLiteral<ui16>(1000);
        const auto data8 = pb.NewDataLiteral<ui16>(10000);
        const auto data9 = pb.NewDataLiteral<ui16>(65535);
        const auto list = pb.NewList(dataType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.DecimalMul(data0, item);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -333);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -666);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -999);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -3330);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -33300);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -333000);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -3330000);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -NYql::NDecimal::Inf());
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestMulTinyInt) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<i8>::Id);
        const auto data0 = pb.NewDecimalLiteral(3631400, 32, 4);
        const auto data1 = pb.NewDataLiteral<i8>(0);
        const auto data2 = pb.NewDataLiteral<i8>(1);
        const auto data3 = pb.NewDataLiteral<i8>(-1);
        const auto data4 = pb.NewDataLiteral<i8>(3);
        const auto data5 = pb.NewDataLiteral<i8>(-3);
        const auto data6 = pb.NewDataLiteral<i8>(100);
        const auto data7 = pb.NewDataLiteral<i8>(-100);
        const auto data8 = pb.NewDataLiteral<i8>(127);
        const auto data9 = pb.NewDataLiteral<i8>(-128);
        const auto list = pb.NewList(dataType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.DecimalMul(data0, item);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 3631400);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -3631400);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 10894200);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -10894200);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 363140000);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -363140000);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 461187800);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -464819200);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestCastAndMulTinyInt) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDecimalType(32, 4);
        const auto data0 = pb.NewDataLiteral<i8>(1);
        const auto data1 = pb.NewDecimalLiteral(3145926, 32, 4);
        const auto data2 = pb.NewDecimalLiteral(-3145926, 32, 4);
        const auto list = pb.NewList(dataType, {data1, data2});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.NewTuple({pb.DecimalMul(item, data0), pb.DecimalMul(item, pb.ToDecimal(data0, 32, 4))});
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == 3145926);
        UNIT_ASSERT(item.GetElement(1).GetInt128() == 3145926);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == -3145926);
        UNIT_ASSERT(item.GetElement(1).GetInt128() == -3145926);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestLongintMul) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDecimalType(10, 0);
        const auto data0 = pb.NewDecimalLiteral(333, 10, 0);
        const auto data1 = pb.NewDecimalLiteral(-100, 10, 0);
        const auto data2 = pb.NewDecimalLiteral(-120, 10, 0);
        const auto data3 = pb.NewDecimalLiteral(3, 10, 0);
        const auto data4 = pb.NewDecimalLiteral(77, 10, 0);
        const auto data5 = pb.NewDecimalLiteral(NYql::NDecimal::Nan(), 10, 0);
        const auto data6 = pb.NewDecimalLiteral(30030031, 10, 0);
        const auto data7 = pb.NewDecimalLiteral(-30030031, 10, 0);
        const auto data8 = pb.NewDecimalLiteral(0, 10, 0);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4, data5, data6, data7, data8});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.DecimalMul(item, data0);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 110889);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -33300);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -39960);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 999);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 25641);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == NYql::NDecimal::Nan());
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == NYql::NDecimal::Inf());
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -NYql::NDecimal::Inf());
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 0);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestScaleUp) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDecimalType(10, 2);
        const auto data0 = pb.NewDecimalLiteral(333, 10, 2);
        const auto data1 = pb.NewDecimalLiteral(-100, 10, 2);
        const auto data2 = pb.NewDecimalLiteral(-120, 10, 2);
        const auto data3 = pb.NewDecimalLiteral(3, 10, 2);
        const auto data4 = pb.NewDecimalLiteral(77, 10, 2);
        const auto data5 = pb.NewDecimalLiteral(122, 10, 2);
        const auto data6 = pb.NewDecimalLiteral(1223, 10, 2);
        const auto data7 = pb.NewDecimalLiteral(-999, 10, 2);
        const auto data8 = pb.NewDecimalLiteral(0, 10, 2);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4, data5, data6, data7, data8});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.ToDecimal(item, 12, 4);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 33300);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -10000);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -12000);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 300);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 7700);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 12200);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 122300);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -99900);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 0);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestScaleDown) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDecimalType(10, 2);
        const auto data0 = pb.NewDecimalLiteral(-251, 10, 2);
        const auto data1 = pb.NewDecimalLiteral(-250, 10, 2);
        const auto data2 = pb.NewDecimalLiteral(-150, 10, 2);
        const auto data3 = pb.NewDecimalLiteral(-51, 10, 2);
        const auto data4 = pb.NewDecimalLiteral(50, 10, 2);
        const auto data5 = pb.NewDecimalLiteral(50, 10, 2);
        const auto data6 = pb.NewDecimalLiteral(51, 10, 2);
        const auto data7 = pb.NewDecimalLiteral(150, 10, 2);
        const auto data8 = pb.NewDecimalLiteral(250, 10, 2);
        const auto data9 = pb.NewDecimalLiteral(251, 10, 2);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.ToDecimal(item, 8, 0);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 3);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestMinMax) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDecimalLiteral(-NYql::NDecimal::Nan(), 13, 2);
        const auto data2 = pb.NewDecimalLiteral(+NYql::NDecimal::Inf(), 13, 2);
        const auto data3 = pb.NewDecimalLiteral(314, 13, 2);
        const auto data4 = pb.NewDecimalLiteral(-213, 13, 2);
        const auto data5 = pb.NewDecimalLiteral(-NYql::NDecimal::Inf(), 13, 2);
        const auto dataType = pb.NewDecimalType(13, 2);
        const auto list = pb.NewList(dataType, {data1, data2, data3, data4, data5});
        const auto pgmReturn = pb.FlatMap(list,
            [&](TRuntimeNode left) {
            return pb.Map(list,
                [&](TRuntimeNode right) {
                return pb.NewTuple({pb.Min(left, right), pb.Max(left, right)});
            });
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == -NYql::NDecimal::Nan());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == -NYql::NDecimal::Nan());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  +NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  +NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  314);
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  314);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -213);
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  -213);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  -NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  314);
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  +NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -213);
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  +NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  +NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  314);
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  314);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  314);
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  +NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  314);
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  314);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -213);
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  314);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  314);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -213);
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  -213);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -213);
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  +NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -213);
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  314);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -213);
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  -213);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  -213);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  -NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  +NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  314);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  -213);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  -NYql::NDecimal::Inf());

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestAggrMinMax) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDecimalLiteral(NYql::NDecimal::Nan(), 13, 2);
        const auto data2 = pb.NewDecimalLiteral(+NYql::NDecimal::Inf(), 13, 2);
        const auto data3 = pb.NewDecimalLiteral(314, 13, 2);
        const auto data4 = pb.NewDecimalLiteral(-213, 13, 2);
        const auto data5 = pb.NewDecimalLiteral(-NYql::NDecimal::Inf(), 13, 2);
        const auto dataType = pb.NewDecimalType(13, 2);
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
        UNIT_ASSERT(item.GetElement(0).GetInt128() == NYql::NDecimal::Nan());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == NYql::NDecimal::Nan());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  +NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  NYql::NDecimal::Nan());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  314);
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  NYql::NDecimal::Nan());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -213);
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  NYql::NDecimal::Nan());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  NYql::NDecimal::Nan());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  NYql::NDecimal::Nan());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  314);
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  +NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -213);
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  +NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  +NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  314);
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  NYql::NDecimal::Nan());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  314);
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  +NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  314);
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  314);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -213);
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  314);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  314);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -213);
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  NYql::NDecimal::Nan());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -213);
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  +NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -213);
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  314);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -213);
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  -213);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  -213);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  NYql::NDecimal::Nan());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  +NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  314);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  -213);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() ==  -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() ==  -NYql::NDecimal::Inf());

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestAddSub) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDecimalLiteral(-NYql::NDecimal::Nan(), 13, 2);
        const auto data2 = pb.NewDecimalLiteral(+NYql::NDecimal::Inf(), 13, 2);
        const auto data3 = pb.NewDecimalLiteral(314, 13, 2);
        const auto data4 = pb.NewDecimalLiteral(-213, 13, 2);
        const auto data5 = pb.NewDecimalLiteral(-NYql::NDecimal::Inf(), 13, 2);

        const auto dataType = pb.NewDecimalType(13, 2);
        const auto list = pb.NewList(dataType, {data1, data2, data3, data4, data5});
        const auto pgmReturn = pb.FlatMap(list,
            [&](TRuntimeNode left) {
            return pb.Map(list,
                [&](TRuntimeNode right) {
                return pb.NewTuple({pb.Add(left, right), pb.Sub(left, right)});
            });
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == NYql::NDecimal::Nan());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == NYql::NDecimal::Nan());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == NYql::NDecimal::Nan());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == NYql::NDecimal::Nan());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == NYql::NDecimal::Nan());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == NYql::NDecimal::Nan());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == NYql::NDecimal::Nan());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == NYql::NDecimal::Nan());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == NYql::NDecimal::Nan());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == NYql::NDecimal::Nan());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == NYql::NDecimal::Nan());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == NYql::NDecimal::Nan());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == NYql::NDecimal::Nan());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == NYql::NDecimal::Nan());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == NYql::NDecimal::Nan());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == NYql::NDecimal::Nan());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == +NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == -NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == 628);
        UNIT_ASSERT(item.GetElement(1).GetInt128() == 0);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == 101);
        UNIT_ASSERT(item.GetElement(1).GetInt128() == 527);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == +NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == NYql::NDecimal::Nan());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == NYql::NDecimal::Nan());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == +NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == -NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == 101);
        UNIT_ASSERT(item.GetElement(1).GetInt128() == -527);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == -426);
        UNIT_ASSERT(item.GetElement(1).GetInt128() == 0);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == +NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == NYql::NDecimal::Nan());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == NYql::NDecimal::Nan());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == NYql::NDecimal::Nan());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == -NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == -NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == -NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == NYql::NDecimal::Nan());

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestCompares) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1l = pb.NewDecimalLiteral(-7, 10, 0);
        const auto data2l = pb.NewDecimalLiteral(3, 10, 0);
        const auto data3l = pb.NewDecimalLiteral(NYql::NDecimal::Nan(), 10, 0);
        const auto data4l = pb.NewDecimalLiteral(-NYql::NDecimal::Inf(), 10, 0);
        const auto data5l = pb.NewDecimalLiteral(+NYql::NDecimal::Inf(), 10, 0);

        const auto data1r = pb.NewDecimalLiteral(-700, 7, 2);
        const auto data2r = pb.NewDecimalLiteral(300, 7, 2);
        const auto data3r = pb.NewDecimalLiteral(NYql::NDecimal::Nan(), 7, 2);
        const auto data4r = pb.NewDecimalLiteral(-NYql::NDecimal::Inf(), 7, 2);
        const auto data5r = pb.NewDecimalLiteral(+NYql::NDecimal::Inf(), 7, 2);

        auto pairType = pb.NewTupleType({pb.NewDecimalType(10, 0), pb.NewDecimalType(7, 2)});
        const auto list = pb.NewList(pairType, {
            pb.NewTuple({data1l, data1r}),
            pb.NewTuple({data1l, data2r}),
            pb.NewTuple({data1l, data3r}),
            pb.NewTuple({data1l, data4r}),
            pb.NewTuple({data1l, data5r}),

            pb.NewTuple({data2l, data1r}),
            pb.NewTuple({data2l, data2r}),
            pb.NewTuple({data2l, data3r}),
            pb.NewTuple({data2l, data4r}),
            pb.NewTuple({data2l, data5r}),

            pb.NewTuple({data3l, data1r}),
            pb.NewTuple({data3l, data2r}),
            pb.NewTuple({data3l, data3r}),
            pb.NewTuple({data3l, data4r}),
            pb.NewTuple({data3l, data5r}),

            pb.NewTuple({data4l, data1r}),
            pb.NewTuple({data4l, data2r}),
            pb.NewTuple({data4l, data3r}),
            pb.NewTuple({data4l, data4r}),
            pb.NewTuple({data4l, data5r}),

            pb.NewTuple({data5l, data1r}),
            pb.NewTuple({data5l, data2r}),
            pb.NewTuple({data5l, data3r}),
            pb.NewTuple({data5l, data4r}),
            pb.NewTuple({data5l, data5r}),
        });

        const auto pgmReturn = pb.Map(list,
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

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
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
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(!item.GetElement(5).template Get<bool>()); // >=

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

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestComparesWithIntegral) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto optType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i64>::Id));

        const auto data1l = pb.NewOptional(pb.NewDataLiteral<i64>(-7LL));
        const auto data2l = pb.NewOptional(pb.NewDataLiteral<i64>(3LL));
        const auto data3l = pb.NewEmptyOptional(optType);
        const auto data4l = pb.NewOptional(pb.NewDataLiteral<i64>(std::numeric_limits<i64>::min()));
        const auto data5l = pb.NewOptional(pb.NewDataLiteral<i64>(std::numeric_limits<i64>::max()));

        const auto data1r = pb.NewDecimalLiteral(-7000000000000000000LL, 20, 18);
        const auto data2r = pb.NewDecimalLiteral(3000000000000000000LL, 20, 18);
        const auto data3r = pb.NewDecimalLiteral(NYql::NDecimal::Nan(), 20, 18);
        const auto data4r = pb.NewDecimalLiteral(-NYql::NDecimal::Inf(), 20, 18);
        const auto data5r = pb.NewDecimalLiteral(+NYql::NDecimal::Inf(), 20, 18);

        auto pairType = pb.NewTupleType({optType, pb.NewDecimalType(20, 18)});
        const auto list = pb.NewList(pairType, {
            pb.NewTuple({data1l, data1r}),
            pb.NewTuple({data1l, data2r}),
            pb.NewTuple({data1l, data3r}),
            pb.NewTuple({data1l, data4r}),
            pb.NewTuple({data1l, data5r}),

            pb.NewTuple({data2l, data1r}),
            pb.NewTuple({data2l, data2r}),
            pb.NewTuple({data2l, data3r}),
            pb.NewTuple({data2l, data4r}),
            pb.NewTuple({data2l, data5r}),

            pb.NewTuple({data3l, data1r}),
            pb.NewTuple({data3l, data2r}),
            pb.NewTuple({data3l, data3r}),
            pb.NewTuple({data3l, data4r}),
            pb.NewTuple({data3l, data5r}),

            pb.NewTuple({data4l, data1r}),
            pb.NewTuple({data4l, data2r}),
            pb.NewTuple({data4l, data3r}),
            pb.NewTuple({data4l, data4r}),
            pb.NewTuple({data4l, data5r}),

            pb.NewTuple({data5l, data1r}),
            pb.NewTuple({data5l, data2r}),
            pb.NewTuple({data5l, data3r}),
            pb.NewTuple({data5l, data4r}),
            pb.NewTuple({data5l, data5r}),
        });

        const auto pgmReturn = pb.Map(list,
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

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
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
        UNIT_ASSERT(!item.GetElement(4).template Get<bool>()); // >
        UNIT_ASSERT(!item.GetElement(5).template Get<bool>()); // >=

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

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestAggrCompares) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDecimalLiteral(-7, 10, 0);
        const auto data2 = pb.NewDecimalLiteral(3, 10, 0);
        const auto data3 = pb.NewDecimalLiteral(NYql::NDecimal::Nan(), 10, 0);
        const auto data4 = pb.NewDecimalLiteral(-NYql::NDecimal::Inf(), 10, 0);
        const auto data5 = pb.NewDecimalLiteral(+NYql::NDecimal::Inf(), 10, 0);

        auto pairType = pb.NewTupleType({pb.NewDecimalType(10, 0), pb.NewDecimalType(10, 0)});
        const auto list = pb.NewList(pairType, {
            pb.NewTuple({data1, data1}),
            pb.NewTuple({data1, data2}),
            pb.NewTuple({data1, data3}),
            pb.NewTuple({data1, data4}),
            pb.NewTuple({data1, data5}),

            pb.NewTuple({data2, data1}),
            pb.NewTuple({data2, data2}),
            pb.NewTuple({data2, data3}),
            pb.NewTuple({data2, data4}),
            pb.NewTuple({data2, data5}),

            pb.NewTuple({data3, data1}),
            pb.NewTuple({data3, data2}),
            pb.NewTuple({data3, data3}),
            pb.NewTuple({data3, data4}),
            pb.NewTuple({data3, data5}),

            pb.NewTuple({data4, data1}),
            pb.NewTuple({data4, data2}),
            pb.NewTuple({data4, data3}),
            pb.NewTuple({data4, data4}),
            pb.NewTuple({data4, data5}),

            pb.NewTuple({data5, data1}),
            pb.NewTuple({data5, data2}),
            pb.NewTuple({data5, data3}),
            pb.NewTuple({data5, data4}),
            pb.NewTuple({data5, data5}),
        });

        const auto pgmReturn = pb.Map(list,
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

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
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

    Y_UNIT_TEST_LLVM(TestIncDec) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDecimalLiteral(-NYql::NDecimal::Inf(), 4, 1);
        const auto data2 = pb.NewDecimalLiteral(-9999, 4, 1);
        const auto data3 = pb.NewDecimalLiteral(-7, 4, 1);
        const auto data4 = pb.NewDecimalLiteral(0, 4, 1);
        const auto data5 = pb.NewDecimalLiteral(13, 4, 1);
        const auto data6 = pb.NewDecimalLiteral(9999, 4, 1);
        const auto data7 = pb.NewDecimalLiteral(+NYql::NDecimal::Inf(), 4, 1);
        const auto data8 = pb.NewDecimalLiteral(NYql::NDecimal::Nan(), 4, 1);

        const auto list = pb.NewList(pb.NewDecimalType(4, 1), {data1, data2, data3, data4, data5, data6, data7, data8});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.NewTuple({pb.Increment(item), pb.Decrement(item)});
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == -NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == -9998);
        UNIT_ASSERT(item.GetElement(1).GetInt128() == -NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == -6);
        UNIT_ASSERT(item.GetElement(1).GetInt128() == -8);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == +1);
        UNIT_ASSERT(item.GetElement(1).GetInt128() == -1);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == 14);
        UNIT_ASSERT(item.GetElement(1).GetInt128() == 12);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == +NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == 9998);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == +NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == +NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == NYql::NDecimal::Nan());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == NYql::NDecimal::Nan());

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestMinusAbs) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDecimalLiteral(-NYql::NDecimal::Nan(), 10, 1);
        const auto data1 = pb.NewDecimalLiteral(-NYql::NDecimal::Inf(), 10, 1);
        const auto data3 = pb.NewDecimalLiteral(-7, 10, 1);
        const auto data4 = pb.NewDecimalLiteral(0, 10, 1);
        const auto data5 = pb.NewDecimalLiteral(13, 10, 1);
        const auto data7 = pb.NewDecimalLiteral(+NYql::NDecimal::Inf(), 10, 1);
        const auto data8 = pb.NewDecimalLiteral(+NYql::NDecimal::Nan(), 10, 1);

        const auto list = pb.NewList(pb.NewDecimalType(10, 1), {data0, data1, data3, data4, data5, data7, data8});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.NewTuple({pb.Minus(item), pb.Abs(item)});
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == -NYql::NDecimal::Nan());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == NYql::NDecimal::Nan());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == 7);
        UNIT_ASSERT(item.GetElement(1).GetInt128() == 7);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == 0);
        UNIT_ASSERT(item.GetElement(1).GetInt128() == 0);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == -13);
        UNIT_ASSERT(item.GetElement(1).GetInt128() == +13);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == -NYql::NDecimal::Inf());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == +NYql::NDecimal::Inf());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0).GetInt128() == NYql::NDecimal::Nan());
        UNIT_ASSERT(item.GetElement(1).GetInt128() == NYql::NDecimal::Nan());

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFromString) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<NUdf::EDataSlot::String>("0.0");
        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("NAN");
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("1.0");
        const auto data3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("-.1");
        const auto data4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("3.1415926");
        const auto data5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("+inf");
        const auto data6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("-INF");
        const auto data7 = pb.NewDataLiteral<NUdf::EDataSlot::String>(".123E+2");
        const auto data8 = pb.NewDataLiteral<NUdf::EDataSlot::String>("56.78e-3");
        const auto type = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto list = pb.NewList(type, {data0, data1, data2, data3, data4, data5, data6, data7, data8});

        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            return pb.StrictFromString(item, pb.NewDecimalType(10, 7));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == NYql::NDecimal::Nan());
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 10000000);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -1000000);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 31415926);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == +NYql::NDecimal::Inf());
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == -NYql::NDecimal::Inf());
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 123000000);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetInt128() == 567800);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestToString) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDecimalLiteral(0, 10, 7);
        const auto data1 = pb.NewDecimalLiteral(NYql::NDecimal::Nan(), 10, 7);
        const auto data2 = pb.NewDecimalLiteral(10000000, 10, 7);
        const auto data3 = pb.NewDecimalLiteral(-1000000, 10, 7);
        const auto data4 = pb.NewDecimalLiteral(31415926, 10, 7);
        const auto data5 = pb.NewDecimalLiteral(+NYql::NDecimal::Inf(), 10, 7);
        const auto data6 = pb.NewDecimalLiteral(-NYql::NDecimal::Inf(), 10, 7);
        const auto type = pb.NewDecimalType(10, 7);
        const auto list = pb.NewList(type, {data0, data1, data2, data3, data4, data5, data6});

        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            return pb.ToString(item);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "0");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "nan");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "1");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "-0.1");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "3.1415926");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "inf");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "-inf");
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFromStringToDouble) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto type = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto data0 = pb.NewDataLiteral<NUdf::EDataSlot::String>("0");
        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("+3.332873");
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("-3.332873");
        const auto data3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("+3.1415926");
        const auto data4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("-3.1415926");

        const auto list = pb.NewList(type, {data0, data1, data2, data3, data4});

        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            return pb.Convert(pb.FromString(item, pb.NewDecimalType(35,25)), pb.NewDataType(NUdf::TDataType<double>::Id));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();

        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<double>(), 0.);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<double>(), +3.332873);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<double>(), -3.332873);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<double>(), +3.1415926);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<double>(), -3.1415926);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFromUtf8ToFloat) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto type = pb.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id);
        const auto data0 = pb.NewDataLiteral<NUdf::EDataSlot::Utf8>("0");
        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::Utf8>("+24.75");
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::Utf8>("-24.75");
        const auto data3 = pb.NewDataLiteral<NUdf::EDataSlot::Utf8>("+42.42");
        const auto data4 = pb.NewDataLiteral<NUdf::EDataSlot::Utf8>("-42.42");

        const auto list = pb.NewList(type, {data0, data1, data2, data3, data4});

        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            return pb.Convert(pb.FromString(item, pb.NewDecimalType(35,25)), pb.NewDataType(NUdf::TDataType<float>::Id));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();

        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<float>(), 0.f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<float>(), +24.75f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<float>(), -24.75f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<float>(), +42.42f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<float>(), -42.42f);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }
}

}
}
