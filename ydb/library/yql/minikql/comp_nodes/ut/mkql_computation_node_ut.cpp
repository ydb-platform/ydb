#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/mkql_node_printer.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <cfloat>
#include <utility>
#include <random>

namespace NKikimr {
namespace NMiniKQL {

namespace {

constexpr auto TotalSambles =
#ifndef NDEBUG
222222U;
#else
22222222ULL;
#endif

}

std::vector<std::pair<i8, double>> MakeSamples() {
    std::default_random_engine eng;
    std::uniform_int_distribution<int> keys(-100, +100);
    std::uniform_real_distribution<double> unif(-999.0, +999.0);

    std::vector<std::pair<i8, double>> samples(TotalSambles);

    eng.seed(std::time(nullptr));
    std::generate(samples.begin(), samples.end(), std::bind(&std::make_pair<i8, double>, std::bind(std::move(keys), std::move(eng)), std::bind(std::move(unif), std::move(eng))));
    return samples;
}

std::vector<std::pair<ui16, double>> MakeOtherSamples() {
    std::default_random_engine eng;
    std::uniform_int_distribution<ui16> keys(0U, 65535U);
    std::uniform_real_distribution<double> unif(-999.0, +999.0);

    std::vector<std::pair<ui16, double>> samples(TotalSambles);

    eng.seed(std::time(nullptr));
    std::generate(samples.begin(), samples.end(), std::bind(&std::make_pair<ui16, double>, std::bind(std::move(keys), std::move(eng)), std::bind(std::move(unif), std::move(eng))));
    return samples;
}

std::vector<std::tuple<ui64, std::string, std::string, double, double, double, double>> MakeTpchSamples() {
    std::default_random_engine eng;
    std::uniform_int_distribution<ui64> dates(694303200000000ULL, 9124596000000005ULL);
    std::uniform_int_distribution<int> keys(0U, 3U);
    std::uniform_real_distribution<double> prices(900., 105000.0);
    std::uniform_real_distribution<double> taxes(0., 0.08);
    std::uniform_real_distribution<double> discs(0., 0.1);
    std::uniform_real_distribution<double> qntts(1., 50.);

    std::random_device rd;
    std::mt19937 gen(rd());

    std::vector<std::tuple<ui64, std::string, std::string, double, double, double, double>> samples(TotalSambles);

    eng.seed(std::time(nullptr));

    std::generate(samples.begin(), samples.end(), [&]() {
        switch(keys(gen)) {
            case 0U: return std::make_tuple(dates(gen), "N", "O", prices(gen), taxes(gen), discs(gen), qntts(gen));
            case 1U: return std::make_tuple(dates(gen), "A", "F", prices(gen), taxes(gen), discs(gen), qntts(gen));
            case 2U: return std::make_tuple(dates(gen), "N", "F", prices(gen), taxes(gen), discs(gen), qntts(gen));
            case 3U: return std::make_tuple(dates(gen), "R", "F", prices(gen), taxes(gen), discs(gen), qntts(gen));
        }
        Y_ABORT("Unexpected");
    });
    return samples;
}

extern const std::vector<std::pair<i8, double>> I8Samples = MakeSamples();
extern const std::vector<std::pair<ui16, double>> Ui16Samples = MakeOtherSamples();
extern const std::vector<std::tuple<ui64, std::string, std::string, double, double, double, double>> TpchSamples = MakeTpchSamples();

Y_UNIT_TEST_SUITE(TMiniKQLComputationNodeTest) {
    Y_UNIT_TEST_LLVM(TestEmbeddedByteAt) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<ui32>(0);
        const auto data1 = pb.NewDataLiteral<ui32>(3);
        const auto data2 = pb.NewDataLiteral<ui32>(9);
        const auto data3 = pb.NewDataLiteral<ui32>(13);
        const auto data4 = pb.NewDataLiteral<ui32>(Max<ui32>());

        const auto str = pb.NewDataLiteral<NUdf::EDataSlot::String>("0123456789AB");
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.ByteAt(str, item);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 0x30);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 0x33);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 0x39);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestStringByteAt) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<ui32>(0);
        const auto data1 = pb.NewDataLiteral<ui32>(3);
        const auto data2 = pb.NewDataLiteral<ui32>(21);
        const auto data3 = pb.NewDataLiteral<ui32>(22);
        const auto data4 = pb.NewDataLiteral<ui32>(Max<ui32>());

        const auto str = pb.NewDataLiteral<NUdf::EDataSlot::String>("0123456789ABCDEFGHIJKL");
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.ByteAt(str, item);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 0x30);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 0x33);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 0x4C);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestCountBitsUI8) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<ui8>(0x00);
        const auto data1 = pb.NewDataLiteral<ui8>(0x01);
        const auto data2 = pb.NewDataLiteral<ui8>(0x10);
        const auto data3 = pb.NewDataLiteral<ui8>(0x13);
        const auto data4 = pb.NewDataLiteral<ui8>(0xF0);
        const auto data5 = pb.NewDataLiteral<ui8>(0x0F);
        const auto data6 = pb.NewDataLiteral<ui8>(0x55);
        const auto data7 = pb.NewDataLiteral<ui8>(0xAA);
        const auto data8 = pb.NewDataLiteral<ui8>(0x7F);
        const auto data9 = pb.NewDataLiteral<ui8>(0xFF);
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui8>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.CountBits(item);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 7);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 8);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestCountBitsUI16) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<ui16>(0x0000);
        const auto data1 = pb.NewDataLiteral<ui16>(0x0100);
        const auto data2 = pb.NewDataLiteral<ui16>(0x0010);
        const auto data3 = pb.NewDataLiteral<ui16>(0x0130);
        const auto data4 = pb.NewDataLiteral<ui16>(0xF000);
        const auto data5 = pb.NewDataLiteral<ui16>(0x0F00);
        const auto data6 = pb.NewDataLiteral<ui16>(0x0550);
        const auto data7 = pb.NewDataLiteral<ui16>(0xA00A);
        const auto data8 = pb.NewDataLiteral<ui16>(0x700F);
        const auto data9 = pb.NewDataLiteral<ui16>(0x0FF0);
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui16>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.CountBits(item);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 7);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 8);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestShiftLeft) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<ui8>(0);
        const auto data1 = pb.NewDataLiteral<ui8>(1);
        const auto data2 = pb.NewDataLiteral<ui8>(7);
        const auto data3 = pb.NewDataLiteral<ui8>(10);
        const auto data4 = pb.NewDataLiteral<ui8>(30);
        const auto data5 = pb.NewDataLiteral<ui8>(32);
        const auto data6 = pb.NewDataLiteral<ui8>(40);
        const auto data7 = pb.NewDataLiteral<ui8>(57);
        const auto data8 = pb.NewDataLiteral<ui8>(200);
        const auto data9 = pb.NewDataLiteral<ui8>(255);
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui8>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.ShiftLeft(pb.NewDataLiteral<ui32>(0x830500F1U), item);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0x830500F1U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0x060A01E2U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0x82807880U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0x1403C400U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0x40000000U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0U);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestShiftRight) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<ui8>(0);
        const auto data1 = pb.NewDataLiteral<ui8>(1);
        const auto data2 = pb.NewDataLiteral<ui8>(7);
        const auto data3 = pb.NewDataLiteral<ui8>(10);
        const auto data4 = pb.NewDataLiteral<ui8>(30);
        const auto data5 = pb.NewDataLiteral<ui8>(32);
        const auto data6 = pb.NewDataLiteral<ui8>(40);
        const auto data7 = pb.NewDataLiteral<ui8>(57);
        const auto data8 = pb.NewDataLiteral<ui8>(200);
        const auto data9 = pb.NewDataLiteral<ui8>(255);
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui8>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.ShiftRight(pb.NewDataLiteral<ui32>(0x830500F1U), item);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0x830500F1U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0x41828078U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0x01060A01U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0x20C140U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0x2U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0U);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestRotLeft) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<ui8>(0);
        const auto data1 = pb.NewDataLiteral<ui8>(1);
        const auto data2 = pb.NewDataLiteral<ui8>(7);
        const auto data3 = pb.NewDataLiteral<ui8>(10);
        const auto data4 = pb.NewDataLiteral<ui8>(30);
        const auto data5 = pb.NewDataLiteral<ui8>(32);
        const auto data6 = pb.NewDataLiteral<ui8>(40);
        const auto data7 = pb.NewDataLiteral<ui8>(57);
        const auto data8 = pb.NewDataLiteral<ui8>(200);
        const auto data9 = pb.NewDataLiteral<ui8>(255);
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui8>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.RotLeft(pb.NewDataLiteral<ui16>(0x87F5U), item);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 0x87F5U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 0x0FEBU);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 0xFAC3U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 0xD61FU);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 0x61FDU);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 0x87F5U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 0xF587U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 0xEB0FU);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 0xF587U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 0xC3FAU);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestRotRight) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<ui8>(0);
        const auto data1 = pb.NewDataLiteral<ui8>(1);
        const auto data2 = pb.NewDataLiteral<ui8>(7);
        const auto data3 = pb.NewDataLiteral<ui8>(10);
        const auto data4 = pb.NewDataLiteral<ui8>(30);
        const auto data5 = pb.NewDataLiteral<ui8>(32);
        const auto data6 = pb.NewDataLiteral<ui8>(40);
        const auto data7 = pb.NewDataLiteral<ui8>(57);
        const auto data8 = pb.NewDataLiteral<ui8>(200);
        const auto data9 = pb.NewDataLiteral<ui8>(255);
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui8>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.RotRight(pb.NewDataLiteral<ui16>(0x87F5U), item);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 0x87F5U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 0xC3FAU);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 0xEB0FU);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 0xFD61U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 0x1FD6U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 0x87F5U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 0xF587U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 0xFAC3U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 0xF587U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 0x0FEBU);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestConvertIntToBool) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<i64>(0);
        const auto data1 = pb.NewDataLiteral<i64>(1);
        const auto data2 = pb.NewDataLiteral<i64>(2);
        const auto data3 = pb.NewDataLiteral<i64>(1024);
        const auto data4 = pb.NewDataLiteral<i64>(-1);
        const auto dataType = pb.NewDataType(NUdf::TDataType<i64>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.Convert(item, pb.NewDataType(NUdf::TDataType<bool>::Id));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.template Get<bool>());
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.template Get<bool>());
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.template Get<bool>());
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.template Get<bool>());
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.template Get<bool>());
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFloatAbs) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<float>::Id);
        const auto data1 = pb.NewDataLiteral<float>(11.433f);
        const auto data2 = pb.NewDataLiteral<float>(-3.14f);
        const auto data3 = pb.NewDataLiteral<float>(0.0f);
        const auto data4 = pb.NewDataLiteral<float>(-HUGE_VALF);
        const auto list = pb.NewList(dataType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.Abs(item);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), 11.433f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), 3.14f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), 0.0f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), HUGE_VALF);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestIntegerAbs) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<i32>::Id);
        const auto data1 = pb.NewDataLiteral<i32>(1334);
        const auto data2 = pb.NewDataLiteral<i32>(-4378);
        const auto data3 = pb.NewDataLiteral<i32>(0);
        const auto data4 = pb.NewDataLiteral<i32>(std::numeric_limits<i32>::min());
        const auto list = pb.NewList(dataType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.Abs(item);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 1334);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 4378);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), std::numeric_limits<i32>::min());
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestToIntegral) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto ten = pb.ToIntegral(pb.NewDataLiteral(float(10.0)), pb.NewDataType(NUdf::TDataType<i32>::Id, true));
        const auto two = pb.ToIntegral(pb.NewDataLiteral(float(2.0)), pb.NewDataType(NUdf::TDataType<ui32>::Id, true));
        const auto pgmReturn = pb.NewTuple({ten, two});

        const auto graph = setup.BuildGraph(pgmReturn);
        UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetElement(0).template Get<i32>(), 10);
        UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetElement(1).template Get<ui32>(), 2U);
    }

    Y_UNIT_TEST_LLVM(TestFloatToI16) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral(0.0f);
        const auto data1 = pb.NewDataLiteral(0.0f*HUGE_VALF);
        const auto data2 = pb.NewDataLiteral(-3.14f);
        const auto data3 = pb.NewDataLiteral(12121324.0f);
        const auto data4 = pb.NewDataLiteral(-7898.8f);
        const auto data5 = pb.NewDataLiteral(210000.0f);
        const auto data6 = pb.NewDataLiteral(HUGE_VALF);
        const auto data7 = pb.NewDataLiteral(-HUGE_VALF);
        const auto data8 = pb.NewDataLiteral(-HUGE_VALF);
        const auto data9 = pb.NewDataLiteral(FLT_MIN/2.0f);
        const auto type = pb.NewDataType(NUdf::TDataType<float>::Id);
        const auto list = pb.NewList(type, {data0, data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            return pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<i16>::Id, true));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i16>(), 0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i16>(), -3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i16>(), -7898);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i16>(), 0);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestDoubleToBool) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral(0.0);
        const auto data1 = pb.NewDataLiteral(0.0*HUGE_VAL);
        const auto data2 = pb.NewDataLiteral(-3.14);
        const auto data3 = pb.NewDataLiteral(12121324.0);
        const auto data4 = pb.NewDataLiteral(-7898.8);
        const auto data5 = pb.NewDataLiteral(210000.0);
        const auto data6 = pb.NewDataLiteral(HUGE_VAL);
        const auto data7 = pb.NewDataLiteral(-HUGE_VAL);
        const auto data8 = pb.NewDataLiteral(-HUGE_VAL);
        const auto data9 = pb.NewDataLiteral(DBL_MIN/2.0);
        const auto type = pb.NewDataType(NUdf::TDataType<double>::Id);
        const auto list = pb.NewList(type, {data0, data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            return pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<bool>::Id, true));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestDoubleToUI32) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral(0.0);
        const auto data1 = pb.NewDataLiteral(0.0*HUGE_VAL);
        const auto data2 = pb.NewDataLiteral(-3.14);
        const auto data3 = pb.NewDataLiteral(12121324.0);
        const auto data4 = pb.NewDataLiteral(-7898.8);
        const auto data5 = pb.NewDataLiteral(210000.0);
        const auto data6 = pb.NewDataLiteral(HUGE_VAL);
        const auto data7 = pb.NewDataLiteral(-HUGE_VAL);
        const auto data8 = pb.NewDataLiteral(-HUGE_VAL);
        const auto data9 = pb.NewDataLiteral(DBL_MIN/2.0);
        const auto type = pb.NewDataType(NUdf::TDataType<double>::Id);
        const auto list = pb.NewList(type, {data0, data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            return pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<ui32>::Id, true));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 12121324U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 210000U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0U);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestUI64ToIntegral) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
        const auto data0 = pb.NewDataLiteral<ui64>(0U);
        const auto data1 = pb.NewDataLiteral<ui64>(1U);
        const auto data2 = pb.NewDataLiteral<ui64>(std::numeric_limits<i8>::max());
        const auto data3 = pb.NewDataLiteral<ui64>(std::numeric_limits<ui8>::max());
        const auto data4 = pb.NewDataLiteral<ui64>(std::numeric_limits<i16>::max());
        const auto data5 = pb.NewDataLiteral<ui64>(std::numeric_limits<ui16>::max());
        const auto data6 = pb.NewDataLiteral<ui64>(std::numeric_limits<i32>::max());
        const auto data7 = pb.NewDataLiteral<ui64>(std::numeric_limits<ui32>::max());
        const auto data8 = pb.NewDataLiteral<ui64>(std::numeric_limits<i64>::max());
        const auto data9 = pb.NewDataLiteral<ui64>(std::numeric_limits<ui64>::max());

        const auto list = pb.NewList(type, {data0, data1, data2, data3, data4, data5, data6, data7, data8, data9});

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

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[0].Get<i8>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[1].Get<ui8>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[2].Get<i16>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[3].Get<ui16>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[5].Get<ui32>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<i64>(), 1);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[0].Get<i8>(), std::numeric_limits<i8>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[1].Get<ui8>(), std::numeric_limits<i8>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[2].Get<i16>(), std::numeric_limits<i8>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[3].Get<ui16>(), std::numeric_limits<i8>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), std::numeric_limits<i8>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[5].Get<ui32>(), std::numeric_limits<i8>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<i64>(), std::numeric_limits<i8>::max());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[1].Get<ui8>(), std::numeric_limits<ui8>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[2].Get<i16>(), std::numeric_limits<ui8>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[3].Get<ui16>(), std::numeric_limits<ui8>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), std::numeric_limits<ui8>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[5].Get<ui32>(), std::numeric_limits<ui8>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<i64>(), std::numeric_limits<ui8>::max());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[2].Get<i16>(), std::numeric_limits<i16>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[3].Get<ui16>(), std::numeric_limits<i16>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), std::numeric_limits<i16>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[5].Get<ui32>(), std::numeric_limits<i16>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<i64>(), std::numeric_limits<i16>::max());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT(!item.GetElements()[2]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[3].Get<ui16>(), std::numeric_limits<ui16>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), std::numeric_limits<ui16>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[5].Get<ui32>(), std::numeric_limits<ui16>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<i64>(), std::numeric_limits<ui16>::max());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT(!item.GetElements()[2]);
        UNIT_ASSERT(!item.GetElements()[3]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), std::numeric_limits<i32>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[5].Get<ui32>(), std::numeric_limits<i32>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<i64>(), std::numeric_limits<i32>::max());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT(!item.GetElements()[2]);
        UNIT_ASSERT(!item.GetElements()[3]);
        UNIT_ASSERT(!item.GetElements()[4]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[5].Get<ui32>(), std::numeric_limits<ui32>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<i64>(), std::numeric_limits<ui32>::max());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT(!item.GetElements()[2]);
        UNIT_ASSERT(!item.GetElements()[3]);
        UNIT_ASSERT(!item.GetElements()[4]);
        UNIT_ASSERT(!item.GetElements()[5]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<i64>(), std::numeric_limits<i64>::max());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT(!item.GetElements()[2]);
        UNIT_ASSERT(!item.GetElements()[3]);
        UNIT_ASSERT(!item.GetElements()[4]);
        UNIT_ASSERT(!item.GetElements()[5]);
        UNIT_ASSERT(!item.GetElements()[6]);

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestI64ToIntegral) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto type = pb.NewDataType(NUdf::TDataType<i64>::Id);
        const auto data0 = pb.NewDataLiteral<i64>(0);
        const auto data1 = pb.NewDataLiteral<i64>(-1);
        const auto data2 = pb.NewDataLiteral<i64>(std::numeric_limits<i8>::min());
        const auto data3 = pb.NewDataLiteral<i64>(std::numeric_limits<i8>::max());
        const auto data4 = pb.NewDataLiteral<i64>(std::numeric_limits<i16>::min());
        const auto data5 = pb.NewDataLiteral<i64>(std::numeric_limits<i16>::max());
        const auto data6 = pb.NewDataLiteral<i64>(std::numeric_limits<i32>::min());
        const auto data7 = pb.NewDataLiteral<i64>(std::numeric_limits<i32>::max());
        const auto data8 = pb.NewDataLiteral<i64>(std::numeric_limits<i64>::min());
        const auto data9 = pb.NewDataLiteral<i64>(std::numeric_limits<i64>::max());

        const auto list = pb.NewList(type, {data0, data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            return pb.NewTuple({
                pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<i8>::Id, true)),
                pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<ui8>::Id, true)),
                pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<i16>::Id, true)),
                pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<ui16>::Id, true)),
                pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<i32>::Id, true)),
                pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<ui32>::Id, true)),
                pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<ui64>::Id, true)),
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
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<ui64>(), 0);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[0].Get<i8>(), -1);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[2].Get<i16>(), -1);
        UNIT_ASSERT(!item.GetElements()[3]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), -1);
        UNIT_ASSERT(!item.GetElements()[5]);
        UNIT_ASSERT(!item.GetElements()[6]);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[0].Get<i8>(), std::numeric_limits<i8>::min());
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[2].Get<i16>(), std::numeric_limits<i8>::min());
        UNIT_ASSERT(!item.GetElements()[3]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), std::numeric_limits<i8>::min());
        UNIT_ASSERT(!item.GetElements()[5]);
        UNIT_ASSERT(!item.GetElements()[6]);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[0].Get<i8>(), std::numeric_limits<i8>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[1].Get<ui8>(), std::numeric_limits<i8>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[2].Get<i16>(), std::numeric_limits<i8>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[3].Get<ui16>(), std::numeric_limits<i8>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), std::numeric_limits<i8>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[5].Get<ui32>(), std::numeric_limits<i8>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<ui64>(), std::numeric_limits<i8>::max());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[2].Get<i16>(), std::numeric_limits<i16>::min());
        UNIT_ASSERT(!item.GetElements()[3]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), std::numeric_limits<i16>::min());
        UNIT_ASSERT(!item.GetElements()[5]);
        UNIT_ASSERT(!item.GetElements()[6]);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[2].Get<i16>(), std::numeric_limits<i16>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[3].Get<ui16>(), std::numeric_limits<i16>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), std::numeric_limits<i16>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[5].Get<ui32>(), std::numeric_limits<i16>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<ui64>(), std::numeric_limits<i16>::max());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT(!item.GetElements()[2]);
        UNIT_ASSERT(!item.GetElements()[3]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), std::numeric_limits<i32>::min());
        UNIT_ASSERT(!item.GetElements()[5]);
        UNIT_ASSERT(!item.GetElements()[6]);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT(!item.GetElements()[2]);
        UNIT_ASSERT(!item.GetElements()[3]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[4].Get<i32>(), std::numeric_limits<i32>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[5].Get<ui32>(), std::numeric_limits<i32>::max());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<ui64>(), std::numeric_limits<i32>::max());

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT(!item.GetElements()[2]);
        UNIT_ASSERT(!item.GetElements()[3]);
        UNIT_ASSERT(!item.GetElements()[4]);
        UNIT_ASSERT(!item.GetElements()[5]);
        UNIT_ASSERT(!item.GetElements()[6]);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElements()[0]);
        UNIT_ASSERT(!item.GetElements()[1]);
        UNIT_ASSERT(!item.GetElements()[2]);
        UNIT_ASSERT(!item.GetElements()[3]);
        UNIT_ASSERT(!item.GetElements()[4]);
        UNIT_ASSERT(!item.GetElements()[5]);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElements()[6].Get<ui64>(), std::numeric_limits<i64>::max());

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFloatFromString) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<NUdf::EDataSlot::String>("0.0");
        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("NAN");
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("-3.14");
        const auto data3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("1212.00");
        const auto data4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("-7898.8");
        const auto data5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("21E4");
        const auto data6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("+inf");
        const auto data7 = pb.NewDataLiteral<NUdf::EDataSlot::String>("-INF");
        const auto type = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto list = pb.NewList(type, {data0, data1, data2, data3, data4, data5, data6, data7});

        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            return pb.StrictFromString(item, pb.NewDataType(NUdf::TDataType<float>::Id));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), 0.f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(std::isnan(item.template Get<float>()));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), -3.14f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), 1212.f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), -7898.8f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), 210000.f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), HUGE_VALF);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), -HUGE_VALF);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestDoubleFromString) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<NUdf::EDataSlot::String>("0.0");
        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("NAN");
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("-3.14");
        const auto data3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("12121324.00");
        const auto data4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("-7898.8");
        const auto data5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("21E4");
        const auto data6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("+inf");
        const auto data7 = pb.NewDataLiteral<NUdf::EDataSlot::String>("-INF");
        const auto type = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto list = pb.NewList(type, {data0, data1, data2, data3, data4, data5, data6, data7});

        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            return pb.StrictFromString(item, pb.NewDataType(NUdf::TDataType<double>::Id));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 0.0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(std::isnan(item.template Get<double>()));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), -3.14);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 12121324.0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), -7898.8);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 210000.0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), HUGE_VAL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), -HUGE_VAL);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFloatToString) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral(0.0f);
        const auto data1 = pb.NewDataLiteral(0.0f*HUGE_VALF);
        const auto data2 = pb.NewDataLiteral(-3.14f);
        const auto data3 = pb.NewDataLiteral(1212.0f);
        const auto data4 = pb.NewDataLiteral(-7898.8f);
        const auto data5 = pb.NewDataLiteral(210000.0f);
        const auto data6 = pb.NewDataLiteral(HUGE_VALF);
        const auto data7 = pb.NewDataLiteral(-HUGE_VALF);
        const auto type = pb.NewDataType(NUdf::TDataType<float>::Id);
        const auto list = pb.NewList(type, {data0, data1, data2, data3, data4, data5, data6, data7});

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
        UNBOXED_VALUE_STR_EQUAL(item, "-3.14");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "1212");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "-7898.8");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "210000");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "inf");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "-inf");
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestInt64ToTimestamp) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral(i64(0LL));
        const auto data1 = pb.NewDataLiteral(i64(1LL));
        const auto data2 = pb.NewDataLiteral(i64(-1LL));
        const auto data3 = pb.NewDataLiteral(std::numeric_limits<i64>::min());
        const auto data4 = pb.NewDataLiteral(std::numeric_limits<i64>::max());
        const auto data5 = pb.NewDataLiteral(i64(4291747200000000LL));
        const auto data6 = pb.NewDataLiteral(i64(4291747199999999LL));
        const auto type = pb.NewDataType(NUdf::TDataType<i64>::Id);
        const auto list = pb.NewList(type, {data0, data1, data2, data3, data4, data5, data6});

        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            return pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<NUdf::TTimestamp>::Id, true));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 0ULL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 1ULL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 4291747199999999ULL);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFloatConvertToUint32) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral(0.0f);
        const auto data3 = pb.NewDataLiteral(1212.0f);
        const auto data5 = pb.NewDataLiteral(210000.0f);
        const auto type = pb.NewDataType(NUdf::TDataType<float>::Id);
        const auto list = pb.NewList(type, {data0, data3, data5});

        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            return pb.Convert(item, pb.NewDataType(NUdf::TDataType<ui32>::Id));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 1212U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 210000U);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFloatConvertToInt32) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral(0.0f);
        const auto data2 = pb.NewDataLiteral(-3.14f);
        const auto data3 = pb.NewDataLiteral(1212.0f);
        const auto data4 = pb.NewDataLiteral(-7898.8f);
        const auto data5 = pb.NewDataLiteral(210000.0f);
        const auto type = pb.NewDataType(NUdf::TDataType<float>::Id);
        const auto list = pb.NewList(type, {data0, data2, data3, data4, data5});

        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            return pb.Convert(item, pb.NewDataType(NUdf::TDataType<i32>::Id));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 1212);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), -7898);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 210000);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestDoubleConvertToInt64) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral(0.0);
        const auto data2 = pb.NewDataLiteral(-3.14);
        const auto data3 = pb.NewDataLiteral(1212.0);
        const auto data4 = pb.NewDataLiteral(-7898.8);
        const auto data5 = pb.NewDataLiteral(210000.0);
        const auto type = pb.NewDataType(NUdf::TDataType<double>::Id);
        const auto list = pb.NewList(type, {data0, data2, data3, data4, data5});

        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            return pb.Convert(item, pb.NewDataType(NUdf::TDataType<i64>::Id));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i64>(), 0LL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i64>(), -3LL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i64>(), 1212LL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i64>(), -7898LL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i64>(), 210000LL);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestDoubleToInt64) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral(0.0);
        const auto data2 = pb.NewDataLiteral(-3.14);
        const auto data3 = pb.NewDataLiteral(1212.0);
        const auto data4 = pb.NewDataLiteral(-7898.8);
        const auto data5 = pb.NewDataLiteral(210000.0);
        const auto data6 = pb.NewDataLiteral(9223372036854776000.0);
        const auto type = pb.NewDataType(NUdf::TDataType<double>::Id);
        const auto list = pb.NewList(type, {data0, data2, data3, data4, data5, data6});

        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            return pb.ToIntegral(item, pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i64>::Id)));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i64>(), 0LL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i64>(), -3LL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i64>(), 1212LL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i64>(), -7898LL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i64>(), 210000LL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestDoubleToString) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral(0.0);
        const auto data1 = pb.NewDataLiteral(0.0*HUGE_VAL);
        const auto data2 = pb.NewDataLiteral(-3.14);
        const auto data3 = pb.NewDataLiteral(12121324.0);
        const auto data4 = pb.NewDataLiteral(-7898.8);
        const auto data5 = pb.NewDataLiteral(210000.0);
        const auto data6 = pb.NewDataLiteral(HUGE_VAL);
        const auto data7 = pb.NewDataLiteral(-HUGE_VAL);
        const auto type = pb.NewDataType(NUdf::TDataType<double>::Id);
        const auto list = pb.NewList(type, {data0, data1, data2, data3, data4, data5, data6, data7});

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
        UNBOXED_VALUE_STR_EQUAL(item, "-3.14");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "12121324");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "-7898.8");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "210000");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "inf");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "-inf");
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestNanvl) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewOptional(pb.NewDataLiteral(0.0f));
        const auto data1 = pb.NewOptional(pb.NewDataLiteral(0.0f*HUGE_VALF));
        const auto data2 = pb.NewOptional(pb.NewDataLiteral(HUGE_VALF));
        const auto data3 = pb.NewOptional(pb.NewDataLiteral(-HUGE_VALF));
        const auto data4 = pb.NewOptional(pb.NewDataLiteral(FLT_MIN/2.0f));
        const auto data5 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<float>::Id);
        const auto data = pb.NewDataLiteral(3.14);

        const auto type = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<float>::Id));
        const auto list = pb.NewList(type, {data0, data1, data2, data3, data4, data5});

        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            return pb.Nanvl(item, data);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 0.0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 3.14);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), HUGE_VAL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), -HUGE_VAL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), FLT_MIN/2.0f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestConvertToFloat) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto seven = pb.NewDataLiteral(i32(7));
        const auto pgmReturn = pb.Convert(seven, pb.NewDataType(NUdf::TDataType<float>::Id));
        const auto graph = setup.BuildGraph(pgmReturn);
        UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().template Get<float>(), 7.0f);
    }

    Y_UNIT_TEST_LLVM(TestAppend) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        auto pgmReturn = pb.NewEmptyList(pb.NewDataType(NUdf::TDataType<ui32>::Id));
        pgmReturn = pb.Append(pgmReturn, pb.NewDataLiteral<ui32>(34));
        pgmReturn = pb.Append(pgmReturn, pb.NewDataLiteral<ui32>(56));

        const auto graph = setup.BuildGraph(pgmReturn);
        UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetListLength(), 2);

        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 34);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 56);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestForkedAppend) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto list1 = pb.NewList(pb.NewDataType(NUdf::TDataType<ui32>::Id), {pb.NewDataLiteral<ui32>(34)});
        const auto list2 = pb.Append(list1, pb.NewDataLiteral<ui32>(56));
        const auto list3 = pb.Append(list1, pb.NewDataLiteral<ui32>(78));
        const auto pgmReturn = pb.NewTuple({list2, list3});

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator1 = graph->GetValue().GetElement(0).GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator1.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 34);
        UNIT_ASSERT(iterator1.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 56);
        UNIT_ASSERT(!iterator1.Next(item));
        UNIT_ASSERT(!iterator1.Next(item));

        const auto iterator2 = graph->GetValue().GetElement(1).GetListIterator();
        UNIT_ASSERT(iterator2.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 34);
        UNIT_ASSERT(iterator2.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 78);
        UNIT_ASSERT(!iterator2.Next(item));
        UNIT_ASSERT(!iterator2.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestPrepend) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        auto pgmReturn = pb.NewEmptyList(pb.NewDataType(NUdf::TDataType<ui32>::Id));
        pgmReturn = pb.Prepend(pb.NewDataLiteral<ui32>(34), pgmReturn);
        pgmReturn = pb.Prepend(pb.NewDataLiteral<ui32>(56), pgmReturn);

        const auto graph = setup.BuildGraph(pgmReturn);
        UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetListLength(), 2);

        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 56);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 34);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestPrependOfVoid) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        auto pgmReturn = pb.NewEmptyListOfVoid();
        pgmReturn = pb.Prepend(pb.NewVoid(), pgmReturn);
        pgmReturn = pb.Prepend(pb.NewVoid(), pgmReturn);

        const auto graph = setup.BuildGraph(pgmReturn);
        UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetListLength(), 0);

        const auto iterator = graph->GetValue().GetListIterator();
        UNIT_ASSERT(!iterator.Skip());
        UNIT_ASSERT(!iterator.Skip());
    }

    Y_UNIT_TEST_LLVM(TestForkedPrepend) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto list = pb.NewEmptyList(pb.NewDataType(NUdf::TDataType<ui32>::Id));
        const auto list1 = pb.Prepend(pb.NewDataLiteral<ui32>(34), list);
        const auto list2 = pb.Prepend(pb.NewDataLiteral<ui32>(56), list1);
        const auto list3 = pb.Prepend(pb.NewDataLiteral<ui32>(78), list1);
        const auto pgmReturn = pb.NewTuple({list2, list3});

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator1 = graph->GetValue().GetElement(0).GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator1.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 56);
        UNIT_ASSERT(iterator1.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 34);
        UNIT_ASSERT(!iterator1.Next(item));
        UNIT_ASSERT(!iterator1.Next(item));

        const auto iterator2 = graph->GetValue().GetElement(1).GetListIterator();
        UNIT_ASSERT(iterator2.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 78);
        UNIT_ASSERT(iterator2.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 34);
        UNIT_ASSERT(!iterator2.Next(item));
        UNIT_ASSERT(!iterator2.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestAppendOfVoid) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        auto pgmReturn = pb.NewEmptyListOfVoid();
        pgmReturn = pb.Append(pgmReturn, pb.NewVoid());
        pgmReturn = pb.Append(pgmReturn, pb.NewVoid());

        const auto graph = setup.BuildGraph(pgmReturn);
        UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetListLength(), 0);

        const auto iterator = graph->GetValue().GetListIterator();
        UNIT_ASSERT(!iterator.Skip());
        UNIT_ASSERT(!iterator.Skip());
    }

    Y_UNIT_TEST_LLVM(TestExtend) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto emptyList = pb.NewEmptyList(pb.NewDataType(NUdf::TDataType<ui32>::Id));
        const auto list1 = pb.Append(emptyList, pb.NewDataLiteral<ui32>(34));
        const auto list2 = pb.Append(emptyList, pb.NewDataLiteral<ui32>(56));
        const auto pgmReturn = pb.Extend({list1, list2});

        const auto graph = setup.BuildGraph(pgmReturn);
        UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetListLength(), 2);

        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 34);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 56);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestExtend3) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto emptyList = pb.NewEmptyList(pb.NewDataType(NUdf::TDataType<ui32>::Id));
        const auto list1 = pb.Append(emptyList, pb.NewDataLiteral<ui32>(34));
        const auto list2 = pb.Append(emptyList, pb.NewDataLiteral<ui32>(56));
        const auto list3 = pb.Append(emptyList, pb.NewDataLiteral<ui32>(7));
        const auto list4 = pb.Append(list3, pb.NewDataLiteral<ui32>(12));
        const auto pgmReturn = pb.Extend({ list1, emptyList, list2, list4 });

        const auto graph = setup.BuildGraph(pgmReturn);
        UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetListLength(), 4);

        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 34);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 56);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 7);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 12);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestExtendOverFlows) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral(0.0);
        const auto data1 = pb.NewDataLiteral(1.1);
        const auto data2 = pb.NewDataLiteral(-3.14);
        const auto data3 = pb.NewDataLiteral(121324.323);
        const auto data4 = pb.NewDataLiteral(-7898.8);
        const auto type = pb.NewDataType(NUdf::TDataType<double>::Id);
        const auto list1 = pb.NewList(type, {data0, data1, data2});
        const auto list2 = pb.NewList(type, {data3, data4});

        const auto pgmReturn = pb.FromFlow(pb.Extend({pb.ToFlow(list2), pb.ToFlow(list1), pb.ToFlow(list2)}));

        const auto graph = setup.BuildGraph(pgmReturn);

        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 121324.323);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), -7898.8);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 0.0);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 1.1);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), -3.14);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 121324.323);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), -7898.8);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestOrderedExtendOverFlows) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral(0.0);
        const auto data1 = pb.NewDataLiteral(1.1);
        const auto data2 = pb.NewDataLiteral(-3.14);
        const auto data3 = pb.NewDataLiteral(121324.323);
        const auto data4 = pb.NewDataLiteral(-7898.8);
        const auto type = pb.NewDataType(NUdf::TDataType<double>::Id);
        const auto list1 = pb.NewList(type, {data0, data1, data2});
        const auto list2 = pb.NewList(type, {data3, data4});

        const auto pgmReturn = pb.FromFlow(pb.OrderedExtend({pb.ToFlow(list2), pb.ToFlow(list1), pb.ToFlow(list2)}));

        const auto graph = setup.BuildGraph(pgmReturn);

        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 121324.323);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), -7898.8);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 0.0);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 1.1);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), -3.14);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 121324.323);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), -7898.8);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestStreamForwardList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto data1 = pb.NewDataLiteral<ui32>(34);
        const auto data2 = pb.NewDataLiteral<ui32>(56);
        const auto data3 = pb.NewDataLiteral<ui32>(7);
        const auto type = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto list = pb.NewList(type, {data1, data2, data3});

        const auto pgmReturn = pb.ForwardList(pb.Iterator(list, {}));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 34);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 56);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 7);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFlowForwardList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto data1 = pb.NewDataLiteral<ui32>(34);
        const auto data2 = pb.NewDataLiteral<ui32>(56);
        const auto data3 = pb.NewDataLiteral<ui32>(7);
        const auto type = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto list = pb.NewList(type, {data1, data2, data3});

        const auto pgmReturn = pb.ForwardList(pb.ToFlow(list));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 34);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 56);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 7);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFlowFromOptional) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral(0.0);
        const auto data1 = pb.NewDataLiteral(1.1);
        const auto data2 = pb.NewDataLiteral(-3.14);
        const auto type = pb.NewDataType(NUdf::TDataType<double>::Id);

        const auto opt0 = pb.NewOptional(data0);
        const auto opt1 = pb.NewOptional(data1);
        const auto opt2 = pb.NewOptional(data2);
        const auto opt = pb.NewEmptyOptional(pb.NewOptionalType(type));


        const auto pgmReturn = pb.FromFlow(pb.OrderedExtend({pb.ToFlow(opt0), pb.ToFlow(opt), pb.ToFlow(opt1), pb.ToFlow(opt), pb.ToFlow(opt2)}));

        const auto graph = setup.BuildGraph(pgmReturn);

        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 0.0);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 1.1);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), -3.14);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestCollectOverFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral(0.0);
        const auto data1 = pb.NewDataLiteral(1.1);
        const auto data2 = pb.NewDataLiteral(-3.14);
        const auto data3 = pb.NewDataLiteral(121324.323);
        const auto data4 = pb.NewDataLiteral(-7898.8);
        const auto type = pb.NewDataType(NUdf::TDataType<double>::Id);
        const auto data = pb.NewList(type, {data0, data1, data2, data3, data4});

        const auto pgmReturn = pb.Collect(pb.ToFlow(data));

        const auto graph = setup.BuildGraph(pgmReturn);

        const auto list = graph->GetValue();
        UNIT_ASSERT_VALUES_EQUAL(list.GetElement(0).template Get<double>(), 0.0);
        UNIT_ASSERT_VALUES_EQUAL(list.GetElement(3).template Get<double>(), 121324.323);
        UNIT_ASSERT_VALUES_EQUAL(list.GetElement(1).template Get<double>(), 1.1);
        UNIT_ASSERT_VALUES_EQUAL(list.GetElement(2).template Get<double>(), -3.14);
        UNIT_ASSERT_VALUES_EQUAL(list.GetElement(4).template Get<double>(), -7898.8);
    }

    Y_UNIT_TEST_LLVM(TestAdd) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(1);
        const auto data2 = pb.NewDataLiteral<ui32>(2);
        const auto pgmReturn = pb.Add(data1, data2);

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto res = graph->GetValue().template Get<ui32>();
        UNIT_ASSERT_VALUES_EQUAL(res, 3);
    }

    Y_UNIT_TEST_LLVM(TestSub) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(7);
        const auto data2 = pb.NewDataLiteral<ui32>(2);
        const auto pgmReturn = pb.Sub(data1, data2);

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto res = graph->GetValue().template Get<ui32>();
        UNIT_ASSERT_VALUES_EQUAL(res, 5);
    }

    Y_UNIT_TEST_LLVM(TestMul) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(3);
        const auto data2 = pb.NewDataLiteral<ui32>(2);
        const auto pgmReturn = pb.Mul(data1, data2);

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto res = graph->GetValue().template Get<ui32>();
        UNIT_ASSERT_VALUES_EQUAL(res, 6);
    }

    Y_UNIT_TEST_LLVM(TestDiv) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(17);
        const auto data2 = pb.NewDataLiteral<ui32>(3);
        const auto pgmReturn = pb.Div(data1, data2);

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto value = graph->GetValue();
        UNIT_ASSERT(value);
        UNIT_ASSERT_VALUES_EQUAL(value.template Get<ui32>(), 5);
    }

    Y_UNIT_TEST_LLVM(TestDivZero) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(17);
        const auto data2 = pb.NewDataLiteral<ui32>(0);
        const auto pgmReturn = pb.Div(data1, data2);

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto value = graph->GetValue();
        UNIT_ASSERT(!value);
    }

    Y_UNIT_TEST_LLVM(TestIDivOverflow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<i32>(Min<i32>());
        const auto data2 = pb.NewDataLiteral<i32>(-1);
        const auto pgmReturn = pb.Div(data1, data2);

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto value = graph->GetValue();
        UNIT_ASSERT(!value);
    }

    Y_UNIT_TEST_LLVM(TestMod) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(17);
        const auto data2 = pb.NewDataLiteral<ui32>(3);
        const auto pgmReturn = pb.Mod(data1, data2);

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto value = graph->GetValue();
        UNIT_ASSERT(value);
        UNIT_ASSERT_VALUES_EQUAL(value.template Get<ui32>(), 2);
    }

    Y_UNIT_TEST_LLVM(TestModZero) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(17);
        const auto data2 = pb.NewDataLiteral<ui32>(0);
        const auto pgmReturn = pb.Mod(data1, data2);

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto value = graph->GetValue();
        UNIT_ASSERT(!value);
    }

    Y_UNIT_TEST_LLVM(TestIModOverflow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<i32>(Min<i32>());
        const auto data2 = pb.NewDataLiteral<i32>(-1);
        const auto pgmReturn = pb.Mod(data1, data2);

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto value = graph->GetValue();
        UNIT_ASSERT(!value);
    }

    Y_UNIT_TEST_LLVM(TestStruct) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(1);
        const auto data2 = pb.NewDataLiteral<ui32>(2);
        const auto data3 = pb.NewDataLiteral<ui32>(3);
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        auto structObj = pb.NewEmptyStruct();
        structObj = pb.AddMember(structObj, "x", data2);
        structObj = pb.AddMember(structObj, "z", data1);
        structObj = pb.AddMember(structObj, "y", data3);
        const auto pgmReturn = pb.NewList(dataType, {
            pb.Member(structObj, "x"),
            pb.Member(structObj, "y"),
            pb.Member(structObj, "z")
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 1);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestMapOverList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto data1 = pb.NewOptional(pb.NewDataLiteral<i32>(1));
        const auto data2 = pb.NewOptional(pb.NewDataLiteral<i32>(2));
        const auto data3 = pb.NewOptional(pb.NewDataLiteral<i32>(3));
        const auto data4 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<i32>::Id);
        const auto list = pb.NewList(dataType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.Sub(pb.Mul(item, data2), data1);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 5);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestMapOverStream) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i16>::Id));
        const auto data0 = pb.NewOptional(pb.NewDataLiteral<i16>(0));
        const auto data1 = pb.NewOptional(pb.NewDataLiteral<i16>(-1));
        const auto data2 = pb.NewOptional(pb.NewDataLiteral<i16>(3));
        const auto data3 = pb.NewOptional(pb.NewDataLiteral<i16>(7));
        const auto data4 = pb.NewOptional(pb.NewDataLiteral<i16>(11));
        const auto data5 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<i16>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data4, data5});

        const auto pgmReturn = pb.Map(pb.Iterator(list, {}),
            [&](TRuntimeNode item) {
            return pb.Div(data3, item);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i16>(), -7);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i16>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i16>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i16>(), 0);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestMapOverFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<NUdf::EDataSlot::String>("PREFIX:");
        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very large string");
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("");
        const auto data3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("small");
        const auto type = pb.NewDataType(NUdf::EDataSlot::String);
        const auto list = pb.NewList(type, {data1, data2, data3});

        const auto pgmReturn = pb.FromFlow(pb.Map(pb.ToFlow(list),
            [&](TRuntimeNode item) {
            return pb.Concat(data0, item);
        }));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNBOXED_VALUE_STR_EQUAL(item, "PREFIX:very large string");
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNBOXED_VALUE_STR_EQUAL(item, "PREFIX:");
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNBOXED_VALUE_STR_EQUAL(item, "PREFIX:small");
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

#ifndef _win_
    Y_UNIT_TEST_LLVM(TestMapUnwrapThrow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i8>::Id));
        const auto data1 = pb.NewOptional(pb.NewDataLiteral<i8>(1));
        const auto data2 = pb.NewOptional(pb.NewDataLiteral<i8>(7));
        const auto data0 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<i8>::Id);
        const auto list = pb.LazyList(pb.NewList(dataType, {data1, data0}));

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.Sub(data2, pb.Unwrap(item, pb.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i8>(), 6);
        UNIT_ASSERT_EXCEPTION(iterator.Next(item), yexception);
    }

    Y_UNIT_TEST_LLVM(TestMapUnwrapThrowMessage) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i8>::Id));
        const auto data1 = pb.NewOptional(pb.NewDataLiteral<i8>(1));
        const auto data2 = pb.NewOptional(pb.NewDataLiteral<i8>(7));
        const auto data0 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<i8>::Id);
        const auto list = pb.LazyList(pb.NewList(dataType, {data1, data0}));

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.Sub(data2, pb.Unwrap(item, pb.Concat(
                pb.NewDataLiteral<NUdf::EDataSlot::String>("a"),
                pb.NewDataLiteral<NUdf::EDataSlot::String>("b")),
                "", 0, 0));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i8>(), 6);
        UNIT_ASSERT_EXCEPTION(iterator.Next(item), yexception);
    }

    Y_UNIT_TEST_LLVM(TestMapEnsureThrowMessage) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<i8>::Id);
        const auto data1 = pb.NewDataLiteral<i8>(1);
        const auto data2 = pb.NewDataLiteral<i8>(7);
        const auto list = pb.LazyList(pb.NewList(dataType, {data1, data2}));

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.Sub(data2,
                pb.Ensure(item, pb.Less(item, data2), pb.NewDataLiteral<NUdf::EDataSlot::String>("BAD"),"", 0, 0));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i8>(), 6);
        UNIT_ASSERT_EXCEPTION(iterator.Next(item), yexception);
    }
#endif
    Y_UNIT_TEST_LLVM(TestMapWithCoalesce) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i16>::Id));
        const auto data0 = pb.NewDataLiteral<i16>(0);
        const auto data1 = pb.NewOptional(pb.NewDataLiteral<i16>(1));
        const auto data2 = pb.NewOptional(pb.NewDataLiteral<i16>(2));
        const auto data3 = pb.NewOptional(pb.NewDataLiteral<i16>(3));
        const auto data = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<i16>::Id);
        const auto list = pb.NewList(dataType, {data1, data2, data3, data});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.Coalesce(item, data0);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i16>(), 1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i16>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i16>(), 3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i16>(), 0);
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestSizeOfOptional) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i16>::Id));
        const auto data0 = pb.NewOptional(pb.NewDataLiteral<i16>(0));
        const auto data1 = pb.NewOptional(pb.NewDataLiteral<i16>(1));
        const auto data = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<i16>::Id);
        const auto list = pb.NewList(dataType, {data1, data0, data});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.Size(item);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestSizeOfOptionalString) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<char*>::Id));
        const auto data0 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("0123456789"));
        const auto data1 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("XYZ"));
        const auto data2 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>(""));
        const auto data3 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("qwertyuioplkjhgfdsazxcvbnm"));
        const auto data = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<char*>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3, data});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.Size(pb.Concat(item, data0));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 20U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 13U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 10U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 36U);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestMapWithIfExists) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<ui16>::Id));
        const auto data0 = pb.NewDataLiteral<ui16>(666);
        const auto data1 = pb.NewOptional(pb.NewDataLiteral<ui16>(1));
        const auto data2 = pb.NewOptional(pb.NewDataLiteral<ui16>(2));
        const auto data3 = pb.NewOptional(pb.NewDataLiteral<ui16>(3));
        const auto data = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui16>::Id);
        const auto list = pb.NewList(dataType, {data1, data2, data3, data});

        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.If(pb.Exists(item), pb.Increment(pb.Unwrap(item, pb.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0)), data0);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), 666);
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestMapOverListLazy) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(1U);
        const auto data2 = pb.NewDataLiteral<ui32>(2U);
        const auto data3 = pb.NewDataLiteral<ui32>(3U);
        const auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto list = pb.NewList(dataType, {data1, data2, data3});

        const auto pgmReturn = pb.Map(pb.LazyList(list),
            [&](TRuntimeNode item) {
                return pb.Add(pb.Mul(item, data2), data1);
            }
        );

        const auto graph = setup.BuildGraph(pgmReturn);

        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 5);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 7);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestMapOverOptional) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(1);
        const auto data2 = pb.NewDataLiteral<ui32>(2);
        const auto list = pb.NewOptional(data1);
        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.Add(pb.Mul(item, data2), data1);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto value = graph->GetValue();
        UNIT_ASSERT(value);
        UNIT_ASSERT_VALUES_EQUAL(value.template Get<ui32>(), 3);
    }

    Y_UNIT_TEST_LLVM(TestFloatMinMax) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<float>(-NAN);
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
                return pb.NewTuple({pb.Min(left, right), pb.Max(left, right)});
            });
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(std::isnan(item.GetElement(0).template Get<float>()));
        UNIT_ASSERT(std::isnan(item.GetElement(1).template Get<float>()));

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<float>(), HUGE_VALF);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<float>(), HUGE_VALF);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<float>(), 3.14f);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<float>(), 3.14f);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<float>(), -2.13f);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<float>(), -2.13f);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<float>(), -HUGE_VALF);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<float>(), -HUGE_VALF);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<float>(), HUGE_VALF);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<float>(), HUGE_VALF);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<float>(), HUGE_VALF);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<float>(), HUGE_VALF);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<float>(), 3.14f);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<float>(), HUGE_VALF);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<float>(), -2.13f);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<float>(), HUGE_VALF);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<float>(), -HUGE_VALF);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<float>(), HUGE_VALF);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<float>(), 3.14f);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<float>(), 3.14f);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<float>(), 3.14f);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<float>(), HUGE_VALF);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<float>(), 3.14f);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<float>(), 3.14f);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<float>(), -2.13f);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<float>(), 3.14f);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<float>(), -HUGE_VALF);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<float>(), 3.14f);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<float>(), -2.13f);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<float>(), -2.13f);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<float>(), -2.13f);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<float>(), HUGE_VALF);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<float>(), -2.13f);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<float>(), 3.14f);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<float>(), -2.13f);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<float>(), -2.13f);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<float>(), -HUGE_VALF);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<float>(), -2.13f);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<float>(), -HUGE_VALF);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<float>(), -HUGE_VALF);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<float>(), -HUGE_VALF);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<float>(), HUGE_VALF);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<float>(), -HUGE_VALF);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<float>(), 3.14f);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<float>(), -HUGE_VALF);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<float>(), -2.13f);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<float>(), -HUGE_VALF);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<float>(), -HUGE_VALF);

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFloatMod) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<float>(-1.75f);
        const auto data2 = pb.NewDataLiteral<float>(3.14f);
        const auto data3 = pb.NewDataLiteral<float>(-6.28f);
        const auto data4 = pb.NewDataLiteral<float>(7.28f);
        const auto dataType = pb.NewDataType(NUdf::TDataType<float>::Id);
        const auto list = pb.NewList(dataType, {data1, data2, data3, data4});
        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.Mod(item, data2);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), -1.75f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), 0.0f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), 0.0f);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), 1.0f);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestDoubleMod) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<double>(-1.75);
        const auto data2 = pb.NewDataLiteral<double>(3.14);
        const auto data3 = pb.NewDataLiteral<double>(-6.28);
        const auto data4 = pb.NewDataLiteral<double>(7.28);
        const auto dataType = pb.NewDataType(NUdf::TDataType<double>::Id);
        const auto list = pb.NewList(dataType, {data1, data2, data3, data4});
        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
            return pb.Mod(item, data2);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), -1.75);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 0.0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 0.0);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<double>(), 1.0);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestDiscardOverFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<NUdf::EDataSlot::String>("000");
        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("100");
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("200");
        const auto data3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("300");
        const auto dataType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3});

        const auto pgmReturn = pb.FromFlow(pb.Discard(pb.ToFlow(list)));
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestHead) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto itemType = pb.NewDataType(NUdf::TDataType<float>::Id);
        const auto listType = pb.NewListType(itemType);

        const auto data0 = pb.NewEmptyList(itemType);
        const auto data1 = pb.NewList(itemType, {pb.NewDataLiteral<float>(-1.5f), pb.NewDataLiteral<float>(0.f), pb.NewDataLiteral<float>(3.14f)});
        const auto data2 = pb.NewList(itemType, {pb.NewDataLiteral<float>(3.14f), pb.NewDataLiteral<float>(-1.5f), pb.NewDataLiteral<float>(0.f)});
        const auto data3 = pb.LazyList(pb.NewList(itemType, {pb.NewDataLiteral<float>(1.1f), pb.NewDataLiteral<float>(-2.2f)}));

        const auto list = pb.NewList(listType, {data0, data1, data2, data3});

        const auto pgmReturn = pb.Map(list, [&](TRuntimeNode item) { return pb.Head(item); });

        const auto graph = setup.BuildGraph(pgmReturn);

        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), -1.5f);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), 3.14f);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), 1.1f);

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestLast) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto itemType = pb.NewDataType(NUdf::TDataType<float>::Id);
        const auto listType = pb.NewListType(itemType);

        const auto data0 = pb.NewEmptyList(itemType);
        const auto data1 = pb.NewList(itemType, {pb.NewDataLiteral<float>(-1.5f), pb.NewDataLiteral<float>(0.f), pb.NewDataLiteral<float>(3.14f)});
        const auto data2 = pb.NewList(itemType, {pb.NewDataLiteral<float>(3.14f), pb.NewDataLiteral<float>(-1.5f), pb.NewDataLiteral<float>(0.f)});
        const auto data3 = pb.LazyList(pb.NewList(itemType, {pb.NewDataLiteral<float>(1.1f), pb.NewDataLiteral<float>(-2.2f)}));

        const auto list = pb.NewList(listType, {data0, data1, data2, data3});

        const auto pgmReturn = pb.Map(list, [&](TRuntimeNode item) { return pb.Last(item); });

        const auto graph = setup.BuildGraph(pgmReturn);

        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), 3.14f);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), 0.f);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<float>(), -2.2f);

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestCoalesce) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(1);
        const auto data2 = pb.NewDataLiteral<ui32>(2);
        const auto data3 = pb.NewDataLiteral<ui32>(3);
        auto pgmReturn = pb.NewEmptyStruct();
        pgmReturn = pb.AddMember(pgmReturn, "A", pb.Coalesce(
            pb.NewOptional(data1), data2));
        pgmReturn = pb.AddMember(pgmReturn, "B", pb.Coalesce(
            pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id), data2));
        pgmReturn = pb.AddMember(pgmReturn, "C", pb.Coalesce(
            pb.NewOptional(data2), pb.NewOptional(data3)));
        pgmReturn = pb.AddMember(pgmReturn, "D", pb.Coalesce(
            pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id), pb.NewOptional(data3)));
        pgmReturn = pb.AddMember(pgmReturn, "E", pb.Coalesce(
            pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id), pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id)));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto value = graph->GetValue();
        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(0).template Get<ui32>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(1).template Get<ui32>(), 2);
        UNIT_ASSERT(value.GetElement(2));
        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(2).template Get<ui32>(), 2);
        UNIT_ASSERT(value.GetElement(3));
        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(3).template Get<ui32>(), 3);
        UNIT_ASSERT(!value.GetElement(4));
    }

    Y_UNIT_TEST_LLVM(TestExists) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui64>(1);
        const auto optionalType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<ui64>::Id));
        const auto list = pb.NewList(optionalType, {pb.NewOptional(data1), pb.NewEmptyOptional(optionalType)});

        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            return pb.Exists(item);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestIf) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(1);
        const auto data2 = pb.NewDataLiteral<ui32>(2);
        const auto truth = pb.NewDataLiteral(true);
        const auto falsehood = pb.NewDataLiteral(false);
        const auto list = pb.NewList(pb.NewDataType(NUdf::TDataType<bool>::Id), {truth, falsehood});
        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            return pb.If(item, data1, data2);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestIfPresent) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(1);
        const auto data2 = pb.NewDataLiteral<ui32>(2);
        const auto filled = pb.NewOptional(data1);
        const auto empty = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        const auto list = pb.NewList(pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<ui32>::Id)), {filled, empty});
        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            return pb.IfPresent({item},
            [&](TRuntimeNode::TList unpacked){
                return unpacked.front();
            }, data2);
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 2);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestIfPresentTwo) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewOptional(pb.NewDataLiteral<i8>(+1));
        const auto data2 = pb.NewOptional(pb.NewDataLiteral<i8>(-1));
        const auto empty1 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<i8>::Id);

        const auto data3 = pb.NewOptional(pb.NewDataLiteral<bool>(true));
        const auto data4= pb.NewOptional(pb.NewDataLiteral<bool>(false));
        const auto empty2 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<bool>::Id);

        const auto list1 = pb.NewList(pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i8>::Id)), {data1, empty1, data2});
        const auto list2 = pb.NewList(pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<bool>::Id)), {data3, empty2, data4});

        const auto pgmReturn = pb.FlatMap(list1,
        [&](TRuntimeNode item1) {
            return pb.FlatMap(list2,
            [&](TRuntimeNode item2) {
                return pb.IfPresent({item1, item2},
                [&](TRuntimeNode::TList unpacked) {
                    return pb.NewOptional(pb.If(unpacked.back(), pb.Minus(unpacked.front()), unpacked.front()));
                }, empty1);
            });
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i8>(), -1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i8>(), +1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i8>(), +1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i8>(), -1);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestIfPresentThree) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewOptional(pb.NewDataLiteral<i8>(+1));
        const auto data2 = pb.NewOptional(pb.NewDataLiteral<i8>(-1));
        const auto empty1 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<i8>::Id);

        const auto data3 = pb.NewOptional(pb.NewDataLiteral<bool>(true));
        const auto data4= pb.NewOptional(pb.NewDataLiteral<bool>(false));
        const auto empty2 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<bool>::Id);

        const auto data5 = pb.NewOptional(pb.NewOptional(pb.NewDataLiteral<i8>(5)));
        const auto data6 = pb.NewOptional(pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<i8>::Id));
        const auto type2 = pb.NewOptionalType(pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i8>::Id)));
        const auto empty3 = pb.NewEmptyOptional(type2);

        const auto list1 = pb.NewList(pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i8>::Id)), {data1, empty1, data2});
        const auto list2 = pb.NewList(pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<bool>::Id)), {data3, empty2, data4});
        const auto list3 = pb.NewList(type2, {data5, empty3, data6});

        const auto pgmReturn = pb.FlatMap(list1,
        [&](TRuntimeNode item1) {
            return pb.FlatMap(list2,
            [&](TRuntimeNode item2) {
                return pb.FlatMap(list3,
                [&](TRuntimeNode item3) {
                    return pb.IfPresent({item1, item2, item3},
                    [&](TRuntimeNode::TList unpacked) {
                        return pb.NewOptional(pb.If(unpacked[1],
                            pb.Add(unpacked.front(), pb.Coalesce(unpacked.back(), pb.NewDataLiteral<i8>(3))),
                            pb.Sub(unpacked.front(), pb.Coalesce(unpacked.back(), pb.NewDataLiteral<i8>(7)))
                        ));
                    }, empty1);
                });
            });
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i8>(), +6);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i8>(), +4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i8>(), -4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i8>(), -6);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i8>(), +4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i8>(), +2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i8>(), -6);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i8>(), -8);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestIfPresentSame) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewOptional(pb.NewDataLiteral<i8>(+1));
        const auto data2 = pb.NewOptional(pb.NewDataLiteral<i8>(-1));
        const auto data0 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<i8>::Id);

        const auto list = pb.NewList(pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i8>::Id)), {data1, data0, data2});

        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            const auto minus = pb.Minus(item);
            return pb.NewTuple({
                pb.IfPresent({minus},
                    [&](TRuntimeNode::TList unpacked) {
                        return pb.Abs(unpacked.front());
                    }, data0),
                pb.IfPresent({minus},
                    [&](TRuntimeNode::TList unpacked) {
                        return pb.Minus(unpacked.front());
                    }, data0),
            });
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i8>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i8>(), 1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i8>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i8>(), -1);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestIncDec) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(7);
        auto pgmReturn = pb.NewEmptyList(pb.NewDataType(NUdf::TDataType<ui32>::Id));
        pgmReturn = pb.Append(pgmReturn, pb.Increment(data1));
        pgmReturn = pb.Append(pgmReturn, pb.Decrement(data1));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 8);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 6);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestLogical) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto truth = pb.NewDataLiteral(true);
        const auto falsehood = pb.NewDataLiteral(false);
        auto pgmReturn = pb.NewEmptyList(pb.NewDataType(NUdf::TDataType<bool>::Id));

        pgmReturn = pb.Append(pgmReturn, pb.And({truth, truth}));
        pgmReturn = pb.Append(pgmReturn, pb.And({truth, falsehood}));
        pgmReturn = pb.Append(pgmReturn, pb.And({falsehood, truth}));
        pgmReturn = pb.Append(pgmReturn, pb.And({falsehood, falsehood}));

        pgmReturn = pb.Append(pgmReturn, pb.Or({truth, truth}));
        pgmReturn = pb.Append(pgmReturn, pb.Or({falsehood, truth}));
        pgmReturn = pb.Append(pgmReturn, pb.Or({truth, falsehood}));
        pgmReturn = pb.Append(pgmReturn, pb.Or({falsehood, falsehood}));

        pgmReturn = pb.Append(pgmReturn, pb.Xor({truth, truth}));
        pgmReturn = pb.Append(pgmReturn, pb.Xor({falsehood, truth}));
        pgmReturn = pb.Append(pgmReturn, pb.Xor({truth, falsehood}));
        pgmReturn = pb.Append(pgmReturn, pb.Xor({falsehood, falsehood}));

        pgmReturn = pb.Append(pgmReturn, pb.Not(truth));
        pgmReturn = pb.Append(pgmReturn, pb.Not(falsehood));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();

        NUdf::TUnboxedValue item;

        // and
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);

        // or
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);

        // xor
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);

        // not
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestZip) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list1 = pb.NewList(pb.NewDataType(NUdf::TDataType<ui32>::Id), {
            pb.NewDataLiteral<ui32>(34),
            pb.NewDataLiteral<ui32>(56)
        });
        const auto list2 = pb.NewList(pb.NewDataType(NUdf::TDataType<char*>::Id), {
            pb.NewDataLiteral<NUdf::EDataSlot::String>("Q"),
            pb.NewDataLiteral<NUdf::EDataSlot::String>("E"),
            pb.NewDataLiteral<NUdf::EDataSlot::String>("W")
        });

        const auto pgmReturn = pb.Zip({list1, list2});

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto list = graph->GetValue();

        UNIT_ASSERT_VALUES_EQUAL(list.GetListLength(), 2U);
        UNIT_ASSERT_VALUES_EQUAL(list.GetElement(0).GetElement(0).template Get<ui32>(), 34);
        UNBOXED_VALUE_STR_EQUAL(list.GetElement(0).GetElement(1), "Q");
        UNIT_ASSERT_VALUES_EQUAL(list.GetElement(1).GetElement(0).template Get<ui32>(), 56);
        UNBOXED_VALUE_STR_EQUAL(list.GetElement(1).GetElement(1), "E");
    }

    Y_UNIT_TEST_LLVM(TestZipLazy) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list1 = pb.NewList(pb.NewDataType(NUdf::TDataType<ui32>::Id), {
            pb.NewDataLiteral<ui32>(34),
            pb.NewDataLiteral<ui32>(56)
        });
        const auto list2 = pb.NewList(pb.NewDataType(NUdf::TDataType<char*>::Id), {
            pb.NewDataLiteral<NUdf::EDataSlot::String>("Q"),
            pb.NewDataLiteral<NUdf::EDataSlot::String>("E"),
            pb.NewDataLiteral<NUdf::EDataSlot::String>("W")
        });

        const auto pgmReturn = pb.Zip({pb.LazyList(list1), list2});

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui32>(), 34);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "Q");
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui32>(), 56);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "E");
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestZipAll) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list1 = pb.NewList(pb.NewDataType(NUdf::TDataType<ui32>::Id), {
            pb.NewDataLiteral<ui32>(34),
            pb.NewDataLiteral<ui32>(56)
        });
        const auto list2 = pb.NewList(pb.NewDataType(NUdf::TDataType<char*>::Id), {
            pb.NewDataLiteral<NUdf::EDataSlot::String>("Q"),
            pb.NewDataLiteral<NUdf::EDataSlot::String>("E"),
            pb.NewDataLiteral<NUdf::EDataSlot::String>("W")
        });

        const auto pgmReturn = pb.ZipAll({list1, list2});

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto list = graph->GetValue();

        UNIT_ASSERT_VALUES_EQUAL(list.GetListLength(), 3U);
        UNIT_ASSERT_VALUES_EQUAL(list.GetElement(0).GetElement(0).template Get<ui32>(), 34);
        UNBOXED_VALUE_STR_EQUAL(list.GetElement(0).GetElement(1), "Q");
        UNIT_ASSERT_VALUES_EQUAL(list.GetElement(1).GetElement(0).template Get<ui32>(), 56);
        UNBOXED_VALUE_STR_EQUAL(list.GetElement(1).GetElement(1), "E");
        UNIT_ASSERT(!list.GetElement(2).GetElement(0));
        UNBOXED_VALUE_STR_EQUAL(list.GetElement(2).GetElement(1), "W");
    }

    Y_UNIT_TEST_LLVM(TestZipAllLazy) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list1 = pb.NewList(pb.NewDataType(NUdf::TDataType<ui32>::Id), {
            pb.NewDataLiteral<ui32>(34),
            pb.NewDataLiteral<ui32>(56)
        });
        const auto list2 = pb.NewList(pb.NewDataType(NUdf::TDataType<char*>::Id), {
            pb.NewDataLiteral<NUdf::EDataSlot::String>("Q"),
            pb.NewDataLiteral<NUdf::EDataSlot::String>("E"),
            pb.NewDataLiteral<NUdf::EDataSlot::String>("W")
        });

        const auto pgmReturn = pb.ZipAll({list1, pb.LazyList(list2)});

        const auto graph = setup.BuildGraph(pgmReturn);

        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui32>(), 34);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "Q");
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui32>(), 56);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "E");
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "W");
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestReduce) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewOptional(pb.NewDataLiteral<ui32>(1));
        const auto data2 = pb.NewOptional(pb.NewDataLiteral<ui32>(2));
        const auto data3 = pb.NewOptional(pb.NewDataLiteral<ui32>(3));
        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<ui32>::Id));
        const auto list = pb.NewList(dataType, {data1, data2, data3});
        const auto empty = pb.AddMember(pb.AddMember(
            pb.NewEmptyStruct(), "Min", pb.NewEmptyOptional(dataType)),
            "Max", pb.NewEmptyOptional(dataType));
        const auto pgmReturn = pb.Reduce(list, empty,
            [&](TRuntimeNode item, TRuntimeNode state1) {
                return pb.AddMember(
                        pb.AddMember(pb.NewEmptyStruct(), "Min", pb.AggrMin(pb.Member(state1, "Min"), item)),
                        "Max", pb.AggrMax(pb.Member(state1, "Max"), item)
                    );
                },
            [&](TRuntimeNode state) {
                return state;
            }, empty,
            [&](TRuntimeNode state1, TRuntimeNode state2) {
                return pb.AddMember(
                    pb.AddMember(pb.NewEmptyStruct(), "Min", pb.AggrMin(pb.Member(state1, "Min"), pb.Member(state2, "Min")))
                    , "Max", pb.AggrMax(pb.Member(state1, "Max"), pb.Member(state2, "Max"))
                );
            });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto value = graph->GetValue();
        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(1).template Get<ui32>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(0).template Get<ui32>(), 3);
    }

    Y_UNIT_TEST_LLVM(TestReduceOverStream) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewOptional(pb.NewDataLiteral<ui32>(1));
        const auto data2 = pb.NewOptional(pb.NewDataLiteral<ui32>(2));
        const auto data3 = pb.NewOptional(pb.NewDataLiteral<ui32>(3));
        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<ui32>::Id));
        const auto list = pb.NewList(dataType, {data1, data2, data3});
        const auto empty = pb.AddMember(pb.AddMember(
            pb.NewEmptyStruct(), "Min", pb.NewEmptyOptional(dataType)),
            "Max", pb.NewEmptyOptional(dataType));
        const auto pgmReturn = pb.Reduce(pb.Iterator(list, {}), empty,
            [&](TRuntimeNode item, TRuntimeNode state1) {
                return pb.AddMember(
                        pb.AddMember(pb.NewEmptyStruct(), "Min", pb.AggrMin(pb.Member(state1, "Min"), item)),
                        "Max", pb.AggrMax(pb.Member(state1, "Max"), item)
                    );
                },
            [&](TRuntimeNode state) {
                return state;
            }, empty,
            [&](TRuntimeNode state1, TRuntimeNode state2) {
                return pb.AddMember(
                    pb.AddMember(pb.NewEmptyStruct(), "Min", pb.AggrMin(pb.Member(state1, "Min"), pb.Member(state2, "Min")))
                    , "Max", pb.AggrMax(pb.Member(state1, "Max"), pb.Member(state2, "Max"))
                );
            });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto value = graph->GetValue();
        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(1).template Get<ui32>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(0).template Get<ui32>(), 3);
    }

    Y_UNIT_TEST_LLVM(TestListLength) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list = pb.NewList(pb.NewDataType(NUdf::TDataType<ui32>::Id),
            {pb.NewDataLiteral<ui32>(34), pb.NewDataLiteral<ui32>(56)});
        const auto pgmReturn = pb.Length(list);

        const auto graph = setup.BuildGraph(pgmReturn);
        UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().template Get<ui64>(), 2);
    }

    Y_UNIT_TEST_LLVM(TestReverse) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list = pb.NewList(pb.NewDataType(NUdf::TDataType<ui32>::Id),
            {pb.NewDataLiteral<ui32>(34), pb.NewDataLiteral<ui32>(56)});
        const auto pgmReturn = pb.Reverse(list);

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 56);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 34);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestSkipForAppend) {
        const ui32 n = 100;
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        for (ui32 i = 0; i < n; ++i) {
            auto list = pb.NewEmptyList(pb.NewDataType(NUdf::TDataType<ui32>::Id));
            for (ui32 j = 0; j < n; ++j) {
                list = pb.Append(list, pb.NewDataLiteral(j));
            }

            const auto skippedList = pb.Skip(list, pb.NewDataLiteral<ui64>(i));
            auto changedList = skippedList;
            for (ui32 j = 0; j < n; ++j) {
                changedList = pb.Prepend(pb.NewDataLiteral(n + n - 1 - j), changedList);
            }

            auto pgmReturn = pb.NewEmptyList(list.GetStaticType());
            pgmReturn = pb.Append(pgmReturn, list);
            pgmReturn = pb.Append(pgmReturn, skippedList);
            pgmReturn = pb.Append(pgmReturn, changedList);

            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iteratorLists = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue test, item;

            UNIT_ASSERT(iteratorLists.Next(test));
            auto iterator = test.GetListIterator();
            for (ui32 j = 0; j < n; ++j) {
                UNIT_ASSERT(iterator.Next(item));
                UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), j);
            }

            UNIT_ASSERT(!iterator.Next(item));
            UNIT_ASSERT(!iterator.Next(item));

            UNIT_ASSERT(iteratorLists.Next(test));
            iterator = test.GetListIterator();
            for (ui32 j = i; j < n; ++j) {
                UNIT_ASSERT(iterator.Next(item));
                UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), j);
            }

            UNIT_ASSERT(!iterator.Next(item));
            UNIT_ASSERT(!iterator.Next(item));

            UNIT_ASSERT(iteratorLists.Next(test));
            iterator = test.GetListIterator();
            for (ui32 j = 0; j < n; ++j) {
                UNIT_ASSERT(iterator.Next(item));
                UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), j + n);
            }

            for (ui32 j = i; j < n; ++j) {
                UNIT_ASSERT(iterator.Next(item));
                UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), j);
            }

            UNIT_ASSERT(!iterator.Next(item));
            UNIT_ASSERT(!iterator.Next(item));
            UNIT_ASSERT(!iteratorLists.Next(test));
            UNIT_ASSERT(!iteratorLists.Next(test));
        }
    }

    Y_UNIT_TEST_LLVM(TestSkipForPrepend) {
        const ui32 n = 100;
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        for (ui32 i = 0; i < n; ++i) {
            auto list = pb.NewEmptyList(pb.NewDataType(NUdf::TDataType<ui32>::Id));
            for (ui32 j = 0; j < n; ++j) {
                list = pb.Prepend(pb.NewDataLiteral(n - 1 - j), list);
            }

            const auto skippedList = pb.Skip(list, pb.NewDataLiteral<ui64>(i));
            auto changedList = skippedList;
            for (ui32 j = 0; j < n; ++j) {
                changedList = pb.Prepend(pb.NewDataLiteral(n + n - 1 - j), changedList);
            }

            const auto pgmReturn = pb.NewList(list.GetStaticType(), {list, skippedList, changedList});

            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iteratorLists = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue test, item;

            UNIT_ASSERT(iteratorLists.Next(test));
            auto iterator = test.GetListIterator();
            for (ui32 j = 0; j < n; ++j) {
                UNIT_ASSERT(iterator.Next(item));
                UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), j);
            }

            UNIT_ASSERT(!iterator.Next(item));
            UNIT_ASSERT(!iterator.Next(item));

            UNIT_ASSERT(iteratorLists.Next(test));
            iterator = test.GetListIterator();
            for (ui32 j = i; j < n; ++j) {
                UNIT_ASSERT(iterator.Next(item));
                UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), j);
            }

            UNIT_ASSERT(!iterator.Next(item));
            UNIT_ASSERT(!iterator.Next(item));

            UNIT_ASSERT(iteratorLists.Next(test));
            iterator = test.GetListIterator();
            for (ui32 j = 0; j < n; ++j) {
                UNIT_ASSERT(iterator.Next(item));
                UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), j + n);
            }

            for (ui32 j = i; j < n; ++j) {
                UNIT_ASSERT(iterator.Next(item));
                UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), j);
            }

            UNIT_ASSERT(!iterator.Next(item));
            UNIT_ASSERT(!iterator.Next(item));
            UNIT_ASSERT(!iteratorLists.Next(test));
            UNIT_ASSERT(!iteratorLists.Next(test));
        }
    }

    Y_UNIT_TEST_LLVM(TestTakeForAppend) {
        const ui32 n = 100;
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        for (ui32 i = 0; i < n; ++i) {
            auto list = pb.NewEmptyList(pb.NewDataType(NUdf::TDataType<ui32>::Id));
            for (ui32 j = 0; j < n; ++j) {
                list = pb.Append(list, pb.NewDataLiteral(j));
            }

            const auto skippedList = pb.Take(list, pb.NewDataLiteral<ui64>(i));
            auto changedList = skippedList;
            for (ui32 j = 0; j < n; ++j) {
                changedList = pb.Append(changedList, pb.NewDataLiteral(n + j));
            }

            const auto pgmReturn = pb.NewList(list.GetStaticType(), {list, skippedList, changedList});

            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iteratorLists = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue test, item;

            UNIT_ASSERT(iteratorLists.Next(test));
            auto iterator = test.GetListIterator();
            for (ui32 j = 0; j < n; ++j) {
                UNIT_ASSERT(iterator.Next(item));
                UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), j);
            }

            UNIT_ASSERT(!iterator.Next(item));
            UNIT_ASSERT(!iterator.Next(item));

            UNIT_ASSERT(iteratorLists.Next(test));
            iterator = test.GetListIterator();
            for (ui32 j = 0; j < i; ++j) {
                UNIT_ASSERT(iterator.Next(item));
                UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), j);
            }

            UNIT_ASSERT(!iterator.Next(item));
            UNIT_ASSERT(!iterator.Next(item));

            UNIT_ASSERT(iteratorLists.Next(test));
            iterator = test.GetListIterator();
            for (ui32 j = 0; j < i; ++j) {
                UNIT_ASSERT(iterator.Next(item));
                UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), j);
            }

            for (ui32 j = 0; j < n; ++j) {
                UNIT_ASSERT(iterator.Next(item));
                UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), j + n);
            }

            UNIT_ASSERT(!iterator.Next(item));
            UNIT_ASSERT(!iterator.Next(item));
            UNIT_ASSERT(!iteratorLists.Next(test));
            UNIT_ASSERT(!iteratorLists.Next(test));
        }
    }

    Y_UNIT_TEST_LLVM(TestTakeForPrepend) {
        const ui32 n = 100;
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        for (ui32 i = 0; i < n; ++i) {
            auto list = pb.NewEmptyList(pb.NewDataType(NUdf::TDataType<ui32>::Id));
            for (ui32 j = 0; j < n; ++j) {
                list = pb.Prepend(pb.NewDataLiteral(n - 1- j), list);
            }

            const auto skippedList = pb.Take(list, pb.NewDataLiteral<ui64>(i));
            auto changedList = skippedList;
            for (ui32 j = 0; j < n; ++j) {
                changedList = pb.Append(changedList, pb.NewDataLiteral(n + j));
            }

            const auto pgmReturn = pb.NewList(list.GetStaticType(), {list, skippedList, changedList});

            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iteratorLists = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue test, item;

            UNIT_ASSERT(iteratorLists.Next(test));
            auto iterator = test.GetListIterator();
            for (ui32 j = 0; j < n; ++j) {
                UNIT_ASSERT(iterator.Next(item));
                UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), j);
            }

            UNIT_ASSERT(!iterator.Next(item));
            UNIT_ASSERT(!iterator.Next(item));

            UNIT_ASSERT(iteratorLists.Next(test));
            iterator = test.GetListIterator();
            for (ui32 j = 0; j < i; ++j) {
                UNIT_ASSERT(iterator.Next(item));
                UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), j);
            }

            UNIT_ASSERT(!iterator.Next(item));
            UNIT_ASSERT(!iterator.Next(item));

            UNIT_ASSERT(iteratorLists.Next(test));
            iterator = test.GetListIterator();
            for (ui32 j = 0; j < i; ++j) {
                UNIT_ASSERT(iterator.Next(item));
                UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), j);
            }

            for (ui32 j = 0; j < n; ++j) {
                UNIT_ASSERT(iterator.Next(item));
                UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), j + n);
            }

            UNIT_ASSERT(!iterator.Next(item));
            UNIT_ASSERT(!iterator.Next(item));
            UNIT_ASSERT(!iteratorLists.Next(test));
            UNIT_ASSERT(!iteratorLists.Next(test));
        }
    }

    Y_UNIT_TEST_LLVM(TestReplicate) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto pgmReturn = pb.Replicate(pb.NewDataLiteral<ui32>(34),
            pb.NewDataLiteral<ui64>(4), "", 0, 0);

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        for (ui32 i = 0; i < 4; ++i) {
            UNIT_ASSERT(iterator.Next(item));
            UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 34);
        }

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestIntegralCasts) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(258);
        const auto pgmReturn = pb.Convert(data1, pb.NewDataType(NUdf::TDataType<ui8>::Id));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto res = graph->GetValue().template Get<ui8>();
        UNIT_ASSERT_VALUES_EQUAL(res, 2);
    }

    Y_UNIT_TEST_LLVM(TestCastFourIntoTrue) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(4);
        const auto pgmReturn = pb.Convert(data1, pb.NewDataType(NUdf::TDataType<bool>::Id));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto res = graph->GetValue().template Get<bool>();
        UNIT_ASSERT(res);
    }

    Y_UNIT_TEST_LLVM(TestSubstring) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("aabbccc");
        const auto start1 = pb.NewDataLiteral<ui32>(3);
        const auto count1 = pb.NewDataLiteral<ui32>(2);
        const auto start2 = pb.NewDataLiteral<ui32>(4);
        const auto count2 = pb.NewDataLiteral<ui32>(10);
        const auto start3 = pb.NewDataLiteral<ui32>(10);
        const auto count3 = pb.NewDataLiteral<ui32>(1);
        const auto start4 = pb.NewDataLiteral<ui32>(0);
        const auto count4 = pb.NewDataLiteral<ui32>(3);

        const auto dataType = pb.NewTupleType({pb.NewDataType(NUdf::TDataType<ui32>::Id), pb.NewDataType(NUdf::TDataType<ui32>::Id)});
        const auto list = pb.NewList(dataType, {
            pb.NewTuple({start1, count1}),
            pb.NewTuple({start2, count2}),
            pb.NewTuple({start3, count3}),
            pb.NewTuple({start4, count4})
        });

        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            return pb.Substring(data1, pb.Nth(item, 0), pb.Nth(item, 1));
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "bc");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "ccc");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "aab");
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestSubstringOptionalArgs) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto null = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        const auto data0 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<const char*>::Id);
        const auto data1 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>(""));
        const auto data2 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("0123456789"));
        const auto data3 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("abcdefghijklmnopqrstuvwxyz"));

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<const char*>::Id));
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3});

        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            return pb.NewTuple({
                pb.Substring(item, null, null),
                pb.Substring(item, pb.NewOptional(pb.NewDataLiteral<ui32>(7U)), null),
                pb.Substring(item, null, pb.NewOptional(pb.NewDataLiteral<ui32>(6U))),
                pb.Substring(item, pb.NewOptional(pb.NewDataLiteral<ui32>(3U)), pb.NewOptional(pb.NewDataLiteral<ui32>(17U)))
            });
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0U));
        UNIT_ASSERT(!item.GetElement(1U));
        UNIT_ASSERT(!item.GetElement(2U));
        UNIT_ASSERT(!item.GetElement(3U));

        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0U), "");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1U), "");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(2U), "");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(3U), "");

        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0U), "0123456789");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1U), "789");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(2U), "012345");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(3U), "3456789");

        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0U), "abcdefghijklmnopqrstuvwxyz");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1U), "hijklmnopqrstuvwxyz");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(2U), "abcdef");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(3U), "defghijklmnopqrst");

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFindAndSubstring) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto type = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<const char*>::Id));
        const auto data0 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<const char*>::Id);
        const auto data1 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>(""));
        const auto data2 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("_/>_/>aab*SEP*bccc"));
        const auto data3 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("*SEP*_/>a>><<___*SEP*a_/>b54b*SEP*c_/>3434cc*SEP*"));
        const auto data4 = pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("(none)"));

        const auto sep1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("");
        const auto sep2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("_/>");
        const auto sep3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("*SEP*");

        const auto list = pb.NewList(type, {data0, data1, data2, data3, data4});
        const auto list1 = pb.NewList(pb.NewDataType(NUdf::TDataType<const char*>::Id), {sep1, sep2, sep3});
        const auto null = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);

        const auto pgmReturn = pb.FlatMap(list,
        [&](TRuntimeNode data) {
            return pb.Map(list1,
                [&](TRuntimeNode sep) {
                    const auto first = pb.Find(data, sep, null);
                    const auto last = pb.RFind(data, sep, null);
                    const auto len = pb.Size(sep);
                    return pb.NewTuple({
                        pb.Substring(data, null, first),
                        pb.Substring(data, pb.Add(first, len), pb.If(pb.Less(first, last), pb.Sub(last, pb.Add(first, len)), pb.NewDataLiteral<ui32>(0U))),
                        pb.Substring(data, pb.Add(last, len), null)
                    });
                });
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0U));
        UNIT_ASSERT(!item.GetElement(1U));
        UNIT_ASSERT(!item.GetElement(2U));

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0U));
        UNIT_ASSERT(!item.GetElement(1U));
        UNIT_ASSERT(!item.GetElement(2U));

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0U));
        UNIT_ASSERT(!item.GetElement(1U));
        UNIT_ASSERT(!item.GetElement(2U));

        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0U), "");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1U), "");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(2U), "");

        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0U), "");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1U), "");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(2U), "");

        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0U), "");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1U), "");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(2U), "");

        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0U), "");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1U), "_/>_/>aab*SEP*bccc");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(2U), "");

        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0U), "");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1U), "");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(2U), "aab*SEP*bccc");

        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0U), "_/>_/>aab");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1U), "");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(2U), "bccc");

        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0U), "");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1U), "*SEP*_/>a>><<___*SEP*a_/>b54b*SEP*c_/>3434cc*SEP*");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(2U), "");

        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0U), "*SEP*");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1U), "a>><<___*SEP*a_/>b54b*SEP*c");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(2U), "3434cc*SEP*");

        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0U), "");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1U), "_/>a>><<___*SEP*a_/>b54b*SEP*c_/>3434cc");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(2U), "");

        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0U), "");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1U), "(none)");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(2U), "");

        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0U), "(none)");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1U), "(none)");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(2U), "(none)");

        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0U), "(none)");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1U), "(none)");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(2U), "(none)");

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFindFromPos) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data = pb.NewDataLiteral<NUdf::EDataSlot::String>("_</|>0123456789</|></|>abcxyz</|>*");
        const auto sep = pb.NewDataLiteral<NUdf::EDataSlot::String>("</|>");

        const auto list = pb.ListFromRange(pb.NewDataLiteral<ui32>(0U), pb.Size(data), pb.NewDataLiteral<ui32>(2U));

        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            return pb.NewTuple({pb.Find(data, sep, item), pb.RFind(data, sep, item)});
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0U).template Get<ui32>(), 1U);
        UNIT_ASSERT(!item.GetElement(1U));

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0U).template Get<ui32>(), 15U);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1U).template Get<ui32>(), 1U);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0U).template Get<ui32>(), 15U);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1U).template Get<ui32>(), 1U);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0U).template Get<ui32>(), 15U);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1U).template Get<ui32>(), 1U);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0U).template Get<ui32>(), 15U);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1U).template Get<ui32>(), 1U);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0U).template Get<ui32>(), 15U);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1U).template Get<ui32>(), 1U);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0U).template Get<ui32>(), 15U);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1U).template Get<ui32>(), 1U);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0U).template Get<ui32>(), 15U);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1U).template Get<ui32>(), 1U);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0U).template Get<ui32>(), 19U);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1U).template Get<ui32>(), 15U);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0U).template Get<ui32>(), 19U);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1U).template Get<ui32>(), 15U);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0U).template Get<ui32>(), 29U);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1U).template Get<ui32>(), 19U);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0U).template Get<ui32>(), 29U);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1U).template Get<ui32>(), 19U);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0U).template Get<ui32>(), 29U);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1U).template Get<ui32>(), 19U);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0U).template Get<ui32>(), 29U);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1U).template Get<ui32>(), 19U);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0U).template Get<ui32>(), 29U);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1U).template Get<ui32>(), 19U);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0U));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1U).template Get<ui32>(), 29U);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0U));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1U).template Get<ui32>(), 29U);

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestListFromRange) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<ui16>(0);
        const auto data1 = pb.NewDataLiteral<ui16>(9);
        const auto data2 = pb.NewDataLiteral<ui16>(1);
        const auto data3 = pb.NewDataLiteral<ui16>(2);
        const auto data4 = pb.NewDataLiteral<ui16>(3);

        const auto list = pb.NewList(pb.NewDataType(NUdf::TDataType<ui16>::Id), {data2, data3, data4});

        const auto pgmReturn = pb.Map(list,
        [&](TRuntimeNode item) {
            const auto l = pb.ListFromRange(data0, data1, item);
            return pb.NewTuple({pb.HasItems(l), pb.Length(l), pb.Head(l), pb.Last(l)});
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0U).template Get<bool>());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1U).template Get<ui64>(), 9ULL);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2U).template Get<ui16>(), 0U);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(3U).template Get<ui16>(), 8U);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0U).template Get<bool>());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1U).template Get<ui64>(), 5ULL);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2U).template Get<ui16>(), 0U);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(3U).template Get<ui16>(), 8U);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item.GetElement(0U).template Get<bool>());
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1U).template Get<ui64>(), 3ULL);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2U).template Get<ui16>(), 0U);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(3U).template Get<ui16>(), 6U);

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestSize) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("aaa");
        const auto data2 = pb.NewDataLiteral<ui32>(3);
        const auto data3 = pb.NewDataLiteral<ui64>(7878786987536ull);
        const auto data4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("qqqqq");
        const auto pgmReturn = pb.NewList(pb.NewDataType(NUdf::TDataType<ui32>::Id),
            {pb.Size(data1), pb.Size(data2), pb.Size(data3), pb.Size(data4)});

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 8);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 5);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestEnumerate) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list1 = pb.NewList(pb.NewDataType(NUdf::TDataType<ui32>::Id),
            {pb.NewDataLiteral<ui32>(34), pb.NewDataLiteral<ui32>(56)});
        const auto pgmReturn = pb.Enumerate(list1);

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui64>(), 0);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<ui32>(), 34);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui64>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<ui32>(), 56);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestEnumerateLazy) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list1 = pb.NewList(pb.NewDataType(NUdf::TDataType<ui32>::Id),
            {pb.NewDataLiteral<ui32>(34), pb.NewDataLiteral<ui32>(56)});
        const auto pgmReturn = pb.Enumerate(pb.LazyList(list1));

        const auto graph = setup.BuildGraph(pgmReturn);

        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui64>(), 0);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<ui32>(), 34);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui64>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<ui32>(), 56);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestEnumerateLazyThenReverse) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list1 = pb.NewList(pb.NewDataType(NUdf::TDataType<ui32>::Id),
            {pb.NewDataLiteral<ui32>(34), pb.NewDataLiteral<ui32>(56)});
        const auto pgmReturn = pb.Reverse(pb.Enumerate(pb.LazyList(list1)));

        const auto graph = setup.BuildGraph(pgmReturn);

        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui64>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<ui32>(), 56);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui64>(), 0);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<ui32>(), 34);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestEnumerateLazyThenSkip) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list1 = pb.NewList(pb.NewDataType(NUdf::TDataType<ui32>::Id),
            {pb.NewDataLiteral<ui32>(34), pb.NewDataLiteral<ui32>(56)});
        const auto one = pb.NewDataLiteral<ui64>(1);
        const auto list = pb.Enumerate(pb.LazyList(list1));
        const auto skip = pb.Skip(list, one);
        const auto pgmReturn = pb.NewTuple({skip, pb.Length(list), pb.Length(skip)});

        const auto graph = setup.BuildGraph(pgmReturn);

        const auto iterator = graph->GetValue().GetElement(0).GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui64>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<ui32>(), 56);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetElement(1).template Get<ui64>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetElement(2).template Get<ui64>(), 1);
    }

    Y_UNIT_TEST_LLVM(TestEnumerateLazyThenTake) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list1 = pb.NewList(pb.NewDataType(NUdf::TDataType<ui32>::Id),
            {pb.NewDataLiteral<ui32>(34), pb.NewDataLiteral<ui32>(56)});
        const auto one = pb.NewDataLiteral<ui64>(1);
        const auto list = pb.Enumerate(pb.LazyList(list1));
        const auto take = pb.Take(list, one);
        const auto pgmReturn = pb.NewTuple({take, pb.Length(list), pb.Length(take)});

        const auto graph = setup.BuildGraph(pgmReturn);

        const auto iterator = graph->GetValue().GetElement(0).GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui64>(), 0);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<ui32>(), 34);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetElement(1).template Get<ui64>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetElement(2).template Get<ui64>(), 1);
    }

    template<bool LLVM>
    void TestSortImpl(bool asc) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto key1 = pb.NewDataLiteral<ui32>(1);
        const auto key2 = pb.NewDataLiteral<ui32>(2);
        const auto key3 = pb.NewDataLiteral<ui32>(3);

        const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("aaa");
        const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("");
        const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("qqq");

        const auto keyType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto payloadType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        auto structType = pb.NewEmptyStructType();
        structType = pb.NewStructType(structType, "payload", payloadType);
        structType = pb.NewStructType(structType, "key", keyType);

        std::vector<std::pair<std::string_view, TRuntimeNode>> map1 = {
            { "key", key1 },
            { "payload", payload1 }
        };

        std::vector<std::pair<std::string_view, TRuntimeNode>> map2 = {
            { "key", key2 },
            { "payload", payload2 }
        };

        std::vector<std::pair<std::string_view, TRuntimeNode>> map3 = {
            { "key", key3 },
            { "payload", payload3 }
        };

        const auto list = pb.NewList(structType, {
            pb.NewStruct(map2),
            pb.NewStruct(map1),
            pb.NewStruct(map3)
        });

        {
            const auto pgmReturn = pb.Sort(list, pb.NewDataLiteral(asc),
                [&](TRuntimeNode item) {
                return pb.Member(item, "key");
            });

            if (asc) {
                // ascending sort
                const auto graph = setup.BuildGraph(pgmReturn);
                const auto iterator = graph->GetValue().GetListIterator();
                NUdf::TUnboxedValue item;

                UNIT_ASSERT(iterator.Next(item));
                UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui32>(), 1);
                UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "aaa");
                UNIT_ASSERT(iterator.Next(item));
                UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui32>(), 2);
                UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "");
                UNIT_ASSERT(iterator.Next(item));
                UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui32>(), 3);
                UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "qqq");
                UNIT_ASSERT(!iterator.Next(item));
                UNIT_ASSERT(!iterator.Next(item));
            } else {
                // descending  sort
                const auto graph = setup.BuildGraph(pgmReturn);
                const auto iterator = graph->GetValue().GetListIterator();
                NUdf::TUnboxedValue item;

                UNIT_ASSERT(iterator.Next(item));
                UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui32>(), 3);
                UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "qqq");
                UNIT_ASSERT(iterator.Next(item));
                UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui32>(), 2);
                UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "");
                UNIT_ASSERT(iterator.Next(item));
                UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui32>(), 1);
                UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "aaa");
                UNIT_ASSERT(!iterator.Next(item));
                UNIT_ASSERT(!iterator.Next(item));
            }
        }
    }

    Y_UNIT_TEST_LLVM(TestSort) {
        TestSortImpl<LLVM>(true);
        TestSortImpl<LLVM>(false);
    }

    using TTriple = std::tuple<ui32, ui32, ui32>;

    TRuntimeNode TupleOrder(TProgramBuilder& pb, bool asc1, bool asc2, bool asc3) {
        TVector<TRuntimeNode> ascending(3);
        ascending[0] = pb.NewDataLiteral(asc1);
        ascending[1] = pb.NewDataLiteral(asc2);
        ascending[2] = pb.NewDataLiteral(asc3);

        TVector<TType*> tupleTypes(3);
        tupleTypes[0] = pb.NewDataType(NUdf::TDataType<bool>::Id);
        tupleTypes[1] = pb.NewDataType(NUdf::TDataType<bool>::Id);
        tupleTypes[2] = pb.NewDataType(NUdf::TDataType<bool>::Id);

        return pb.NewTuple(pb.NewTupleType(tupleTypes), ascending);
    }

    template<bool LLVM>
    TVector<TTriple> SortTuples(TSetup<LLVM>& setup, TRuntimeNode list, TRuntimeNode order)
    {
        auto& pb = *setup.PgmBuilder;
        const auto pgmReturn = pb.Sort(list, order, [](TRuntimeNode item) { return item; });
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();

        TVector<TTriple> result;
        for (NUdf::TUnboxedValue value; iterator.Next(value);) {
            ui32 first = value.GetElement(0).template Get<ui32>();
            ui32 second = value.GetElement(1).template Get<ui32>();
            ui32 third = value.GetElement(2).template Get<ui32>();
            result.push_back(TTriple{ first, second, third });
        }
        UNIT_ASSERT(!iterator.Skip());
        return result;
    }

    Y_UNIT_TEST_LLVM(TestSortTuples) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listMaker = [&]()
        {
            TTriple testData[] = {
                { 1, 1, 1 },
                { 1, 1, 2 },
                { 1, 2, 3 },
                { 1, 3, 0 },
                { 2, 0, 1 },
                { 2, 1, 0 },
            };

            TVector<TRuntimeNode> tuplesList;
            for (ui32 i = 0; i < Y_ARRAY_SIZE(testData); i++) {
                TVector<TRuntimeNode> elements(3);
                elements[0] = pb.NewDataLiteral(std::get<0>(testData[i]));
                elements[1] = pb.NewDataLiteral(std::get<1>(testData[i]));
                elements[2] = pb.NewDataLiteral(std::get<2>(testData[i]));
                tuplesList.push_back(pb.NewTuple(elements));
            }

            TVector<TType*> tupleTypes(3);
            tupleTypes[0] = pb.NewDataType(NUdf::TDataType<ui32>::Id);
            tupleTypes[1] = pb.NewDataType(NUdf::TDataType<ui32>::Id);
            tupleTypes[2] = pb.NewDataType(NUdf::TDataType<ui32>::Id);

            return pb.NewList(pb.NewTupleType(tupleTypes), tuplesList);
        };

        {
            TRuntimeNode order = TupleOrder(pb, true, true, true);
            TTriple expectedData[] = {
                { 1, 1, 1 },
                { 1, 1, 2 },
                { 1, 2, 3 },
                { 1, 3, 0 },
                { 2, 0, 1 },
                { 2, 1, 0 },
            };
            TVector<TTriple> expected(expectedData, expectedData + sizeof(expectedData) / sizeof(*expectedData));
            TVector<TTriple> result = SortTuples<LLVM>(setup, listMaker(), order);
            UNIT_ASSERT_EQUAL(result, expected);
        }

        {
            TRuntimeNode order = TupleOrder(pb, false, false, false);
            TTriple expectedData[] = {
                { 2, 1, 0 },
                { 2, 0, 1 },
                { 1, 3, 0 },
                { 1, 2, 3 },
                { 1, 1, 2 },
                { 1, 1, 1 },
            };
            TVector<TTriple> expected(expectedData, expectedData + sizeof(expectedData) / sizeof(*expectedData));
            TVector<TTriple> result = SortTuples<LLVM>(setup, listMaker(), order);
            UNIT_ASSERT_EQUAL(result, expected);
        }

        {
            TRuntimeNode order = TupleOrder(pb, true, false, true);
            TTriple expectedData[] = {
                { 1, 3, 0 },
                { 1, 2, 3 },
                { 1, 1, 1 },
                { 1, 1, 2 },
                { 2, 1, 0 },
                { 2, 0, 1 },
            };
            TVector<TTriple> expected(expectedData, expectedData + sizeof(expectedData) / sizeof(*expectedData));
            TVector<TTriple> result = SortTuples<LLVM>(setup, listMaker(), order);
            UNIT_ASSERT_EQUAL(result, expected);
        }

        {
            TRuntimeNode order = TupleOrder(pb, false, true, false);
            TTriple expectedData[] = {
                { 2, 0, 1 },
                { 2, 1, 0 },
                { 1, 1, 2 },
                { 1, 1, 1 },
                { 1, 2, 3 },
                { 1, 3, 0 },
            };
            TVector<TTriple> expected(expectedData, expectedData + sizeof(expectedData) / sizeof(*expectedData));
            TVector<TTriple> result = SortTuples<LLVM>(setup, listMaker(), order);
            UNIT_ASSERT_EQUAL(result, expected);
        }
    }

    Y_UNIT_TEST_LLVM(TestAsList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto pgmReturn = pb.AsList(pb.NewDataLiteral<ui32>(34));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 34);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestListIfTrue) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto pgmReturn = pb.ListIf(pb.NewDataLiteral(true),
            pb.NewDataLiteral<ui32>(34));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 34);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestListIfFalse) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto pgmReturn = pb.ListIf(pb.NewDataLiteral(false),
            pb.NewDataLiteral<ui32>(34));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        UNIT_ASSERT(!iterator.Skip());
        UNIT_ASSERT(!iterator.Skip());
    }

    Y_UNIT_TEST_LLVM(TestNth) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto element0 = pb.NewDataLiteral<ui32>(34);
        const auto element1 = pb.NewDataLiteral<ui32>(56);
        const auto tuple = pb.NewTuple({element0, element1});
        const auto pgmReturn = pb.Nth(tuple, 1);

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto res = graph->GetValue().template Get<ui32>();
        UNIT_ASSERT_VALUES_EQUAL(res, 56);
    }

    Y_UNIT_TEST_LLVM(NonDeterministicEnv) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<float>(1);
        const auto data2 = pb.NewDataLiteral<float>(2);
        const auto data3 = pb.NewDataLiteral<float>(3);
        const auto list = pb.NewList(pb.NewDataType(NUdf::TDataType<float>::Id), {data1, data2, data3});
        const auto pgmReturn = pb.Sort(list,
            pb.NewDataLiteral(false),
            [](TRuntimeNode item) { return item; }
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(std::abs(item.template Get<float>() - 3.0) < 0.001);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(std::abs(item.template Get<float>() - 2.0) < 0.001);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(std::abs(item.template Get<float>() - 1.0) < 0.001);
        UNIT_ASSERT(false == iterator.Next(item));
        UNIT_ASSERT(false == iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestIndexDictContains) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto item1 = pb.NewDataLiteral<ui32>(7);
        const auto item2 = pb.NewDataLiteral<ui32>(16);
        const auto list = pb.NewList(pb.NewDataType(NUdf::TDataType<ui32>::Id), {item1, item2});
        const auto dict = pb.ToIndexDict(list);
        const auto type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
        const auto key0 = pb.NewDataLiteral<ui64>(0);
        const auto key1 = pb.NewDataLiteral<ui64>(1);
        const auto key2 = pb.NewDataLiteral<ui64>(2);
        const auto keys = pb.NewList(type, {key0, key1, key2});
        const auto pgmReturn = pb.Map(keys,
            [&](TRuntimeNode key) { return pb.Contains(dict, key); }
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestIndexDictLookup) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto item1 = pb.NewDataLiteral<i32>(7);
        const auto item2 = pb.NewDataLiteral<i32>(16);
        const auto list = pb.NewList(pb.NewDataType(NUdf::TDataType<i32>::Id), {item1, item2});
        const auto dict = pb.ToIndexDict(list);
        const auto type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
        const auto key0 = pb.NewDataLiteral<ui64>(0);
        const auto key1 = pb.NewDataLiteral<ui64>(1);
        const auto key2 = pb.NewDataLiteral<ui64>(2);
        const auto keys = pb.NewList(type, {key0, key1, key2});
        const auto pgmReturn = pb.Map(keys,
            [&](TRuntimeNode key) { return pb.Lookup(dict, key); }
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item);
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 7);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item);
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<i32>(), 16);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestIndexDictContainsLazy) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto item1 = pb.NewDataLiteral<ui32>(7);
        const auto item2 = pb.NewDataLiteral<ui32>(16);
        const auto list = pb.NewList(pb.NewDataType(NUdf::TDataType<ui32>::Id), {item1, item2});
        const auto dict = pb.ToIndexDict(pb.LazyList(list));
        const auto type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
        const auto key0 = pb.NewDataLiteral<ui64>(0);
        const auto key1 = pb.NewDataLiteral<ui64>(1);
        const auto key2 = pb.NewDataLiteral<ui64>(2);
        const auto keys = pb.NewList(type, {key0, key1, key2});
        const auto pgmReturn = pb.Map(keys,
            [&](TRuntimeNode key) { return pb.Contains(dict, key); }
        );

        const auto graph = setup.BuildGraph(pgmReturn);

        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), true);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<bool>(), false);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestIndexDictLookupLazy) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto item1 = pb.NewDataLiteral<ui32>(7);
        const auto item2 = pb.NewDataLiteral<ui32>(16);
        const auto list = pb.NewList(pb.NewDataType(NUdf::TDataType<ui32>::Id), {item1, item2});
        const auto dict = pb.ToIndexDict(pb.LazyList(list));
        const auto type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
        const auto key0 = pb.NewDataLiteral<ui64>(0);
        const auto key1 = pb.NewDataLiteral<ui64>(1);
        const auto key2 = pb.NewDataLiteral<ui64>(2);
        const auto keys = pb.NewList(type, {key0, key1, key2});
        const auto pgmReturn = pb.Map(keys,
            [&](TRuntimeNode key) { return pb.Lookup(dict, key); }
        );

        const auto graph = setup.BuildGraph(pgmReturn);

        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item);
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 7);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(item);
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 16);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestToBytes) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto type = pb.NewDataType(NUdf::TDataType<float>::Id);
        const auto data1 = pb.NewDataLiteral(0.f);
        const auto data2 = pb.NewDataLiteral(-3.14f);
        const auto data3 = pb.NewDataLiteral(-HUGE_VALF);
        const auto data4 = pb.NewDataLiteral(HUGE_VALF);
        const auto list = pb.NewList(type, {data1, data2, data3, data4});
        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
                return pb.ToBytes(item);
            });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "\x00\x00\x00\x00");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "\xc3\xf5\x48\xc0");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "\x00\x00\x80\xff");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "\x00\x00\x80\x7f");
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestToBytesOpt) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto type = pb.NewDataType(NUdf::TDataType<float>::Id, true);
        const auto data0 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<float>::Id);
        const auto data1 = pb.NewOptional(pb.NewDataLiteral(0.f));
        const auto data2 = pb.NewOptional(pb.NewDataLiteral(-3.14f));
        const auto data3 = pb.NewOptional(pb.NewDataLiteral(-HUGE_VALF));
        const auto data4 = pb.NewOptional(pb.NewDataLiteral(HUGE_VALF));
        const auto list = pb.NewList(type, {data0, data1, data2, data3, data4});
        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
                return pb.ToBytes(item);
            });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "\x00\x00\x00\x00");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "\xc3\xf5\x48\xc0");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "\x00\x00\x80\xff");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "\x00\x00\x80\x7f");
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestToStringTemporarryUtf8) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto type = pb.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id);
        const auto data0 = pb.NewDataLiteral<NUdf::EDataSlot::Utf8>("long prefix ");
        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::Utf8>("01234567890 long string");
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::Utf8>("01234567890 very long string");
        const auto list = pb.NewList(type, {data1, data2});
        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
                return pb.ToString(pb.Concat(data0, item));
            });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "long prefix 01234567890 long string");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "long prefix 01234567890 very long string");
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFromString) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto type = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("234");
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("abc");
        const auto list = pb.NewList(type, {data1, data2});
        const auto pgmReturn = pb.Map(list,
            [&](TRuntimeNode item) {
                return pb.FromString(item, pb.NewDataType(NUdf::TDataType<ui32>::Id));
            });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 234);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestStrictFromString) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        TVector<TRuntimeNode> tupleItems;
        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("234");
        tupleItems.push_back(pb.StrictFromString(data1, pb.NewDataType(NUdf::TDataType<ui32>::Id)));
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("-1");
        tupleItems.push_back(pb.StrictFromString(data2, pb.NewDataType(NUdf::TDataType<i32>::Id)));
        const auto data3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("3.1415926");
        tupleItems.push_back(pb.StrictFromString(data3, pb.NewDataType(NUdf::TDataType<double>::Id)));
        const auto pgmReturn = pb.NewTuple(tupleItems);

        {
            const auto graph = setup.BuildGraph(pgmReturn);
            const auto res = graph->GetValue();
            UNIT_ASSERT_VALUES_EQUAL(res.GetElement(0).template Get<ui32>(), 234);
            UNIT_ASSERT_VALUES_EQUAL(res.GetElement(1).template Get<i32>(), -1);
            UNIT_ASSERT_VALUES_EQUAL(res.GetElement(2).template Get<double>(), 3.1415926);
        }

        {
            const auto wrongData = pb.NewDataLiteral<NUdf::EDataSlot::String>("vgfsbhj");
            const auto fail = pb.StrictFromString(wrongData, pb.NewDataType(NUdf::TDataType<ui32>::Id));
            const auto failgraph = setup.BuildGraph(fail, {});
            UNIT_ASSERT_EXCEPTION(failgraph->GetValue(), yexception);
        }

        {
            const auto wrongData = pb.NewDataLiteral<NUdf::EDataSlot::String>("");
            const auto fail = pb.StrictFromString(wrongData, pb.NewDataType(NUdf::TDataType<double>::Id));
            const auto failgraph = setup.BuildGraph(fail, {});
            UNIT_ASSERT_EXCEPTION(failgraph->GetValue(), yexception);
        }
    }

    Y_UNIT_TEST_LLVM(TestFromBytes) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        TVector<TRuntimeNode> tupleItems;
        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>(TString("\xEA\x00\x00\x00", 4));
        tupleItems.push_back(pb.FromBytes(data1, NUdf::TDataType<ui32>::Id));
        const auto data2 = pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<const char*>::Id);
        tupleItems.push_back(pb.FromBytes(data2, NUdf::TDataType<ui32>::Id));
        const auto pgmReturn = pb.NewTuple(tupleItems);

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto res = graph->GetValue();
        UNIT_ASSERT(res.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(res.GetElement(0).template Get<ui32>(), 234);
        UNIT_ASSERT(!res.GetElement(1));
    }

    Y_UNIT_TEST_LLVM(TestMTRand) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto seed = pb.NewDataLiteral<ui64>(42);
        auto pgmReturn = pb.NewEmptyList(pb.NewDataType(NUdf::TDataType<ui64>::Id));
        auto rnd = pb.NewMTRand(seed);
        const ui32 n = 5;
        const ui64 expectedValues[n] = {
            13930160852258120406ull,
            11788048577503494824ull,
            13874630024467741450ull,
            2513787319205155662ull,
            16662371453428439381ull,
        };
        for (ui32 i = 0; i < n; ++i) {
            const auto pair = pb.NextMTRand(rnd);
            pgmReturn = pb.Append(pgmReturn, pb.Nth(pair, 0));
            rnd = pb.Nth(pair, 1);
        }

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;

        for (ui32 i = 0; i < n; ++i) {
            UNIT_ASSERT(iterator.Next(item));
            const ui64 value = item.template Get<ui64>();
            //Cout << value << Endl;
            UNIT_ASSERT_VALUES_EQUAL(value, expectedValues[i]);
        }

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestRandom) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const double expectedValue1 = 0.13387664401253274;
        const ui64 expectedValue2 = 2516265689700432462;

        const auto rnd1 = pb.Random({});
        const auto rnd2 = pb.RandomNumber({});
        TVector<TRuntimeNode> args;
        args.push_back(rnd1);
        args.push_back(rnd2);
        const auto pgmReturn = pb.NewTuple(args);

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto tuple = graph->GetValue();
        UNIT_ASSERT_DOUBLES_EQUAL(tuple.GetElement(0).template Get<double>(), expectedValue1, 1e-3);
        UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(1).template Get<ui64>(), expectedValue2);
    }

    Y_UNIT_TEST_LLVM(TestNow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const ui64 expectedValue = 10000000000000;

        const auto ts = pb.Now({});
        TVector<TRuntimeNode> args;
        args.push_back(ts);
        const auto pgmReturn = pb.NewTuple(args);

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto tuple = graph->GetValue();
        UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(0).template Get<ui64>(), expectedValue);
    }

    Y_UNIT_TEST_LLVM(TestSkipAndTakeOverStream) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto type = pb.NewDataType(NUdf::TDataType<ui8>::Id);
        const auto data0 = pb.NewDataLiteral<ui8>(0);
        const auto data1 = pb.NewDataLiteral<ui8>(1);
        const auto data2 = pb.NewDataLiteral<ui8>(2);
        const auto data3 = pb.NewDataLiteral<ui8>(3);
        const auto data4 = pb.NewDataLiteral<ui8>(4);
        const auto data5 = pb.NewDataLiteral<ui8>(5);
        const auto data6 = pb.NewDataLiteral<ui8>(6);
        const auto data7 = pb.NewDataLiteral<ui8>(7);
        const auto data8 = pb.NewDataLiteral<ui8>(8);
        const auto data9 = pb.NewDataLiteral<ui8>(9);
        const auto list = pb.NewList(type, {data0, data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.Take(pb.Skip(pb.Iterator(list, {}), pb.NewDataLiteral<ui64>(4ULL)), pb.NewDataLiteral<ui64>(3ULL));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 4);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 5);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 6);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestSkipAndTakeOverFlow) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto type = pb.NewDataType(NUdf::TDataType<ui8>::Id);
        const auto data0 = pb.NewDataLiteral<ui8>(0);
        const auto data1 = pb.NewDataLiteral<ui8>(1);
        const auto data2 = pb.NewDataLiteral<ui8>(2);
        const auto data3 = pb.NewDataLiteral<ui8>(3);
        const auto data4 = pb.NewDataLiteral<ui8>(4);
        const auto data5 = pb.NewDataLiteral<ui8>(5);
        const auto data6 = pb.NewDataLiteral<ui8>(6);
        const auto data7 = pb.NewDataLiteral<ui8>(7);
        const auto data8 = pb.NewDataLiteral<ui8>(8);
        const auto data9 = pb.NewDataLiteral<ui8>(9);
        const auto list = pb.NewList(type, {data0, data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.FromFlow(pb.Take(pb.Skip(pb.ToFlow(list), pb.NewDataLiteral<ui64>(4ULL)), pb.NewDataLiteral<ui64>(3ULL)));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 4);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 5);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui8>(), 6);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestLazyListFromArray) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto type = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto data0 = pb.NewDataLiteral<ui32>(1U);
        const auto data1 = pb.NewDataLiteral<ui32>(2U);
        const auto data2 = pb.NewDataLiteral<ui32>(3U);
        const auto array = pb.NewList(type, {data0, data1, data2});

        const auto pgmReturn = pb.LazyList(array);

        const auto graph = setup.BuildGraph(pgmReturn);

        const auto list = graph->GetValue();
        UNIT_ASSERT_VALUES_EQUAL(list.GetListLength(), 3U);
        UNIT_ASSERT_VALUES_EQUAL(list.GetElements(), nullptr);
        UNIT_ASSERT_VALUES_EQUAL(list.GetElement(0).template Get<ui32>(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(list.GetElement(1).template Get<ui32>(), 2U);
        UNIT_ASSERT_VALUES_EQUAL(list.GetElement(2).template Get<ui32>(), 3U);
    }

    Y_UNIT_TEST_LLVM(TestCollectLazyList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto type = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto data0 = pb.NewDataLiteral<ui32>(1U);
        const auto data1 = pb.NewDataLiteral<ui32>(2U);
        const auto data2 = pb.NewDataLiteral<ui32>(3U);
        const auto array = pb.NewList(type, {data0, data1, data2});

        const auto pgmReturn = pb.Collect(pb.LazyList(array));

        const auto graph = setup.BuildGraph(pgmReturn);

        const auto list = graph->GetValue();
        UNIT_ASSERT_VALUES_EQUAL(list.GetListLength(), 3U);
        UNIT_ASSERT(list.GetElements());
        UNIT_ASSERT_VALUES_EQUAL(list.GetElement(0).template Get<ui32>(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(list.GetElement(1).template Get<ui32>(), 2U);
        UNIT_ASSERT_VALUES_EQUAL(list.GetElement(2).template Get<ui32>(), 3U);
    }

    Y_UNIT_TEST_LLVM(TestAddAllTimezones) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto zones = pb.ListFromRange(pb.NewDataLiteral<ui16>(0U), pb.NewDataLiteral<ui16>(1000U), pb.NewDataLiteral<ui16>(1U));
        const auto pgmReturn = pb.Collect(pb.Map(zones, [&](TRuntimeNode id){ return pb.AddTimezone(pb.CurrentUtcDate({id}), id); }));
        const auto graph = setup.BuildGraph(pgmReturn);

        const auto list = graph->GetValue();
        UNIT_ASSERT(list.GetElement(0));
        UNIT_ASSERT(list.GetElement(1));
        UNIT_ASSERT(list.GetElement(2));
        UNIT_ASSERT(list.GetElement(585));
        UNIT_ASSERT(!list.GetElement(586));
        UNIT_ASSERT(list.GetElement(587));
        UNIT_ASSERT(list.GetElement(592));
        UNIT_ASSERT(!list.GetElement(593));
        UNIT_ASSERT(list.GetElement(594));
    }

    Y_UNIT_TEST_LLVM(TestRemoveTimezone) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto type = pb.NewDataType(NUdf::TDataType<const char*>::Id);
        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("");
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("2019-10-24T13:01:37,Zulu");
        const auto data3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("2019-10-24T13:01:37,Japan");
        const auto data4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("2019-10-24T13:01:37,Jamaica");

        const auto datetimeType = pb.NewDataType(NUdf::EDataSlot::Datetime, true);
        const auto datetimeTypeTz = pb.NewDataType(NUdf::EDataSlot::TzDatetime, true);

        const auto list = pb.NewList(type, {data1, data2, data3, data4});

        const auto pgmReturn = pb.FlatMap(list,
            [&](TRuntimeNode data) {
                return pb.Convert(pb.FromString(data, datetimeTypeTz), datetimeType);
            }
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto value = graph->GetValue();

        UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), 3ULL);

        UNIT_ASSERT(!value.GetElement(0U).GetTimezoneId());
        UNIT_ASSERT(!value.GetElement(1U).GetTimezoneId());
        UNIT_ASSERT(!value.GetElement(2U).GetTimezoneId());

        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(0U).template Get<ui32>(), 1571922097U);
        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(1U).template Get<ui32>(), 1571889697U);
        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(2U).template Get<ui32>(), 1571940097U);
    }
#if !defined(MKQL_RUNTIME_VERSION) || MKQL_RUNTIME_VERSION >= 25u
    Y_UNIT_TEST_LLVM(TestSqueezeToList) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral(0.0);
        const auto data1 = pb.NewDataLiteral(1.1);
        const auto data2 = pb.NewDataLiteral(-3.14);
        const auto data3 = pb.NewDataLiteral(121324.323);
        const auto data4 = pb.NewDataLiteral(-7898.8);
        const auto type = pb.NewDataType(NUdf::TDataType<double>::Id);
        const auto list = pb.NewList(type, {data0, data1, data2, data3, data4});

        const auto pgmReturn = pb.FromFlow(pb.SqueezeToList(pb.ToFlow(list), pb.NewDataLiteral<ui64>(1000ULL)));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue full;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(full));
        NUdf::TUnboxedValue stub;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(stub));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(stub));

        UNIT_ASSERT_VALUES_EQUAL(full.GetListLength(), 5ULL);
        UNIT_ASSERT_VALUES_EQUAL(full.GetElement(0U).template Get<double>(), 0.0);
        UNIT_ASSERT_VALUES_EQUAL(full.GetElement(1U).template Get<double>(), 1.1);
        UNIT_ASSERT_VALUES_EQUAL(full.GetElement(2U).template Get<double>(), -3.14);
        UNIT_ASSERT_VALUES_EQUAL(full.GetElement(3U).template Get<double>(), 121324.323);
        UNIT_ASSERT_VALUES_EQUAL(full.GetElement(4U).template Get<double>(), -7898.8);
    }

    Y_UNIT_TEST_LLVM(TestSqueezeToListWithLimit) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral(0.0f);
        const auto data1 = pb.NewDataLiteral(1.1f);
        const auto data2 = pb.NewDataLiteral(-3.14f);
        const auto data3 = pb.NewDataLiteral(12.323f);
        const auto data4 = pb.NewDataLiteral(-7898.8f);
        const auto type = pb.NewDataType(NUdf::TDataType<float>::Id);
        const auto list = pb.NewList(type, {data0, data1, data2, data3, data4});

        const auto pgmReturn = pb.FromFlow(pb.SqueezeToList(pb.ToFlow(list), pb.NewDataLiteral<ui64>(3ULL)));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue full;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(full));
        NUdf::TUnboxedValue stub;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(stub));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(stub));

        UNIT_ASSERT_VALUES_EQUAL(full.GetListLength(), 3ULL);
        UNIT_ASSERT_VALUES_EQUAL(full.GetElement(0U).template Get<float>(), 0.0f);
        UNIT_ASSERT_VALUES_EQUAL(full.GetElement(1U).template Get<float>(), 1.1f);
        UNIT_ASSERT_VALUES_EQUAL(full.GetElement(2U).template Get<float>(), -3.14f);
    }
#endif
    Y_UNIT_TEST_LLVM(TestPerfHolders) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto ui32Type = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto strType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto structType = pb.NewStructType({{"key", ui32Type}, {"value", strType}});
        const auto mark = pb.NewDataLiteral<NUdf::EDataSlot::String>("!");

        const auto listType = pb.NewListType(structType);
        TCallableBuilder listRet(pb.GetTypeEnvironment(), "TestList", listType);
        const auto listNode = listRet.Build();

        const auto pgmReturn = pb.Map(pb.LazyList(TRuntimeNode(listNode, false)),
            [&](TRuntimeNode item) {
            return pb.NewStruct({
                {"key", pb.Member(item, "key")},
                {"value", pb.AggrConcat(mark, pb.Member(item, "value"))}
            });
        });

        std::vector<ui32> src(10000U);
        std::iota(src.begin(), src.end(), 0U);

        const auto myStructFactory = [](const THolderFactory& factory, ui32 i) {
            NUdf::TUnboxedValue* itemsPtr = nullptr;
            const auto structObj = factory.CreateDirectArrayHolder(2U, itemsPtr);
            itemsPtr[0] = NUdf::TUnboxedValuePod(ui32(i));
            itemsPtr[1] = MakeString("ABCDEFGHIJKL");
            return structObj;
        };

        const auto t1 = TInstant::Now();
        {
            const auto graph = setup.BuildGraph(pgmReturn, {listNode});
            NUdf::TUnboxedValue* items = nullptr;
            graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(src.size(), items));
            std::transform(src.cbegin(), src.cend(), items, std::bind(myStructFactory, std::ref(graph->GetHolderFactory()), std::placeholders::_1));

            const auto iterator = graph->GetValue().GetListIterator();
            ui32 i = 0U;
            for (NUdf::TUnboxedValue current; iterator.Next(current); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(current.GetElement(0).template Get<ui32>(), i);
                UNBOXED_VALUE_STR_EQUAL(current.GetElement(1), "!ABCDEFGHIJKL");
            }
        }

        const auto t2 = TInstant::Now();
        Cout << t2 - t1 << Endl;
    }

    Y_UNIT_TEST_LLVM(TestPerfGrep) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto ui32Type = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto strType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        auto structType = pb.NewEmptyStructType();
        structType = pb.NewStructType(structType, "key", ui32Type);
        structType = pb.NewStructType(structType, "value", strType);
        const auto keyIndex = AS_TYPE(TStructType, structType)->GetMemberIndex("key");
        const auto valueIndex = AS_TYPE(TStructType, structType)->GetMemberIndex("value");

        TRuntimeNode rowArg = pb.Arg(structType);

        const auto pgmReturn = pb.Equals(pb.Member(rowArg, "value"),
            pb.NewDataLiteral<NUdf::EDataSlot::String>("ABCDE"));

        const auto t1 = TInstant::Now();
        {
            const auto graph = setup.BuildGraph(pgmReturn, {rowArg.GetNode()});
            const auto row = graph->GetEntryPoint(0, true);

            NUdf::TUnboxedValue* items = nullptr;
            const auto structObj = graph->GetHolderFactory().CreateDirectArrayHolder(2, items);
            graph->Prepare();
            const ui32 n = 10000;
            for (ui32 i = 0; i < n; ++i) {
                items[keyIndex] = NUdf::TUnboxedValuePod(i);
                items[valueIndex] = NUdf::TUnboxedValuePod::Embedded("ABCDF");
                row->SetValue(graph->GetContext(), NUdf::TUnboxedValuePod(structObj));
                const bool keep = graph->GetValue().template Get<bool>();
                UNIT_ASSERT(!keep);
            }
        }

        const auto t2 = TInstant::Now();
        Cout << t2 - t1 << Endl;
    }

    Y_NO_INLINE NUdf::TUnboxedValuePod SpecialFunc(const NUdf::TUnboxedValuePod* args) {
        const auto stringRef = args[1].AsStringRef();
        return NUdf::TUnboxedValuePod(stringRef.Size() == 5 && std::memcmp(stringRef.Data(), "ABCDE", 5) == 0);
    }

    Y_UNIT_TEST_LLVM(TestPerfGrepSpecialFunc) {
        const auto t1 = TInstant::Now();
        {
            NUdf::TUnboxedValuePod items[2];
            const ui32 n = 10000;
            for (ui32 i = 0; i < n; ++i) {
                items[0] = NUdf::TUnboxedValuePod(i);
                items[1] = NUdf::TUnboxedValuePod::Embedded("ABCDF");
                bool keep = SpecialFunc(items).template Get<bool>();
                UNIT_ASSERT(!keep);
            }
        }

        const auto t2 = TInstant::Now();
        Cout << t2 - t1 << Endl;
    }
}

}
}
