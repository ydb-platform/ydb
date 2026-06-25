#include "mkql_computation_node_ut.h"
#include "mkql_program_builder_test_utils.h"

#include <yql/essentials/minikql/mkql_node_printer.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

#include <cfloat>
#include <utility>
#include <random>

namespace NKikimr {
namespace NMiniKQL {

namespace {

constexpr auto TotalSambles = 222222U;

template <typename T>
void AssertNumericValuesEqual(T actual, T expected) {
    if constexpr (std::is_floating_point_v<T>) {
        UNIT_ASSERT(std::abs(actual - expected) < 0.0003);
    } else {
        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }
}

} // namespace

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
        switch (keys(gen)) {
            case 0U:
                return std::make_tuple(dates(gen), "N", "O", prices(gen), taxes(gen), discs(gen), qntts(gen));
            case 1U:
                return std::make_tuple(dates(gen), "A", "F", prices(gen), taxes(gen), discs(gen), qntts(gen));
            case 2U:
                return std::make_tuple(dates(gen), "N", "F", prices(gen), taxes(gen), discs(gen), qntts(gen));
            case 3U:
                return std::make_tuple(dates(gen), "R", "F", prices(gen), taxes(gen), discs(gen), qntts(gen));
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

    const auto str = NTest::ConvertValueToLiteralNode(pb, TStringBuf("0123456789AB"));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{0, 3, 9, 13, Max<ui32>()});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.ByteAt(str, item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<ui8>>{{0x30}, {0x33}, {0x39}, {}, {}});
}

Y_UNIT_TEST_LLVM(TestStringByteAt) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto str = NTest::ConvertValueToLiteralNode(pb, TStringBuf("0123456789ABCDEFGHIJKL"));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{0, 3, 21, 22, Max<ui32>()});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.ByteAt(str, item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<ui8>>{{0x30}, {0x33}, {0x4C}, {}, {}});
}

Y_UNIT_TEST_LLVM(TestCountBitsUI8) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui8>{0x00, 0x01, 0x10, 0x13, 0xF0, 0x0F, 0x55, 0xAA, 0x7F, 0xFF});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.CountBits(item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui8>{0, 1, 1, 3, 4, 4, 4, 4, 7, 8});
}

Y_UNIT_TEST_LLVM(TestCountBitsUI16) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui16>{0x0000, 0x0100, 0x0010, 0x0130, 0xF000, 0x0F00, 0x0550, 0xA00A, 0x700F, 0x0FF0});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.CountBits(item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui8>{0, 1, 1, 3, 4, 4, 4, 4, 7, 8});
}

Y_UNIT_TEST_LLVM(TestShiftLeft) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui8>{0, 1, 7, 10, 30, 32, 40, 57, 200, 255});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.ShiftLeft(NTest::ConvertValueToLiteralNode(pb, ui32(0x830500F1U)), item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{
                                                          0x830500F1U, 0x060A01E2U, 0x82807880U, 0x1403C400U, 0x40000000U,
                                                          0U, 0U, 0U, 0U, 0U});
}

Y_UNIT_TEST_LLVM(TestShiftRight) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui8>{0, 1, 7, 10, 30, 32, 40, 57, 200, 255});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.ShiftRight(NTest::ConvertValueToLiteralNode(pb, ui32(0x830500F1U)), item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{
                                                          0x830500F1U, 0x41828078U, 0x01060A01U, 0x20C140U, 0x2U,
                                                          0U, 0U, 0U, 0U, 0U});
}

Y_UNIT_TEST_LLVM(TestRotLeft) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui8>{0, 1, 7, 10, 30, 32, 40, 57, 200, 255});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.RotLeft(NTest::ConvertValueToLiteralNode(pb, ui16(0x87F5U)), item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui16>{
                                                          0x87F5U, 0x0FEBU, 0xFAC3U, 0xD61FU, 0x61FDU,
                                                          0x87F5U, 0xF587U, 0xEB0FU, 0xF587U, 0xC3FAU});
}

Y_UNIT_TEST_LLVM(TestRotRight) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui8>{0, 1, 7, 10, 30, 32, 40, 57, 200, 255});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.RotRight(NTest::ConvertValueToLiteralNode(pb, ui16(0x87F5U)), item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui16>{
                                                          0x87F5U, 0xC3FAU, 0xEB0FU, 0xFD61U, 0x1FD6U,
                                                          0x87F5U, 0xF587U, 0xFAC3U, 0xF587U, 0x0FEBU});
}

Y_UNIT_TEST_LLVM(TestConvertIntToBool) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<i64>{0, 1, 2, 1024, -1});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Convert(item, NTest::ConvertToMinikqlType<bool>(pb));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<bool>{false, true, true, true, true});
}

Y_UNIT_TEST_LLVM(TestFloatAbs) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<float>{11.433f, -3.14f, 0.0f, -HUGE_VALF});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Abs(item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<float>{11.433f, 3.14f, 0.0f, HUGE_VALF});
}

Y_UNIT_TEST_LLVM(TestIntegerAbs) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<i32>{1334, -4378, 0, Min<i32>() + 1});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Abs(item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<i32>{1334, 4378, 0, Max<i32>()});
}

Y_UNIT_TEST_LLVM(TestToIntegral) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto ten = pb.ToIntegral(NTest::ConvertValueToLiteralNode(pb, float(10.0)), pb.NewDataType(NUdf::TDataType<i32>::Id, true));
    const auto two = pb.ToIntegral(NTest::ConvertValueToLiteralNode(pb, float(2.0)), pb.NewDataType(NUdf::TDataType<ui32>::Id, true));
    const auto pgmReturn = pb.NewTuple({ten, two});

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), std::tuple<TMaybe<i32>, TMaybe<ui32>>{i32(10), ui32(2U)});
}

Y_UNIT_TEST_LLVM(TestFloatToI16) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<float>{
                                                               0.0f, 0.0f * HUGE_VALF, -3.14f, 12121324.0f, -7898.8f,
                                                               210000.0f, HUGE_VALF, -HUGE_VALF, -HUGE_VALF, FLT_MIN / 2.0f});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<i16>::Id, true));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<i16>>{
                                                          i16(0), {}, i16(-3), {}, i16(-7898), {}, {}, {}, {}, i16(0)});
}

Y_UNIT_TEST_LLVM(TestDoubleToBool) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<double>{
                                                               0.0, 0.0 * HUGE_VAL, -3.14, 12121324.0, -7898.8,
                                                               210000.0, HUGE_VAL, -HUGE_VAL, -HUGE_VAL, DBL_MIN / 2.0});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<bool>::Id, true));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<bool>>{
                                                          false, {}, true, true, true, true, true, true, true, false});
}

Y_UNIT_TEST_LLVM(TestDoubleToUI32) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<double>{
                                                               0.0, 0.0 * HUGE_VAL, -3.14, 12121324.0, -7898.8,
                                                               210000.0, HUGE_VAL, -HUGE_VAL, -HUGE_VAL, DBL_MIN / 2.0});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<ui32>::Id, true));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<ui32>>{
                                                          ui32(0U), {}, {}, ui32(12121324U), {}, ui32(210000U), {}, {}, {}, ui32(0U)});
}

Y_UNIT_TEST_LLVM(TestUI64ToIntegral) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui64>{
                                                               0U, 1U,
                                                               ui64(Max<i8>()), ui64(Max<ui8>()),
                                                               ui64(Max<i16>()), ui64(Max<ui16>()),
                                                               ui64(Max<i32>()), ui64(Max<ui32>()),
                                                               ui64(Max<i64>()), Max<ui64>()});

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

    using TTuple = std::tuple<TMaybe<i8>, TMaybe<ui8>, TMaybe<i16>, TMaybe<ui16>, TMaybe<i32>, TMaybe<ui32>, TMaybe<i64>>;
    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTuple>{
                                                          {i8(0), ui8(0), i16(0), ui16(0), i32(0), ui32(0), i64(0)},
                                                          {i8(1), ui8(1), i16(1), ui16(1), i32(1), ui32(1), i64(1)},
                                                          {Max<i8>(), ui8(Max<i8>()), i16(Max<i8>()), ui16(Max<i8>()), i32(Max<i8>()), ui32(Max<i8>()), i64(Max<i8>())},
                                                          {{}, Max<ui8>(), i16(Max<ui8>()), ui16(Max<ui8>()), i32(Max<ui8>()), ui32(Max<ui8>()), i64(Max<ui8>())},
                                                          {{}, {}, Max<i16>(), ui16(Max<i16>()), i32(Max<i16>()), ui32(Max<i16>()), i64(Max<i16>())},
                                                          {{}, {}, {}, Max<ui16>(), i32(Max<ui16>()), ui32(Max<ui16>()), i64(Max<ui16>())},
                                                          {{}, {}, {}, {}, Max<i32>(), ui32(Max<i32>()), i64(Max<i32>())},
                                                          {{}, {}, {}, {}, {}, Max<ui32>(), i64(Max<ui32>())},
                                                          {{}, {}, {}, {}, {}, {}, Max<i64>()},
                                                          {{}, {}, {}, {}, {}, {}, {}},
                                                      });
}

Y_UNIT_TEST_LLVM(TestI64ToIntegral) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<i64>{
                                                               0, -1,
                                                               i64(Min<i8>()), i64(Max<i8>()),
                                                               i64(Min<i16>()), i64(Max<i16>()),
                                                               i64(Min<i32>()), i64(Max<i32>()),
                                                               Min<i64>(), Max<i64>()});

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

    using TTuple = std::tuple<TMaybe<i8>, TMaybe<ui8>, TMaybe<i16>, TMaybe<ui16>, TMaybe<i32>, TMaybe<ui32>, TMaybe<ui64>>;
    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTuple>{
                                                          {i8(0), ui8(0), i16(0), ui16(0), i32(0), ui32(0), ui64(0)},
                                                          {i8(-1), {}, i16(-1), {}, i32(-1), {}, {}},
                                                          {Min<i8>(), {}, i16(Min<i8>()), {}, i32(Min<i8>()), {}, {}},
                                                          {Max<i8>(), ui8(Max<i8>()), i16(Max<i8>()), ui16(Max<i8>()), i32(Max<i8>()), ui32(Max<i8>()), ui64(Max<i8>())},
                                                          {{}, {}, Min<i16>(), {}, i32(Min<i16>()), {}, {}},
                                                          {{}, {}, Max<i16>(), ui16(Max<i16>()), i32(Max<i16>()), ui32(Max<i16>()), ui64(Max<i16>())},
                                                          {{}, {}, {}, {}, Min<i32>(), {}, {}},
                                                          {{}, {}, {}, {}, Max<i32>(), ui32(Max<i32>()), ui64(Max<i32>())},
                                                          {{}, {}, {}, {}, {}, {}, {}},
                                                          {{}, {}, {}, {}, {}, {}, ui64(Max<i64>())},
                                                      });
}

Y_UNIT_TEST_LLVM(TestFloatFromString) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TStringBuf>{
                                                               "0.0", "NAN", "-3.14", "1212.00", "-7898.8", "21E4", "+inf", "-INF"});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.StrictFromString(item, NTest::ConvertToMinikqlType<float>(pb));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, 0.f);
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT(std::isnan(item.template Get<float>()));
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, -3.14f);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, 1212.f);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, -7898.8f);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, 210000.f);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, HUGE_VALF);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, -HUGE_VALF);
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TestDoubleFromString) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TStringBuf>{
                                                               "0.0", "NAN", "-3.14", "12121324.00", "-7898.8", "21E4", "+inf", "-INF"});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.StrictFromString(item, NTest::ConvertToMinikqlType<double>(pb));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, 0.0);
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT(std::isnan(item.template Get<double>()));
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, -3.14);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, 12121324.0);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, -7898.8);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, 210000.0);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, HUGE_VAL);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, -HUGE_VAL);
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TestFloatToString) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<float>{
                                                               0.0f, 0.0f * HUGE_VALF, -3.14f, 1212.0f, -7898.8f, 210000.0f, HUGE_VALF, -HUGE_VALF});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.ToString(item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TStringBuf>{
                                                          "0", "nan", "-3.14", "1212", "-7898.8", "210000", "inf", "-inf"});
}

Y_UNIT_TEST_LLVM(TestInt64ToTimestamp) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<i64>{
                                                               0LL, 1LL, -1LL, Min<i64>(), Max<i64>(),
                                                               4291747200000000LL, 4291747199999999LL});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<NUdf::TTimestamp>::Id, true));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, ui64(0ULL));
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, ui64(1ULL));
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT(!item);
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT(!item);
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT(!item);
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT(!item);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, ui64(4291747199999999ULL));
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TestFloatConvertToUint32) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<float>{0.0f, 1212.0f, 210000.0f});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Convert(item, NTest::ConvertToMinikqlType<ui32>(pb));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{0U, 1212U, 210000U});
}

Y_UNIT_TEST_LLVM(TestFloatConvertToInt32) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<float>{0.0f, -3.14f, 1212.0f, -7898.8f, 210000.0f});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Convert(item, NTest::ConvertToMinikqlType<i32>(pb));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<i32>{0, -3, 1212, -7898, 210000});
}

Y_UNIT_TEST_LLVM(TestDoubleConvertToInt64) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<double>{0.0, -3.14, 1212.0, -7898.8, 210000.0});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Convert(item, NTest::ConvertToMinikqlType<i64>(pb));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<i64>{0LL, -3LL, 1212LL, -7898LL, 210000LL});
}

Y_UNIT_TEST_LLVM(TestDoubleToInt64) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<double>{
                                                               0.0, -3.14, 1212.0, -7898.8, 210000.0, 9223372036854776000.0});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.ToIntegral(item, NTest::ConvertToMinikqlType<TMaybe<i64>>(pb));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<i64>>{
                                                          i64(0LL), i64(-3LL), i64(1212LL), i64(-7898LL), i64(210000LL), {}});
}

Y_UNIT_TEST_LLVM(TestDoubleToString) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<double>{
                                                               0.0, 0.0 * HUGE_VAL, -3.14, 12121324.0, -7898.8, 210000.0, HUGE_VAL, -HUGE_VAL});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.ToString(item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TStringBuf>{
                                                          "0", "nan", "-3.14", "12121324", "-7898.8", "210000", "inf", "-inf"});
}

Y_UNIT_TEST_LLVM(TestNanvl) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<float>>{
                                                               float(0.0f), float(0.0f * HUGE_VALF), float(HUGE_VALF), float(-HUGE_VALF), float(FLT_MIN / 2.0f), {}});
    const auto data = NTest::ConvertValueToLiteralNode(pb, double(3.14));

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Nanvl(item, data);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, 0.0);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, 3.14);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, HUGE_VAL);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, -HUGE_VAL);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, double(FLT_MIN / 2.0f));
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT(!item);
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TestConvertToFloat) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto seven = NTest::ConvertValueToLiteralNode(pb, i32(7));
    const auto pgmReturn = pb.Convert(seven, NTest::ConvertToMinikqlType<float>(pb));
    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), 7.0f);
}

Y_UNIT_TEST_LLVM(TestAppend) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    auto pgmReturn = pb.NewEmptyList(NTest::ConvertToMinikqlType<ui32>(pb));
    pgmReturn = pb.Append(pgmReturn, NTest::ConvertValueToLiteralNode(pb, ui32(34)));
    pgmReturn = pb.Append(pgmReturn, NTest::ConvertValueToLiteralNode(pb, ui32(56)));

    const auto graph = setup.BuildGraph(pgmReturn);
    UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetListLength(), 2);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{34, 56});
}

Y_UNIT_TEST_LLVM(TestForkedAppend) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{34});
    const auto list2 = pb.Append(list1, NTest::ConvertValueToLiteralNode(pb, ui32(56)));
    const auto list3 = pb.Append(list1, NTest::ConvertValueToLiteralNode(pb, ui32(78)));
    const auto pgmReturn = pb.NewTuple({list2, list3});

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue().GetElement(0), TVector<ui32>{34, 56});
    AssertUnboxedValueElementEqual(graph->GetValue().GetElement(1), TVector<ui32>{34, 78});
}

Y_UNIT_TEST_LLVM(TestPrepend) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    auto pgmReturn = pb.NewEmptyList(NTest::ConvertToMinikqlType<ui32>(pb));
    pgmReturn = pb.Prepend(NTest::ConvertValueToLiteralNode(pb, ui32(34)), pgmReturn);
    pgmReturn = pb.Prepend(NTest::ConvertValueToLiteralNode(pb, ui32(56)), pgmReturn);

    const auto graph = setup.BuildGraph(pgmReturn);
    UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetListLength(), 2);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{56, 34});
}

Y_UNIT_TEST_LLVM(TestPrependOfVoid) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    auto pgmReturn = pb.NewEmptyListOfVoid();
    pgmReturn = pb.Prepend(NTest::ConvertValueToLiteralNode(pb, NTest::TSingularVoid{}), pgmReturn);
    pgmReturn = pb.Prepend(NTest::ConvertValueToLiteralNode(pb, NTest::TSingularVoid{}), pgmReturn);

    const auto graph = setup.BuildGraph(pgmReturn);
    UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetListLength(), 0);

    const auto iterator = graph->GetValue().GetListIterator();
    UNIT_ASSERT(!iterator.Skip());
    UNIT_ASSERT(!iterator.Skip());
}

Y_UNIT_TEST_LLVM(TestForkedPrepend) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto list = pb.NewEmptyList(NTest::ConvertToMinikqlType<ui32>(pb));
    const auto list1 = pb.Prepend(NTest::ConvertValueToLiteralNode(pb, ui32(34)), list);
    const auto list2 = pb.Prepend(NTest::ConvertValueToLiteralNode(pb, ui32(56)), list1);
    const auto list3 = pb.Prepend(NTest::ConvertValueToLiteralNode(pb, ui32(78)), list1);
    const auto pgmReturn = pb.NewTuple({list2, list3});

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue().GetElement(0), TVector<ui32>{56, 34});
    AssertUnboxedValueElementEqual(graph->GetValue().GetElement(1), TVector<ui32>{78, 34});
}

Y_UNIT_TEST_LLVM(TestAppendOfVoid) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    auto pgmReturn = pb.NewEmptyListOfVoid();
    pgmReturn = pb.Append(pgmReturn, NTest::ConvertValueToLiteralNode(pb, NTest::TSingularVoid{}));
    pgmReturn = pb.Append(pgmReturn, NTest::ConvertValueToLiteralNode(pb, NTest::TSingularVoid{}));

    const auto graph = setup.BuildGraph(pgmReturn);
    UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetListLength(), 0);

    const auto iterator = graph->GetValue().GetListIterator();
    UNIT_ASSERT(!iterator.Skip());
    UNIT_ASSERT(!iterator.Skip());
}

Y_UNIT_TEST_LLVM(TestExtend) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto emptyList = pb.NewEmptyList(NTest::ConvertToMinikqlType<ui32>(pb));
    const auto list1 = pb.Append(emptyList, NTest::ConvertValueToLiteralNode(pb, ui32(34)));
    const auto list2 = pb.Append(emptyList, NTest::ConvertValueToLiteralNode(pb, ui32(56)));
    const auto pgmReturn = pb.Extend({list1, list2});

    const auto graph = setup.BuildGraph(pgmReturn);
    UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetListLength(), 2);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{34, 56});
}

Y_UNIT_TEST_LLVM(TestExtend3) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto emptyList = pb.NewEmptyList(NTest::ConvertToMinikqlType<ui32>(pb));
    const auto list1 = pb.Append(emptyList, NTest::ConvertValueToLiteralNode(pb, ui32(34)));
    const auto list2 = pb.Append(emptyList, NTest::ConvertValueToLiteralNode(pb, ui32(56)));
    const auto list3 = pb.Append(emptyList, NTest::ConvertValueToLiteralNode(pb, ui32(7)));
    const auto list4 = pb.Append(list3, NTest::ConvertValueToLiteralNode(pb, ui32(12)));
    const auto pgmReturn = pb.Extend({list1, emptyList, list2, list4});

    const auto graph = setup.BuildGraph(pgmReturn);
    UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetListLength(), 4);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{34, 56, 7, 12});
}

Y_UNIT_TEST_LLVM(TestExtendOverFlows) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<double>{0.0, 1.1, -3.14});
    const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<double>{121324.323, -7898.8});

    const auto pgmReturn = pb.FromFlow(pb.Extend({pb.ToFlow(list1), pb.ToFlow(list2), pb.ToFlow(list2)}));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(),
                                   NYql::NUdf::TUnboxedValueComparatorStreamView<double>({121324.323, -7898.8, 0.0, 1.1, -3.14, 121324.323, -7898.8}));
}

class TGraphInput: public TComputationValue<TGraphInput> {
    using TBase = TComputationValue<TGraphInput>;

public:
    TGraphInput(TMemoryUsageInfo* memInfo, ui32 value)
        : TBase(memInfo)
        , Value(value)
    {
    }

    NUdf::EFetchStatus GetReturnStatus() const {
        return Status;
    }

    void SetReturnStatus(NUdf::EFetchStatus status) {
        Status = status;
    }

private:
    NUdf::EFetchStatus Fetch(NKikimr::NUdf::TUnboxedValue& item) final {
        if (Status == NUdf::EFetchStatus::Ok) {
            item = NKikimr::NUdf::TUnboxedValuePod(Value);
        }
        return Status;
    }

    NUdf::EFetchStatus WideFetch(NKikimr::NUdf::TUnboxedValue*, ui32) final {
        UNIT_ASSERT(false);
        return Status;
    }

    const ui32 Value;
    NUdf::EFetchStatus Status = NUdf::EFetchStatus::Yield;
};

Y_UNIT_TEST_QUAD(TestExtendYields, LLVM, WIDE) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto type = TStreamType::Create(pb.NewDataType(NUdf::TDataType<ui32>::Id), pb.GetTypeEnvironment());
    const auto arg0 = pb.Arg(type);
    const auto arg1 = pb.Arg(type);

    const auto pgmReturn = WIDE ? pb.FromFlow(
                                      pb.NarrowMap(
                                          pb.Extend({pb.ExpandMap(pb.ToFlow(arg0), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }),
                                                     pb.ExpandMap(pb.ToFlow(arg1), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; })}),
                                          [&](TRuntimeNode::TList items) { return items[0]; }))
                                : pb.FromFlow(pb.Extend({pb.ToFlow(arg0), pb.ToFlow(arg1)}));

    const auto graph = setup.BuildGraph(pgmReturn, {arg0.GetNode(), arg1.GetNode()});

    NUdf::TUnboxedValuePod inputPod0 = graph->GetHolderFactory().template Create<TGraphInput>(0);
    NUdf::TUnboxedValuePod inputPod1 = graph->GetHolderFactory().template Create<TGraphInput>(1);

    auto* input0 = dynamic_cast<TGraphInput*>(inputPod0.AsRawBoxed());
    auto* input1 = dynamic_cast<TGraphInput*>(inputPod1.AsRawBoxed());

    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(inputPod0));
    graph->GetEntryPoint(1, true)->SetValue(graph->GetContext(), std::move(inputPod1));

    const auto iterator = graph->GetValue();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Yield, iterator.Fetch(item));
    input0->SetReturnStatus(NUdf::EFetchStatus::Ok);
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
    UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 0);
    input0->SetReturnStatus(NUdf::EFetchStatus::Yield);
    input1->SetReturnStatus(NUdf::EFetchStatus::Ok);
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
    UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 1);
    input1->SetReturnStatus(NUdf::EFetchStatus::Yield);
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Yield, iterator.Fetch(item));
    input0->SetReturnStatus(NUdf::EFetchStatus::Finish);
    input1->SetReturnStatus(NUdf::EFetchStatus::Ok);
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
    UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), 1);
    input1->SetReturnStatus(NUdf::EFetchStatus::Finish);
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
}

Y_UNIT_TEST_QUAD(TestExtendRandomYields, LLVM, WIDE) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto type = TStreamType::Create(pb.NewDataType(NUdf::TDataType<ui32>::Id), pb.GetTypeEnvironment());
    const auto arg0 = pb.Arg(type);
    const auto arg1 = pb.Arg(type);
    const auto arg2 = pb.Arg(type);
    const auto arg3 = pb.Arg(type);

    const auto expandLambda = [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; };

    const auto pgmReturn = WIDE ? pb.FromFlow(
                                      pb.NarrowMap(
                                          pb.Extend({
                                              pb.ExpandMap(pb.ToFlow(arg0), expandLambda),
                                              pb.ExpandMap(pb.ToFlow(arg1), expandLambda),
                                              pb.ExpandMap(pb.ToFlow(arg2), expandLambda),
                                              pb.ExpandMap(pb.ToFlow(arg3), expandLambda),
                                          }),
                                          [&](TRuntimeNode::TList items) { return items[0]; }))
                                : pb.FromFlow(pb.Extend({pb.ToFlow(arg0), pb.ToFlow(arg1), pb.ToFlow(arg2), pb.ToFlow(arg3)}));

    const auto graph = setup.BuildGraph(pgmReturn, {arg0.GetNode(), arg1.GetNode(), arg2.GetNode(), arg3.GetNode()});

    TGraphInput* inputs[4];

    for (ui32 i = 0; i < 4; i++) {
        NUdf::TUnboxedValuePod inputPod = graph->GetHolderFactory().template Create<TGraphInput>(i);
        inputs[i] = dynamic_cast<TGraphInput*>(inputPod.AsRawBoxed());
        graph->GetEntryPoint(i, true)->SetValue(graph->GetContext(), std::move(inputPod));
    }

    const auto iterator = graph->GetValue();
    NUdf::TUnboxedValue item;

    const auto yieldCountLambda = [&]() -> ui32 {
        ui32 result = 0;
        for (ui32 i = 0; i < 4; i++) {
            if (inputs[i]->GetReturnStatus() == NUdf::EFetchStatus::Yield) {
                result++;
            }
        }
        return result;
    };

    ui32 finishCount = 0;
    ui32 iterationCount = 0;
    ui32 okCount = 0;

    TIntrusivePtr<IRandomProvider> random = CreateDeterministicRandomProvider(1);

    while (true) {
        ui64 mask = random->GenRand() & 0x1F;
        if (mask & 0x10) {
            mask = 0;
        }

        switch (++iterationCount) {
            case 600:
            case 800:
            case 900:
            case 1000:
                inputs[finishCount]->SetReturnStatus(NUdf::EFetchStatus::Finish);
                finishCount++;
                break;
            default:
                for (ui32 i = 0; i < 4; i++) {
                    auto status = inputs[i]->GetReturnStatus();
                    if (status != NUdf::EFetchStatus::Finish) {
                        auto newStatus = mask & (1 << i) ? NUdf::EFetchStatus::Ok : NUdf::EFetchStatus::Yield;
                        if (status != newStatus) {
                            inputs[i]->SetReturnStatus(newStatus);
                        }
                    }
                }
        }

        auto itemStatus = iterator.Fetch(item);

        switch (itemStatus) {
            case NUdf::EFetchStatus::Ok:
                UNIT_ASSERT(inputs[item.template Get<ui32>()]->GetReturnStatus() == NUdf::EFetchStatus::Ok);
                okCount++;
                break;
            case NUdf::EFetchStatus::Yield:
                UNIT_ASSERT(yieldCountLambda() == 4 - finishCount && (finishCount < 4));
                break;
            case NUdf::EFetchStatus::Finish:
                UNIT_ASSERT(finishCount == 4);
                break;
        }

        if (finishCount == 4) {
            break;
        }
    }
    UNIT_ASSERT_C(okCount * 4 >= iterationCount, "Less than 25% OKs, add more bits to masks");
    UNIT_ASSERT_C(okCount * 4 <= iterationCount * 3, "More than 75% OKs, remove bits from masks");
}

Y_UNIT_TEST_LLVM(TestOrderedExtendOverFlows) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<double>{0.0, 1.1, -3.14});
    const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<double>{121324.323, -7898.8});

    const auto pgmReturn = pb.FromFlow(pb.OrderedExtend({pb.ToFlow(list2), pb.ToFlow(list1), pb.ToFlow(list2)}));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(),
                                   NYql::NUdf::TUnboxedValueComparatorStreamView<double>({121324.323, -7898.8, 0.0, 1.1, -3.14, 121324.323, -7898.8}));
}

Y_UNIT_TEST_LLVM(TestStreamForwardList) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{34, 56, 7});

    const auto pgmReturn = pb.ForwardList(pb.Iterator(list, {}));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{34, 56, 7});
}

Y_UNIT_TEST_LLVM(TestFlowForwardList) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{34, 56, 7});

    const auto pgmReturn = pb.ForwardList(pb.ToFlow(list));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{34, 56, 7});
}

Y_UNIT_TEST_LLVM(TestFlowFromOptional) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto opt0 = NTest::ConvertValueToLiteralNode(pb, TMaybe<double>{0.0});
    const auto opt1 = NTest::ConvertValueToLiteralNode(pb, TMaybe<double>{1.1});
    const auto opt2 = NTest::ConvertValueToLiteralNode(pb, TMaybe<double>{-3.14});
    const auto opt = NTest::ConvertValueToLiteralNode(pb, TMaybe<double>{});

    const auto pgmReturn = pb.FromFlow(pb.OrderedExtend({pb.ToFlow(opt0), pb.ToFlow(opt), pb.ToFlow(opt1), pb.ToFlow(opt), pb.ToFlow(opt2)}));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(),
                                   NYql::NUdf::TUnboxedValueComparatorStreamView<double>({0.0, 1.1, -3.14}));
}

Y_UNIT_TEST_LLVM(TestCollectOverFlow) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data = NTest::ConvertValueToLiteralNode(pb, TVector<double>{0.0, 1.1, -3.14, 121324.323, -7898.8});

    const auto pgmReturn = pb.Collect(pb.ToFlow(data));

    const auto graph = setup.BuildGraph(pgmReturn);

    const auto list = graph->GetValue();
    AssertUnboxedValueElementEqual(list.GetElement(0), 0.0);
    AssertUnboxedValueElementEqual(list.GetElement(3), 121324.323);
    AssertUnboxedValueElementEqual(list.GetElement(1), 1.1);
    AssertUnboxedValueElementEqual(list.GetElement(2), -3.14);
    AssertUnboxedValueElementEqual(list.GetElement(4), -7898.8);
}

Y_UNIT_TEST_LLVM(TestAdd) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto pgmReturn = pb.Add(NTest::ConvertValueToLiteralNode(pb, ui32(1)), NTest::ConvertValueToLiteralNode(pb, ui32(2)));
    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), ui32(3));
}

Y_UNIT_TEST_LLVM(TestSub) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto pgmReturn = pb.Sub(NTest::ConvertValueToLiteralNode(pb, ui32(7)), NTest::ConvertValueToLiteralNode(pb, ui32(2)));
    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), ui32(5));
}

Y_UNIT_TEST_LLVM(TestMul) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto pgmReturn = pb.Mul(NTest::ConvertValueToLiteralNode(pb, ui32(3)), NTest::ConvertValueToLiteralNode(pb, ui32(2)));
    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), ui32(6));
}

Y_UNIT_TEST_LLVM(TestDiv) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto pgmReturn = pb.Div(NTest::ConvertValueToLiteralNode(pb, ui32(17)), NTest::ConvertValueToLiteralNode(pb, ui32(3)));
    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TMaybe<ui32>{ui32(5)});
}

Y_UNIT_TEST_LLVM(TestDivZero) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto pgmReturn = pb.Div(NTest::ConvertValueToLiteralNode(pb, ui32(17)), NTest::ConvertValueToLiteralNode(pb, ui32(0)));
    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TMaybe<ui32>{});
}

Y_UNIT_TEST_LLVM(TestIDivOverflow) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto pgmReturn = pb.Div(NTest::ConvertValueToLiteralNode(pb, i32(Min<i32>())), NTest::ConvertValueToLiteralNode(pb, i32(-1)));
    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TMaybe<i32>{});
}

Y_UNIT_TEST_LLVM(TestMod) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto pgmReturn = pb.Mod(NTest::ConvertValueToLiteralNode(pb, ui32(17)), NTest::ConvertValueToLiteralNode(pb, ui32(3)));
    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TMaybe<ui32>{ui32(2)});
}

Y_UNIT_TEST_LLVM(TestModZero) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto pgmReturn = pb.Mod(NTest::ConvertValueToLiteralNode(pb, ui32(17)), NTest::ConvertValueToLiteralNode(pb, ui32(0)));
    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TMaybe<ui32>{});
}

Y_UNIT_TEST_LLVM(TestIModOverflow) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto pgmReturn = pb.Mod(NTest::ConvertValueToLiteralNode(pb, i32(Min<i32>())), NTest::ConvertValueToLiteralNode(pb, i32(-1)));
    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TMaybe<i32>{});
}

Y_UNIT_TEST_LLVM(TestStruct) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto structObj = pb.NewEmptyStruct();
    structObj = pb.AddMember(structObj, "x", NTest::ConvertValueToLiteralNode(pb, ui32(2)));
    structObj = pb.AddMember(structObj, "z", NTest::ConvertValueToLiteralNode(pb, ui32(1)));
    structObj = pb.AddMember(structObj, "y", NTest::ConvertValueToLiteralNode(pb, ui32(3)));
    const auto pgmReturn = pb.NewList(NTest::ConvertToMinikqlType<ui32>(pb), {pb.Member(structObj, "x"),
                                                                              pb.Member(structObj, "y"),
                                                                              pb.Member(structObj, "z")});

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{2, 3, 1});
}

Y_UNIT_TEST_LLVM(TestMapOverList) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data1 = NTest::ConvertValueToLiteralNode(pb, TMaybe<i32>{i32(1)});
    const auto data2 = NTest::ConvertValueToLiteralNode(pb, TMaybe<i32>{i32(2)});
    const auto data3 = NTest::ConvertValueToLiteralNode(pb, TMaybe<i32>{i32(3)});
    const auto data4 = NTest::ConvertValueToLiteralNode(pb, TMaybe<i32>{});
    const auto list = pb.NewList(NTest::ConvertToMinikqlType<TMaybe<i32>>(pb), {data1, data2, data3, data4});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Sub(pb.Mul(item, data2), data1);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<i32>>{i32(1), i32(3), i32(5), {}});
}

Y_UNIT_TEST_LLVM(TestMapOverStream) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, TMaybe<i16>{i16(0)});
    const auto data1 = NTest::ConvertValueToLiteralNode(pb, TMaybe<i16>{i16(-1)});
    const auto data2 = NTest::ConvertValueToLiteralNode(pb, TMaybe<i16>{i16(3)});
    const auto data3 = NTest::ConvertValueToLiteralNode(pb, TMaybe<i16>{i16(7)});
    const auto data4 = NTest::ConvertValueToLiteralNode(pb, TMaybe<i16>{i16(11)});
    const auto data5 = NTest::ConvertValueToLiteralNode(pb, TMaybe<i16>{});
    const auto list = pb.NewList(NTest::ConvertToMinikqlType<TMaybe<i16>>(pb), {data0, data1, data2, data3, data4, data5});

    const auto pgmReturn = pb.Map(pb.Iterator(list, {}),
                                  [&](TRuntimeNode item) {
                                      return pb.Div(data3, item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(),
                                   NYql::NUdf::TUnboxedValueComparatorStreamView<TMaybe<i16>>({{}, i16(-7), i16(2), i16(1), i16(0), {}}));
}

Y_UNIT_TEST_LLVM(TestMapOverFlow) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, TStringBuf("PREFIX:"));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TStringBuf>{"very large string", "", "small"});

    const auto pgmReturn = pb.FromFlow(pb.Map(pb.ToFlow(list),
                                              [&](TRuntimeNode item) {
                                                  return pb.Concat(data0, item);
                                              }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(),
                                   NYql::NUdf::TUnboxedValueComparatorStreamView<TStringBuf>({"PREFIX:very large string", "PREFIX:", "PREFIX:small"}));
}

#ifndef _win_
Y_UNIT_TEST_LLVM(TestMapUnwrapThrow) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data2 = NTest::ConvertValueToLiteralNode(pb, TMaybe<i8>{i8(7)});
    const auto list = pb.LazyList(NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<i8>>{i8(1), {}}));

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Sub(data2, pb.Unwrap(item, NTest::ConvertValueToLiteralNode(pb, TStringBuf("")), "", 0, 0));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, i8(6));
    UNIT_ASSERT_EXCEPTION(iterator.Next(item), yexception);
}

Y_UNIT_TEST_LLVM(TestMapUnwrapThrowMessage) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data2 = NTest::ConvertValueToLiteralNode(pb, TMaybe<i8>{i8(7)});
    const auto list = pb.LazyList(NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<i8>>{i8(1), {}}));

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Sub(data2, pb.Unwrap(item, pb.Concat(NTest::ConvertValueToLiteralNode(pb, TStringBuf("a")),
                                                                                     NTest::ConvertValueToLiteralNode(pb, TStringBuf("b"))),
                                                                     "", 0, 0));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, i8(6));
    UNIT_ASSERT_EXCEPTION(iterator.Next(item), yexception);
}

Y_UNIT_TEST_LLVM(TestMapEnsureThrowMessage) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data2 = NTest::ConvertValueToLiteralNode(pb, i8(7));
    const auto list = pb.LazyList(NTest::ConvertValueToLiteralNode(pb, TVector<i8>{1, 7}));

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Sub(data2,
                                                    pb.Ensure(item, pb.Less(item, data2), NTest::ConvertValueToLiteralNode(pb, TStringBuf("BAD")), "", 0, 0));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, i8(6));
    UNIT_ASSERT_EXCEPTION(iterator.Next(item), yexception);
}
#endif
Y_UNIT_TEST_LLVM(TestMapWithCoalesce) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, i16(0));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<i16>>{i16(1), i16(2), i16(3), {}});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Coalesce(item, data0);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<i16>{1, 2, 3, 0});
}

Y_UNIT_TEST_LLVM(TestSizeOfOptional) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<i16>>{i16(1), i16(0), {}});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Size(item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<ui32>>{ui32(2), ui32(2), {}});
}

Y_UNIT_TEST_LLVM(TestSizeOfOptionalString) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, TMaybe<TStringBuf>{"0123456789"});
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<TStringBuf>>{
                                                               TStringBuf("0123456789"), TStringBuf("XYZ"), TStringBuf(""), TStringBuf("qwertyuioplkjhgfdsazxcvbnm"), {}});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Size(pb.Concat(item, data0));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<ui32>>{ui32(20), ui32(13), ui32(10), ui32(36), {}});
}

Y_UNIT_TEST_LLVM(TestMapWithIfExists) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, ui16(666));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<ui16>>{ui16(1), ui16(2), ui16(3), {}});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.If(pb.Exists(item), pb.Increment(pb.Unwrap(item, NTest::ConvertValueToLiteralNode(pb, TStringBuf("")), "", 0, 0)), data0);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui16>{ui16(2), ui16(3), ui16(4), ui16(666)});
}

Y_UNIT_TEST_LLVM(TestMapOverListLazy) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    // data1 and data2 are used as runtime operands in the lambda body.
    const auto data1 = pb.NewDataLiteral<ui32>(1U);
    const auto data2 = pb.NewDataLiteral<ui32>(2U);
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{1U, 2U, 3U});

    const auto pgmReturn = pb.Map(pb.LazyList(list),
                                  [&](TRuntimeNode item) {
                                      return pb.Add(pb.Mul(item, data2), data1);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{3U, 5U, 7U});
}

Y_UNIT_TEST_LLVM(TestMapOverOptional) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    // data1 and data2 are used as runtime operands in the lambda body.
    const auto data1 = pb.NewDataLiteral<ui32>(1);
    const auto data2 = pb.NewDataLiteral<ui32>(2);
    const auto list = NTest::ConvertValueToLiteralNode(pb, TMaybe<ui32>{ui32(1)});
    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Add(pb.Mul(item, data2), data1);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TMaybe<ui32>{ui32(3)});
}

Y_UNIT_TEST_LLVM(TestFloatMinMax) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<float>{-NAN, HUGE_VALF, 3.14f, -2.13f, -HUGE_VALF});
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

    // NaN op anything => NaN
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT(std::isnan(item.GetElement(0).template Get<float>()));
    UNIT_ASSERT(std::isnan(item.GetElement(1).template Get<float>()));

    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, std::tuple<float, float>{HUGE_VALF, HUGE_VALF});
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, std::tuple<float, float>{3.14f, 3.14f});
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, std::tuple<float, float>{-2.13f, -2.13f});
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, std::tuple<float, float>{-HUGE_VALF, -HUGE_VALF});

    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, std::tuple<float, float>{HUGE_VALF, HUGE_VALF});
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, std::tuple<float, float>{HUGE_VALF, HUGE_VALF});
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, std::tuple<float, float>{3.14f, HUGE_VALF});
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, std::tuple<float, float>{-2.13f, HUGE_VALF});
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, std::tuple<float, float>{-HUGE_VALF, HUGE_VALF});

    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, std::tuple<float, float>{3.14f, 3.14f});
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, std::tuple<float, float>{3.14f, HUGE_VALF});
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, std::tuple<float, float>{3.14f, 3.14f});
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, std::tuple<float, float>{-2.13f, 3.14f});
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, std::tuple<float, float>{-HUGE_VALF, 3.14f});

    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, std::tuple<float, float>{-2.13f, -2.13f});
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, std::tuple<float, float>{-2.13f, HUGE_VALF});
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, std::tuple<float, float>{-2.13f, 3.14f});
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, std::tuple<float, float>{-2.13f, -2.13f});
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, std::tuple<float, float>{-HUGE_VALF, -2.13f});

    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, std::tuple<float, float>{-HUGE_VALF, -HUGE_VALF});
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, std::tuple<float, float>{-HUGE_VALF, HUGE_VALF});
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, std::tuple<float, float>{-HUGE_VALF, 3.14f});
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, std::tuple<float, float>{-HUGE_VALF, -2.13f});
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, std::tuple<float, float>{-HUGE_VALF, -HUGE_VALF});

    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TestFloatMod) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    // data2 is used as a runtime operand in the lambda body.
    const auto data2 = pb.NewDataLiteral<float>(3.14f);
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<float>{-1.75f, 3.14f, -6.28f, 7.28f});
    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Mod(item, data2);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<float>{-1.75f, 0.0f, 0.0f, 1.0f});
}

Y_UNIT_TEST_LLVM(TestDoubleMod) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    // data2 is used as a runtime operand in the lambda body.
    const auto data2 = pb.NewDataLiteral<double>(3.14);
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<double>{-1.75, 3.14, -6.28, 7.28});
    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Mod(item, data2);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<double>{-1.75, 0.0, 0.0, 1.0});
}

Y_UNIT_TEST_LLVM(TestDiscardOverFlow) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TStringBuf>{"000", "100", "200", "300"});

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

    const auto itemType = NTest::ConvertToMinikqlType<float>(pb);
    const auto listType = pb.NewListType(itemType);

    const auto data0 = pb.NewEmptyList(itemType);
    const auto data1 = NTest::ConvertValueToLiteralNode(pb, TVector<float>{-1.5f, 0.f, 3.14f});
    const auto data2 = NTest::ConvertValueToLiteralNode(pb, TVector<float>{3.14f, -1.5f, 0.f});
    const auto data3 = pb.LazyList(NTest::ConvertValueToLiteralNode(pb, TVector<float>{1.1f, -2.2f}));

    const auto list = pb.NewList(listType, {data0, data1, data2, data3});

    const auto pgmReturn = pb.Map(list, [&](TRuntimeNode item) { return pb.Head(item); });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<float>>{
                                                          {}, float(-1.5f), float(3.14f), float(1.1f)});
}

Y_UNIT_TEST_LLVM(TestLast) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto itemType = NTest::ConvertToMinikqlType<float>(pb);
    const auto listType = pb.NewListType(itemType);

    const auto data0 = pb.NewEmptyList(itemType);
    const auto data1 = NTest::ConvertValueToLiteralNode(pb, TVector<float>{-1.5f, 0.f, 3.14f});
    const auto data2 = NTest::ConvertValueToLiteralNode(pb, TVector<float>{3.14f, -1.5f, 0.f});
    const auto data3 = pb.LazyList(NTest::ConvertValueToLiteralNode(pb, TVector<float>{1.1f, -2.2f}));

    const auto list = pb.NewList(listType, {data0, data1, data2, data3});

    const auto pgmReturn = pb.Map(list, [&](TRuntimeNode item) { return pb.Last(item); });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<float>>{
                                                          {}, float(3.14f), float(0.f), float(-2.2f)});
}

Y_UNIT_TEST_LLVM(TestCoalesce) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    // Dynamic struct construction with AddMember requires pb.NewEmptyStruct (justified use).
    const auto data2 = NTest::ConvertValueToLiteralNode(pb, ui32(2));
    auto pgmReturn = pb.NewEmptyStruct();
    pgmReturn = pb.AddMember(pgmReturn, "A", pb.Coalesce(NTest::ConvertValueToLiteralNode(pb, TMaybe<ui32>{ui32(1)}), data2));
    pgmReturn = pb.AddMember(pgmReturn, "B", pb.Coalesce(NTest::ConvertValueToLiteralNode(pb, TMaybe<ui32>{}), data2));
    pgmReturn = pb.AddMember(pgmReturn, "C", pb.Coalesce(NTest::ConvertValueToLiteralNode(pb, TMaybe<ui32>{ui32(2)}), NTest::ConvertValueToLiteralNode(pb, TMaybe<ui32>{ui32(3)})));
    pgmReturn = pb.AddMember(pgmReturn, "D", pb.Coalesce(NTest::ConvertValueToLiteralNode(pb, TMaybe<ui32>{}), NTest::ConvertValueToLiteralNode(pb, TMaybe<ui32>{ui32(3)})));
    pgmReturn = pb.AddMember(pgmReturn, "E", pb.Coalesce(NTest::ConvertValueToLiteralNode(pb, TMaybe<ui32>{}), NTest::ConvertValueToLiteralNode(pb, TMaybe<ui32>{})));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto value = graph->GetValue();
    AssertUnboxedValueElementEqual(value.GetElement(0), ui32(1));
    AssertUnboxedValueElementEqual(value.GetElement(1), ui32(2));
    AssertUnboxedValueElementEqual(value.GetElement(2), TMaybe<ui32>{ui32(2)});
    AssertUnboxedValueElementEqual(value.GetElement(3), TMaybe<ui32>{ui32(3)});
    AssertUnboxedValueElementEqual(value.GetElement(4), TMaybe<ui32>{});
}

Y_UNIT_TEST_LLVM(TestExists) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<ui64>>{ui64(1), {}});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Exists(item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<bool>{true, false});
}

Y_UNIT_TEST_LLVM(TestIf) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data1 = NTest::ConvertValueToLiteralNode(pb, ui32(1));
    const auto data2 = NTest::ConvertValueToLiteralNode(pb, ui32(2));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<bool>{true, false});
    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.If(item, data1, data2);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{ui32(1), ui32(2)});
}

Y_UNIT_TEST_LLVM(TestIfPresent) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data2 = NTest::ConvertValueToLiteralNode(pb, ui32(2));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<ui32>>{ui32(1), {}});
    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.IfPresent({item},
                                                          [&](TRuntimeNode::TList unpacked) {
                                                              return unpacked.front();
                                                          }, data2);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{ui32(1), ui32(2)});
}

Y_UNIT_TEST_LLVM(TestIfPresentTwo) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<i8>>{i8(+1), {}, i8(-1)});
    const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<bool>>{true, {}, false});
    const auto empty1 = NTest::ConvertValueToLiteralNode(pb, TMaybe<i8>{});

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
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<i8>{i8(-1), i8(+1), i8(+1), i8(-1)});
}

Y_UNIT_TEST_LLVM(TestIfPresentThree) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<i8>>{i8(+1), {}, i8(-1)});
    const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<bool>>{true, {}, false});
    const auto list3 = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<TMaybe<i8>>>{TMaybe<i8>{i8(5)}, {}, TMaybe<i8>{}});
    const auto empty1 = NTest::ConvertValueToLiteralNode(pb, TMaybe<i8>{});
    const auto coalesce3 = NTest::ConvertValueToLiteralNode(pb, i8(3));
    const auto coalesce7 = NTest::ConvertValueToLiteralNode(pb, i8(7));

    const auto pgmReturn = pb.FlatMap(list1,
                                      [&](TRuntimeNode item1) {
                                          return pb.FlatMap(list2,
                                                            [&](TRuntimeNode item2) {
                                                                return pb.FlatMap(list3,
                                                                                  [&](TRuntimeNode item3) {
                                                                                      return pb.IfPresent({item1, item2, item3},
                                                                                                          [&](TRuntimeNode::TList unpacked) {
                                                                                                              return pb.NewOptional(pb.If(unpacked[1],
                                                                                                                                          pb.Add(unpacked.front(), pb.Coalesce(unpacked.back(), coalesce3)),
                                                                                                                                          pb.Sub(unpacked.front(), pb.Coalesce(unpacked.back(), coalesce7))));
                                                                                                          }, empty1);
                                                                                  });
                                                            });
                                      });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<i8>{i8(+6), i8(+4), i8(-4), i8(-6), i8(+4), i8(+2), i8(-6), i8(-8)});
}

Y_UNIT_TEST_LLVM(TestIfPresentSame) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<i8>>{i8(+1), {}, i8(-1)});
    const auto data0 = NTest::ConvertValueToLiteralNode(pb, TMaybe<i8>{});

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
    using TTuple = std::tuple<TMaybe<i8>, TMaybe<i8>>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTuple>{
                                                          {i8(1), i8(1)},
                                                          {{}, {}},
                                                          {i8(1), i8(-1)},
                                                      });
}

Y_UNIT_TEST_LLVM(TestIncDec) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data1 = NTest::ConvertValueToLiteralNode(pb, ui32(7));
    auto pgmReturn = pb.NewEmptyList(NTest::ConvertToMinikqlType<ui32>(pb));
    pgmReturn = pb.Append(pgmReturn, pb.Increment(data1));
    pgmReturn = pb.Append(pgmReturn, pb.Decrement(data1));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{ui32(8), ui32(6)});
}

Y_UNIT_TEST_LLVM(TestLogical) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto truth = NTest::ConvertValueToLiteralNode(pb, true);
    const auto falsehood = NTest::ConvertValueToLiteralNode(pb, false);
    auto pgmReturn = pb.NewEmptyList(NTest::ConvertToMinikqlType<bool>(pb));

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
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<bool>{
                                                          // and
                                                          true, false, false, false,
                                                          // or
                                                          true, true, true, false,
                                                          // xor
                                                          false, true, true, false,
                                                          // not
                                                          false, true,
                                                      });
}

Y_UNIT_TEST_LLVM(TestZip) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{34U, 56U});
    const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<TStringBuf>{"Q", "E", "W"});

    const auto pgmReturn = pb.Zip({list1, list2});

    const auto graph = setup.BuildGraph(pgmReturn);
    using TTuple = std::tuple<ui32, TString>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTuple>{
                                                          {ui32(34), TString("Q")},
                                                          {ui32(56), TString("E")},
                                                      });
}

Y_UNIT_TEST_LLVM(TestZipLazy) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{34U, 56U});
    const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<TStringBuf>{"Q", "E", "W"});

    const auto pgmReturn = pb.Zip({pb.LazyList(list1), list2});

    const auto graph = setup.BuildGraph(pgmReturn);
    using TTuple = std::tuple<ui32, TString>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTuple>{
                                                          {ui32(34), TString("Q")},
                                                          {ui32(56), TString("E")},
                                                      });
}

Y_UNIT_TEST_LLVM(TestZipAll) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{34U, 56U});
    const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<TStringBuf>{"Q", "E", "W"});

    const auto pgmReturn = pb.ZipAll({list1, list2});

    const auto graph = setup.BuildGraph(pgmReturn);
    using TTuple = std::tuple<TMaybe<ui32>, TString>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTuple>{
                                                          {ui32(34), TString("Q")},
                                                          {ui32(56), TString("E")},
                                                          {{}, TString("W")},
                                                      });
}

Y_UNIT_TEST_LLVM(TestZipAllLazy) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{34U, 56U});
    const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<TStringBuf>{"Q", "E", "W"});

    const auto pgmReturn = pb.ZipAll({list1, pb.LazyList(list2)});

    const auto graph = setup.BuildGraph(pgmReturn);
    using TTuple = std::tuple<TMaybe<ui32>, TString>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTuple>{
                                                          {ui32(34), TString("Q")},
                                                          {ui32(56), TString("E")},
                                                          {{}, TString("W")},
                                                      });
}

Y_UNIT_TEST_LLVM(TestReduce) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<ui32>>{ui32(1), ui32(2), ui32(3)});
    const auto dataType = NTest::ConvertToMinikqlType<TMaybe<ui32>>(pb);
    const auto empty = pb.AddMember(pb.AddMember(
                                        pb.NewEmptyStruct(), "Min", pb.NewEmptyOptional(dataType)),
                                    "Max", pb.NewEmptyOptional(dataType));
    const auto pgmReturn = pb.Reduce(list, empty,
                                     [&](TRuntimeNode item, TRuntimeNode state1) { return pb.AddMember(
                                                                                       pb.AddMember(pb.NewEmptyStruct(), "Min", pb.AggrMin(pb.Member(state1, "Min"), item)),
                                                                                       "Max", pb.AggrMax(pb.Member(state1, "Max"), item)); },
                                     [&](TRuntimeNode state) { return state; }, empty,
                                     [&](TRuntimeNode state1, TRuntimeNode state2) { return pb.AddMember(
                                                                                         pb.AddMember(pb.NewEmptyStruct(), "Min", pb.AggrMin(pb.Member(state1, "Min"), pb.Member(state2, "Min"))), "Max", pb.AggrMax(pb.Member(state1, "Max"), pb.Member(state2, "Max"))); });

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto value = graph->GetValue();
    AssertUnboxedValueElementEqual(value.GetElement(1), ui32(1));
    AssertUnboxedValueElementEqual(value.GetElement(0), ui32(3));
}

Y_UNIT_TEST_LLVM(TestReduceOverStream) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<ui32>>{ui32(1), ui32(2), ui32(3)});
    const auto dataType = NTest::ConvertToMinikqlType<TMaybe<ui32>>(pb);
    const auto empty = pb.AddMember(pb.AddMember(
                                        pb.NewEmptyStruct(), "Min", pb.NewEmptyOptional(dataType)),
                                    "Max", pb.NewEmptyOptional(dataType));
    const auto pgmReturn = pb.Reduce(pb.Iterator(list, {}), empty,
                                     [&](TRuntimeNode item, TRuntimeNode state1) { return pb.AddMember(
                                                                                       pb.AddMember(pb.NewEmptyStruct(), "Min", pb.AggrMin(pb.Member(state1, "Min"), item)),
                                                                                       "Max", pb.AggrMax(pb.Member(state1, "Max"), item)); },
                                     [&](TRuntimeNode state) { return state; }, empty,
                                     [&](TRuntimeNode state1, TRuntimeNode state2) { return pb.AddMember(
                                                                                         pb.AddMember(pb.NewEmptyStruct(), "Min", pb.AggrMin(pb.Member(state1, "Min"), pb.Member(state2, "Min"))), "Max", pb.AggrMax(pb.Member(state1, "Max"), pb.Member(state2, "Max"))); });

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto value = graph->GetValue();
    AssertUnboxedValueElementEqual(value.GetElement(1), ui32(1));
    AssertUnboxedValueElementEqual(value.GetElement(0), ui32(3));
}

Y_UNIT_TEST_LLVM(TestListLength) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{34U, 56U});
    const auto pgmReturn = pb.Length(list);

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), ui64(2));
}

Y_UNIT_TEST_LLVM(TestReverse) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{34U, 56U});
    const auto pgmReturn = pb.Reverse(list);

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{56U, 34U});
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
            list = pb.Prepend(pb.NewDataLiteral(n - 1 - j), list);
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

    const auto pgmReturn = pb.Replicate(NTest::ConvertValueToLiteralNode(pb, ui32(34)),
                                        NTest::ConvertValueToLiteralNode(pb, ui64(4)), "", 0, 0);

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{34U, 34U, 34U, 34U});
}

Y_UNIT_TEST_LLVM(TestIntegralCasts) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto pgmReturn = pb.Convert(NTest::ConvertValueToLiteralNode(pb, ui32(258)),
                                      NTest::ConvertToMinikqlType<ui8>(pb));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), ui8(2));
}

Y_UNIT_TEST_LLVM(TestCastFourIntoTrue) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto pgmReturn = pb.Convert(NTest::ConvertValueToLiteralNode(pb, ui32(4)),
                                      NTest::ConvertToMinikqlType<bool>(pb));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), true);
}

Y_UNIT_TEST_LLVM(TestSubstring) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data1 = NTest::ConvertValueToLiteralNode(pb, TStringBuf("aabbccc"));
    using TPair = std::tuple<ui32, ui32>;
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TPair>{
                                                               {ui32(3), ui32(2)},
                                                               {ui32(4), ui32(10)},
                                                               {ui32(10), ui32(1)},
                                                               {ui32(0), ui32(3)},
                                                           });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Substring(data1, pb.Nth(item, 0), pb.Nth(item, 1));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TStringBuf>{"bc", "ccc", "", "aab"});
}

Y_UNIT_TEST_LLVM(TestSubstringOptionalArgs) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto null = NTest::ConvertValueToLiteralNode(pb, TMaybe<ui32>{});
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<TStringBuf>>{
                                                               {}, TStringBuf(""), TStringBuf("0123456789"), TStringBuf("abcdefghijklmnopqrstuvwxyz")});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.NewTuple({pb.Substring(item, null, null),
                                                          pb.Substring(item, NTest::ConvertValueToLiteralNode(pb, TMaybe<ui32>{ui32(7)}), null),
                                                          pb.Substring(item, null, NTest::ConvertValueToLiteralNode(pb, TMaybe<ui32>{ui32(6)})),
                                                          pb.Substring(item, NTest::ConvertValueToLiteralNode(pb, TMaybe<ui32>{ui32(3)}), NTest::ConvertValueToLiteralNode(pb, TMaybe<ui32>{ui32(17)}))});
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    using TTuple = std::tuple<TMaybe<TStringBuf>, TMaybe<TStringBuf>, TMaybe<TStringBuf>, TMaybe<TStringBuf>>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTuple>{
                                                          {{}, {}, {}, {}},
                                                          {TStringBuf(""), TStringBuf(""), TStringBuf(""), TStringBuf("")},
                                                          {TStringBuf("0123456789"), TStringBuf("789"), TStringBuf("012345"), TStringBuf("3456789")},
                                                          {TStringBuf("abcdefghijklmnopqrstuvwxyz"), TStringBuf("hijklmnopqrstuvwxyz"), TStringBuf("abcdef"), TStringBuf("defghijklmnopqrst")},
                                                      });
}

Y_UNIT_TEST_LLVM(TestFindAndSubstring) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<TStringBuf>>{
                                                               {}, TStringBuf(""), TStringBuf("_/>_/>aab*SEP*bccc"),
                                                               TStringBuf("*SEP*_/>a>><<___*SEP*a_/>b54b*SEP*c_/>3434cc*SEP*"), TStringBuf("(none)")});
    const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<TStringBuf>{"", "_/>", "*SEP*"});
    const auto null = NTest::ConvertValueToLiteralNode(pb, TMaybe<ui32>{});
    const auto zero = NTest::ConvertValueToLiteralNode(pb, ui32(0U));

    const auto pgmReturn = pb.FlatMap(list,
                                      [&](TRuntimeNode data) {
                                          return pb.Map(list1,
                                                        [&](TRuntimeNode sep) {
                                                            const auto first = pb.Find(data, sep, null);
                                                            const auto last = pb.RFind(data, sep, null);
                                                            const auto len = pb.Size(sep);
                                                            return pb.NewTuple({pb.Substring(data, null, first),
                                                                                pb.Substring(data, pb.Add(first, len), pb.If(pb.Less(first, last), pb.Sub(last, pb.Add(first, len)), zero)),
                                                                                pb.Substring(data, pb.Add(last, len), null)});
                                                        });
                                      });

    const auto graph = setup.BuildGraph(pgmReturn);
    using TTuple = std::tuple<TMaybe<TStringBuf>, TMaybe<TStringBuf>, TMaybe<TStringBuf>>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTuple>{
                                                          {{}, {}, {}},
                                                          {{}, {}, {}},
                                                          {{}, {}, {}},
                                                          {TStringBuf(""), TStringBuf(""), TStringBuf("")},
                                                          {TStringBuf(""), TStringBuf(""), TStringBuf("")},
                                                          {TStringBuf(""), TStringBuf(""), TStringBuf("")},
                                                          {TStringBuf(""), TStringBuf("_/>_/>aab*SEP*bccc"), TStringBuf("")},
                                                          {TStringBuf(""), TStringBuf(""), TStringBuf("aab*SEP*bccc")},
                                                          {TStringBuf("_/>_/>aab"), TStringBuf(""), TStringBuf("bccc")},
                                                          {TStringBuf(""), TStringBuf("*SEP*_/>a>><<___*SEP*a_/>b54b*SEP*c_/>3434cc*SEP*"), TStringBuf("")},
                                                          {TStringBuf("*SEP*"), TStringBuf("a>><<___*SEP*a_/>b54b*SEP*c"), TStringBuf("3434cc*SEP*")},
                                                          {TStringBuf(""), TStringBuf("_/>a>><<___*SEP*a_/>b54b*SEP*c_/>3434cc"), TStringBuf("")},
                                                          {TStringBuf(""), TStringBuf("(none)"), TStringBuf("")},
                                                          {TStringBuf("(none)"), TStringBuf("(none)"), TStringBuf("(none)")},
                                                          {TStringBuf("(none)"), TStringBuf("(none)"), TStringBuf("(none)")},
                                                      });
}

Y_UNIT_TEST_LLVM(TestFindFromPos) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data = NTest::ConvertValueToLiteralNode(pb, TStringBuf("_</|>0123456789</|></|>abcxyz</|>*"));
    const auto sep = NTest::ConvertValueToLiteralNode(pb, TStringBuf("</|>"));

    const auto list = pb.ListFromRange(NTest::ConvertValueToLiteralNode(pb, ui32(0U)), pb.Size(data),
                                       NTest::ConvertValueToLiteralNode(pb, ui32(2U)));

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.NewTuple({pb.Find(data, sep, item), pb.RFind(data, sep, item)});
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    using TTuple = std::tuple<TMaybe<ui32>, TMaybe<ui32>>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTuple>{
                                                          {ui32(1U), {}},
                                                          {ui32(15U), ui32(1U)},
                                                          {ui32(15U), ui32(1U)},
                                                          {ui32(15U), ui32(1U)},
                                                          {ui32(15U), ui32(1U)},
                                                          {ui32(15U), ui32(1U)},
                                                          {ui32(15U), ui32(1U)},
                                                          {ui32(15U), ui32(1U)},
                                                          {ui32(19U), ui32(15U)},
                                                          {ui32(19U), ui32(15U)},
                                                          {ui32(29U), ui32(19U)},
                                                          {ui32(29U), ui32(19U)},
                                                          {ui32(29U), ui32(19U)},
                                                          {ui32(29U), ui32(19U)},
                                                          {ui32(29U), ui32(19U)},
                                                          {{}, ui32(29U)},
                                                          {{}, ui32(29U)},
                                                      });
}

Y_UNIT_TEST_LLVM(TestListFromRange) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, ui16(0));
    const auto data1 = NTest::ConvertValueToLiteralNode(pb, ui16(9));
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui16>{ui16(1), ui16(2), ui16(3)});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      const auto l = pb.ListFromRange(data0, data1, item);
                                      return pb.NewTuple({pb.HasItems(l), pb.Length(l), pb.Head(l), pb.Last(l)});
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    using TTuple = std::tuple<bool, ui64, TMaybe<ui16>, TMaybe<ui16>>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTuple>{
                                                          {true, ui64(9), ui16(0), ui16(8)},
                                                          {true, ui64(5), ui16(0), ui16(8)},
                                                          {true, ui64(3), ui16(0), ui16(6)},
                                                      });
}

Y_UNIT_TEST_LLVM(TestSize) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data1 = NTest::ConvertValueToLiteralNode(pb, TStringBuf("aaa"));
    const auto data2 = NTest::ConvertValueToLiteralNode(pb, ui32(3));
    const auto data3 = NTest::ConvertValueToLiteralNode(pb, ui64(7878786987536ull));
    const auto data4 = NTest::ConvertValueToLiteralNode(pb, TStringBuf("qqqqq"));
    const auto pgmReturn = pb.NewList(NTest::ConvertToMinikqlType<ui32>(pb),
                                      {pb.Size(data1), pb.Size(data2), pb.Size(data3), pb.Size(data4)});

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{ui32(3), ui32(4), ui32(8), ui32(5)});
}

Y_UNIT_TEST_LLVM(TestEnumerate) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{34U, 56U});
    const auto pgmReturn = pb.Enumerate(list1);

    const auto graph = setup.BuildGraph(pgmReturn);
    using TTuple = std::tuple<ui64, ui32>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTuple>{{ui64(0), ui32(34)}, {ui64(1), ui32(56)}});
}

Y_UNIT_TEST_LLVM(TestEnumerateLazy) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{34U, 56U});
    const auto pgmReturn = pb.Enumerate(pb.LazyList(list1));

    const auto graph = setup.BuildGraph(pgmReturn);
    using TTuple = std::tuple<ui64, ui32>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTuple>{{ui64(0), ui32(34)}, {ui64(1), ui32(56)}});
}

Y_UNIT_TEST_LLVM(TestEnumerateLazyThenReverse) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{34U, 56U});
    const auto pgmReturn = pb.Reverse(pb.Enumerate(pb.LazyList(list1)));

    const auto graph = setup.BuildGraph(pgmReturn);
    using TTuple = std::tuple<ui64, ui32>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TTuple>{{ui64(1), ui32(56)}, {ui64(0), ui32(34)}});
}

Y_UNIT_TEST_LLVM(TestEnumerateLazyThenSkip) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{34U, 56U});
    const auto one = NTest::ConvertValueToLiteralNode(pb, ui64(1));
    const auto list = pb.Enumerate(pb.LazyList(list1));
    const auto skip = pb.Skip(list, one);
    const auto pgmReturn = pb.NewTuple({skip, pb.Length(list), pb.Length(skip)});

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto result = graph->GetValue();

    using TTuple = std::tuple<ui64, ui32>;
    AssertUnboxedValueElementEqual(result.GetElement(0), TVector<TTuple>{{ui64(1), ui32(56)}});
    AssertUnboxedValueElementEqual(result.GetElement(1), ui64(2));
    AssertUnboxedValueElementEqual(result.GetElement(2), ui64(1));
}

Y_UNIT_TEST_LLVM(TestEnumerateLazyThenTake) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{34U, 56U});
    const auto one = NTest::ConvertValueToLiteralNode(pb, ui64(1));
    const auto list = pb.Enumerate(pb.LazyList(list1));
    const auto take = pb.Take(list, one);
    const auto pgmReturn = pb.NewTuple({take, pb.Length(list), pb.Length(take)});

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto result = graph->GetValue();

    using TTuple = std::tuple<ui64, ui32>;
    AssertUnboxedValueElementEqual(result.GetElement(0), TVector<TTuple>{{ui64(0), ui32(34)}});
    AssertUnboxedValueElementEqual(result.GetElement(1), ui64(2));
    AssertUnboxedValueElementEqual(result.GetElement(2), ui64(1));
}

template <bool LLVM>
void TestSortImpl(bool asc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TInRow = NTest::TStructType<NTest::TStructMember<"key", ui32>,
                                      NTest::TStructMember<"payload", TStringBuf>>;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TInRow>{
                                                               {{{2U}, {""}}},
                                                               {{{1U}, {"aaa"}}},
                                                               {{{3U}, {"qqq"}}},
                                                           });

    {
        const auto pgmReturn = pb.Sort(list, NTest::ConvertValueToLiteralNode(pb, asc),
                                       [&](TRuntimeNode item) {
                                           return pb.Member(item, "key");
                                       });
        const auto graph = setup.BuildGraph(pgmReturn);

        using TOutRow = std::tuple<ui32, TString>;

        const auto expected = asc ? TVector<TOutRow>{
                                        {{1U}, {"aaa"}},
                                        {{2U}, {""}},
                                        {{3U}, {"qqq"}},
                                    }
                                  : TVector<TOutRow>{
                                        {{3U}, {"qqq"}},
                                        {{2U}, {""}},
                                        {{1U}, {"aaa"}},
                                    };
        AssertUnboxedValueElementEqual(graph->GetValue(), expected);
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

template <bool LLVM>
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
        result.push_back(TTriple{first, second, third});
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
            {1, 1, 1},
            {1, 1, 2},
            {1, 2, 3},
            {1, 3, 0},
            {2, 0, 1},
            {2, 1, 0},
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
            {1, 1, 1},
            {1, 1, 2},
            {1, 2, 3},
            {1, 3, 0},
            {2, 0, 1},
            {2, 1, 0},
        };
        TVector<TTriple> expected(expectedData, expectedData + sizeof(expectedData) / sizeof(*expectedData));
        TVector<TTriple> result = SortTuples<LLVM>(setup, listMaker(), order);
        UNIT_ASSERT_EQUAL(result, expected);
    }

    {
        TRuntimeNode order = TupleOrder(pb, false, false, false);
        TTriple expectedData[] = {
            {2, 1, 0},
            {2, 0, 1},
            {1, 3, 0},
            {1, 2, 3},
            {1, 1, 2},
            {1, 1, 1},
        };
        TVector<TTriple> expected(expectedData, expectedData + sizeof(expectedData) / sizeof(*expectedData));
        TVector<TTriple> result = SortTuples<LLVM>(setup, listMaker(), order);
        UNIT_ASSERT_EQUAL(result, expected);
    }

    {
        TRuntimeNode order = TupleOrder(pb, true, false, true);
        TTriple expectedData[] = {
            {1, 3, 0},
            {1, 2, 3},
            {1, 1, 1},
            {1, 1, 2},
            {2, 1, 0},
            {2, 0, 1},
        };
        TVector<TTriple> expected(expectedData, expectedData + sizeof(expectedData) / sizeof(*expectedData));
        TVector<TTriple> result = SortTuples<LLVM>(setup, listMaker(), order);
        UNIT_ASSERT_EQUAL(result, expected);
    }

    {
        TRuntimeNode order = TupleOrder(pb, false, true, false);
        TTriple expectedData[] = {
            {2, 0, 1},
            {2, 1, 0},
            {1, 1, 2},
            {1, 1, 1},
            {1, 2, 3},
            {1, 3, 0},
        };
        TVector<TTriple> expected(expectedData, expectedData + sizeof(expectedData) / sizeof(*expectedData));
        TVector<TTriple> result = SortTuples<LLVM>(setup, listMaker(), order);
        UNIT_ASSERT_EQUAL(result, expected);
    }
}

Y_UNIT_TEST_LLVM(TestAsList) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto pgmReturn = pb.AsList(NTest::ConvertValueToLiteralNode(pb, ui32(34)));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{34U});
}

Y_UNIT_TEST_LLVM(TestListIfTrue) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto pgmReturn = pb.ListIf(NTest::ConvertValueToLiteralNode(pb, true),
                                     NTest::ConvertValueToLiteralNode(pb, ui32(34)));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{34U});
}

Y_UNIT_TEST_LLVM(TestListIfFalse) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto pgmReturn = pb.ListIf(NTest::ConvertValueToLiteralNode(pb, false),
                                     NTest::ConvertValueToLiteralNode(pb, ui32(34)));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{});
}

Y_UNIT_TEST_LLVM(TestNth) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto tuple = NTest::ConvertValueToLiteralNode(pb, std::tuple<ui32, ui32>{34U, 56U});
    const auto pgmReturn = pb.Nth(tuple, 1);

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), ui32(56));
}

Y_UNIT_TEST_LLVM(NonDeterministicEnv) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<float>{1.f, 2.f, 3.f});
    const auto pgmReturn = pb.Sort(list,
                                   NTest::ConvertValueToLiteralNode(pb, false),
                                   [](TRuntimeNode item) { return item; });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<float>{3.f, 2.f, 1.f});
}

Y_UNIT_TEST_LLVM(TestIndexDictContains) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{7U, 16U});
    const auto dict = pb.ToIndexDict(list);
    const auto keys = NTest::ConvertValueToLiteralNode(pb, TVector<ui64>{0ULL, 1ULL, 2ULL});
    const auto pgmReturn = pb.Map(keys,
                                  [&](TRuntimeNode key) { return pb.Contains(dict, key); });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<bool>{true, true, false});
}

Y_UNIT_TEST_LLVM(TestIndexDictLookup) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<i32>{7, 16});
    const auto dict = pb.ToIndexDict(list);
    const auto keys = NTest::ConvertValueToLiteralNode(pb, TVector<ui64>{0ULL, 1ULL, 2ULL});
    const auto pgmReturn = pb.Map(keys,
                                  [&](TRuntimeNode key) { return pb.Lookup(dict, key); });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<i32>>{i32(7), i32(16), {}});
}

Y_UNIT_TEST_LLVM(TestIndexDictContainsLazy) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{7U, 16U});
    const auto dict = pb.ToIndexDict(pb.LazyList(list));
    const auto keys = NTest::ConvertValueToLiteralNode(pb, TVector<ui64>{0ULL, 1ULL, 2ULL});
    const auto pgmReturn = pb.Map(keys,
                                  [&](TRuntimeNode key) { return pb.Contains(dict, key); });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<bool>{true, true, false});
}

Y_UNIT_TEST_LLVM(TestIndexDictLookupLazy) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{7U, 16U});
    const auto dict = pb.ToIndexDict(pb.LazyList(list));
    const auto keys = NTest::ConvertValueToLiteralNode(pb, TVector<ui64>{0ULL, 1ULL, 2ULL});
    const auto pgmReturn = pb.Map(keys,
                                  [&](TRuntimeNode key) { return pb.Lookup(dict, key); });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<ui32>>{ui32(7), ui32(16), {}});
}

Y_UNIT_TEST_LLVM(TestToBytes) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<float>{0.f, -3.14f, -HUGE_VALF, HUGE_VALF});
    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.ToBytes(item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TStringBuf>{
                                                          TStringBuf("\x00\x00\x00\x00", 4),
                                                          TStringBuf("\xc3\xf5\x48\xc0", 4),
                                                          TStringBuf("\x00\x00\x80\xff", 4),
                                                          TStringBuf("\x00\x00\x80\x7f", 4),
                                                      });
}

Y_UNIT_TEST_LLVM(TestToBytesOpt) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<float>>{
                                                               {}, 0.f, -3.14f, -HUGE_VALF, HUGE_VALF});
    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.ToBytes(item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<TStringBuf>>{
                                                          {},
                                                          TStringBuf("\x00\x00\x00\x00", 4),
                                                          TStringBuf("\xc3\xf5\x48\xc0", 4),
                                                          TStringBuf("\x00\x00\x80\xff", 4),
                                                          TStringBuf("\x00\x00\x80\x7f", 4),
                                                      });
}

Y_UNIT_TEST_LLVM(TestToStringTemporarryUtf8) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, NTest::TUtf8{"long prefix "});
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<NTest::TUtf8>{
                                                               {"01234567890 long string"}, {"01234567890 very long string"}});
    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.ToString(pb.Concat(data0, item));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TStringBuf>{
                                                          "long prefix 01234567890 long string",
                                                          "long prefix 01234567890 very long string",
                                                      });
}

Y_UNIT_TEST_LLVM(TestFromString) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TStringBuf>{"234", "abc"});
    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.FromString(item, NTest::ConvertToMinikqlType<ui32>(pb));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<ui32>>{ui32(234), {}});
}

Y_UNIT_TEST_LLVM(TestStrictFromString) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    {
        TVector<TRuntimeNode> tupleItems;
        tupleItems.push_back(pb.StrictFromString(NTest::ConvertValueToLiteralNode(pb, TStringBuf("234")), NTest::ConvertToMinikqlType<ui32>(pb)));
        tupleItems.push_back(pb.StrictFromString(NTest::ConvertValueToLiteralNode(pb, TStringBuf("-1")), NTest::ConvertToMinikqlType<i32>(pb)));
        tupleItems.push_back(pb.StrictFromString(NTest::ConvertValueToLiteralNode(pb, TStringBuf("3.1415926")), NTest::ConvertToMinikqlType<double>(pb)));
        const auto graph = setup.BuildGraph(pb.NewTuple(tupleItems));
        const auto res = graph->GetValue();
        AssertUnboxedValueElementEqual(res.GetElement(0), ui32(234));
        AssertUnboxedValueElementEqual(res.GetElement(1), i32(-1));
        UNIT_ASSERT_VALUES_EQUAL(res.GetElement(2).template Get<double>(), 3.1415926);
    }

    {
        const auto fail = pb.StrictFromString(NTest::ConvertValueToLiteralNode(pb, TStringBuf("vgfsbhj")), NTest::ConvertToMinikqlType<ui32>(pb));
        const auto failgraph = setup.BuildGraph(fail, {});
        UNIT_ASSERT_EXCEPTION(failgraph->GetValue(), yexception);
    }

    {
        const auto fail = pb.StrictFromString(NTest::ConvertValueToLiteralNode(pb, TStringBuf("")), NTest::ConvertToMinikqlType<double>(pb));
        const auto failgraph = setup.BuildGraph(fail, {});
        UNIT_ASSERT_EXCEPTION(failgraph->GetValue(), yexception);
    }
}

Y_UNIT_TEST_LLVM(TestFromBytes) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    TVector<TRuntimeNode> tupleItems;
    tupleItems.push_back(pb.FromBytes(NTest::ConvertValueToLiteralNode(pb, TStringBuf("\xEA\x00\x00\x00", 4)), NTest::ConvertToMinikqlType<ui32>(pb)));
    tupleItems.push_back(pb.FromBytes(NTest::ConvertValueToLiteralNode(pb, TMaybe<TStringBuf>{}), NTest::ConvertToMinikqlType<ui32>(pb)));
    const auto pgmReturn = pb.NewTuple(tupleItems);

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto res = graph->GetValue();
    AssertUnboxedValueElementEqual(res.GetElement(0), TMaybe<ui32>{ui32(234)});
    AssertUnboxedValueElementEqual(res.GetElement(1), TMaybe<ui32>{});
}

Y_UNIT_TEST_LLVM(TestMTRand) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto seed = NTest::ConvertValueToLiteralNode(pb, ui64(42));
    auto pgmReturn = pb.NewEmptyList(NTest::ConvertToMinikqlType<ui64>(pb));
    auto rnd = pb.NewMTRand(seed);
    const ui32 n = 5;
    for (ui32 i = 0; i < n; ++i) {
        const auto pair = pb.NextMTRand(rnd);
        pgmReturn = pb.Append(pgmReturn, pb.Nth(pair, 0));
        rnd = pb.Nth(pair, 1);
    }

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui64>{
                                                          13930160852258120406ull,
                                                          11788048577503494824ull,
                                                          13874630024467741450ull,
                                                          2513787319205155662ull,
                                                          16662371453428439381ull,
                                                      });
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

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui8>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    const auto pgmReturn = pb.Take(pb.Skip(pb.Iterator(list, {}), pb.NewDataLiteral<ui64>(4ULL)), pb.NewDataLiteral<ui64>(3ULL));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<ui8>({ui8(4), ui8(5), ui8(6)}));
}

Y_UNIT_TEST_LLVM(TestSkipAndTakeOverFlow) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui8>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    const auto pgmReturn = pb.FromFlow(pb.Take(pb.Skip(pb.ToFlow(list), pb.NewDataLiteral<ui64>(4ULL)), pb.NewDataLiteral<ui64>(3ULL)));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<ui8>({ui8(4), ui8(5), ui8(6)}));
}

Y_UNIT_TEST_LLVM(TestLazyListFromArray) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto array = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{1U, 2U, 3U});
    const auto pgmReturn = pb.LazyList(array);

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto list = graph->GetValue();

    UNIT_ASSERT_VALUES_EQUAL(list.GetListLength(), 3U);
    UNIT_ASSERT_VALUES_EQUAL(list.GetElements(), nullptr);
    AssertUnboxedValueElementEqual(list, TVector<ui32>{1U, 2U, 3U});
}

Y_UNIT_TEST_LLVM(TestCollectLazyList) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto array = NTest::ConvertValueToLiteralNode(pb, TVector<ui32>{1U, 2U, 3U});
    const auto pgmReturn = pb.Collect(pb.LazyList(array));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto list = graph->GetValue();

    UNIT_ASSERT_VALUES_EQUAL(list.GetListLength(), 3U);
    UNIT_ASSERT(list.GetElements());
    AssertUnboxedValueElementEqual(list, TVector<ui32>{1U, 2U, 3U});
}

Y_UNIT_TEST_LLVM(TestAddAllTimezones) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto zones = pb.ListFromRange(pb.NewDataLiteral<ui16>(0U), pb.NewDataLiteral<ui16>(1000U), pb.NewDataLiteral<ui16>(1U));
    const auto pgmReturn = pb.Collect(pb.Map(zones, [&](TRuntimeNode id) { return pb.AddTimezone(pb.CurrentUtcDate({id}), id); }));
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

    const auto datetimeType = pb.NewDataType(NUdf::EDataSlot::Datetime, true);
    const auto datetimeTypeTz = pb.NewDataType(NUdf::EDataSlot::TzDatetime, true);

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TStringBuf>{"", "2019-10-24T13:01:37,Zulu", "2019-10-24T13:01:37,Japan", "2019-10-24T13:01:37,Jamaica"});

    const auto pgmReturn = pb.FlatMap(list,
                                      [&](TRuntimeNode data) {
                                          return pb.Convert(pb.FromString(data, datetimeTypeTz), datetimeType);
                                      });

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

Y_UNIT_TEST_LLVM(TestSqueezeToList) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<double>{0.0, 1.1, -3.14, 121324.323, -7898.8});
    const auto pgmReturn = pb.FromFlow(pb.SqueezeToList(pb.ToFlow(list), pb.NewDataLiteral<ui64>(1000ULL)));

    const auto graph = setup.BuildGraph(pgmReturn);
    // SqueezeToList emits one list item then Finish — verify via stream-view of length 1
    const auto stream = graph->GetValue();
    NUdf::TUnboxedValue full;
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, stream.Fetch(full));
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, stream.Fetch(full));

    UNIT_ASSERT_VALUES_EQUAL(full.GetListLength(), 5ULL);
    AssertUnboxedValueElementEqual(full, TVector<double>{0.0, 1.1, -3.14, 121324.323, -7898.8});
}

Y_UNIT_TEST_LLVM(TestSqueezeToListWithLimit) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<float>{0.0f, 1.1f, -3.14f, 12.323f, -7898.8f});
    const auto pgmReturn = pb.FromFlow(pb.SqueezeToList(pb.ToFlow(list), pb.NewDataLiteral<ui64>(3ULL)));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto stream = graph->GetValue();
    NUdf::TUnboxedValue full;
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, stream.Fetch(full));
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, stream.Fetch(full));

    UNIT_ASSERT_VALUES_EQUAL(full.GetListLength(), 3ULL);
    AssertUnboxedValueElementEqual(full, TVector<float>{0.0f, 1.1f, -3.14f});
}

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
                                      return pb.NewStruct({{"key", pb.Member(item, "key")},
                                                           {"value", pb.AggrConcat(mark, pb.Member(item, "value"))}});
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

Y_UNIT_TEST_LLVM(TestBuiltinsAdd) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto testAdd = [&](auto val1, auto val2, auto expected) {
        using T = decltype(val1);
        const auto data1 = pb.NewDataLiteral<T>(val1);
        const auto data2 = pb.NewDataLiteral<T>(val2);
        const auto result = pb.Add(data1, data2);
        const auto graph = setup.BuildGraph(result);

        AssertNumericValuesEqual(graph->GetValue().template Get<T>(), expected);
    };

    testAdd(Max<i8>(), i8(1), Min<i8>());
    testAdd(Max<ui8>(), ui8(1), ui8(0));
    testAdd(Max<i16>(), i16(1), Min<i16>());
    testAdd(Max<ui16>(), ui16(1), ui16(0));
    testAdd(Max<i32>(), i32(1), Min<i32>());
    testAdd(Max<ui32>(), ui32(1), ui32(0));
    testAdd(Max<i64>(), i64(1), Min<i64>());
    testAdd(Max<ui64>(), ui64(1), ui64(0));

    testAdd(3.14f, 2.71f, 5.85f);
    testAdd(1.5, 2.5, 4.0);
}

Y_UNIT_TEST_LLVM(TestBuiltinsSub) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto testSub = [&](auto val1, auto val2, auto expected) {
        using T = decltype(val1);
        const auto data1 = pb.NewDataLiteral<T>(val1);
        const auto data2 = pb.NewDataLiteral<T>(val2);
        const auto result = pb.Sub(data1, data2);
        const auto graph = setup.BuildGraph(result);
        AssertNumericValuesEqual(graph->GetValue().template Get<T>(), expected);
    };

    testSub(Min<i8>(), i8(1), Max<i8>());
    testSub(ui8(0), ui8(1), Max<ui8>());
    testSub(Min<i16>(), i16(1), Max<i16>());
    testSub(ui16(0), ui16(1), Max<ui16>());
    testSub(Min<i32>(), i32(1), Max<i32>());
    testSub(ui32(0), ui32(1), Max<ui32>());
    testSub(Min<i64>(), i64(1), Max<i64>());
    testSub(ui64(0), ui64(1), Max<ui64>());

    testSub(5.5f, 2.3f, 3.2f);
    testSub(10.0, 3.5, 6.5);
}

Y_UNIT_TEST_LLVM(TestBuiltinsMul) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto testMul = [&](auto val1, auto val2, auto expected) {
        using T = decltype(val1);
        const auto data1 = pb.NewDataLiteral<T>(val1);
        const auto data2 = pb.NewDataLiteral<T>(val2);
        const auto result = pb.Mul(data1, data2);
        const auto graph = setup.BuildGraph(result);
        AssertNumericValuesEqual(graph->GetValue().template Get<T>(), expected);
    };

    testMul(Max<i8>(), i8(2), i8(-2));
    testMul(Max<ui8>(), ui8(2), ui8(254));
    testMul(Max<i16>(), i16(2), i16(-2));
    testMul(Max<ui16>(), ui16(2), ui16(65534));
    testMul(Max<i32>(), i32(2), i32(-2));
    testMul(Max<ui32>(), ui32(2), ui32(4294967294));
    testMul(Max<i64>(), i64(2), i64(-2));
    testMul(Max<ui64>(), ui64(2), ui64(18446744073709551614ULL));

    testMul(2.5f, 4.0f, 10.0f);
    testMul(3.0, 7.0, 21.0);
}

Y_UNIT_TEST_LLVM(TestBuiltinsInc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto testInc = [&](auto val, auto expected) {
        using T = decltype(val);
        const auto data = pb.NewDataLiteral<T>(val);
        const auto result = pb.Increment(data);
        const auto graph = setup.BuildGraph(result);
        AssertNumericValuesEqual(graph->GetValue().template Get<T>(), expected);
    };

    testInc(Max<i8>(), Min<i8>());
    testInc(Max<ui8>(), ui8(0));
    testInc(Max<i16>(), Min<i16>());
    testInc(Max<ui16>(), ui16(0));
    testInc(Max<i32>(), Min<i32>());
    testInc(Max<ui32>(), ui32(0));
    testInc(Max<i64>(), Min<i64>());
    testInc(Max<ui64>(), ui64(0));

    testInc(5.5f, 6.5f);
    testInc(10.0, 11.0);
}

Y_UNIT_TEST_LLVM(TestBuiltinsDec) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto testDec = [&](auto val, auto expected) {
        using T = decltype(val);
        const auto data = pb.NewDataLiteral<T>(val);
        const auto result = pb.Decrement(data);
        const auto graph = setup.BuildGraph(result);
        AssertNumericValuesEqual(graph->GetValue().template Get<T>(), expected);
    };

    testDec(Min<i8>(), Max<i8>());
    testDec(ui8(0), Max<ui8>());
    testDec(Min<i16>(), Max<i16>());
    testDec(ui16(0), Max<ui16>());
    testDec(Min<i32>(), Max<i32>());
    testDec(ui32(0), Max<ui32>());
    testDec(Min<i64>(), Max<i64>());
    testDec(ui64(0), Max<ui64>());

    testDec(7.5f, 6.5f);
    testDec(20.0, 19.0);
}

Y_UNIT_TEST_LLVM(TestBuiltinsMinus) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto testMinus = [&](auto val, auto expected) {
        using T = decltype(val);
        const auto data = pb.NewDataLiteral<T>(val);
        const auto result = pb.Minus(data);
        const auto graph = setup.BuildGraph(result);
        AssertNumericValuesEqual(graph->GetValue().template Get<T>(), expected);
    };

    testMinus(Min<i8>(), Min<i8>());
    testMinus(Max<ui8>(), ui8(1));
    testMinus(Min<i16>(), Min<i16>());
    testMinus(Max<ui16>(), ui16(1));
    testMinus(Min<i32>(), Min<i32>());
    testMinus(Max<ui32>(), ui32(1));
    testMinus(Min<i64>(), Min<i64>());
    testMinus(Max<ui64>(), ui64(1));

    testMinus(3.14f, -3.14f);
    testMinus(2.5, -2.5);
}

Y_UNIT_TEST_LLVM(TestBuiltinsAbs) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto testAbs = [&](auto val, auto expected) {
        using T = decltype(val);
        const auto data = pb.NewDataLiteral<T>(val);
        const auto result = pb.Abs(data);
        const auto graph = setup.BuildGraph(result);
        AssertNumericValuesEqual(graph->GetValue().template Get<T>(), expected);
    };

    // Test INT_MIN cases - this is UB in abs() but we're not fixing it per user request
    // These tests document the current behavior (wraps to INT_MIN)
    testAbs(Min<i8>(), Min<i8>());
    testAbs(Min<i16>(), Min<i16>());
    testAbs(Min<i32>(), Min<i32>());
    testAbs(Min<i64>(), Min<i64>());

    testAbs(-3.14f, 3.14f);
    testAbs(-2.5, 2.5);
}
} // Y_UNIT_TEST_SUITE(TMiniKQLComputationNodeTest)

} // namespace NMiniKQL
} // namespace NKikimr
