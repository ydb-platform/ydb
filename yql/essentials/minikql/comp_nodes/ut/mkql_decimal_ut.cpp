#include "mkql_computation_node_ut.h"
#include "mkql_program_builder_test_utils.h"

#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLDecimalTest) {
Y_UNIT_TEST_LLVM(TestNanvl) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data = NTest::ConvertValueToLiteralNode(pb, NTest::TDecimalLiteral<13, 5>{314159});
    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<TMaybe<NTest::TDecimalLiteral<13, 5>>>{
                                                           {NTest::TDecimalLiteral<13, 5>{0}},
                                                           {NTest::TDecimalLiteral<13, 5>{NYql::NDecimal::Nan()}},
                                                           {NTest::TDecimalLiteral<13, 5>{+NYql::NDecimal::Inf()}},
                                                           {NTest::TDecimalLiteral<13, 5>{-NYql::NDecimal::Inf()}},
                                                           {},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Nanvl(item, data);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<NYql::NDecimal::TInt128>>{
                                                          TMaybe<NYql::NDecimal::TInt128>{NYql::NDecimal::TInt128(0)},
                                                          TMaybe<NYql::NDecimal::TInt128>{NYql::NDecimal::TInt128(314159)},
                                                          TMaybe<NYql::NDecimal::TInt128>{+NYql::NDecimal::Inf()},
                                                          TMaybe<NYql::NDecimal::TInt128>{-NYql::NDecimal::Inf()},
                                                          TMaybe<NYql::NDecimal::TInt128>{}});
}

Y_UNIT_TEST_LLVM(TestToIntegral) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<13, 1>>{
                                                           {0},
                                                           {NYql::NDecimal::Nan()},
                                                           {+NYql::NDecimal::Inf()},
                                                           {-NYql::NDecimal::Inf()},
                                                           {1270},
                                                           {-1280},
                                                           {2550},
                                                           {-2560},
                                                           {2560},
                                                           {-2570},
                                                           {327670},
                                                           {-327680},
                                                           {655350},
                                                           {-655360},
                                                           {21474836470},
                                                           {-21474836480},
                                                           {21474836480},
                                                           {-21474836490},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.NewTuple({pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<i8>::Id, true)),
                                                          pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<ui8>::Id, true)),
                                                          pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<i16>::Id, true)),
                                                          pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<ui16>::Id, true)),
                                                          pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<i32>::Id, true)),
                                                          pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<ui32>::Id, true)),
                                                          pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<i64>::Id, true)),
                                                          pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<ui64>::Id, true))});
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    using TRow = std::tuple<TMaybe<i8>, TMaybe<ui8>, TMaybe<i16>, TMaybe<ui16>, TMaybe<i32>, TMaybe<ui32>, TMaybe<i64>, TMaybe<ui64>>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TRow>{
                                                          {i8(0), ui8(0), i16(0), ui16(0), i32(0), ui32(0), i64(0), ui64(0)},
                                                          {{}, {}, {}, {}, {}, {}, {}, {}},
                                                          {{}, {}, {}, {}, {}, {}, {}, {}},
                                                          {{}, {}, {}, {}, {}, {}, {}, {}},
                                                          {i8(127), ui8(127), i16(127), ui16(127), i32(127), ui32(127), i64(127), ui64(127)},
                                                          {i8(-128), {}, i16(-128), {}, i32(-128), {}, i64(-128), {}},
                                                          {{}, ui8(255), i16(255), ui16(255), i32(255), ui32(255), i64(255), ui64(255)},
                                                          {{}, {}, i16(-256), {}, i32(-256), {}, i64(-256), {}},
                                                          {{}, {}, i16(256), ui16(256), i32(256), ui32(256), i64(256), ui64(256)},
                                                          {{}, {}, i16(-257), {}, i32(-257), {}, i64(-257), {}},
                                                          {{}, {}, i16(32767), ui16(32767), i32(32767), ui32(32767), i64(32767), ui64(32767)},
                                                          {{}, {}, i16(-32768), {}, i32(-32768), {}, i64(-32768), {}},
                                                          {{}, {}, {}, ui16(65535), i32(65535), ui32(65535), i64(65535), ui64(65535)},
                                                          {{}, {}, {}, {}, i32(-65536), {}, i64(-65536), {}},
                                                          {{}, {}, {}, {}, i32(2147483647), ui32(2147483647), i64(2147483647), ui64(2147483647)},
                                                          {{}, {}, {}, {}, i32(-2147483648), {}, i64(-2147483648), {}},
                                                          {{}, {}, {}, {}, {}, ui32(2147483648U), i64(2147483648), ui64(2147483648)},
                                                          {{}, {}, {}, {}, {}, {}, i64(-2147483649LL), {}},
                                                      });
}

Y_UNIT_TEST_LLVM(TestToFloat) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<10, 3>>{
                                                           {2123},
                                                           {233},
                                                           {0},
                                                           {-3277823},
                                                           {-1},
                                                           {7128},
                                                           {NYql::NDecimal::Nan()},
                                                           {+NYql::NDecimal::Inf()},
                                                           {-NYql::NDecimal::Inf()},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Convert(item, pb.NewDataType(NUdf::TDataType<float>::Id));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, 2.123f);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, 0.233f);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, 0.0f);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, -3277.823f);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, -0.001f);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, 7.128f);
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

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<10, 5>>{
                                                           {2123},
                                                           {233},
                                                           {0},
                                                           {-3277823},
                                                           {-1},
                                                           {7128},
                                                           {NYql::NDecimal::Nan()},
                                                           {+NYql::NDecimal::Inf()},
                                                           {-NYql::NDecimal::Inf()},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Convert(item, pb.NewDataType(NUdf::TDataType<double>::Id));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, 0.02123);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, 0.00233);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, 0.0);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, -32.77823);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, -0.00001);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, 0.07128);
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

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, NTest::TDecimalLiteral<10, 0>{2});
    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<10, 0>>{
                                                           {2},
                                                           {23},
                                                           {-23},
                                                           {25},
                                                           {-25},
                                                           {1},
                                                           {-1},
                                                           {3},
                                                           {-3},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.DecimalDiv(item, data0);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<NYql::NDecimal::TInt128>{1, 12, -12, 12, -12, 0, 0, 2, -2});
}

Y_UNIT_TEST_LLVM(TestDivInt) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, NTest::TDecimalLiteral<9, 3>{-238973});
    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<i8>{i8(0), i8(-1), i8(-128), i8(3), i8(5), i8(-7), i8(13), i8(-19), i8(42)});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.DecimalDiv(data0, item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<NYql::NDecimal::TInt128>{
                                                          -NYql::NDecimal::Inf(), 238973, 1866, -79658, -47795, 34139, -18383, 12577, -5690});
}

Y_UNIT_TEST_LLVM(TestMod) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, NTest::TDecimalLiteral<5, 2>{-12323});
    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<5, 2>>{
                                                           {-12323},
                                                           {0},
                                                           {NYql::NDecimal::Inf()},
                                                           {-1},
                                                           {2},
                                                           {-3},
                                                           {NYql::NDecimal::Nan()},
                                                           {7},
                                                           {-10000},
                                                           {12329},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.DecimalMod(data0, item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<NYql::NDecimal::TInt128>{
                                                          0, NYql::NDecimal::Nan(), NYql::NDecimal::Nan(), 0, -1, -2, NYql::NDecimal::Nan(), -3, -2323, -12323});
}

Y_UNIT_TEST_LLVM(TestModInt) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, NTest::TDecimalLiteral<3, 2>{-743});
    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<i16>{i16(0), i16(1), i16(-2), i16(3), i16(4), i16(-5), i16(8), i16(10), i16(-10)});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.DecimalMod(data0, item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<NYql::NDecimal::TInt128>{
                                                          NYql::NDecimal::Nan(), -43, -143, -143, -343, -243, -743, -743, -743});
}

Y_UNIT_TEST_LLVM(TestMul) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, NTest::TDecimalLiteral<10, 2>{333});
    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<10, 2>>{
                                                           {333},
                                                           {-100},
                                                           {-120},
                                                           {3},
                                                           {77},
                                                           {122},
                                                           {1223},
                                                           {-999},
                                                           {0},
                                                           {-3003003003LL},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.DecimalMul(item, data0);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<NYql::NDecimal::TInt128>{
                                                          1109, -333, -400, 10, 256, 406, 4073, -3327, 0, -NYql::NDecimal::Inf()});
}

Y_UNIT_TEST_LLVM(TestMulUInt) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, NTest::TDecimalLiteral<7, 2>{-333});
    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<ui16>{ui16(0), ui16(1), ui16(2), ui16(3), ui16(10), ui16(100), ui16(1000), ui16(10000), ui16(65535)});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.DecimalMul(data0, item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<NYql::NDecimal::TInt128>{
                                                          0, -333, -666, -999, -3330, -33300, -333000, -3330000, -NYql::NDecimal::Inf()});
}

Y_UNIT_TEST_LLVM(TestMulTinyInt) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, NTest::TDecimalLiteral<32, 4>{3631400});
    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<i8>{i8(0), i8(1), i8(-1), i8(3), i8(-3), i8(100), i8(-100), i8(127), i8(-128)});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.DecimalMul(data0, item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<NYql::NDecimal::TInt128>{
                                                          0, 3631400, -3631400, 10894200, -10894200, 363140000, -363140000, 461187800, -464819200});
}

Y_UNIT_TEST_LLVM(TestCastAndMulTinyInt) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, i8(1));
    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<32, 4>>{
                                                           {3145926},
                                                           {-3145926},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.NewTuple({pb.DecimalMul(item, data0), pb.DecimalMul(item, pb.ToDecimal(data0, 32, 4))});
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    using TRow = std::tuple<NYql::NDecimal::TInt128, NYql::NDecimal::TInt128>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TRow>{
                                                          TRow{3145926, 3145926},
                                                          TRow{-3145926, -3145926},
                                                      });
}

Y_UNIT_TEST_LLVM(TestLongintMul) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, NTest::TDecimalLiteral<10, 0>{333});
    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<10, 0>>{
                                                           {333},
                                                           {-100},
                                                           {-120},
                                                           {3},
                                                           {77},
                                                           {NYql::NDecimal::Nan()},
                                                           {30030031},
                                                           {-30030031},
                                                           {0},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.DecimalMul(item, data0);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<NYql::NDecimal::TInt128>{
                                                          110889, -33300, -39960, 999, 25641, NYql::NDecimal::Nan(), NYql::NDecimal::Inf(), -NYql::NDecimal::Inf(), 0});
}

Y_UNIT_TEST_LLVM(TestScaleUp) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<10, 2>>{
                                                           {333},
                                                           {-100},
                                                           {-120},
                                                           {3},
                                                           {77},
                                                           {122},
                                                           {1223},
                                                           {-999},
                                                           {0},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.ToDecimal(item, 12, 4);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<NYql::NDecimal::TInt128>{
                                                          33300, -10000, -12000, 300, 7700, 12200, 122300, -99900, 0});
}

Y_UNIT_TEST_LLVM(TestScaleDown) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<10, 2>>{
                                                           {-251},
                                                           {-250},
                                                           {-150},
                                                           {-51},
                                                           {50},
                                                           {50},
                                                           {51},
                                                           {150},
                                                           {250},
                                                           {251},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.ToDecimal(item, 8, 0);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<NYql::NDecimal::TInt128>{
                                                          -3, -2, -2, -1, 0, 0, 1, 2, 2, 3});
}

Y_UNIT_TEST_LLVM(TestMinMax) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<13, 2>>{
                                                           {NYql::NDecimal::Nan()},
                                                           {+NYql::NDecimal::Inf()},
                                                           {314},
                                                           {-213},
                                                           {-NYql::NDecimal::Inf()},
                                                       });
    const auto pgmReturn = pb.FlatMap(list,
                                      [&](TRuntimeNode left) {
                                          return pb.Map(list,
                                                        [&](TRuntimeNode right) {
                                                            return pb.NewTuple({pb.Min(left, right), pb.Max(left, right)});
                                                        });
                                      });

    const auto graph = setup.BuildGraph(pgmReturn);
    using TRow = std::tuple<NYql::NDecimal::TInt128, NYql::NDecimal::TInt128>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TRow>{
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                          TRow{+NYql::NDecimal::Inf(), +NYql::NDecimal::Inf()},
                                                          TRow{314, 314},
                                                          TRow{-213, -213},
                                                          TRow{-NYql::NDecimal::Inf(), -NYql::NDecimal::Inf()},
                                                          TRow{NYql::NDecimal::Inf(), NYql::NDecimal::Inf()},
                                                          TRow{NYql::NDecimal::Inf(), NYql::NDecimal::Inf()},
                                                          TRow{314, +NYql::NDecimal::Inf()},
                                                          TRow{-213, +NYql::NDecimal::Inf()},
                                                          TRow{-NYql::NDecimal::Inf(), +NYql::NDecimal::Inf()},
                                                          TRow{314, 314},
                                                          TRow{314, +NYql::NDecimal::Inf()},
                                                          TRow{314, 314},
                                                          TRow{-213, 314},
                                                          TRow{-NYql::NDecimal::Inf(), 314},
                                                          TRow{-213, -213},
                                                          TRow{-213, +NYql::NDecimal::Inf()},
                                                          TRow{-213, 314},
                                                          TRow{-213, -213},
                                                          TRow{-NYql::NDecimal::Inf(), -213},
                                                          TRow{-NYql::NDecimal::Inf(), -NYql::NDecimal::Inf()},
                                                          TRow{-NYql::NDecimal::Inf(), +NYql::NDecimal::Inf()},
                                                          TRow{-NYql::NDecimal::Inf(), 314},
                                                          TRow{-NYql::NDecimal::Inf(), -213},
                                                          TRow{-NYql::NDecimal::Inf(), -NYql::NDecimal::Inf()},
                                                      });
}

Y_UNIT_TEST_LLVM(TestAggrMinMax) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<13, 2>>{
                                                           {NYql::NDecimal::Nan()},
                                                           {+NYql::NDecimal::Inf()},
                                                           {314},
                                                           {-213},
                                                           {-NYql::NDecimal::Inf()},
                                                       });
    const auto pgmReturn = pb.FlatMap(list,
                                      [&](TRuntimeNode left) {
                                          return pb.Map(list,
                                                        [&](TRuntimeNode right) {
                                                            return pb.NewTuple({pb.AggrMin(left, right), pb.AggrMax(left, right)});
                                                        });
                                      });

    const auto graph = setup.BuildGraph(pgmReturn);
    using TRow = std::tuple<NYql::NDecimal::TInt128, NYql::NDecimal::TInt128>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TRow>{
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                          TRow{+NYql::NDecimal::Inf(), NYql::NDecimal::Nan()},
                                                          TRow{314, NYql::NDecimal::Nan()},
                                                          TRow{-213, NYql::NDecimal::Nan()},
                                                          TRow{-NYql::NDecimal::Inf(), NYql::NDecimal::Nan()},
                                                          TRow{NYql::NDecimal::Inf(), NYql::NDecimal::Nan()},
                                                          TRow{NYql::NDecimal::Inf(), NYql::NDecimal::Inf()},
                                                          TRow{314, +NYql::NDecimal::Inf()},
                                                          TRow{-213, +NYql::NDecimal::Inf()},
                                                          TRow{-NYql::NDecimal::Inf(), +NYql::NDecimal::Inf()},
                                                          TRow{314, NYql::NDecimal::Nan()},
                                                          TRow{314, +NYql::NDecimal::Inf()},
                                                          TRow{314, 314},
                                                          TRow{-213, 314},
                                                          TRow{-NYql::NDecimal::Inf(), 314},
                                                          TRow{-213, NYql::NDecimal::Nan()},
                                                          TRow{-213, +NYql::NDecimal::Inf()},
                                                          TRow{-213, 314},
                                                          TRow{-213, -213},
                                                          TRow{-NYql::NDecimal::Inf(), -213},
                                                          TRow{-NYql::NDecimal::Inf(), NYql::NDecimal::Nan()},
                                                          TRow{-NYql::NDecimal::Inf(), +NYql::NDecimal::Inf()},
                                                          TRow{-NYql::NDecimal::Inf(), 314},
                                                          TRow{-NYql::NDecimal::Inf(), -213},
                                                          TRow{-NYql::NDecimal::Inf(), -NYql::NDecimal::Inf()},
                                                      });
}

Y_UNIT_TEST_LLVM(TestAddSub) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<13, 2>>{
                                                           {NYql::NDecimal::Nan()},
                                                           {+NYql::NDecimal::Inf()},
                                                           {314},
                                                           {-213},
                                                           {-NYql::NDecimal::Inf()},
                                                       });
    const auto pgmReturn = pb.FlatMap(list,
                                      [&](TRuntimeNode left) {
                                          return pb.Map(list,
                                                        [&](TRuntimeNode right) {
                                                            return pb.NewTuple({pb.Add(left, right), pb.Sub(left, right)});
                                                        });
                                      });

    const auto graph = setup.BuildGraph(pgmReturn);
    using TRow = std::tuple<NYql::NDecimal::TInt128, NYql::NDecimal::TInt128>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TRow>{
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                          TRow{NYql::NDecimal::Inf(), NYql::NDecimal::Nan()},
                                                          TRow{NYql::NDecimal::Inf(), NYql::NDecimal::Inf()},
                                                          TRow{NYql::NDecimal::Inf(), NYql::NDecimal::Inf()},
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Inf()},
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                          TRow{+NYql::NDecimal::Inf(), -NYql::NDecimal::Inf()},
                                                          TRow{628, 0},
                                                          TRow{101, 527},
                                                          TRow{-NYql::NDecimal::Inf(), +NYql::NDecimal::Inf()},
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                          TRow{+NYql::NDecimal::Inf(), -NYql::NDecimal::Inf()},
                                                          TRow{101, -527},
                                                          TRow{-426, 0},
                                                          TRow{-NYql::NDecimal::Inf(), +NYql::NDecimal::Inf()},
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                          TRow{NYql::NDecimal::Nan(), -NYql::NDecimal::Inf()},
                                                          TRow{-NYql::NDecimal::Inf(), -NYql::NDecimal::Inf()},
                                                          TRow{-NYql::NDecimal::Inf(), -NYql::NDecimal::Inf()},
                                                          TRow{-NYql::NDecimal::Inf(), NYql::NDecimal::Nan()},
                                                      });
}

Y_UNIT_TEST_LLVM(TestCompares) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<std::tuple<NTest::TDecimalLiteral<10, 0>, NTest::TDecimalLiteral<7, 2>>>{
                                                           {{-7}, {-700}},
                                                           {{-7}, {300}},
                                                           {{-7}, {NYql::NDecimal::Nan()}},
                                                           {{-7}, {-NYql::NDecimal::Inf()}},
                                                           {{-7}, {+NYql::NDecimal::Inf()}},

                                                           {{3}, {-700}},
                                                           {{3}, {300}},
                                                           {{3}, {NYql::NDecimal::Nan()}},
                                                           {{3}, {-NYql::NDecimal::Inf()}},
                                                           {{3}, {+NYql::NDecimal::Inf()}},

                                                           {{NYql::NDecimal::Nan()}, {-700}},
                                                           {{NYql::NDecimal::Nan()}, {300}},
                                                           {{NYql::NDecimal::Nan()}, {NYql::NDecimal::Nan()}},
                                                           {{NYql::NDecimal::Nan()}, {-NYql::NDecimal::Inf()}},
                                                           {{NYql::NDecimal::Nan()}, {+NYql::NDecimal::Inf()}},

                                                           {{-NYql::NDecimal::Inf()}, {-700}},
                                                           {{-NYql::NDecimal::Inf()}, {300}},
                                                           {{-NYql::NDecimal::Inf()}, {NYql::NDecimal::Nan()}},
                                                           {{-NYql::NDecimal::Inf()}, {-NYql::NDecimal::Inf()}},
                                                           {{-NYql::NDecimal::Inf()}, {+NYql::NDecimal::Inf()}},

                                                           {{+NYql::NDecimal::Inf()}, {-700}},
                                                           {{+NYql::NDecimal::Inf()}, {300}},
                                                           {{+NYql::NDecimal::Inf()}, {NYql::NDecimal::Nan()}},
                                                           {{+NYql::NDecimal::Inf()}, {-NYql::NDecimal::Inf()}},
                                                           {{+NYql::NDecimal::Inf()}, {+NYql::NDecimal::Inf()}},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.NewTuple({pb.Equals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.NotEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.Less(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.LessOrEqual(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.Greater(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.GreaterOrEqual(pb.Nth(item, 0), pb.Nth(item, 1))});
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    using TRow = std::tuple<bool, bool, bool, bool, bool, bool>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TRow>{
                                                          TRow{true, false, false, true, false, true},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, false, false, false, false},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{true, false, false, true, false, true},
                                                          TRow{false, true, false, false, false, false},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, false, false, false, false},
                                                          TRow{false, true, false, false, false, false},
                                                          TRow{false, true, false, false, false, false},
                                                          TRow{false, true, false, false, false, false},
                                                          TRow{false, true, false, false, false, false},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, false, false, false, false},
                                                          TRow{true, false, false, true, false, true},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{false, true, false, false, false, false},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{true, false, false, true, false, true},
                                                      });
}

Y_UNIT_TEST_LLVM(TestComparesWithIntegral) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<std::tuple<TMaybe<i64>, NTest::TDecimalLiteral<20, 18>>>{
                                                           {{-7LL}, {-7000000000000000000LL}},
                                                           {{-7LL}, {3000000000000000000LL}},
                                                           {{-7LL}, {NYql::NDecimal::Nan()}},
                                                           {{-7LL}, {-NYql::NDecimal::Inf()}},
                                                           {{-7LL}, {+NYql::NDecimal::Inf()}},

                                                           {{3LL}, {-7000000000000000000LL}},
                                                           {{3LL}, {3000000000000000000LL}},
                                                           {{3LL}, {NYql::NDecimal::Nan()}},
                                                           {{3LL}, {-NYql::NDecimal::Inf()}},
                                                           {{3LL}, {+NYql::NDecimal::Inf()}},

                                                           {TMaybe<i64>{}, {-7000000000000000000LL}},
                                                           {TMaybe<i64>{}, {3000000000000000000LL}},
                                                           {TMaybe<i64>{}, {NYql::NDecimal::Nan()}},
                                                           {TMaybe<i64>{}, {-NYql::NDecimal::Inf()}},
                                                           {TMaybe<i64>{}, {+NYql::NDecimal::Inf()}},

                                                           {{Min<i64>()}, {-7000000000000000000LL}},
                                                           {{Min<i64>()}, {3000000000000000000LL}},
                                                           {{Min<i64>()}, {NYql::NDecimal::Nan()}},
                                                           {{Min<i64>()}, {-NYql::NDecimal::Inf()}},
                                                           {{Min<i64>()}, {+NYql::NDecimal::Inf()}},

                                                           {{Max<i64>()}, {-7000000000000000000LL}},
                                                           {{Max<i64>()}, {3000000000000000000LL}},
                                                           {{Max<i64>()}, {NYql::NDecimal::Nan()}},
                                                           {{Max<i64>()}, {-NYql::NDecimal::Inf()}},
                                                           {{Max<i64>()}, {+NYql::NDecimal::Inf()}},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.NewTuple({pb.Equals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.NotEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.Less(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.LessOrEqual(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.Greater(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.GreaterOrEqual(pb.Nth(item, 0), pb.Nth(item, 1))});
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    using TRow = std::tuple<TMaybe<bool>, TMaybe<bool>, TMaybe<bool>, TMaybe<bool>, TMaybe<bool>, TMaybe<bool>>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TRow>{
                                                          {true, false, false, true, false, true},
                                                          {false, true, true, true, false, false},
                                                          {false, true, false, false, false, false},
                                                          {false, true, false, false, true, true},
                                                          {false, true, true, true, false, false},
                                                          {false, true, false, false, true, true},
                                                          {true, false, false, true, false, true},
                                                          {false, true, false, false, false, false},
                                                          {false, true, false, false, true, true},
                                                          {false, true, true, true, false, false},
                                                          {{}, {}, {}, {}, {}, {}},
                                                          {{}, {}, {}, {}, {}, {}},
                                                          {{}, {}, {}, {}, {}, {}},
                                                          {{}, {}, {}, {}, {}, {}},
                                                          {{}, {}, {}, {}, {}, {}},
                                                          {false, true, true, true, false, false},
                                                          {false, true, true, true, false, false},
                                                          {false, true, false, false, false, false},
                                                          {true, false, false, true, false, true},
                                                          {false, true, true, true, false, false},
                                                          {false, true, false, false, true, true},
                                                          {false, true, false, false, true, true},
                                                          {false, true, false, false, false, false},
                                                          {false, true, false, false, true, true},
                                                          {true, false, false, true, false, true},
                                                      });
}

Y_UNIT_TEST_LLVM(TestAggrCompares) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<std::tuple<NTest::TDecimalLiteral<10, 0>, NTest::TDecimalLiteral<10, 0>>>{
                                                           {{-7}, {-7}},
                                                           {{-7}, {3}},
                                                           {{-7}, {NYql::NDecimal::Nan()}},
                                                           {{-7}, {-NYql::NDecimal::Inf()}},
                                                           {{-7}, {+NYql::NDecimal::Inf()}},

                                                           {{3}, {-7}},
                                                           {{3}, {3}},
                                                           {{3}, {NYql::NDecimal::Nan()}},
                                                           {{3}, {-NYql::NDecimal::Inf()}},
                                                           {{3}, {+NYql::NDecimal::Inf()}},

                                                           {{NYql::NDecimal::Nan()}, {-7}},
                                                           {{NYql::NDecimal::Nan()}, {3}},
                                                           {{NYql::NDecimal::Nan()}, {NYql::NDecimal::Nan()}},
                                                           {{NYql::NDecimal::Nan()}, {-NYql::NDecimal::Inf()}},
                                                           {{NYql::NDecimal::Nan()}, {+NYql::NDecimal::Inf()}},

                                                           {{-NYql::NDecimal::Inf()}, {-7}},
                                                           {{-NYql::NDecimal::Inf()}, {3}},
                                                           {{-NYql::NDecimal::Inf()}, {NYql::NDecimal::Nan()}},
                                                           {{-NYql::NDecimal::Inf()}, {-NYql::NDecimal::Inf()}},
                                                           {{-NYql::NDecimal::Inf()}, {+NYql::NDecimal::Inf()}},

                                                           {{+NYql::NDecimal::Inf()}, {-7}},
                                                           {{+NYql::NDecimal::Inf()}, {3}},
                                                           {{+NYql::NDecimal::Inf()}, {NYql::NDecimal::Nan()}},
                                                           {{+NYql::NDecimal::Inf()}, {-NYql::NDecimal::Inf()}},
                                                           {{+NYql::NDecimal::Inf()}, {+NYql::NDecimal::Inf()}},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.NewTuple({pb.AggrEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.AggrNotEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.AggrLess(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.AggrLessOrEqual(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.AggrGreater(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.AggrGreaterOrEqual(pb.Nth(item, 0), pb.Nth(item, 1))});
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    using TRow = std::tuple<bool, bool, bool, bool, bool, bool>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TRow>{
                                                          TRow{true, false, false, true, false, true},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{true, false, false, true, false, true},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{true, false, false, true, false, true},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{true, false, false, true, false, true},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{true, false, false, true, false, true},
                                                      });
}

Y_UNIT_TEST_LLVM(TestIncDec) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<4, 1>>{
                                                           {-NYql::NDecimal::Inf()},
                                                           {-9999},
                                                           {-7},
                                                           {0},
                                                           {13},
                                                           {9999},
                                                           {+NYql::NDecimal::Inf()},
                                                           {NYql::NDecimal::Nan()},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.NewTuple({pb.Increment(item), pb.Decrement(item)});
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    using TRow = std::tuple<NYql::NDecimal::TInt128, NYql::NDecimal::TInt128>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TRow>{
                                                          TRow{-NYql::NDecimal::Inf(), -NYql::NDecimal::Inf()},
                                                          TRow{NYql::NDecimal::TInt128(-9998), -NYql::NDecimal::Inf()},
                                                          TRow{NYql::NDecimal::TInt128(-6), NYql::NDecimal::TInt128(-8)},
                                                          TRow{NYql::NDecimal::TInt128(1), NYql::NDecimal::TInt128(-1)},
                                                          TRow{NYql::NDecimal::TInt128(14), NYql::NDecimal::TInt128(12)},
                                                          TRow{+NYql::NDecimal::Inf(), NYql::NDecimal::TInt128(9998)},
                                                          TRow{+NYql::NDecimal::Inf(), +NYql::NDecimal::Inf()},
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                      });
}
Y_UNIT_TEST_LLVM(TestMinusAbs) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<10, 1>>{
                                                           {-NYql::NDecimal::Inf()},
                                                           {-7},
                                                           {0},
                                                           {13},
                                                           {+NYql::NDecimal::Inf()},
                                                           {NYql::NDecimal::Nan()},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.NewTuple({pb.Minus(item), pb.Abs(item)});
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    using TRow = std::tuple<NYql::NDecimal::TInt128, NYql::NDecimal::TInt128>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TRow>{
                                                          TRow{+NYql::NDecimal::Inf(), +NYql::NDecimal::Inf()},
                                                          TRow{NYql::NDecimal::TInt128(7), NYql::NDecimal::TInt128(7)},
                                                          TRow{NYql::NDecimal::TInt128(0), NYql::NDecimal::TInt128(0)},
                                                          TRow{NYql::NDecimal::TInt128(-13), NYql::NDecimal::TInt128(13)},
                                                          TRow{-NYql::NDecimal::Inf(), +NYql::NDecimal::Inf()},
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                      });
}
Y_UNIT_TEST_LLVM(TestFromString) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<TStringBuf>{
                                                           "0.0", "NAN", "1.0", "-.1", "3.1415926", "+inf", "-INF", ".123E+2", "56.78e-3",
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.StrictFromString(item, pb.NewDecimalType(10, 7));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<NYql::NDecimal::TInt128>{
                                                          NYql::NDecimal::TInt128(0),
                                                          NYql::NDecimal::Nan(),
                                                          NYql::NDecimal::TInt128(10000000),
                                                          NYql::NDecimal::TInt128(-1000000),
                                                          NYql::NDecimal::TInt128(31415926),
                                                          +NYql::NDecimal::Inf(),
                                                          -NYql::NDecimal::Inf(),
                                                          NYql::NDecimal::TInt128(123000000),
                                                          NYql::NDecimal::TInt128(567800),
                                                      });
}
Y_UNIT_TEST_LLVM(TestToString) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<10, 7>>{
                                                           {0},
                                                           {NYql::NDecimal::Nan()},
                                                           {10000000},
                                                           {-1000000},
                                                           {31415926},
                                                           {+NYql::NDecimal::Inf()},
                                                           {-NYql::NDecimal::Inf()},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.ToString(item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TStringBuf>{
                                                          "0", "nan", "1", "-0.1", "3.1415926", "inf", "-inf",
                                                      });
}
Y_UNIT_TEST_LLVM(TestFromStringToDouble) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<TStringBuf>{
                                                           "0", "+3.332873", "-3.332873", "+3.1415926", "-3.1415926",
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Convert(pb.FromString(item, pb.NewDecimalType(35, 25)), pb.NewDataType(NUdf::TDataType<double>::Id));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<double>{
                                                          0., +3.332873, -3.332873, +3.1415926, -3.1415926,
                                                      });
}
Y_UNIT_TEST_LLVM(TestFromUtf8ToFloat) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TUtf8>{
                                                           {TStringBuf("0")},
                                                           {TStringBuf("+24.75")},
                                                           {TStringBuf("-24.75")},
                                                           {TStringBuf("+42.42")},
                                                           {TStringBuf("-42.42")},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Convert(pb.FromString(item, pb.NewDecimalType(35, 25)), pb.NewDataType(NUdf::TDataType<float>::Id));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<float>{
                                                          0.f, +24.75f, -24.75f, +42.42f, -42.42f,
                                                      });
}
} // Y_UNIT_TEST_SUITE(TMiniKQLDecimalTest)

} // namespace NMiniKQL
} // namespace NKikimr
