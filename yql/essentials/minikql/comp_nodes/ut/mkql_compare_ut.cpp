#include "mkql_computation_node_ut.h"
#include "mkql_program_builder_test_utils.h"

#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>
#include <yql/essentials/minikql/mkql_node_printer.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_list_adapter.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

#include <cfloat>
#include <utility>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLCompareTest) {
Y_UNIT_TEST_LLVM(SqlString) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TOptStr = TMaybe<TStringBuf>;
    using TRow = std::tuple<TOptStr, TOptStr>;
    using TResult = std::tuple<TMaybe<bool>, TMaybe<bool>, TMaybe<bool>, TMaybe<bool>, TMaybe<bool>, TMaybe<bool>>;

    const TOptStr null{};
    const TOptStr s010{TStringBuf("010")};
    const TOptStr s020{TStringBuf("020")};

    auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TRow>{
                                                         {null, null},
                                                         {null, s010},
                                                         {null, s020},
                                                         {s010, null},
                                                         {s010, s010},
                                                         {s010, s020},
                                                         {s020, null},
                                                         {s020, s010},
                                                         {s020, s020},
                                                     });

    auto pgmReturn = pb.Map(list,
                            [&](TRuntimeNode item) {
                                return pb.NewTuple({pb.Equals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.NotEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.Less(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.LessOrEqual(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.Greater(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.GreaterOrEqual(pb.Nth(item, 0), pb.Nth(item, 1))});
                            });

    auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TResult>{
                                                          {{}, {}, {}, {}, {}, {}},                // null, null
                                                          {{}, {}, {}, {}, {}, {}},                // null, "010"
                                                          {{}, {}, {}, {}, {}, {}},                // null, "020"
                                                          {{}, {}, {}, {}, {}, {}},                // "010", null
                                                          {true, false, false, true, false, true}, // "010", "010"
                                                          {false, true, true, true, false, false}, // "010", "020"
                                                          {{}, {}, {}, {}, {}, {}},                // "020", null
                                                          {false, true, false, false, true, true}, // "020", "010"
                                                          {true, false, false, true, false, true}, // "020", "020"
                                                      });
}

Y_UNIT_TEST_LLVM(AggrString) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TOptStr = TMaybe<TStringBuf>;
    using TRow = std::tuple<TOptStr, TOptStr>;
    using TResult = std::tuple<bool, bool, bool, bool, bool, bool>;

    const TOptStr null{};
    const TOptStr s010{TStringBuf("010")};
    const TOptStr s020{TStringBuf("020")};

    auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TRow>{
                                                         {null, null},
                                                         {null, s010},
                                                         {null, s020},
                                                         {s010, null},
                                                         {s010, s010},
                                                         {s010, s020},
                                                         {s020, null},
                                                         {s020, s010},
                                                         {s020, s020},
                                                     });

    auto pgmReturn = pb.Map(list,
                            [&](TRuntimeNode item) {
                                return pb.NewTuple({pb.AggrEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.AggrNotEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.AggrLess(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.AggrLessOrEqual(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.AggrGreater(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.AggrGreaterOrEqual(pb.Nth(item, 0), pb.Nth(item, 1))});
                            });

    auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TResult>{
                                                          {true, false, false, true, false, true}, // null, null  (aggr treats null as min)
                                                          {false, true, true, true, false, false}, // null, "010"
                                                          {false, true, true, true, false, false}, // null, "020"
                                                          {false, true, false, false, true, true}, // "010", null
                                                          {true, false, false, true, false, true}, // "010", "010"
                                                          {false, true, true, true, false, false}, // "010", "020"
                                                          {false, true, false, false, true, true}, // "020", null
                                                          {false, true, false, false, true, true}, // "020", "010"
                                                          {true, false, false, true, false, true}, // "020", "020"
                                                      });
}

Y_UNIT_TEST_LLVM(SqlFloats) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TRow = std::tuple<float, double>;
    using TResult = std::tuple<bool, bool, bool, bool, bool, bool>;

    // NaN comparisons: SQL treats NaN != NaN, and all order comparisons with NaN are false
    const float nanF = 0.0f * HUGE_VALF;
    const double nanD = 0.0 * HUGE_VAL;

    auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TRow>{
                                                         {-7.0f, -7.0},
                                                         {-7.0f, 3.0},
                                                         {-7.0f, nanD},
                                                         {3.0f, -7.0},
                                                         {3.0f, 3.0},
                                                         {3.0f, nanD},
                                                         {nanF, -7.0},
                                                         {nanF, 3.0},
                                                         {nanF, nanD},
                                                     });

    auto pgmReturn = pb.Map(list,
                            [&](TRuntimeNode item) {
                                return pb.NewTuple({pb.Equals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.NotEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.Less(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.LessOrEqual(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.Greater(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.GreaterOrEqual(pb.Nth(item, 0), pb.Nth(item, 1))});
                            });

    auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TResult>{
                                                          {true, false, false, true, false, true},   // -7f == -7d
                                                          {false, true, true, true, false, false},   // -7f < 3d
                                                          {false, true, false, false, false, false}, // -7f vs NaN
                                                          {false, true, false, false, true, true},   // 3f > -7d
                                                          {true, false, false, true, false, true},   // 3f == 3d
                                                          {false, true, false, false, false, false}, // 3f vs NaN
                                                          {false, true, false, false, false, false}, // NaN vs -7d
                                                          {false, true, false, false, false, false}, // NaN vs 3d
                                                          {false, true, false, false, false, false}, // NaN vs NaN
                                                      });
}

Y_UNIT_TEST_LLVM(AggrFloats) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TRow = std::tuple<float, float>;
    using TResult = std::tuple<bool, bool, bool, bool, bool, bool>;

    const float nanF = 0.0f * HUGE_VALF;

    auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TRow>{
                                                         {-7.0f, -7.0f},
                                                         {-7.0f, 3.0f},
                                                         {-7.0f, nanF},
                                                         {3.0f, -7.0f},
                                                         {3.0f, 3.0f},
                                                         {3.0f, nanF},
                                                         {nanF, -7.0f},
                                                         {nanF, 3.0f},
                                                         {nanF, nanF},
                                                     });

    auto pgmReturn = pb.Map(list,
                            [&](TRuntimeNode item) {
                                return pb.NewTuple({pb.AggrEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.AggrNotEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.AggrLess(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.AggrLessOrEqual(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.AggrGreater(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.AggrGreaterOrEqual(pb.Nth(item, 0), pb.Nth(item, 1))});
                            });

    auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TResult>{
                                                          {true, false, false, true, false, true}, // -7 == -7
                                                          {false, true, true, true, false, false}, // -7 < 3
                                                          {false, true, true, true, false, false}, // -7 < NaN (NaN is max in aggr)
                                                          {false, true, false, false, true, true}, // 3 > -7
                                                          {true, false, false, true, false, true}, // 3 == 3
                                                          {false, true, true, true, false, false}, // 3 < NaN
                                                          {false, true, false, false, true, true}, // NaN > -7
                                                          {false, true, false, false, true, true}, // NaN > 3
                                                          {true, false, false, true, false, true}, // NaN == NaN
                                                      });
}

Y_UNIT_TEST_LLVM(SqlSigned) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TRow = std::tuple<i16, i32>;
    using TResult = std::tuple<bool, bool, bool, bool, bool, bool>;

    auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TRow>{
                                                         {i16(-1), i32(-1)},
                                                         {i16(-1), i32(+1)},
                                                         {i16(+1), i32(-1)},
                                                         {i16(+1), i32(+1)},
                                                     });

    auto pgmReturn = pb.Map(list,
                            [&](TRuntimeNode item) {
                                return pb.NewTuple({pb.Equals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.NotEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.Less(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.LessOrEqual(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.Greater(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.GreaterOrEqual(pb.Nth(item, 0), pb.Nth(item, 1))});
                            });

    auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TResult>{
                                                          {true, false, false, true, false, true}, // -1 == -1
                                                          {false, true, true, true, false, false}, // -1 < +1
                                                          {false, true, false, false, true, true}, // +1 > -1
                                                          {true, false, false, true, false, true}, // +1 == +1
                                                      });
}

Y_UNIT_TEST_LLVM(SqlSignedAndUnsigned) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TRow = std::tuple<i8, ui8>;
    using TResult = std::tuple<bool, bool, bool, bool, bool, bool>;

    auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TRow>{
                                                         {i8(-1), ui8(0xFF)},
                                                         {i8(-1), ui8(127)},
                                                         {i8(127), ui8(0xFF)},
                                                         {i8(127), ui8(127)},
                                                     });

    auto pgmReturn = pb.Map(list,
                            [&](TRuntimeNode item) {
                                return pb.NewTuple({pb.Equals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.NotEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.Less(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.LessOrEqual(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.Greater(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.GreaterOrEqual(pb.Nth(item, 0), pb.Nth(item, 1))});
                            });

    auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TResult>{
                                                          {false, true, true, true, false, false}, // -1 < 0xFF (255)
                                                          {false, true, true, true, false, false}, // -1 < 127
                                                          {false, true, true, true, false, false}, // 127 < 0xFF (255)
                                                          {true, false, false, true, false, true}, // 127 == 127
                                                      });
}

Y_UNIT_TEST_LLVM(SqlUnsignedAndSigned) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TRow = std::tuple<ui16, i64>;
    using TResult = std::tuple<bool, bool, bool, bool, bool, bool>;

    auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TRow>{
                                                         {ui16(0), i64(-1)},
                                                         {ui16(0), i64(0xFFFF)},
                                                         {ui16(0xFFFF), i64(-1)},
                                                         {ui16(0xFFFF), i64(0xFFFF)},
                                                     });

    auto pgmReturn = pb.Map(list,
                            [&](TRuntimeNode item) {
                                return pb.NewTuple({pb.Equals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.NotEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.Less(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.LessOrEqual(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.Greater(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                    pb.GreaterOrEqual(pb.Nth(item, 0), pb.Nth(item, 1))});
                            });

    auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TResult>{
                                                          {false, true, false, false, true, true}, // 0 > -1 (0 as ui16 is > -1 as i64)
                                                          {false, true, true, true, false, false}, // 0 < 65535
                                                          {false, true, false, false, true, true}, // 65535 > -1
                                                          {true, false, false, true, false, true}, // 65535 == 65535
                                                      });
}

Y_UNIT_TEST_LLVM(SimpleSqlString) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto opt1 = NTest::ConvertValueToLiteralNode(pb, TMaybe<ui32>{1});
    auto opt2 = NTest::ConvertValueToLiteralNode(pb, TMaybe<ui32>{2});
    auto optEmpty = NTest::ConvertValueToLiteralNode(pb, TMaybe<ui32>{});
    auto pgmReturn = pb.NewEmptyList(NTest::ConvertToMinikqlType<TMaybe<bool>>(pb));

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

    auto opt1s = NTest::ConvertValueToLiteralNode(pb, TMaybe<TStringBuf>{"A"});
    auto opt2s = NTest::ConvertValueToLiteralNode(pb, TMaybe<TStringBuf>{"B"});
    auto optEmptys = NTest::ConvertValueToLiteralNode(pb, TMaybe<TStringBuf>{});

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

    using MB = TMaybe<bool>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<MB>{
                                                          // equals ui32
                                                          true, false,
                                                          {},
                                                          {},
                                                          {},
                                                          // not equals ui32
                                                          false, true,
                                                          {},
                                                          {},
                                                          {},
                                                          // less ui32
                                                          false, true, false,
                                                          {},
                                                          {},
                                                          {},
                                                          // less or equal ui32
                                                          true, true, false,
                                                          {},
                                                          {},
                                                          {},
                                                          // greater ui32
                                                          false, false, true,
                                                          {},
                                                          {},
                                                          {},
                                                          // greater or equal ui32
                                                          true, false, true,
                                                          {},
                                                          {},
                                                          {},
                                                          // equals string
                                                          true, false,
                                                          {},
                                                          {},
                                                          {},
                                                          // not equals string
                                                          false, true,
                                                          {},
                                                          {},
                                                          {},
                                                      });
}

Y_UNIT_TEST_LLVM(TzMin) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto zones = pb.ListFromRange(NTest::ConvertValueToLiteralNode(pb, ui16(1ULL)), NTest::ConvertValueToLiteralNode(pb, ui16(7ULL)), NTest::ConvertValueToLiteralNode(pb, ui16(1ULL)));
    const auto dates = pb.ListFromRange(NTest::ConvertValueToLiteralNode(pb, ui32(1ULL)), NTest::ConvertValueToLiteralNode(pb, ui32(7ULL)), NTest::ConvertValueToLiteralNode(pb, ui32(1ULL)));

    const auto source = pb.Map(pb.Zip({pb.Reverse(dates), zones}),
                               [&](TRuntimeNode item) {
                                   return pb.AddTimezone(pb.ToIntegral(pb.Nth(item, 0U), pb.NewDataType(NUdf::EDataSlot::Datetime, true)), pb.Nth(item, 1U));
                               });

    const auto pgmReturn = pb.ToString(pb.Unwrap(pb.Fold1(source,
                                                          [&](TRuntimeNode item) { return item; },
                                                          [&](TRuntimeNode item, TRuntimeNode state) { return pb.Min(item, state); }), NTest::ConvertValueToLiteralNode(pb, TStringBuf("")), __FILE__, __LINE__, 0));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto result = graph->GetValue();
    AssertUnboxedValueElementEqual(result, TStringBuf("1970-01-01T03:00:01,Africa/Asmara"));
}

Y_UNIT_TEST_LLVM(TzMax) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto zones = pb.ListFromRange(NTest::ConvertValueToLiteralNode(pb, ui16(1ULL)), NTest::ConvertValueToLiteralNode(pb, ui16(7ULL)), NTest::ConvertValueToLiteralNode(pb, ui16(1ULL)));
    const auto dates = pb.ListFromRange(NTest::ConvertValueToLiteralNode(pb, ui32(1ULL)), NTest::ConvertValueToLiteralNode(pb, ui32(7ULL)), NTest::ConvertValueToLiteralNode(pb, ui32(1ULL)));

    const auto source = pb.Map(pb.Zip({dates, pb.Reverse(zones)}),
                               [&](TRuntimeNode item) {
                                   return pb.AddTimezone(pb.ToIntegral(pb.Nth(item, 0U), pb.NewDataType(NUdf::EDataSlot::Datetime, true)), pb.Nth(item, 1U));
                               });

    const auto pgmReturn = pb.ToString(pb.Unwrap(pb.Fold1(source,
                                                          [&](TRuntimeNode item) { return item; },
                                                          [&](TRuntimeNode item, TRuntimeNode state) { return pb.Max(item, state); }), NTest::ConvertValueToLiteralNode(pb, TStringBuf("")), __FILE__, __LINE__, 0));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto result = graph->GetValue();
    AssertUnboxedValueElementEqual(result, TStringBuf("1970-01-01T03:00:06,Europe/Moscow"));
}

Y_UNIT_TEST_LLVM(TzAggrMin) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto zones = pb.ListFromRange(NTest::ConvertValueToLiteralNode(pb, ui16(1ULL)), NTest::ConvertValueToLiteralNode(pb, ui16(7ULL)), NTest::ConvertValueToLiteralNode(pb, ui16(1ULL)));
    const auto dates = pb.ListFromRange(NTest::ConvertValueToLiteralNode(pb, ui32(1ULL)), NTest::ConvertValueToLiteralNode(pb, ui32(7ULL)), NTest::ConvertValueToLiteralNode(pb, ui32(1ULL)));

    const auto source = pb.FlatMap(zones,
                                   [&](TRuntimeNode zone) {
                                       return pb.Map(dates,
                                                     [&](TRuntimeNode date) {
                                                         return pb.AddTimezone(pb.ToIntegral(date, pb.NewDataType(NUdf::EDataSlot::Datetime, true)), zone);
                                                     });
                                   });

    const auto pgmReturn = pb.ToString(pb.Unwrap(pb.Fold1(source,
                                                          [&](TRuntimeNode item) { return item; },
                                                          [&](TRuntimeNode item, TRuntimeNode state) { return pb.AggrMin(item, state); }), NTest::ConvertValueToLiteralNode(pb, TStringBuf("")), __FILE__, __LINE__, 0));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto result = graph->GetValue();
    AssertUnboxedValueElementEqual(result, TStringBuf("1970-01-01T03:00:01,Europe/Moscow"));
}

Y_UNIT_TEST_LLVM(TzAggrMax) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto zones = pb.ListFromRange(NTest::ConvertValueToLiteralNode(pb, ui16(1ULL)), NTest::ConvertValueToLiteralNode(pb, ui16(7ULL)), NTest::ConvertValueToLiteralNode(pb, ui16(1ULL)));
    const auto dates = pb.ListFromRange(NTest::ConvertValueToLiteralNode(pb, ui32(1ULL)), NTest::ConvertValueToLiteralNode(pb, ui32(7ULL)), NTest::ConvertValueToLiteralNode(pb, ui32(1ULL)));

    const auto source = pb.FlatMap(dates,
                                   [&](TRuntimeNode date) {
                                       return pb.Map(zones,
                                                     [&](TRuntimeNode zone) {
                                                         return pb.AddTimezone(pb.ToIntegral(date, pb.NewDataType(NUdf::EDataSlot::Datetime, true)), zone);
                                                     });
                                   });

    const auto pgmReturn = pb.ToString(pb.Unwrap(pb.Fold1(source,
                                                          [&](TRuntimeNode item) { return item; },
                                                          [&](TRuntimeNode item, TRuntimeNode state) { return pb.AggrMax(item, state); }), NTest::ConvertValueToLiteralNode(pb, TStringBuf("")), __FILE__, __LINE__, 0));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto result = graph->GetValue();
    AssertUnboxedValueElementEqual(result, TStringBuf("1970-01-01T03:00:06,Africa/Asmara"));
}

Y_UNIT_TEST_LLVM(TestAggrMinMaxFloats) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<float>{0.0f * HUGE_VALF, HUGE_VALF, 3.14f, -2.13f, -HUGE_VALF});
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
} // Y_UNIT_TEST_SUITE(TMiniKQLCompareTest)

} // namespace NMiniKQL
} // namespace NKikimr
