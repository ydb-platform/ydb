#include "mkql_computation_node_ut.h"
#include "mkql_program_builder_test_utils.h"

#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {
template <bool UseLLVM, typename T>
TRuntimeNode MakeList(TSetup<UseLLVM>& setup, T Start, T End, i64 Step, const auto dateType) {
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto start = pb.Unwrap(pb.ToIntegral(NTest::ConvertValueToLiteralNode(pb, T(Start)), dateType), NTest::ConvertValueToLiteralNode(pb, TStringBuf("")), "", 0, 0);

    const auto end = pb.Unwrap(pb.ToIntegral(NTest::ConvertValueToLiteralNode(pb, T(End)), dateType), NTest::ConvertValueToLiteralNode(pb, TStringBuf("")), "", 0, 0);

    const auto step = pb.NewDataLiteral<NUdf::EDataSlot::Interval>(
        NUdf::TStringRef((const char*)&Step, sizeof(Step)));

    return pb.Collect(pb.ToFlow(pb.ListFromRange(start, end, step)));
}
} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLListFromRangeTest) {
Y_UNIT_TEST_LLVM(TestCorrectDate) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    ui16 start = 140;
    ui16 end = 150;
    i64 step = 86400000000LL;
    const auto dateType = pb.NewDataType(NUdf::EDataSlot::Date, true);

    const auto dates = MakeList(setup, start, end, step, dateType);
    const auto graph = setup.BuildGraph(dates);
    const auto list = graph->GetValue();
    AssertUnboxedValueElementEqual(list, TVector<ui16>{140, 141, 142, 143, 144, 145, 146, 147, 148, 149});
}
Y_UNIT_TEST_LLVM(TestCorrectDateReverse) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    ui16 start = 150;
    ui16 end = 140;
    i64 step = -86400000000LL;
    const auto dateType = pb.NewDataType(NUdf::EDataSlot::Date, true);

    const auto dates = MakeList(setup, start, end, step, dateType);
    const auto graph = setup.BuildGraph(dates);
    const auto list = graph->GetValue();
    AssertUnboxedValueElementEqual(list, TVector<ui16>{150, 149, 148, 147, 146, 145, 144, 143, 142, 141});
}
Y_UNIT_TEST_LLVM(TestCorrectDatetime) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    ui32 start = 140;
    ui32 end = 150;
    i64 step = 1000000LL;
    const auto dateType = pb.NewDataType(NUdf::EDataSlot::Datetime, true);

    const auto dates = MakeList(setup, start, end, step, dateType);
    const auto graph = setup.BuildGraph(dates);
    const auto list = graph->GetValue();
    AssertUnboxedValueElementEqual(list, TVector<ui32>{140, 141, 142, 143, 144, 145, 146, 147, 148, 149});
}
Y_UNIT_TEST_LLVM(TestCorrectTimestamp) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    ui64 start = 140;
    ui64 end = 150;
    i64 step = 1LL;
    const auto dateType = pb.NewDataType(NUdf::EDataSlot::Timestamp, true);

    const auto dates = MakeList(setup, start, end, step, dateType);
    const auto graph = setup.BuildGraph(dates);
    const auto list = graph->GetValue();
    AssertUnboxedValueElementEqual(list, TVector<ui64>{140, 141, 142, 143, 144, 145, 146, 147, 148, 149});
}
Y_UNIT_TEST_LLVM(TestWrongIntervalForDate) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    ui16 start = 140;
    ui16 end = 150;
    i64 step = 86400000001LL;
    const auto dateType = pb.NewDataType(NUdf::EDataSlot::Date, true);

    const auto dates = MakeList(setup, start, end, step, dateType);
    const auto graph = setup.BuildGraph(dates);
    const auto list = graph->GetValue();
    AssertUnboxedValueElementEqual(list, TVector<ui16>{});
}
Y_UNIT_TEST_LLVM(TestWrongIntervalForDatetime) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    ui32 start = 140;
    ui32 end = 150;
    i64 step = 1000003LL;
    const auto dateType = pb.NewDataType(NUdf::EDataSlot::Datetime, true);

    const auto dates = MakeList(setup, start, end, step, dateType);
    const auto graph = setup.BuildGraph(dates);
    const auto list = graph->GetValue();
    AssertUnboxedValueElementEqual(list, TVector<ui32>{});
}
Y_UNIT_TEST_LLVM(TestWrongStartType) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto value0 = ui32(1000000);
    const auto start = NTest::ConvertValueToLiteralNode(pb, ui32(value0));

    const auto value1 = ui32(1000005);
    const auto end = pb.NewDataLiteral<NUdf::EDataSlot::Datetime>(
        NUdf::TStringRef((const char*)&value1, sizeof(value1)));

    const auto value2 = i64(1000001LL);
    const auto step = pb.NewDataLiteral<NUdf::EDataSlot::Interval>(
        NUdf::TStringRef((const char*)&value2, sizeof(value2)));

    UNIT_ASSERT_EXCEPTION(pb.ListFromRange(start, end, step), yexception);
}
Y_UNIT_TEST_LLVM(TestWrongEndType) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto value0 = ui32(1000000);
    const auto start = pb.NewDataLiteral<NUdf::EDataSlot::Datetime>(
        NUdf::TStringRef((const char*)&value0, sizeof(value0)));

    const auto value1 = ui32(1000005);
    const auto end = NTest::ConvertValueToLiteralNode(pb, ui32(value1));

    const auto value2 = i64(1000001LL);
    const auto step = pb.NewDataLiteral<NUdf::EDataSlot::Interval>(
        NUdf::TStringRef((const char*)&value2, sizeof(value2)));

    UNIT_ASSERT_EXCEPTION(pb.ListFromRange(start, end, step), yexception);
}
Y_UNIT_TEST_LLVM(TestWrongStepType) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto value0 = ui32(1000000);
    const auto start = pb.NewDataLiteral<NUdf::EDataSlot::Datetime>(
        NUdf::TStringRef((const char*)&value0, sizeof(value0)));

    const auto value1 = ui32(1000005);
    const auto end = pb.NewDataLiteral<NUdf::EDataSlot::Datetime>(
        NUdf::TStringRef((const char*)&value1, sizeof(value1)));

    const auto value2 = i64(1000001LL);
    const auto step = NTest::ConvertValueToLiteralNode(pb, ui32(value2));

    UNIT_ASSERT_EXCEPTION(pb.ListFromRange(start, end, step), yexception);
}
Y_UNIT_TEST_LLVM(TestEmptyListDate) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    ui16 start = 150;
    ui16 end = 144;
    i64 step = 86400000000LL;
    const auto dateType = pb.NewDataType(NUdf::EDataSlot::Date, true);

    const auto dates = MakeList(setup, start, end, step, dateType);
    const auto graph = setup.BuildGraph(dates);
    const auto list = graph->GetValue();
    AssertUnboxedValueElementEqual(list, TVector<ui16>{});
}
Y_UNIT_TEST_LLVM(TestWrongStartEndTypes) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto value0 = ui16(140);
    const auto start = pb.NewDataLiteral<NUdf::EDataSlot::Date>(
        NUdf::TStringRef((const char*)&value0, sizeof(value0)));

    const auto value1 = ui32(140 * 60 * 60 * 24 + 5);
    const auto end = pb.NewDataLiteral<NUdf::EDataSlot::Datetime>(
        NUdf::TStringRef((const char*)&value1, sizeof(value1)));

    const auto value2 = i64(2000000LL); // 2 Seconds
    const auto step = pb.NewDataLiteral<NUdf::EDataSlot::Interval>(
        NUdf::TStringRef((const char*)&value2, sizeof(value2)));

    UNIT_ASSERT_EXCEPTION(pb.ListFromRange(start, end, step), yexception);
}
Y_UNIT_TEST_LLVM(TestMinOverflowForDate) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    ui16 start = 4;
    ui16 end = 0;
    i64 step = -518400000000LL; // -6 days
    const auto dateType = pb.NewDataType(NUdf::EDataSlot::Date, true);

    const auto dates = MakeList(setup, start, end, step, dateType);
    const auto graph = setup.BuildGraph(dates);
    const auto list = graph->GetValue();
    AssertUnboxedValueElementEqual(list, TVector<ui16>{4});
}
Y_UNIT_TEST_LLVM(TestMinOverflowForDatetime) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    ui32 start = 9;
    ui32 end = 0;
    i64 step = -10000000LL; // -10 seconds
    const auto dateType = pb.NewDataType(NUdf::EDataSlot::Datetime, true);

    const auto dates = MakeList(setup, start, end, step, dateType);
    const auto graph = setup.BuildGraph(dates);
    const auto list = graph->GetValue();
    AssertUnboxedValueElementEqual(list, TVector<ui32>{9});
}
Y_UNIT_TEST_LLVM(TestMinOverflowForTimestamp) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    ui64 start = 100;
    ui64 end = 10;
    i64 step = -110LL; // -110 microseconds
    const auto dateType = pb.NewDataType(NUdf::EDataSlot::Timestamp, true);

    const auto dates = MakeList(setup, start, end, step, dateType);
    const auto graph = setup.BuildGraph(dates);
    const auto list = graph->GetValue();
    AssertUnboxedValueElementEqual(list, TVector<ui64>{100});
}

Y_UNIT_TEST_LLVM(TestMaxOverflowForDate) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    ui16 start = 100;
    ui16 end = NYql::NUdf::MAX_DATE - 1;
    i64 step = (NYql::NUdf::MAX_DATE - 1) * 24LL * 60 * 60 * 1000000;
    const auto dateType = pb.NewDataType(NUdf::EDataSlot::Date, true);

    const auto dates = MakeList(setup, start, end, step, dateType);
    const auto graph = setup.BuildGraph(dates);
    const auto list = graph->GetValue();
    AssertUnboxedValueElementEqual(list, TVector<ui16>{100});
}
Y_UNIT_TEST_LLVM(TestMaxOverflowForDatetime) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    ui32 start = NYql::NUdf::MAX_DATETIME - 123;
    ui32 end = NYql::NUdf::MAX_DATETIME - 1;
    i64 step = (NYql::NUdf::MAX_DATETIME - 1) * 1000000LL;
    const auto dateType = pb.NewDataType(NUdf::EDataSlot::Datetime, true);

    const auto dates = MakeList(setup, start, end, step, dateType);
    const auto graph = setup.BuildGraph(dates);
    const auto list = graph->GetValue();
    AssertUnboxedValueElementEqual(list, TVector<ui32>{NYql::NUdf::MAX_DATETIME - 123});
}
Y_UNIT_TEST_LLVM(TestMaxOverflowForTimestamp) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    ui64 start = NYql::NUdf::MAX_TIMESTAMP - 123;
    ui64 end = NYql::NUdf::MAX_TIMESTAMP - 1;
    i64 step = NYql::NUdf::MAX_TIMESTAMP - 1;
    const auto dateType = pb.NewDataType(NUdf::EDataSlot::Timestamp, true);

    const auto dates = MakeList(setup, start, end, step, dateType);
    const auto graph = setup.BuildGraph(dates);
    const auto list = graph->GetValue();
    AssertUnboxedValueElementEqual(list, TVector<ui64>{NYql::NUdf::MAX_TIMESTAMP - 123});
}
Y_UNIT_TEST_LLVM(TestDifferentTimezonesForTzDate) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto dateType = pb.NewDataType(NUdf::EDataSlot::Date, true);
    const auto canada = NTest::ConvertValueToLiteralNode(pb, ui16(375U));
    const auto europe = NTest::ConvertValueToLiteralNode(pb, ui16(459U));
    const auto value2 = i64(24LL * 60 * 60 * 1000000); // 1 Day
    const auto step = pb.NewDataLiteral<NUdf::EDataSlot::Interval>(
        NUdf::TStringRef((const char*)&value2, sizeof(value2)));

    const auto day1 = pb.ToIntegral(NTest::ConvertValueToLiteralNode(pb, ui16(123)), dateType);
    const auto day2 = pb.ToIntegral(NTest::ConvertValueToLiteralNode(pb, ui16(123 + 5)), dateType);
    const auto date1 = pb.Unwrap(pb.AddTimezone(day1, canada), NTest::ConvertValueToLiteralNode(pb, TStringBuf("")), "", 0, 0);
    const auto date2 = pb.Unwrap(pb.AddTimezone(day2, europe), NTest::ConvertValueToLiteralNode(pb, TStringBuf("")), "", 0, 0);
    const auto dates = pb.ListFromRange(date1, date2, step);

    const auto graph = setup.BuildGraph(dates);
    const auto list = graph->GetValue();
    const auto iterator = list.GetListIterator();
    UNIT_ASSERT_VALUES_EQUAL(list.GetListLength(), 5);
    NUdf::TUnboxedValue item;
    for (size_t i = 123; i < 123 + 5; i++) {
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), i);
        UNIT_ASSERT_VALUES_EQUAL(item.GetTimezoneId(), 375U);
    }
}
Y_UNIT_TEST_LLVM(TestSameTimezonesForTzDate) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto dateType = pb.NewDataType(NUdf::EDataSlot::Date, true);
    const auto canada = NTest::ConvertValueToLiteralNode(pb, ui16(375U));
    const auto value2 = i64(24LL * 60 * 60 * 1000000); // 1 Day
    const auto step = pb.NewDataLiteral<NUdf::EDataSlot::Interval>(
        NUdf::TStringRef((const char*)&value2, sizeof(value2)));

    const auto day1 = pb.ToIntegral(NTest::ConvertValueToLiteralNode(pb, ui16(123)), dateType);
    const auto day2 = pb.ToIntegral(NTest::ConvertValueToLiteralNode(pb, ui16(123 + 5)), dateType);
    const auto date1 = pb.Unwrap(pb.AddTimezone(day1, canada), NTest::ConvertValueToLiteralNode(pb, TStringBuf("")), "", 0, 0);
    const auto date2 = pb.Unwrap(pb.AddTimezone(day2, canada), NTest::ConvertValueToLiteralNode(pb, TStringBuf("")), "", 0, 0);
    const auto dates = pb.ListFromRange(date1, date2, step);

    const auto graph = setup.BuildGraph(dates);
    const auto list = graph->GetValue();
    const auto iterator = list.GetListIterator();
    UNIT_ASSERT_VALUES_EQUAL(list.GetListLength(), 5);
    NUdf::TUnboxedValue item;
    for (size_t i = 123; i < 123 + 5; i++) {
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui16>(), i);
        UNIT_ASSERT_VALUES_EQUAL(item.GetTimezoneId(), 375U);
    }
}
Y_UNIT_TEST_LLVM(TestDifferentTimezonesForTzDatetime) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto dateType = pb.NewDataType(NUdf::EDataSlot::Datetime, true);
    const auto canada = NTest::ConvertValueToLiteralNode(pb, ui16(375U));
    const auto europe = NTest::ConvertValueToLiteralNode(pb, ui16(459U));
    const auto value2 = i64(1000000LL); // 1 Second
    const auto step = pb.NewDataLiteral<NUdf::EDataSlot::Interval>(
        NUdf::TStringRef((const char*)&value2, sizeof(value2)));

    const auto day1 = pb.ToIntegral(NTest::ConvertValueToLiteralNode(pb, ui32(123)), dateType);
    const auto day2 = pb.ToIntegral(NTest::ConvertValueToLiteralNode(pb, ui32(123 + 5)), dateType);
    const auto date1 = pb.Unwrap(pb.AddTimezone(day1, canada), NTest::ConvertValueToLiteralNode(pb, TStringBuf("")), "", 0, 0);
    const auto date2 = pb.Unwrap(pb.AddTimezone(day2, europe), NTest::ConvertValueToLiteralNode(pb, TStringBuf("")), "", 0, 0);
    const auto dates = pb.ListFromRange(date1, date2, step);

    const auto graph = setup.BuildGraph(dates);
    const auto list = graph->GetValue();
    const auto iterator = list.GetListIterator();
    UNIT_ASSERT_VALUES_EQUAL(list.GetListLength(), 5);
    NUdf::TUnboxedValue item;
    for (size_t i = 123; i < 123 + 5; i++) {
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), i);
        UNIT_ASSERT_VALUES_EQUAL(item.GetTimezoneId(), 375U);
    }
}
Y_UNIT_TEST_LLVM(TestDifferentTimezonesForTzTimestamp) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto dateType = pb.NewDataType(NUdf::EDataSlot::Timestamp, true);
    const auto europe = NTest::ConvertValueToLiteralNode(pb, ui16(459U));
    const auto canada = NTest::ConvertValueToLiteralNode(pb, ui16(375U));
    const auto value2 = i64(1LL); // 1 Microsecond
    const auto step = pb.NewDataLiteral<NUdf::EDataSlot::Interval>(
        NUdf::TStringRef((const char*)&value2, sizeof(value2)));

    const auto day1 = pb.ToIntegral(NTest::ConvertValueToLiteralNode(pb, ui64(123)), dateType);
    const auto day2 = pb.ToIntegral(NTest::ConvertValueToLiteralNode(pb, ui64(123 + 5)), dateType);
    const auto date1 = pb.Unwrap(pb.AddTimezone(day1, europe), NTest::ConvertValueToLiteralNode(pb, TStringBuf("")), "", 0, 0);
    const auto date2 = pb.Unwrap(pb.AddTimezone(day2, canada), NTest::ConvertValueToLiteralNode(pb, TStringBuf("")), "", 0, 0);
    const auto dates = pb.ListFromRange(date1, date2, step);

    const auto graph = setup.BuildGraph(dates);
    const auto list = graph->GetValue();
    const auto iterator = list.GetListIterator();
    UNIT_ASSERT_VALUES_EQUAL(list.GetListLength(), 5);
    NUdf::TUnboxedValue item;
    for (size_t i = 123; i < 123 + 5; i++) {
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), i);
        UNIT_ASSERT_VALUES_EQUAL(item.GetTimezoneId(), 459U);
    }
}

Y_UNIT_TEST_LLVM(TestResverseUnsignedShorts) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto from = NTest::ConvertValueToLiteralNode(pb, ui16(60000U));
    const auto to = NTest::ConvertValueToLiteralNode(pb, ui16(59990U));
    const auto step = NTest::ConvertValueToLiteralNode(pb, i16(-2));

    const auto dates = pb.Collect(pb.ToFlow(pb.ListFromRange(from, to, step)));

    const auto graph = setup.BuildGraph(dates);
    const auto list = graph->GetValue();
    AssertUnboxedValueElementEqual(list, TVector<ui16>{60000U, 59998U, 59996U, 59994U, 59992U});
}
} // Y_UNIT_TEST_SUITE(TMiniKQLListFromRangeTest)
} // namespace NMiniKQL
} // namespace NKikimr
