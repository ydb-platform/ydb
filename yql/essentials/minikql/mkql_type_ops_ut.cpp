#include <yql/essentials/parser/pg_wrapper/pg_compat.h>

#include "mkql_type_ops.h"
#include "mkql_alloc.h"

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/type_info/tz/tz.h>

#include <util/stream/format.h>
#include <util/stream/str.h>

extern "C" {
#include <yql/essentials/parser/pg_wrapper/postgresql/src/include/datatype/timestamp.h>
#include <yql/essentials/parser/pg_wrapper/postgresql/src/include/utils/datetime.h>
}

using namespace NYql;
using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

Y_UNIT_TEST_SUITE(TMiniKQLTypeOps) {
    Y_UNIT_TEST(IsLeapYear) {
        UNIT_ASSERT(IsLeapYear(-401));
        UNIT_ASSERT(!IsLeapYear(-400));
        UNIT_ASSERT(!IsLeapYear(-101));
        UNIT_ASSERT(!IsLeapYear(-100));
        UNIT_ASSERT(IsLeapYear(-5));
        UNIT_ASSERT(!IsLeapYear(-4));
        UNIT_ASSERT(IsLeapYear(-1));
        UNIT_ASSERT(!IsLeapYear(1));
        UNIT_ASSERT(IsLeapYear(4));
        UNIT_ASSERT(!IsLeapYear(100));
        UNIT_ASSERT(IsLeapYear(400));
        UNIT_ASSERT(!IsLeapYear(1970));
        UNIT_ASSERT(IsLeapYear(2000));
        UNIT_ASSERT(IsLeapYear(2400));
    }

    Y_UNIT_TEST(Date16vs32) {
        for (ui16 value16 = 0; value16 < NUdf::MAX_DATE; ++value16) {
            const NUdf::TUnboxedValue& strDate16 = ValueToString(NUdf::EDataSlot::Date, NUdf::TUnboxedValuePod(value16));
            UNIT_ASSERT(strDate16.HasValue());
            auto value32 = ValueFromString(NUdf::EDataSlot::Date32, strDate16.AsStringRef());
            UNIT_ASSERT(value32.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(value16, value32.Get<i32>());
            const NUdf::TUnboxedValue& strDate32 = ValueToString(NUdf::EDataSlot::Date32, NUdf::TUnboxedValuePod(value32.Get<i32>()));
            UNIT_ASSERT(strDate32.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(strDate16.AsStringRef(), strDate32.AsStringRef());
        }
    }

    Y_UNIT_TEST(SplitDate16vs32) {
        for (ui16 date = 0; date < NUdf::MAX_DATE; ++date) {
            ui32 year16, month16, day16, dayOfYear16, weekOfYear16, weekOfYearIso8601_16, dayOfWeek16;
            SplitDate(date, year16, month16, day16);
            EnrichDate(date, dayOfYear16, weekOfYear16, weekOfYearIso8601_16, dayOfWeek16);

            i32 year;
            ui32 month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek;

            SplitDate32(date, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(year16, year);
            UNIT_ASSERT_VALUES_EQUAL(month16, month);
            UNIT_ASSERT_VALUES_EQUAL(day16, day);
            UNIT_ASSERT_VALUES_EQUAL(dayOfYear16, dayOfYear);
            UNIT_ASSERT_VALUES_EQUAL(dayOfWeek16, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(weekOfYear16, weekOfYear);
            UNIT_ASSERT_VALUES_EQUAL(weekOfYearIso8601_16, weekOfYearIso8601);
        }
    }

    using TestFunction = void(*)(ui16 tzId, ui32 beginDate, size_t step);
    void RunTestForAllTimezones(TestFunction test, size_t step) {
        std::uniform_int_distribution<ui64> urdist;
        std::default_random_engine rand;
        rand.seed(std::time(nullptr));

        const ui32 beginDate = urdist(rand) % step;

        const auto timezones = NTi::GetTimezones();
        for (size_t tzId = 0; tzId < timezones.size(); tzId++) {
            // XXX: Several timezones are missing, so skip them.
            if (timezones[tzId].empty()) {
                continue;
            }
            test(tzId, beginDate, step);
        }
    }

    void TestSplitMakeTzDate16vs32(ui16 tzId, ui32 beginDate, size_t step) {
        // Narrow date components.
        ui16 date_n;
        ui32 year_n;
        ui32 month_n, day_n, dayOfYear_n, weekOfYear_n, weekOfYearIso8601_n, dayOfWeek_n;
        // Wide date components.
        i32 date_w;
        i32 year_w;
        ui32 month_w, day_w, dayOfYear_w, weekOfYear_w, weekOfYearIso8601_w, dayOfWeek_w;

        for (ui16 date = beginDate; date < NUdf::MAX_DATE; date += step) {
            UNIT_ASSERT(SplitTzDate(date, year_n, month_n, day_n, dayOfYear_n,
                                    weekOfYear_n, weekOfYearIso8601_n,
                                    dayOfWeek_n, tzId));
            UNIT_ASSERT(SplitTzDate32(date, year_w, month_w, day_w, dayOfYear_w,
                                      weekOfYear_w, weekOfYearIso8601_w,
                                      dayOfWeek_w, tzId));

            UNIT_ASSERT_VALUES_EQUAL(year_n, year_w);
            UNIT_ASSERT_VALUES_EQUAL(month_n, month_w);
            UNIT_ASSERT_VALUES_EQUAL(day_n, day_w);
            UNIT_ASSERT_VALUES_EQUAL(dayOfYear_n, dayOfYear_w);
            UNIT_ASSERT_VALUES_EQUAL(dayOfWeek_n, dayOfWeek_w);
            UNIT_ASSERT_VALUES_EQUAL(weekOfYear_n, weekOfYear_w);
            UNIT_ASSERT_VALUES_EQUAL(weekOfYearIso8601_n, weekOfYearIso8601_w);

            UNIT_ASSERT(MakeTzDate(year_n, month_n, day_n, date_n, tzId));
            UNIT_ASSERT(MakeTzDate32(year_w, month_w, day_w, date_w, tzId));
            UNIT_ASSERT_VALUES_EQUAL_C(date_w, date_n, date);
        }
    }

    Y_UNIT_TEST(SplitMakeTzDate16vs32) {
        constexpr size_t dateStep = 100;
        RunTestForAllTimezones(TestSplitMakeTzDate16vs32, dateStep);
    }

    void TestSplitMakeTzDatetime32vs64(ui16 tzId, ui32 beginDatetime, size_t step) {
        // Narrow datetime components.
        ui32 datetime_n;
        ui32 year_n;
        ui32 month_n, day_n, hour_n, minute_n, second_n;
        ui32 dayOfYear_n, weekOfYear_n, weekOfYearIso8601_n, dayOfWeek_n;
        // Wide datetime components.
        i64 datetime_w;
        i32 year_w;
        ui32 month_w, day_w, hour_w, minute_w, second_w;
        ui32 dayOfYear_w, weekOfYear_w, weekOfYearIso8601_w, dayOfWeek_w;

        for (ui32 datetime = beginDatetime; datetime < NUdf::MAX_DATETIME; datetime += step) {
            UNIT_ASSERT(SplitTzDatetime(datetime, year_n, month_n, day_n, hour_n,
                                        minute_n, second_n, dayOfYear_n,
                                        weekOfYear_n, weekOfYearIso8601_n,
                                        dayOfWeek_n, tzId));
            UNIT_ASSERT(SplitTzDatetime64(datetime, year_w, month_w, day_w, hour_w,
                                          minute_w, second_w, dayOfYear_w,
                                          weekOfYear_w, weekOfYearIso8601_w,
                                          dayOfWeek_w, tzId));
            UNIT_ASSERT_VALUES_EQUAL(year_n, year_w);
            UNIT_ASSERT_VALUES_EQUAL(month_n, month_w);
            UNIT_ASSERT_VALUES_EQUAL(day_n, day_w);
            UNIT_ASSERT_VALUES_EQUAL(dayOfYear_n, dayOfYear_w);
            UNIT_ASSERT_VALUES_EQUAL(dayOfWeek_n, dayOfWeek_w);
            UNIT_ASSERT_VALUES_EQUAL(weekOfYear_n, weekOfYear_w);
            UNIT_ASSERT_VALUES_EQUAL(weekOfYearIso8601_n, weekOfYearIso8601_w);
            UNIT_ASSERT_VALUES_EQUAL(hour_n, hour_w);
            UNIT_ASSERT_VALUES_EQUAL(minute_n, minute_w);
            UNIT_ASSERT_VALUES_EQUAL(second_n, second_w);

            UNIT_ASSERT(MakeTzDatetime(year_n, month_n, day_n, hour_n, minute_n,
                                       second_n, datetime_n, tzId));
            UNIT_ASSERT(MakeTzDatetime64(year_w, month_w, day_w, hour_w,
                                         minute_w, second_w, datetime_w, tzId));
            UNIT_ASSERT_VALUES_EQUAL_C(datetime_w, datetime_n, datetime);
        }
    }

    Y_UNIT_TEST(SplitMakeTzDatetime32vs64) {
        constexpr size_t datetimeStep = 100 * 86400;
        RunTestForAllTimezones(TestSplitMakeTzDatetime32vs64, datetimeStep);
    }

    Y_UNIT_TEST(SplitMakeTzDateSingle) {
        ui32 y, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek;
        ui16 tzId = 1;
        i32 value = 0;

        UNIT_ASSERT(SplitTzDate(value, y, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId));
        UNIT_ASSERT_VALUES_EQUAL(y, 1970);
        UNIT_ASSERT_VALUES_EQUAL(month, 1);
        UNIT_ASSERT_VALUES_EQUAL(day, 2);

        ui32 dt;
        UNIT_ASSERT(MakeTzDatetime(y, month, day, 0, 0, 0, dt, tzId));
        UNIT_ASSERT_VALUES_EQUAL(value, dt/86400u);
    }

    Y_UNIT_TEST(SplitMakeTzDate32Single) {
        i32 y;
        ui32 month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek;
        ui16 tzId = 1;
        i32 value = 0;

        UNIT_ASSERT(SplitTzDate32(value, y, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId));
        UNIT_ASSERT_VALUES_EQUAL(y, 1970);
        UNIT_ASSERT_VALUES_EQUAL(month, 1);
        UNIT_ASSERT_VALUES_EQUAL(day, 2);

        i32 d32;
        UNIT_ASSERT(MakeTzDate32(y, month, day, d32, tzId));
        UNIT_ASSERT_VALUES_EQUAL(value, d32);
    }

    Y_UNIT_TEST(SplitDate32CornerCases) {
        i32 year;
        ui32 month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek;

        UNIT_ASSERT(SplitDate32(NYql::NUdf::MIN_DATE32, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek));
        UNIT_ASSERT_VALUES_EQUAL(NYql::NUdf::MIN_YEAR32, year);
        UNIT_ASSERT_VALUES_EQUAL(1, month);
        UNIT_ASSERT_VALUES_EQUAL(1, day);
        UNIT_ASSERT_VALUES_EQUAL(1, dayOfYear);
        UNIT_ASSERT_VALUES_EQUAL(7, dayOfWeek);
        UNIT_ASSERT_VALUES_EQUAL(1, weekOfYear);
        UNIT_ASSERT_VALUES_EQUAL(52, weekOfYearIso8601);

        UNIT_ASSERT(SplitDate32(NYql::NUdf::MAX_DATE32, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek));
        UNIT_ASSERT_VALUES_EQUAL(NYql::NUdf::MAX_YEAR32 - 1, year);
        UNIT_ASSERT_VALUES_EQUAL(12, month);
        UNIT_ASSERT_VALUES_EQUAL(31, day);
        UNIT_ASSERT_VALUES_EQUAL(365, dayOfYear);
        UNIT_ASSERT_VALUES_EQUAL(6, dayOfWeek);
        UNIT_ASSERT_VALUES_EQUAL(53, weekOfYear);
        UNIT_ASSERT_VALUES_EQUAL(52, weekOfYearIso8601);

        // -4713-11-24
        UNIT_ASSERT(SplitDate32(DATETIME_MIN_JULIAN - UNIX_EPOCH_JDATE, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek));
        UNIT_ASSERT_VALUES_EQUAL(JULIAN_MINYEAR - 1, year);
        UNIT_ASSERT_VALUES_EQUAL(JULIAN_MINMONTH, month);
        UNIT_ASSERT_VALUES_EQUAL(JULIAN_MINDAY, day);
        UNIT_ASSERT_VALUES_EQUAL(328, dayOfYear);
        UNIT_ASSERT_VALUES_EQUAL(1, dayOfWeek);
        UNIT_ASSERT_VALUES_EQUAL(48, weekOfYear);
        UNIT_ASSERT_VALUES_EQUAL(48, weekOfYearIso8601);

        // 0001-01-01
        UNIT_ASSERT(SplitDate32(-719162, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek));
        UNIT_ASSERT_VALUES_EQUAL(1, year);
        UNIT_ASSERT_VALUES_EQUAL(1, month);
        UNIT_ASSERT_VALUES_EQUAL(1, day);
        UNIT_ASSERT_VALUES_EQUAL(1, dayOfYear);
        UNIT_ASSERT_VALUES_EQUAL(1, dayOfWeek);
        UNIT_ASSERT_VALUES_EQUAL(1, weekOfYear);
        UNIT_ASSERT_VALUES_EQUAL(1, weekOfYearIso8601);

        UNIT_ASSERT(SplitDate32(0, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek));
        UNIT_ASSERT_VALUES_EQUAL(1970, year);
        UNIT_ASSERT_VALUES_EQUAL(1, month);
        UNIT_ASSERT_VALUES_EQUAL(1, day);
        UNIT_ASSERT_VALUES_EQUAL(1, dayOfYear);
        UNIT_ASSERT_VALUES_EQUAL(4, dayOfWeek);
        UNIT_ASSERT_VALUES_EQUAL(1, weekOfYear);
        UNIT_ASSERT_VALUES_EQUAL(1, weekOfYearIso8601);

        UNIT_ASSERT(SplitDate32(-1, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek));
        UNIT_ASSERT_VALUES_EQUAL(1969, year);
        UNIT_ASSERT_VALUES_EQUAL(12, month);
        UNIT_ASSERT_VALUES_EQUAL(31, day);
        UNIT_ASSERT_VALUES_EQUAL(365, dayOfYear);
        UNIT_ASSERT_VALUES_EQUAL(3, dayOfWeek);
        UNIT_ASSERT_VALUES_EQUAL(53, weekOfYear);
        UNIT_ASSERT_VALUES_EQUAL(1, weekOfYearIso8601);
    }

    Y_UNIT_TEST(Date32vsPostgres) {
        int value;
        UNIT_ASSERT(MakeDate32(JULIAN_MINYEAR, JULIAN_MINMONTH, JULIAN_MINDAY, value));
        for (; value < NUdf::MAX_DATE32; ++value) {
            i32 year;
            ui32 month, day;
            UNIT_ASSERT(SplitDate32(value, year, month, day));
            if (year < 0) {
                year++;
            }
            UNIT_ASSERT_VALUES_EQUAL(value, date2j(year, month, day) - UNIX_EPOCH_JDATE);
        }
    }

    Y_UNIT_TEST(PostgresVsDate32) {
        for (int value = DATETIME_MIN_JULIAN; value < DATE_END_JULIAN; ++value) {
            int year, month, day;
            j2date(value, &year, &month, &day);
            i32 date32;
            if (year <= 0) {
                year--;
            }
            UNIT_ASSERT(MakeDate32(year, static_cast<ui32>(month), static_cast<ui32>(day), date32));
            UNIT_ASSERT_VALUES_EQUAL(date32, value - UNIX_EPOCH_JDATE);

            ui32 dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek;
            i32 y;
            ui32 m, d;
            SplitDate32(date32, y, m, d, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(dayOfWeek % 7, j2day(value));

            if (date32 == NUdf::MAX_DATE32) {
                break;
            }
        }
    }

    Y_UNIT_TEST(Datetime32vs64) {
        TScopedAlloc alloc(__LOCATION__);
        for (ui32 v32 = 0; v32 <= 86400; ++v32) {
            const NUdf::TUnboxedValue str32 = ValueToString(NUdf::EDataSlot::Datetime, NUdf::TUnboxedValuePod(v32));
            UNIT_ASSERT(str32.HasValue());
            auto v64 = ValueFromString(NUdf::EDataSlot::Datetime64, str32.AsStringRef());
            UNIT_ASSERT(v64.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(v32, v64.Get<i64>());
            const NUdf::TUnboxedValue str64 = ValueToString(NUdf::EDataSlot::Datetime64, NUdf::TUnboxedValuePod(v64.Get<i64>()));
            UNIT_ASSERT(str64.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(str32.AsStringRef(), str64.AsStringRef());
        }
    }

    Y_UNIT_TEST(TimestampOldVsNew) {
        TScopedAlloc alloc(__LOCATION__);
        for (ui64 val = 0; val <= 86400000000; val += 1000003) {
            const NUdf::TUnboxedValue str32 = ValueToString(NUdf::EDataSlot::Timestamp, NUdf::TUnboxedValuePod(val));
            UNIT_ASSERT(str32.HasValue());
            auto v64 = ValueFromString(NUdf::EDataSlot::Timestamp64, str32.AsStringRef());
            UNIT_ASSERT(v64.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(val, v64.Get<i64>());
            const NUdf::TUnboxedValue str64 = ValueToString(NUdf::EDataSlot::Timestamp64, NUdf::TUnboxedValuePod(v64.Get<i64>()));
            UNIT_ASSERT(str64.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(str32.AsStringRef(), str64.AsStringRef());
        }
    }

    Y_UNIT_TEST(IntervalOldVsNew) {
        TScopedAlloc alloc(__LOCATION__);
        for (ui64 val = -86400000000; val <= 86400000000; val += 1000003) {
            const NUdf::TUnboxedValue str32 = ValueToString(NUdf::EDataSlot::Interval, NUdf::TUnboxedValuePod(val));
            UNIT_ASSERT(str32.HasValue());
            auto v64 = ValueFromString(NUdf::EDataSlot::Interval64, str32.AsStringRef());
            UNIT_ASSERT(v64.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(val, v64.Get<i64>());
            const NUdf::TUnboxedValue str64 = ValueToString(NUdf::EDataSlot::Interval64, NUdf::TUnboxedValuePod(v64.Get<i64>()));
            UNIT_ASSERT(str64.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(str32.AsStringRef(), str64.AsStringRef());
        }
    }

    Y_UNIT_TEST(DateInOut) {
        ui32 year = 1970;
        ui32 month = 1;
        ui32 day = 1;
        for (ui16 packed = 0U; year < NUdf::MAX_YEAR; ++packed) {
            const NUdf::TUnboxedValue& out = ValueToString(NUdf::EDataSlot::Date, NUdf::TUnboxedValuePod(packed));
            TStringStream expected;
            expected << LeftPad(year, 4, '0') << '-' << LeftPad(month, 2, '0') << '-' << LeftPad(day, 2, '0');
            UNIT_ASSERT_VALUES_EQUAL_C(TStringBuf(out.AsStringRef()), expected.Str(), "Packed value: " << packed);

            const auto out2 = ValueFromString(NUdf::EDataSlot::Date, expected.Str());
            UNIT_ASSERT_C(out2, "Date value: " << expected.Str());
            UNIT_ASSERT_VALUES_EQUAL_C(out2.Get<ui16>(), packed, "Date value: " << expected.Str());

            ++day;
            ui32 monthLength = 31;
            if (month == 4 || month == 6 || month == 9 || month == 11) {
                monthLength = 30;
            } else if (month == 2) {
                bool isLeap = (year % 4 == 0);
                if (year % 100 == 0) {
                    isLeap = year % 400 == 0;
                }

                monthLength = isLeap ? 29 : 28;
            }

            if (day > monthLength) {
                day = 1;
                ++month;
            }

            if (month > 12) {
                month = 1;
                ++year;
            }
        }
    }

    Y_UNIT_TEST(AllTimezones) {
        auto count = InitTimezones();
        UNIT_ASSERT_VALUES_EQUAL(count, 600);
        for (ui32 i = 0; i < count; ++i) {
            if (const auto name = FindTimezoneIANAName(i)) {
                UNIT_ASSERT(!name->empty());
                UNIT_ASSERT_VALUES_EQUAL(FindTimezoneId(*name), i);
            }
        }

        UNIT_ASSERT(!FindTimezoneIANAName(count));
        UNIT_ASSERT(FindTimezoneId("Europe/Moscow"));

        UNIT_ASSERT(!FindTimezoneId(""));
        UNIT_ASSERT(!FindTimezoneId("BadZone"));
    }

    Y_UNIT_TEST(TimezoneDatesSerialization) {
        ui16 tzId;

        ui16 date;
        UNIT_ASSERT(DeserializeTzDate(TStringBuilder() << "\x00\xea"sv << "\x00\x01"sv, date, tzId));
        UNIT_ASSERT_VALUES_EQUAL(date, 234);
        UNIT_ASSERT_VALUES_EQUAL(tzId, 1);

        {
            TStringStream out;
            SerializeTzDate(date, tzId, out);
            UNIT_ASSERT_VALUES_EQUAL(out.Str(), TStringBuilder() << "\x00\xea"sv << "\x00\x01"sv);
        }

        ui32 datetime;
        UNIT_ASSERT(DeserializeTzDatetime(TStringBuilder() << "\x00\x00\x02\x37"sv << "\x00\x01"sv, datetime, tzId));
        UNIT_ASSERT_VALUES_EQUAL(datetime, 567);
        UNIT_ASSERT_VALUES_EQUAL(tzId, 1);

        {
            TStringStream out;
            SerializeTzDatetime(datetime, tzId, out);
            UNIT_ASSERT_VALUES_EQUAL(out.Str(), TStringBuilder() << "\x00\x00\x02\x37"sv << "\x00\x01"sv);
        }

        ui64 timestamp;
        UNIT_ASSERT(DeserializeTzTimestamp(TStringBuilder() << "\x00\x00\x00\x00\x00\x00\x03\x7a"sv << "\x00\x01"sv, timestamp, tzId));
        UNIT_ASSERT_VALUES_EQUAL(timestamp, 890);
        UNIT_ASSERT_VALUES_EQUAL(tzId, 1);

        {
            TStringStream out;
            SerializeTzTimestamp(timestamp, tzId, out);
            UNIT_ASSERT_VALUES_EQUAL(out.Str(), TStringBuilder() << "\x00\x00\x00\x00\x00\x00\x03\x7a"sv << "\x00\x01"sv);
        }
    }

    NUdf::TUnboxedValuePod ParseTimestamp(NUdf::TStringRef buf) {
        return ValueFromString(NUdf::EDataSlot::Timestamp, buf);
    }

    Y_UNIT_TEST(TimestampSerialization) {
        UNIT_ASSERT(!ParseTimestamp("2020-07-28T21:46:05.55045#"));
        UNIT_ASSERT(!ParseTimestamp("2020-07-28T21:46:05.55045"));
        UNIT_ASSERT(!ParseTimestamp("2020-07-28T21:46:05."));
        UNIT_ASSERT(!ParseTimestamp("2020-07-28T21:46:05.Z"));
        UNIT_ASSERT(!ParseTimestamp("2020-071-28T21:46:05.1Z"));

        UNIT_ASSERT(!!ParseTimestamp("2020-07-28T21:46:05.1Z"));
        UNIT_ASSERT(!!ParseTimestamp("2020-07-28T21:46:05.1+01:00"));

        UNIT_ASSERT(!ParseTimestamp("4294969318-09-4294967318T14:28:17Z"));
        const auto& val1 = ParseTimestamp("2022-09-15T16:42:01.123456Z");
        const auto& val2 = ParseTimestamp("2022-09-15T16:42:01.123456131231223Z");

        UNIT_ASSERT(!!val1);
        UNIT_ASSERT(!!val2);
        UNIT_ASSERT_VALUES_EQUAL(val1.Get<ui64>(), val2.Get<ui64>());

        const auto& val3 = ParseTimestamp("2022-09-15T18:16:01.123456Z");
        const auto& val4 = ParseTimestamp("2022-09-15T16:42:01.123456131231223-12:34");

        UNIT_ASSERT(!!val3);
        UNIT_ASSERT(!!val4);
        UNIT_ASSERT_VALUES_EQUAL(val1.Get<ui64>(), val2.Get<ui64>());
    }

    NUdf::TUnboxedValuePod ParseDatetime(NUdf::TStringRef buf) {
        return ValueFromString(NUdf::EDataSlot::Datetime, buf);
    }

    Y_UNIT_TEST(DatetimeSeriailization) {
        UNIT_ASSERT(!ParseDatetime("2020-07-28T21:46:05.55045#"));
        UNIT_ASSERT(!ParseDatetime("2020-07-28T21:46:05.55045"));
        UNIT_ASSERT(!ParseDatetime("2020-07-28T21:46:05"));
        UNIT_ASSERT(!ParseDatetime("2020-07-28T21:46:05."));
        UNIT_ASSERT(!ParseDatetime("2020-071-28T21:46:05Z"));

        UNIT_ASSERT(!!ParseDatetime("2020-07-28T21:46:05Z"));
        UNIT_ASSERT(!!ParseDatetime("2020-07-28T21:46:05+01:00"));

        UNIT_ASSERT(!ParseDatetime("4294969318-09-4294967318T14:28:17Z"));

        const auto& val1 = ParseDatetime("2022-09-15T04:08:01Z");
        const auto& val2 = ParseDatetime("2022-09-15T16:42:01+12:34");

        UNIT_ASSERT(!!val1);
        UNIT_ASSERT(!!val2);
        UNIT_ASSERT_VALUES_EQUAL(val1.Get<ui32>(), val2.Get<ui32>());

    }
}
