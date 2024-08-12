#include <ydb/library/yql/parser/pg_wrapper/pg_compat.h>

#include "mkql_type_ops.h"
#include "mkql_alloc.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/format.h>
#include <util/stream/str.h>

#include <contrib/libs/cctz/include/cctz/civil_time.h>
#include <contrib/libs/cctz/include/cctz/time_zone.h>
#include <inttypes.h>

extern "C" {
#include <ydb/library/yql/parser/pg_wrapper/postgresql/src/include/datatype/timestamp.h>
#include <ydb/library/yql/parser/pg_wrapper/postgresql/src/include/utils/datetime.h>
#include <ydb/library/yql/parser/pg_wrapper/postgresql/src/include/utils/date.h>
#include <ydb/library/yql/parser/pg_wrapper/postgresql/src/include/pgtime.h>
#include <time.h>
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

            /*
            ui32 dayOfYear16, weekOfYear16, weekOfYearIso8601_16, dayOfWeek16;
            ui32 dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek;
            EnrichDate(value16, dayOfYear16, weekOfYear16, weekOfYearIso8601_16, dayOfWeek16);
            EnrichDate32(value16, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(dayOfYear16, dayOfYear);
            UNIT_ASSERT_VALUES_EQUAL(weekOfYear16, weekOfYear);
            UNIT_ASSERT_VALUES_EQUAL(weekOfYearIso8601_16, weekOfYearIso8601);
            UNIT_ASSERT_VALUES_EQUAL(dayOfWeek16, dayOfWeek);
            */
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
            UNIT_ASSERT_VALUES_EQUAL_C(weekOfYear16, weekOfYear, "doy " << dayOfYear << " dow " << dayOfWeek << " month " << month << " day " << day << " date " << date);
            UNIT_ASSERT_VALUES_EQUAL_C(weekOfYearIso8601_16, weekOfYearIso8601, "doy " << dayOfYear << " dow " << dayOfWeek << " month " << month << " day " << day << " date " << date);
        }
    }

    Y_UNIT_TEST(GmTime) {
        // time_t t = time(NULL);
        time_t t = 4294967295ul;
        auto* tm = gmtime(&t);
        UNIT_ASSERT(tm != NULL);
        Cerr
            << "\n" << tm->tm_sec
            << "\n" << tm->tm_min
            << "\n" << tm->tm_hour
            << "\n" << tm->tm_mday
            << "\n" << tm->tm_mon			/* origin 1, not 0! */
            << "\n" << tm->tm_year + 1900
            << "\n" << tm->tm_wday
            << "\n" << tm->tm_yday
            << "\n" << tm->tm_isdst
            << "\n" << tm->tm_gmtoff
            << Endl;
    }

    Y_UNIT_TEST(cctz) {
        Cerr << "year min " << std::numeric_limits<std::int_least64_t>::min()
            << " max " << std::numeric_limits<std::int_least64_t>::max()
            << Endl;

        /*
        cctz::time_zone tz;
        cctz::load_time_zone("Africa/Lagos", &tz);
        i64 utcSeconds = 0;
        auto cs = cctz::convert(std::chrono::system_clock::from_time_t(utcSeconds), tz);
        Cerr << "\ncctz " << utcSeconds << "\n"
            << cs.year() << "-" << cs.month() << "-" << cs.day() << "T" << cs.hour() << ":" << cs.minute() << ":" << cs.second() << " tz " << tz << Endl;
    */
    }

    Y_UNIT_TEST(SplitTzDate32_Bissau) {
            i32 year;
            ui32 month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek;
            ui16 tzId = 11;

            // TODO failed due to historical ofssets
            SplitTzDate32(NYql::NUdf::MIN_DATE32, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);

            SplitTzDate32(NYql::NUdf::MAX_DATE32, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);
            UNIT_ASSERT_VALUES_EQUAL(NYql::NUdf::MAX_YEAR32 - 1, year);
            UNIT_ASSERT_VALUES_EQUAL(12, month);
            UNIT_ASSERT_VALUES_EQUAL(31, day);
            UNIT_ASSERT_VALUES_EQUAL(365, dayOfYear);
            UNIT_ASSERT_VALUES_EQUAL(6, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(53, weekOfYear);
            UNIT_ASSERT_VALUES_EQUAL(52, weekOfYearIso8601);

            // -4713-11-23
            SplitTzDate32(DATETIME_MIN_JULIAN - UNIX_EPOCH_JDATE, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);
            UNIT_ASSERT_VALUES_EQUAL(JULIAN_MINYEAR - 1, year);
            UNIT_ASSERT_VALUES_EQUAL(JULIAN_MINMONTH, month);
            UNIT_ASSERT_VALUES_EQUAL(JULIAN_MINDAY - 1, day);
            UNIT_ASSERT_VALUES_EQUAL(327, dayOfYear);
            UNIT_ASSERT_VALUES_EQUAL(7, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(47, weekOfYear);
            UNIT_ASSERT_VALUES_EQUAL(47, weekOfYearIso8601);

            // 0001-01-01
            SplitTzDate32(-719162, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);
            UNIT_ASSERT_VALUES_EQUAL(-1, year);
            UNIT_ASSERT_VALUES_EQUAL(12, month);
            UNIT_ASSERT_VALUES_EQUAL(31, day);
            UNIT_ASSERT_VALUES_EQUAL(366, dayOfYear);
            UNIT_ASSERT_VALUES_EQUAL(7, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(53, weekOfYear);
            UNIT_ASSERT_VALUES_EQUAL(52, weekOfYearIso8601);

            SplitTzDate32(0, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);
            UNIT_ASSERT_VALUES_EQUAL(1969, year);
            UNIT_ASSERT_VALUES_EQUAL(12, month);
            UNIT_ASSERT_VALUES_EQUAL(31, day);
            UNIT_ASSERT_VALUES_EQUAL(365, dayOfYear);
            UNIT_ASSERT_VALUES_EQUAL(3, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(53, weekOfYear);
            UNIT_ASSERT_VALUES_EQUAL(1, weekOfYearIso8601);

            SplitTzDate32(-1, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);
            UNIT_ASSERT_VALUES_EQUAL(1969, year);
            UNIT_ASSERT_VALUES_EQUAL(12, month);
            UNIT_ASSERT_VALUES_EQUAL(30, day);
            UNIT_ASSERT_VALUES_EQUAL(364, dayOfYear);
            UNIT_ASSERT_VALUES_EQUAL(2, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(53, weekOfYear);
            UNIT_ASSERT_VALUES_EQUAL(1, weekOfYearIso8601);
    }

    Y_UNIT_TEST(SplitTzDate32_Zero) {
            i32 year;
            ui32 month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek;
            ui16 tzId = 0;

            SplitTzDate32(NYql::NUdf::MIN_DATE32, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);
            UNIT_ASSERT_VALUES_EQUAL(NYql::NUdf::MIN_YEAR32, year);
            UNIT_ASSERT_VALUES_EQUAL(1, month);
            UNIT_ASSERT_VALUES_EQUAL(1, day);
            UNIT_ASSERT_VALUES_EQUAL(1, dayOfYear);
            UNIT_ASSERT_VALUES_EQUAL(7, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(1, weekOfYear);
            UNIT_ASSERT_VALUES_EQUAL(52, weekOfYearIso8601);

            SplitTzDate32(NYql::NUdf::MAX_DATE32, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);
            UNIT_ASSERT_VALUES_EQUAL(NYql::NUdf::MAX_YEAR32 - 1, year);
            UNIT_ASSERT_VALUES_EQUAL(12, month);
            UNIT_ASSERT_VALUES_EQUAL(31, day);
            UNIT_ASSERT_VALUES_EQUAL(365, dayOfYear);
            UNIT_ASSERT_VALUES_EQUAL(6, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(53, weekOfYear);
            UNIT_ASSERT_VALUES_EQUAL(52, weekOfYearIso8601);

            // -4713-11-24
            SplitTzDate32(DATETIME_MIN_JULIAN - UNIX_EPOCH_JDATE, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);
            UNIT_ASSERT_VALUES_EQUAL(JULIAN_MINYEAR - 1, year);
            UNIT_ASSERT_VALUES_EQUAL(JULIAN_MINMONTH, month);
            UNIT_ASSERT_VALUES_EQUAL(JULIAN_MINDAY, day);
            UNIT_ASSERT_VALUES_EQUAL(328, dayOfYear);
            UNIT_ASSERT_VALUES_EQUAL(1, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(48, weekOfYear);
            UNIT_ASSERT_VALUES_EQUAL(48, weekOfYearIso8601);

            // 0001-01-01
            SplitTzDate32(-719162, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);
            UNIT_ASSERT_VALUES_EQUAL(1, year);
            UNIT_ASSERT_VALUES_EQUAL(1, month);
            UNIT_ASSERT_VALUES_EQUAL(1, day);
            UNIT_ASSERT_VALUES_EQUAL(1, dayOfYear);
            UNIT_ASSERT_VALUES_EQUAL(1, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(1, weekOfYear);
            UNIT_ASSERT_VALUES_EQUAL(1, weekOfYearIso8601);

            SplitTzDate32(0, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);
            UNIT_ASSERT_VALUES_EQUAL(1970, year);
            UNIT_ASSERT_VALUES_EQUAL(1, month);
            UNIT_ASSERT_VALUES_EQUAL(1, day);
            UNIT_ASSERT_VALUES_EQUAL(1, dayOfYear);
            UNIT_ASSERT_VALUES_EQUAL(4, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(1, weekOfYear);
            UNIT_ASSERT_VALUES_EQUAL(1, weekOfYearIso8601);

            SplitTzDate32(-1, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);
            UNIT_ASSERT_VALUES_EQUAL(1969, year);
            UNIT_ASSERT_VALUES_EQUAL(12, month);
            UNIT_ASSERT_VALUES_EQUAL(31, day);
            UNIT_ASSERT_VALUES_EQUAL(365, dayOfYear);
            UNIT_ASSERT_VALUES_EQUAL(3, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(53, weekOfYear);
            UNIT_ASSERT_VALUES_EQUAL(1, weekOfYearIso8601);
    }

    Y_UNIT_TEST(SplitTzDate32_Moscow) {
            i32 year;
            ui32 month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek;
            ui16 tzId = 1;

            SplitTzDate32(NYql::NUdf::MIN_DATE32, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);
            UNIT_ASSERT_VALUES_EQUAL(NYql::NUdf::MIN_YEAR32, year);
            UNIT_ASSERT_VALUES_EQUAL(1, month);
            UNIT_ASSERT_VALUES_EQUAL(1, day);
            UNIT_ASSERT_VALUES_EQUAL(1, dayOfYear);
            UNIT_ASSERT_VALUES_EQUAL(7, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(1, weekOfYear);
            UNIT_ASSERT_VALUES_EQUAL(52, weekOfYearIso8601);

            SplitTzDate32(NYql::NUdf::MAX_DATE32, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);
            UNIT_ASSERT_VALUES_EQUAL(NYql::NUdf::MAX_YEAR32 - 1, year);
            UNIT_ASSERT_VALUES_EQUAL(12, month);
            UNIT_ASSERT_VALUES_EQUAL(31, day);
            UNIT_ASSERT_VALUES_EQUAL(365, dayOfYear);
            UNIT_ASSERT_VALUES_EQUAL(6, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(53, weekOfYear);
            UNIT_ASSERT_VALUES_EQUAL(52, weekOfYearIso8601);

            // -4713-11-24
            SplitTzDate32(DATETIME_MIN_JULIAN - UNIX_EPOCH_JDATE, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);
            UNIT_ASSERT_VALUES_EQUAL(JULIAN_MINYEAR - 1, year);
            UNIT_ASSERT_VALUES_EQUAL(JULIAN_MINMONTH, month);
            UNIT_ASSERT_VALUES_EQUAL(JULIAN_MINDAY, day);
            UNIT_ASSERT_VALUES_EQUAL(328, dayOfYear);
            UNIT_ASSERT_VALUES_EQUAL(1, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(48, weekOfYear);
            UNIT_ASSERT_VALUES_EQUAL(48, weekOfYearIso8601);

            // 0001-01-01
            SplitTzDate32(-719162, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);
            UNIT_ASSERT_VALUES_EQUAL(1, year);
            UNIT_ASSERT_VALUES_EQUAL(1, month);
            UNIT_ASSERT_VALUES_EQUAL(1, day);
            UNIT_ASSERT_VALUES_EQUAL(1, dayOfYear);
            UNIT_ASSERT_VALUES_EQUAL(1, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(1, weekOfYear);
            UNIT_ASSERT_VALUES_EQUAL(1, weekOfYearIso8601);

            SplitTzDate32(0, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);
            UNIT_ASSERT_VALUES_EQUAL(1970, year);
            UNIT_ASSERT_VALUES_EQUAL(1, month);
            UNIT_ASSERT_VALUES_EQUAL(1, day);
            UNIT_ASSERT_VALUES_EQUAL(1, dayOfYear);
            UNIT_ASSERT_VALUES_EQUAL(4, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(1, weekOfYear);
            UNIT_ASSERT_VALUES_EQUAL(1, weekOfYearIso8601);

            SplitTzDate32(-1, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);
            UNIT_ASSERT_VALUES_EQUAL(1969, year);
            UNIT_ASSERT_VALUES_EQUAL(12, month);
            UNIT_ASSERT_VALUES_EQUAL(31, day);
            UNIT_ASSERT_VALUES_EQUAL(365, dayOfYear);
            UNIT_ASSERT_VALUES_EQUAL(3, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(53, weekOfYear);
            UNIT_ASSERT_VALUES_EQUAL(1, weekOfYearIso8601);
    }

    Y_UNIT_TEST(MakeSplitDate32) {
            i32 year;
            ui32 month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek;
            ui16 tzId = 1;

            i32 value = -719163;

            SplitDate32(value, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(-1, year);
            UNIT_ASSERT_VALUES_EQUAL(12, month);
            UNIT_ASSERT_VALUES_EQUAL(31, day);

            i32 date32;
            UNIT_ASSERT(MakeDate32(year, month, day, date32));
            UNIT_ASSERT_VALUES_EQUAL(value, date32);
            UNIT_ASSERT(!MakeDate32(0, month, day, date32));

            SplitTzDate32(value, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);
            UNIT_ASSERT_VALUES_EQUAL(-1, year);
            UNIT_ASSERT_VALUES_EQUAL(12, month);
            UNIT_ASSERT_VALUES_EQUAL(31, day);
            UNIT_ASSERT_VALUES_EQUAL(-719163, value);
    }

    Y_UNIT_TEST(SplitDate32CornerCases) {
            i32 year;
            ui32 month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek;

            SplitDate32(NYql::NUdf::MIN_DATE32, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(NYql::NUdf::MIN_YEAR32, year);
            UNIT_ASSERT_VALUES_EQUAL(1, month);
            UNIT_ASSERT_VALUES_EQUAL(1, day);
            UNIT_ASSERT_VALUES_EQUAL(1, dayOfYear);
            UNIT_ASSERT_VALUES_EQUAL(7, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(1, weekOfYear);
            UNIT_ASSERT_VALUES_EQUAL(52, weekOfYearIso8601);

            SplitDate32(NYql::NUdf::MAX_DATE32, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(NYql::NUdf::MAX_YEAR32 - 1, year);
            UNIT_ASSERT_VALUES_EQUAL(12, month);
            UNIT_ASSERT_VALUES_EQUAL(31, day);
            UNIT_ASSERT_VALUES_EQUAL(365, dayOfYear);
            UNIT_ASSERT_VALUES_EQUAL(6, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(53, weekOfYear);
            UNIT_ASSERT_VALUES_EQUAL(52, weekOfYearIso8601);

            // -4713-11-24
            SplitDate32(DATETIME_MIN_JULIAN - UNIX_EPOCH_JDATE, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(JULIAN_MINYEAR - 1, year);
            UNIT_ASSERT_VALUES_EQUAL(JULIAN_MINMONTH, month);
            UNIT_ASSERT_VALUES_EQUAL(JULIAN_MINDAY, day);
            UNIT_ASSERT_VALUES_EQUAL(328, dayOfYear);
            UNIT_ASSERT_VALUES_EQUAL(1, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(48, weekOfYear);
            UNIT_ASSERT_VALUES_EQUAL(48, weekOfYearIso8601);

            // 0001-01-01
            SplitDate32(-719162, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(1, year);
            UNIT_ASSERT_VALUES_EQUAL(1, month);
            UNIT_ASSERT_VALUES_EQUAL(1, day);
            UNIT_ASSERT_VALUES_EQUAL(1, dayOfYear);
            UNIT_ASSERT_VALUES_EQUAL(1, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(1, weekOfYear);
            UNIT_ASSERT_VALUES_EQUAL(1, weekOfYearIso8601);

            SplitDate32(0, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(1970, year);
            UNIT_ASSERT_VALUES_EQUAL(1, month);
            UNIT_ASSERT_VALUES_EQUAL(1, day);
            UNIT_ASSERT_VALUES_EQUAL(1, dayOfYear);
            UNIT_ASSERT_VALUES_EQUAL(4, dayOfWeek);
            UNIT_ASSERT_VALUES_EQUAL(1, weekOfYear);
            UNIT_ASSERT_VALUES_EQUAL(1, weekOfYearIso8601);

            SplitDate32(-1, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek);
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
