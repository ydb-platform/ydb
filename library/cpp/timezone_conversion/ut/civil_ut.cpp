#include <library/cpp/timezone_conversion/civil.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/stream/str.h>

namespace NDatetime {
    inline bool operator==(const NDatetime::TCivilDiff& x, const NDatetime::TCivilDiff& y) {
        return x.Unit == y.Unit && x.Value == y.Value;
    }
}

template <>
inline void Out<NDatetime::TCivilDiff>(IOutputStream& out, const NDatetime::TCivilDiff& diff) {
    out << "(" << diff.Value << "," << diff.Unit << ")";
}

Y_UNIT_TEST_SUITE(DateTime) {
    Y_UNIT_TEST(Calc) {
        NDatetime::TCivilSecond s(2017, 2, 1, 10, 12, 9);
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::Calc<NDatetime::TCivilDay>(s, 2), NDatetime::TCivilDay(2017, 2, 3));
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::Calc<NDatetime::TCivilDay>(s, -2), NDatetime::TCivilDay(2017, 1, 30));
    }
    Y_UNIT_TEST(Adds) {
        NDatetime::TCivilSecond s(2017, 2, 1, 10, 12, 9);
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::AddDays(s, 2), NDatetime::TCivilSecond(2017, 2, 3, 10, 12, 9));
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::AddMonths(s, -2), NDatetime::TCivilSecond(2016, 12, 1, 10, 12, 9));
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::AddYears(s, -55), NDatetime::TCivilSecond(1962, 2, 1, 10, 12, 9));
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::AddHours(s, 40), NDatetime::TCivilSecond(2017, 2, 3, 2, 12, 9));
    }
    Y_UNIT_TEST(Convert) {
        TInstant absTime = TInstant::Seconds(1500299239);
        NDatetime::TTimeZone lax = NDatetime::GetTimeZone("America/Los_Angeles");
        NDatetime::TCivilSecond dt1 = NDatetime::Convert(absTime, lax);
        NDatetime::TCivilSecond dt2(2017, 7, 17, 6, 47, 19);
        UNIT_ASSERT_VALUES_EQUAL(dt1, dt2);
        UNIT_ASSERT_VALUES_EQUAL(absTime, NDatetime::Convert(dt2, lax));
        UNIT_ASSERT_EXCEPTION(NDatetime::Convert(absTime, "Unknown time zone"), NDatetime::TInvalidTimezone);
    }
    Y_UNIT_TEST(UTCOffsetTimezone) {
        NDatetime::TTimeZone lax = NDatetime::GetTimeZone("UTC+12");
        auto lookup = lax.lookup(std::chrono::system_clock::from_time_t(0));
        UNIT_ASSERT_VALUES_EQUAL(12 * 60 * 60, lookup.offset);
        lax = NDatetime::GetTimeZone("UTC-10");
        lookup = lax.lookup(std::chrono::system_clock::from_time_t(0));
        UNIT_ASSERT_VALUES_EQUAL(-10 * 60 * 60, lookup.offset);
        lax = NDatetime::GetTimeZone("UTC");
        lookup = lax.lookup(std::chrono::system_clock::from_time_t(0));
        UNIT_ASSERT_VALUES_EQUAL(0, lookup.offset);
        lax = NDatetime::GetTimeZone("UTC+0");
        lookup = lax.lookup(std::chrono::system_clock::from_time_t(0));
        UNIT_ASSERT_VALUES_EQUAL(0, lookup.offset);
        lax = NDatetime::GetTimeZone("UTC-2");
        lookup = lax.lookup(std::chrono::system_clock::from_time_t(0));
        UNIT_ASSERT_VALUES_EQUAL(-2 * 60 * 60, lookup.offset);
        lax = NDatetime::GetTimeZone("UTC-00:30");
        lookup = lax.lookup(std::chrono::system_clock::from_time_t(0));
        UNIT_ASSERT_VALUES_EQUAL(-30 * 60, lookup.offset);
        lax = NDatetime::GetTimeZone("UTC-0241");
        lookup = lax.lookup(std::chrono::system_clock::from_time_t(0));
        UNIT_ASSERT_VALUES_EQUAL(-(2 * 60 + 41) * 60, lookup.offset);
        UNIT_ASSERT_EXCEPTION(NDatetime::GetTimeZone("UTCUnknown"), NDatetime::TInvalidTimezone);
        UNIT_ASSERT_EXCEPTION(NDatetime::GetTimeZone("UTC+:"), NDatetime::TInvalidTimezone);
        UNIT_ASSERT_EXCEPTION(NDatetime::GetTimeZone("UTC+24:01"), NDatetime::TInvalidTimezone);
        UNIT_ASSERT_EXCEPTION(NDatetime::GetTimeZone("UTC+20:"), NDatetime::TInvalidTimezone);
        UNIT_ASSERT_EXCEPTION(NDatetime::GetTimeZone("UTC+20:60"), NDatetime::TInvalidTimezone);
        UNIT_ASSERT_EXCEPTION(NDatetime::GetTimeZone("UTC+20:30:"), NDatetime::TInvalidTimezone);
    }
    Y_UNIT_TEST(ParseOffset) {
        int offset;
        UNIT_ASSERT(!NDatetime::TryParseOffset("Unknown", offset));
        UNIT_ASSERT(!NDatetime::TryParseOffset("+:", offset));
        UNIT_ASSERT(!NDatetime::TryParseOffset("+24:01", offset));
        UNIT_ASSERT(!NDatetime::TryParseOffset("+20:", offset));
        UNIT_ASSERT(!NDatetime::TryParseOffset("+20:60", offset));
        UNIT_ASSERT(!NDatetime::TryParseOffset("+20:30:", offset));
        UNIT_ASSERT(NDatetime::TryParseOffset("+03", offset));
        UNIT_ASSERT_VALUES_EQUAL(offset, 10800);
        UNIT_ASSERT(NDatetime::TryParseOffset("-10", offset));
        UNIT_ASSERT_VALUES_EQUAL(offset, -36000);
        UNIT_ASSERT(NDatetime::TryParseOffset("+01:00", offset));
        UNIT_ASSERT_VALUES_EQUAL(offset, 3600);
        UNIT_ASSERT(NDatetime::TryParseOffset("-11:30", offset));
        UNIT_ASSERT_VALUES_EQUAL(offset, -41400);
        UNIT_ASSERT(NDatetime::TryParseOffset("+0130", offset));
        UNIT_ASSERT_VALUES_EQUAL(offset, 5400);
        UNIT_ASSERT(NDatetime::TryParseOffset("-0200", offset));
        UNIT_ASSERT_VALUES_EQUAL(offset, -7200);
    }
    Y_UNIT_TEST(Format) {
        NDatetime::TTimeZone lax = NDatetime::GetTimeZone("America/Los_Angeles");
        NDatetime::TCivilSecond tp(2013, 1, 2, 3, 4, 5);
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::Format("%H:%M:%S", tp, lax), "03:04:05");
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::Format("%H:%M:%E3S", tp, lax), "03:04:05.000");
    }
    Y_UNIT_TEST(Weekday) {
        NDatetime::TCivilDay d(2013, 1, 2);
        NDatetime::TWeekday wd = NDatetime::GetWeekday(d);
        UNIT_ASSERT_VALUES_EQUAL(wd, NDatetime::TWeekday::wednesday);
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::NextWeekday(d, NDatetime::TWeekday::monday), NDatetime::TCivilDay(2013, 1, 7));
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::PrevWeekday(d, NDatetime::TWeekday::monday), NDatetime::TCivilDay(2012, 12, 31));
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::WeekdayOnTheWeek(d, NDatetime::TWeekday::monday), NDatetime::TCivilDay(2012, 12, 31));
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::WeekdayOnTheWeek(d, NDatetime::TWeekday::wednesday), NDatetime::TCivilDay(2013, 1, 2));
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::WeekdayOnTheWeek(d, NDatetime::TWeekday::friday), NDatetime::TCivilDay(2013, 1, 4));
    }
    Y_UNIT_TEST(WeekdayFromCivilSecond) {
        NDatetime::TCivilSecond s(2013, 1, 2, 10, 12, 9);
        NDatetime::TWeekday wd = NDatetime::GetWeekday(s);
        UNIT_ASSERT_VALUES_EQUAL(wd, NDatetime::TWeekday::wednesday);
    }
    Y_UNIT_TEST(CivilUnit) {
        using namespace NDatetime;

        UNIT_ASSERT_VALUES_EQUAL(GetCivilUnit<TCivilMonth>(), ECivilUnit::Month);
        UNIT_ASSERT_VALUES_EQUAL(GetCivilUnit(TCivilHour{}), ECivilUnit::Hour);

        UNIT_ASSERT_VALUES_EQUAL(TCivilTime<ECivilUnit::Day>(2017, 1, 11), TCivilDay(2017, 1, 11));

        NDatetime::TCivilSecond s(2017, 2, 1, 10, 12, 9);

        UNIT_ASSERT_VALUES_EQUAL(
            NDatetime::AddCivil(s, TCivilDiff{2, ECivilUnit::Day}),
            NDatetime::AddDays(s, 2));
        UNIT_ASSERT_VALUES_EQUAL(
            NDatetime::AddCivil(s, TCivilDiff{-2, ECivilUnit::Month}),
            NDatetime::AddMonths(s, -2));
        UNIT_ASSERT_VALUES_EQUAL(
            NDatetime::AddCivil(s, TCivilDiff{-55, ECivilUnit::Year}),
            NDatetime::AddYears(s, -55));
        UNIT_ASSERT_VALUES_EQUAL(
            NDatetime::AddCivil(s, TCivilDiff{40, ECivilUnit::Hour}),
            NDatetime::AddHours(s, 40));

        UNIT_ASSERT_VALUES_EQUAL(
            GetCivilDiff(TCivilSecond(2017, 10), TCivilSecond(2017, 7), ECivilUnit::Month),
            TCivilDiff(3, ECivilUnit::Month));

        UNIT_ASSERT_VALUES_EQUAL(
            GetCivilDiff(TCivilSecond(2017, 10, 1), TCivilSecond(2017, 9, 30), ECivilUnit::Month),
            TCivilDiff(1, ECivilUnit::Month));

        UNIT_ASSERT_VALUES_EQUAL(
            GetCivilDiff(TCivilSecond(2017, 10, 1), TCivilSecond(2017, 9, 31), ECivilUnit::Month),
            TCivilDiff(0, ECivilUnit::Month));
    }

    Y_UNIT_TEST(TestYearWeekNmb) {

        // YEAR 2021 - start from Friday, first dates (1-3) will have week# 0
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::GetYearWeek(NDatetime::TCivilDay{2021, 1, 1}), 0);
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::GetYearWeek(NDatetime::TCivilDay{2021, 1, 2}), 0);
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::GetYearWeek(NDatetime::TCivilDay{2021, 1, 3}), 0);
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::GetYearWeek(NDatetime::TCivilDay{2021, 1, 4}), 1);

        UNIT_ASSERT_VALUES_EQUAL(NDatetime::GetYearWeek(NDatetime::TCivilDay{2021, 1, 1}, true), 53);
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::GetYearWeek(NDatetime::TCivilDay{2021, 1, 2}, true), 53);
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::GetYearWeek(NDatetime::TCivilDay{2021, 1, 3}, true), 53);
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::GetYearWeek(NDatetime::TCivilDay{2021, 1, 4}, true), 54);

        UNIT_ASSERT_VALUES_EQUAL(NDatetime::GetYearWeek(NDatetime::TCivilDay{2021, 2, 28}), 8);
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::GetYearWeek(NDatetime::TCivilDay{2021, 2, 29}), 9); // <- this is invalid date, should be normalized to March 1
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::GetYearWeek(NDatetime::TCivilDay{2021, 3, 1}), 9);

        UNIT_ASSERT_VALUES_EQUAL(NDatetime::GetYearWeek(NDatetime::TCivilDay{2021, 12, 26}), 51);
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::GetYearWeek(NDatetime::TCivilDay{2021, 12, 27}), 52);
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::GetYearWeek(NDatetime::TCivilDay{2021, 12, 31}), 52);

        // YEAR 2020 - start from Wednesday, all dates start from week# 1
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::GetYearWeek(NDatetime::TCivilDay{2020, 1, 1}), 1);
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::GetYearWeek(NDatetime::TCivilDay{2020, 1, 5}), 1);
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::GetYearWeek(NDatetime::TCivilDay{2020, 1, 6}), 2);

        UNIT_ASSERT_VALUES_EQUAL(NDatetime::GetYearWeek(NDatetime::TCivilDay{2020, 2, 28}), 9);
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::GetYearWeek(NDatetime::TCivilDay{2020, 2, 29}), 9);
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::GetYearWeek(NDatetime::TCivilDay{2020, 3, 1}), 9);
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::GetYearWeek(NDatetime::TCivilDay{2020, 3, 2}), 10);

        UNIT_ASSERT_VALUES_EQUAL(NDatetime::GetYearWeek(NDatetime::TCivilDay{2020, 12, 31}), 53);

        // Max possible delta - calcuate week # for 31 Dec 2021 from 1 Jan 2020
        UNIT_ASSERT_VALUES_EQUAL(NDatetime::GetYearWeek(NDatetime::TCivilDay{2021, 12, 31}, true), 105);
    }
}
