#pragma once

#include <util/datetime/base.h>

#include <contrib/libs/cctz/include/cctz/civil_time.h>
#include <contrib/libs/cctz/include/cctz/time_zone.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>

#if __clang__ && __cpp_constexpr >= 201304
#define CONSTEXPR_M constexpr
#else
#define CONSTEXPR_M inline
#endif

namespace NDatetime {
    /** Exception class which throws when time zone is not valid
     */
    class TInvalidTimezone: public yexception {
    };

    using TSystemClock = std::chrono::system_clock;
    using TTimePoint = std::chrono::time_point<TSystemClock>;

    /*
     * An opaque class representing past, present, and future rules of
     * mapping between absolute and civil times in a given region.
     * It is very lightweight and may be passed by value.
     */
    using TTimeZone = cctz::time_zone;

    using TCivilYear = cctz::civil_year;
    using TCivilMonth = cctz::civil_month;
    using TCivilDay = cctz::civil_day;
    using TCivilHour = cctz::civil_hour;
    using TCivilMinute = cctz::civil_minute;
    using TCivilSecond = cctz::civil_second;
    using TWeekday = cctz::weekday;
    using TDiff = cctz::diff_t;

    using TYear = cctz::year_t;
    using TMonth = cctz::detail::month_t;

    enum class ECivilUnit : int {
        Second = 0,
        Minute = 1,
        Hour = 2,
        Day = 3,
        Month = 4,
        Year = 5
    };

    namespace NDetail {
        template <typename T>
        struct TGetCivilUnit;

        template <ECivilUnit Unit>
        struct TGetCivilTime;
    }

    template <typename T>
    CONSTEXPR_M ECivilUnit GetCivilUnit(const T& = {}) {
        return NDetail::TGetCivilUnit<T>::Value;
    }

    template <ECivilUnit Unit>
    using TCivilTime = typename NDetail::TGetCivilTime<Unit>::TResult;

    /**
     * Class with variable unit diff.
     */
    struct TCivilDiff {
        TDiff Value = 0;
        ECivilUnit Unit = ECivilUnit::Second;

        TCivilDiff() = default;
        TCivilDiff(TDiff value, ECivilUnit unit)
            : Value(value)
            , Unit(unit)
        {
        }

        /**
         * Straightfoward implementation of operators <, == and unit conversions
         * can be potentially misleading (e.g. 1 month == 30 days?);
         * we leave it to user to implement it properly for each application.
         */
    };

    /**
     * Gets the time zone by an IANA name.
     * @param name  A name in IANA format (e.g., "Europe/Moscow").
     * @note        After you request a time zone for the first time, it is cached
     *              in a (thread-safe) cache, so subsequent requests to the
     *              same time zone should be extremely fast.
     * @throw       TInvalidTimezone if the name is invalid.
     * @see         http://www.iana.org/time-zones
     */
    TTimeZone GetTimeZone(TStringBuf name);

    /**
     *Helper for get timezone offset from timezone string
     * Examples:
     * "+01:30" -> 5400
     * "-10" -> -36000
     * "-0200" -> -7200
     */
    bool TryParseOffset(TStringBuf name, int& offset);

    /**
     * Returns a time zone that is a fixed offset (seconds east) from UTC.
     * Note: If the absolute value of the offset is greater than 24 hours
     * you'll get UTC (i.e., zero offset) instead.
     */
    TTimeZone GetFixedTimeZone(const long offset);

    /** Convert civil time from one timezone to another
     * @param[in] src is source time with 'from' timezone
     * @param[in] from is a initial timezone
     * @param[in] from is a destination timezone
     * @return a civil time
     */
    template <typename T>
    T Convert(const T& src, const TTimeZone& from, const TTimeZone& to) {
        return cctz::convert(cctz::convert(src, from), to);
    }

    /** Convert absolute time to civil time by rules from timezone.
     * @param[in] absTime is an absolute time which is used to convert
     * @param[in] tz is a loaded timezone
     * @return a civil time
     *
     * Note: This function doesn't work properly for dates before 1 Jan 1970!
     */
    TCivilSecond Convert(const TInstant& absTime, const TTimeZone& tz);

    /** Convert absolute time to civil time by rules from timezone which will be loaded.
     * @throw InvalidTimezone if the name is invalid.
     * @param[in] absTime is an absolute time which is used to convert
     * @param[in] tzName is a timezone name which will be loaded
     * @return a civil time
     *
     * Note: This function doesn't work properly for dates before 1 Jan 1970!
     */
    TCivilSecond Convert(const TInstant& absTime, TStringBuf tzName);

    /** Convert a civil time to absolute by using rules from timezone
     *
     * Note: This function doesn't work properly for dates before 1 Jan 1970!
     */
    TInstant Convert(const TCivilSecond& s, const TTimeZone& tz);

    /** Just to simply calculations between dates/times.
     * NDatetime::Calc<TCivilDay>(TCivilSecond(2001, 1, 1, 10, 10, 10), 5); // returns TCivilDay(2001, 1, 6);
     * @param[in] tp is a timepoint with which calc will be
     * @param[in] diff is quantity of which will be added (of type T) to the tp
     * @return the calculated T type
     */
    template <typename T, typename S>
    inline T Calc(const S& tp, TDiff diff) {
        return T(tp) + diff;
    }

    /** Non-template methods for adding dates/times.
    * @param[in] tp is a timepoint with which calc will be
    * @param[in] diff is quantity of which will be added to the tp
    * @return the calculated TCivilSecond object
    */
    TCivilSecond AddYears(const TCivilSecond& tp, TDiff diff);
    TCivilSecond AddMonths(const TCivilSecond& tp, TDiff diff);
    TCivilSecond AddDays(const TCivilSecond& tp, TDiff diff);
    TCivilSecond AddHours(const TCivilSecond& tp, TDiff diff);
    TCivilSecond AddMinutes(const TCivilSecond& tp, TDiff diff);
    TCivilSecond AddSeconds(const TCivilSecond& tp, TDiff diff);

    /** Method to add TCivilDiff
     * @param[in] tp is a timepoint with which calc will be
     * @param[in] diff is quantity of which will be added to the tp
     * @return the calculated TCivilSecond object
     */
    TCivilSecond AddCivil(const TCivilSecond& tp, TCivilDiff diff);

    /** Method to subtract to civil dates/times and get TCivilDiff.
     * First casts to unit, then subtracts;
     * e.g. GetCivilDiff(2017-10-01, 2017-09-30, Month) = 1.
     *
     * @param[in] tpX is a timepoint
     * @param[in] tpY is a timepoint to subtract from tpX
     * @param[in] unit is a civil time unit to use in subtraction
     * @return the calculated diff as TCivilDiff object
     */
    TCivilDiff GetCivilDiff(const TCivilSecond& tpX, const TCivilSecond& tpY, ECivilUnit unit);

    /** Formats the given TimePoint in the given TTimeZone according to
     * the provided format string. Uses strftime()-like formatting options,
     * with the following extensions:
     *
     *   - %Ez  - RFC3339-compatible numeric time zone (+hh:mm or -hh:mm)
     *   - %E#S - Seconds with # digits of fractional precision
     *   - %E*S - Seconds with full fractional precision (a literal '*')
     *   - %E#f - Fractional seconds with # digits of precision
     *   - %E*f - Fractional seconds with full precision (a literal '*')
     *   - %E4Y - Four-character years (-999 ... -001, 0000, 0001 ... 9999)
     *
     * Note that %E0S behaves like %S, and %E0f produces no characters.  In
     * contrast %E*f always produces at least one digit, which may be '0'.
     *
     * Note that %Y produces as many characters as it takes to fully render the
     * year. A year outside of [-999:9999] when formatted with %E4Y will produce
     * more than four characters, just like %Y.
     *
     * Tip: Format strings should include the UTC offset (e.g., %z or %Ez) so that
     * the resultng string uniquely identifies an absolute time.
     *
     * Example:
     *   NDatetime::TTimeZone lax = NDatetime::GetTimeZone("America/Los_Angeles");
     *   NDatetime::TCivilSecond tp(2013, 1, 2, 3, 4, 5);
     *   TString f = NDatetime::Format("%H:%M:%S", tp, lax);      // "03:04:05"
     *   TString f = NDatetime::Format("%H:%M:%E3S", tp, lax);    //"03:04:05.000"
     */
    template <typename TP>
    TString Format(TStringBuf fmt, const TP& tp, const TTimeZone& tz) {
        return TString(cctz::format(static_cast<std::string>(fmt), TTimePoint(cctz::convert(tp, tz)), tz));
    }

    /** Returns the weekday by day.
     * @param[in] day is a given day
     * @return a weekday (enum)
     */
    CONSTEXPR_M TWeekday GetWeekday(const TCivilDay& day) noexcept {
        return cctz::get_weekday(day);
    }

    /** Returns the weekday by day.
     * @param[in] second is a given seconds
     * @return a weekday (enum)
     */
    CONSTEXPR_M TWeekday GetWeekday(const TCivilSecond& second) noexcept {
        return cctz::get_weekday(second);
    }

    /** Returns the TCivilDay that strictly follows or precedes the given
      * civil_day, and that falls on the given weekday.
      * @code
        For example, given:

            August 2015
        Su Mo Tu We Th Fr Sa
                           1
         2  3  4  5  6  7  8
         9 10 11 12 13 14 15
        16 17 18 19 20 21 22
        23 24 25 26 27 28 29
        30 31

        TCivilDay a(2015, 8, 13);  // GetWeekday(a) == TWeekday::thursday
        TCivilDay b = NextWeekday(a, TWeekday::thursday);  // b = 2015-08-20
        TCivilDay c = PrevWeekday(a, TWeekday::thursday);  // c = 2015-08-06
        TCivilDay d = NearestWeekday(a, TWeekday::thursday);  // d = 2015-08-13

        TCivilDay e = ...
        // Gets the following Thursday if e is not already Thursday
        TCivilDay thurs1 = PrevWeekday(e, TWeekday::thursday) + 7;
        // Gets the previous Thursday if e is not already Thursday
        TCivilDay thurs2 = NextWeekday(e, TWeekday::thursday) - 7;
     * @endcode
     * @see PrevWeekday()
     * @see NearestWeekday()
     * @param[in] cd is a current day
     * @param[in] wd is a weekday which wanetd for find on next week
     * @return a civil day on weekday next week
     */
    CONSTEXPR_M TCivilDay NextWeekday(const TCivilDay& cd, TWeekday wd) noexcept {
        return cctz::next_weekday(cd, wd);
    }

    /** Returns prev weekday. See the description of NextWeekday().
     * @see NextWeekday()
     * @see NearestWeekday()
     * @param[in] cd is a current day
     * @param[in] wd is a weekday which is looking for (backward)
     * @return a first occurence of the given weekday (backward)
     */
    CONSTEXPR_M TCivilDay PrevWeekday(const TCivilDay& cd, TWeekday wd) noexcept {
        return cctz::prev_weekday(cd, wd);
    }

    /** Find a nearest occurence of the given weekday forward (could be current day).
     * @see NextWeekday()
     * @param[in] cd is a current day
     * @param[in] wd is a weekday which is looking for (current day or later)
     * @return first occurence (including current day) of the given weekday
     */
    CONSTEXPR_M TCivilDay NearestWeekday(const TCivilDay& cd, TWeekday wd) noexcept {
        return get_weekday(cd) != wd ? next_weekday(cd, wd) : cd;
    }

    /** Find the date of the given weekday within the given week.
     * @param[in] cd is a current day
     * @param[in] wd is a requested week day
     * @return day within a week of a given day
     */
    CONSTEXPR_M TCivilDay WeekdayOnTheWeek(const TCivilDay& cd, TWeekday wd) noexcept {
        const auto d = get_weekday(cd);
        if (d == wd)
            return cd;

        return d < wd ? NextWeekday(cd, wd) : PrevWeekday(cd, wd);
    }

    /** Returns an absolute day of year by given day.
     */
    CONSTEXPR_M int GetYearDay(const TCivilDay& cd) noexcept {
        return cctz::get_yearday(cd);
    }

    CONSTEXPR_M int DaysPerMonth(TYear year, TMonth month) noexcept {
        return cctz::detail::impl::days_per_month(year, month);
    }

    CONSTEXPR_M int DaysPerYear(TYear year, TMonth month) noexcept {
        return cctz::detail::impl::days_per_year(year, month);
    }

    /** Calculate week number for the given date
     * @param[in] cd is a current day
     * @param[in] usePreviousYear (optional) true if calculate week number from previous year
     *
     * The week number starts from 1 for the first week, where Thursday exist (see ISO8601), i.e.
     * Jan 2021
     * week#  mo tu we th fr sa su
     * 53                  1  2  3
     * 01      4  5  6  7  8  9 10
     * 02     11 ...
     * Jan 2020
     * week#  mo tu we th fr sa su
     * 01            1  2  3  4  5
     * 02      6  7  8  9 10 11...
     *
     * In case if you received zero value, you may call function again with usePreviousYear=true
     * Also you may use usePreviousYear to calculate week difference between two dates in different year
     */
     CONSTEXPR_M int GetYearWeek(const TCivilDay& cd, bool usePreviousYear = false) noexcept {
         const auto jan1 = NDatetime::GetWeekday(NDatetime::TCivilDay{cd.year() - (usePreviousYear ? 1 : 0), 1, 1});
         const auto daysCount = GetYearDay(cd) + (usePreviousYear ? DaysPerYear(cd.year()-1, 1) : 0);

         return (daysCount + static_cast<int>(jan1) - 1) / 7 + (jan1 == cctz::weekday::monday || jan1 == cctz::weekday::tuesday || jan1 == cctz::weekday::wednesday);
     }
}

#include "civil-inl.h"

#undef CONSTEXPR_M
