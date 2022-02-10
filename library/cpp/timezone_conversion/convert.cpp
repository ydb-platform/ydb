#include "convert.h"

#include <contrib/libs/cctz/include/cctz/civil_time.h>

#include <util/generic/yexception.h>

#include <chrono>

namespace NDatetime {
    static constexpr i64 TM_YEAR_OFFSET = 1900;
    using TSystemClock = std::chrono::system_clock;
    using TTimePoint = std::chrono::time_point<TSystemClock>;

    static TSimpleTM CivilToTM(const cctz::civil_second& cs, const cctz::time_zone::absolute_lookup& al) {
        cctz::civil_day cd(cs);
        TSimpleTM tm;
        tm.GMTOff = al.offset;
        tm.Year = cs.year() - TM_YEAR_OFFSET;
        tm.YDay = cctz::get_yearday(cd);
        tm.Mon = cs.month() - 1;
        tm.MDay = cs.day();
        tm.Hour = cs.hour();
        tm.Min = cs.minute();
        tm.Sec = cs.second();
        tm.IsDst = al.is_dst;
        tm.IsLeap = LeapYearAD(cs.year());

        switch (cctz::get_weekday(cd)) {
            case cctz::weekday::monday:
                tm.WDay = 1;
                break;
            case cctz::weekday::tuesday:
                tm.WDay = 2;
                break;
            case cctz::weekday::wednesday:
                tm.WDay = 3;
                break;
            case cctz::weekday::thursday:
                tm.WDay = 4;
                break;
            case cctz::weekday::friday:
                tm.WDay = 5;
                break;
            case cctz::weekday::saturday:
                tm.WDay = 6;
                break;
            case cctz::weekday::sunday:
                tm.WDay = 0;
                break;
        }

        return tm;
    }

    /*
    TTimeZone GetTimeZone(const TString& name) {
        TTimeZone result;
        if (!cctz::load_time_zone(name, &result)) {
            ythrow yexception() << "Failed to load time zone " << name << ", " << result.name();
        }
        return result;
    }
    */

    TTimeZone GetUtcTimeZone() {
        return cctz::utc_time_zone();
    }

    TTimeZone GetLocalTimeZone() {
        return cctz::local_time_zone();
    }

    TSimpleTM ToCivilTime(const TInstant& absoluteTime, const TTimeZone& tz) {
        TTimePoint tp = TSystemClock::from_time_t(absoluteTime.TimeT());
        return CivilToTM(cctz::convert(tp, tz), tz.lookup(tp));
    }

    TSimpleTM CreateCivilTime(const TTimeZone& tz, ui32 year, ui32 mon, ui32 day, ui32 h, ui32 m, ui32 s) {
        cctz::civil_second cs(year, mon, day, h, m, s);
        return CivilToTM(cs, tz.lookup(tz.lookup(cs).pre));
    }

    TInstant ToAbsoluteTime(const TSimpleTM& civilTime, const TTimeZone& tz) {
        cctz::civil_second cs(
            civilTime.Year + TM_YEAR_OFFSET,
            civilTime.Mon + 1,
            civilTime.MDay,
            civilTime.Hour,
            civilTime.Min,
            civilTime.Sec);
        return TInstant::Seconds(TSystemClock::to_time_t(tz.lookup(cs).pre));
    }
}
