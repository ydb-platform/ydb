#include "civil.h"

#include <util/stream/output.h>
#include <util/stream/format.h>
#include <util/string/ascii.h>

namespace {
    bool TryParseInt(TStringBuf& s, int& dst, size_t maxDigits) {
        int res = 0;
        size_t i = 0;
        while (i < maxDigits && !s.empty() && IsAsciiDigit(s[0])) {
            res = res * 10 + (s[0] - '0');
            ++i;
            s.Skip(1);
        }
        if (i == 0) {
            return false;
        }
        dst = res;
        return true;
    }

    bool TryParseUTCGMTOffsetTimezone(TStringBuf name, int& offset) {
        static constexpr TStringBuf OFFSET_PREFIX_UTC = "UTC";
        static constexpr TStringBuf OFFSET_PREFIX_GMT = "GMT";
        if (!name.SkipPrefix(OFFSET_PREFIX_UTC)) {
            // Sometimes timezones from client devices look like 'GMT+03:00'
            // This format is not standard but can be translated like UTC+xxx
            if (!name.SkipPrefix(OFFSET_PREFIX_GMT)) {
                return false;
            }
        }
        return NDatetime::TryParseOffset(name, offset);
    }
} // anonymous namespace

namespace NDatetime {
    bool TryParseOffset(TStringBuf name, int& offset) {
        if (name.empty()) {
            return false;
        }
        bool negative;
        if (name[0] == '+') {
            negative = false;
        } else if (name[0] == '-') {
            negative = true;
        } else {
            return false;
        }
        name.Skip(1);
        int hour;
        int minute = 0;
        if (!TryParseInt(name, hour, 2) || hour > 24) {
            return false;
        }
        if (!name.empty()) {
            if (name[0] == ':') {
                name.Skip(1);
            }
            if (!TryParseInt(name, minute, 2) || minute >= 60) {
                return false;
            }
            if (!name.empty()) {
                return false;
            }
        }
        if (hour == 24 && minute != 0) {
            return false;
        }
        offset = (hour * 60 + minute) * 60;
        if (negative)
            offset = -offset;
        return true;
    }

    TTimeZone GetTimeZone(TStringBuf name) {
        int offset;
        // Try to preparse constant timezones like:
        // UTC+03:00
        // GMT+03:00
        // Note constant timezones like 'Etc-03' will be handed in cctz library
        if (TryParseUTCGMTOffsetTimezone(name, offset)) {
            return GetFixedTimeZone(offset);
        }
        TTimeZone result;
        if (!cctz::load_time_zone(static_cast<std::string>(name), &result)) {
            ythrow TInvalidTimezone() << "Failed to load time zone " << name << ", " << result.name();
        }
        return result;
    }

    TTimeZone GetFixedTimeZone(const long offset) {
        return cctz::fixed_time_zone(std::chrono::seconds(offset));
    }

    TCivilSecond Convert(const TInstant& absTime, const TTimeZone& tz) {
        return cctz::convert(TSystemClock::from_time_t(absTime.TimeT()), tz);
    }

    TCivilSecond Convert(const TInstant& absTime, TStringBuf tzName) {
        TTimeZone tz = GetTimeZone(tzName);
        return cctz::convert(TSystemClock::from_time_t(absTime.TimeT()), tz);
    }

    TInstant Convert(const TCivilSecond& tp, const TTimeZone& tz) {
        return TInstant::Seconds(cctz::convert(tp, tz).time_since_epoch().count());
    }

    TCivilSecond AddYears(const TCivilSecond& tp, TDiff diff) {
        TCivilYear newYear = Calc<TCivilYear>(tp, diff);
        return NDatetime::TCivilSecond(newYear.year(), tp.month(), tp.day(), tp.hour(), tp.minute(), tp.second());
    }

    TCivilSecond AddMonths(const TCivilSecond& tp, TDiff diff) {
        TCivilMonth newMonth = Calc<TCivilMonth>(tp, diff);
        return NDatetime::TCivilSecond(newMonth.year(), newMonth.month(), tp.day(), tp.hour(), tp.minute(), tp.second());
    }

    TCivilSecond AddDays(const TCivilSecond& tp, TDiff diff) {
        TCivilDay newDay = Calc<TCivilDay>(tp, diff);
        return NDatetime::TCivilSecond(newDay.year(), newDay.month(), newDay.day(), tp.hour(), tp.minute(), tp.second());
    }

    TCivilSecond AddHours(const TCivilSecond& tp, TDiff diff) {
        TCivilHour newHour = Calc<TCivilHour>(tp, diff);
        return NDatetime::TCivilSecond(newHour.year(), newHour.month(), newHour.day(), newHour.hour(), tp.minute(), tp.second());
    }

    TCivilSecond AddMinutes(const TCivilSecond& tp, TDiff diff) {
        TCivilMinute newMinute = Calc<TCivilMinute>(tp, diff);
        return NDatetime::TCivilSecond(newMinute.year(), newMinute.month(), newMinute.day(), newMinute.hour(), newMinute.minute(), tp.second());
    }

    TCivilSecond AddSeconds(const TCivilSecond& tp, TDiff diff) {
        return Calc<TCivilSecond>(tp, diff);
    }

    TCivilSecond AddCivil(const TCivilSecond& tp, TCivilDiff diff) {
        switch (diff.Unit) {
            case ECivilUnit::Second: {
                return AddSeconds(tp, diff.Value);
            }
            case ECivilUnit::Minute: {
                return AddMinutes(tp, diff.Value);
            }
            case ECivilUnit::Hour: {
                return AddHours(tp, diff.Value);
            }
            case ECivilUnit::Day: {
                return AddDays(tp, diff.Value);
            }
            case ECivilUnit::Month: {
                return AddMonths(tp, diff.Value);
            }
            case ECivilUnit::Year: {
                return AddYears(tp, diff.Value);
            }
            default: {
                ythrow yexception() << "Unexpected civil unit value " << static_cast<int>(diff.Unit);
            }
        }
    }

    TCivilDiff GetCivilDiff(const TCivilSecond& tpX, const TCivilSecond& tpY, ECivilUnit unit) {
        switch (unit) {
            case ECivilUnit::Second: {
                return {tpX - tpY, unit};
            }
            case ECivilUnit::Minute: {
                return {static_cast<TCivilMinute>(tpX) - static_cast<TCivilMinute>(tpY), unit};
            }
            case ECivilUnit::Hour: {
                return {static_cast<TCivilHour>(tpX) - static_cast<TCivilHour>(tpY), unit};
            }
            case ECivilUnit::Day: {
                return {static_cast<TCivilDay>(tpX) - static_cast<TCivilDay>(tpY), unit};
            }
            case ECivilUnit::Month: {
                return {static_cast<TCivilMonth>(tpX) - static_cast<TCivilMonth>(tpY), unit};
            }
            case ECivilUnit::Year: {
                return {static_cast<TCivilYear>(tpX) - static_cast<TCivilYear>(tpY), unit};
            }
            default: {
                ythrow yexception() << "Unexpected civil unit value " << static_cast<int>(unit);
            }
        }
    }
}

template <>
void Out<NDatetime::TCivilYear>(IOutputStream& out, const NDatetime::TCivilYear& y) {
    out << y.year();
}

template <>
void Out<NDatetime::TCivilMonth>(IOutputStream& out, const NDatetime::TCivilMonth& m) {
    out << NDatetime::TCivilYear(m) << '-' << LeftPad(m.month(), 2, '0');
}

template <>
void Out<NDatetime::TCivilDay>(IOutputStream& out, const NDatetime::TCivilDay& d) {
    out << NDatetime::TCivilMonth(d) << '-' << LeftPad(d.day(), 2, '0');
}

template <>
void Out<NDatetime::TCivilHour>(IOutputStream& out, const NDatetime::TCivilHour& h) {
    out << NDatetime::TCivilDay(h) << 'T' << LeftPad(h.hour(), 2, '0');
}

template <>
void Out<NDatetime::TCivilMinute>(IOutputStream& out, const NDatetime::TCivilMinute& m) {
    out << NDatetime::TCivilHour(m) << ':' << LeftPad(m.minute(), 2, '0');
}

template <>
void Out<NDatetime::TCivilSecond>(IOutputStream& out, const NDatetime::TCivilSecond& s) {
    out << NDatetime::TCivilMinute(s) << ':' << LeftPad(s.second(), 2, '0');
}

template <>
void Out<NDatetime::TWeekday>(IOutputStream& out, NDatetime::TWeekday wd) {
    using namespace cctz;
    switch (wd) {
        case weekday::monday:
            out << TStringBuf("Monday");
            break;
        case weekday::tuesday:
            out << TStringBuf("Tuesday");
            break;
        case weekday::wednesday:
            out << TStringBuf("Wednesday");
            break;
        case weekday::thursday:
            out << TStringBuf("Thursday");
            break;
        case weekday::friday:
            out << TStringBuf("Friday");
            break;
        case weekday::saturday:
            out << TStringBuf("Saturday");
            break;
        case weekday::sunday:
            out << TStringBuf("Sunday");
            break;
    }
}
