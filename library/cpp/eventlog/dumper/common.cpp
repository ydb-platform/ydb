#include "common.h"

#include <contrib/libs/re2/re2/re2.h>

#include <util/datetime/base.h>
#include <util/datetime/parser.h>
#include <util/string/cast.h>

static time_t RecentTime(int h, int m, int s) {
    time_t now = time(nullptr);
    tm tmTmp;
    localtime_r(&now, &tmTmp);
    tmTmp.tm_hour = h;
    tmTmp.tm_min = m;
    tmTmp.tm_sec = s;
    time_t today = mktime(&tmTmp);
    tmTmp.tm_mday -= 1;
    time_t yesterday = mktime(&tmTmp);
    return today <= now ? today : yesterday;
}

static bool ParseRecentTime(const TString& str, time_t& result) {
    RE2 RecentTimePattern("(\\d{1,2}):(\\d{2})(?::(\\d{2}))?");
    re2::StringPiece hStr, mStr, sStr;
    if (!RE2::FullMatch({str.data(), str.size()}, RecentTimePattern, &hStr, &mStr, &sStr)) {
        return false;
    }
    int h = FromString<int>(hStr.data(), hStr.length());
    int m = FromString<int>(mStr.data(), mStr.length());
    int s = FromString<int>(sStr.data(), sStr.length(), 0);
    if (h > 23 || m > 59 || s > 59) {
        return false;
    }
    result = RecentTime(h, m, s);
    return true;
}

namespace {
    class TDefaultOffset8601Parser: public TIso8601DateTimeParser {
    public:
        TDefaultOffset8601Parser(int offsetHours) {
            DateTimeFields.ZoneOffsetMinutes = offsetHours * 60;
        }
    };
} // namespace

static bool ParseISO8601DateTimeWithDefaultOffset(TStringBuf str, int offsetHours, time_t& result) {
    TDefaultOffset8601Parser parser{offsetHours};

    if (!parser.ParsePart(str.data(), str.size())) {
        return false;
    }

    const TInstant instant = parser.GetResult(TInstant::Max());
    if (instant == TInstant::Max()) {
        return false;
    }

    result = instant.TimeT();
    return true;
}

ui64 ParseTime(const TString& str, ui64 defValue, int offset) {
    if (!str) {
        return defValue;
    }

    time_t utcTime;

    if (ParseISO8601DateTimeWithDefaultOffset(str, offset, utcTime)) {
        return (ui64)utcTime * 1000000;
    }

    if (ParseRecentTime(str, utcTime)) {
        return (ui64)utcTime * 1000000;
    }

    // if conversion fails, TryFromString leaves defValue unchanged
    TryFromString<ui64>(str, defValue);
    return defValue;
}
