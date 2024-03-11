#include <library/cpp/timezone_conversion/convert.h>
#include <library/cpp/testing/unittest/gtest.h>

using namespace NDatetime;

template <>
void Out<TSimpleTM>(IOutputStream& os, TTypeTraits<TSimpleTM>::TFuncParam value) {
    os << value.ToString() << ", dst: " << int(value.IsDst);
}

TSimpleTM ZonedTm(i32 utcHours, bool isDst, ui32 year, ui32 mon, ui32 day, ui32 h, ui32 m, ui32 s) {
    TSimpleTM res(year, mon, day, h, m, s);
    res.GMTOff = utcHours * 60 * 60;
    res.IsDst = isDst;
    return res;
}

void CompareCivilTimes(const TSimpleTM& expected, const TSimpleTM& actual) {
    EXPECT_EQ(expected.GMTOff, actual.GMTOff);
    EXPECT_EQ(expected.Year, actual.Year);
    EXPECT_EQ(expected.Mon, actual.Mon);
    EXPECT_EQ(expected.MDay, actual.MDay);
    EXPECT_EQ(expected.WDay, actual.WDay);
    EXPECT_EQ(expected.Hour, actual.Hour);
    EXPECT_EQ(expected.Min, actual.Min);
    EXPECT_EQ(expected.Sec, actual.Sec);
    EXPECT_EQ(expected.IsDst, actual.IsDst);
    EXPECT_EQ(expected.IsLeap, actual.IsLeap);
}

#define CHECK_ROUND_TRIP(tz, unixTime, civil) \
    EXPECT_EQ(                                \
        TInstant::Seconds(unixTime),          \
        ToAbsoluteTime(civil, tz));           \
    CompareCivilTimes(                        \
        civil,                                \
        ToCivilTime(TInstant::Seconds(unixTime), tz));

// Tests only unambiguous civil times (i.e., those that occurred exactly once).
TEST(TimeZoneConversion, Simple) {
    TTimeZone msk = GetTimeZone("Europe/Moscow");
    // Before and after the temporary switch to UTC+3 in 2010.
    CHECK_ROUND_TRIP(
        msk,
        1288475999,
        ZonedTm(+4, true, 2010, 10, 31, 1, 59, 59));
    CHECK_ROUND_TRIP(
        msk,
        1288475999 + 3 * 60 * 60,
        ZonedTm(+3, false, 2010, 10, 31, 3, 59, 59));

    // Before and after the permanent switch to UTC+4 in 2011.
    CHECK_ROUND_TRIP(
        msk,
        1301180399,
        ZonedTm(+3, false, 2011, 3, 27, 1, 59, 59));
    CHECK_ROUND_TRIP(
        msk,
        1301180399 + 60 * 60,
        ZonedTm(+4, false, 2011, 3, 27, 3, 59, 59));

    // Some random moment between 2011 and 2014 when UTC+4 (no DST) was in place.
    CHECK_ROUND_TRIP(
        msk,
        1378901234,
        ZonedTm(+4, false, 2013, 9, 11, 16, 7, 14));

    // As of right now (i.e., as I'm writing this) Moscow is in UTC+3 (no DST).
    CHECK_ROUND_TRIP(
        msk,
        1458513396,
        ZonedTm(+3, false, 2016, 3, 21, 1, 36, 36));

    // Please add a new test if the current president moves Moscow back to UTC+4
    // or introduces DST again.
}

TEST(TimeZoneConversion, TestRepeatedDate) {
    TTimeZone ekb = GetTimeZone("Asia/Yekaterinburg");

    CompareCivilTimes(
        ZonedTm(+6, true, 2010, 10, 31, 2, 30, 0),
        ToCivilTime(TInstant::Seconds(1288470600), ekb));

    CompareCivilTimes(
        ZonedTm(+5, false, 2010, 10, 31, 2, 30, 0),
        ToCivilTime(TInstant::Seconds(1288474200), ekb));

    CompareCivilTimes(
        ZonedTm(+5, false, 2016, 5, 10, 9, 8, 7),
        CreateCivilTime(ekb, 2016, 5, 10, 9, 8, 7));

    CompareCivilTimes(
        ZonedTm(+6, true, 2010, 10, 31, 2, 30, 0),
        CreateCivilTime(ekb, 2010, 10, 31, 2, 30, 0));

    // The earlier timestamp should be chosen.
    EXPECT_EQ(
        TInstant::Seconds(1288470600),
        ToAbsoluteTime(TSimpleTM(2010, 10, 31, 2, 30, 0), ekb));
}

TEST(TimeZoneConversion, TestSkippedDate) {
    TTimeZone nsk = GetTimeZone("Asia/Novosibirsk");

    CompareCivilTimes(
        ZonedTm(+6, false, 2011, 3, 27, 1, 30, 0),
        ToCivilTime(TInstant::Seconds(1301167800), nsk));

    CompareCivilTimes(
        ZonedTm(+7, false, 2011, 3, 27, 3, 30, 0),
        ToCivilTime(TInstant::Seconds(1301171400), nsk));

    EXPECT_EQ(
        TInstant::Seconds(1301171400),
        ToAbsoluteTime(TSimpleTM(2011, 3, 27, 2, 30, 0), nsk));

    EXPECT_EQ(
        TInstant::Seconds(1301171400),
        ToAbsoluteTime(TSimpleTM(2011, 3, 27, 3, 30, 0), nsk));
}

TEST(TimeZoneConversion, Utc) {
    CHECK_ROUND_TRIP(
        GetUtcTimeZone(),
        1451703845,
        ZonedTm(0, false, 2016, 1, 2, 3, 4, 5));
}

TEST(TimeZoneConversion, Local) {
    TTimeZone local = GetLocalTimeZone();
    auto nowAbsolute = TInstant::Now();
    auto nowCivilLocal = ToCivilTime(nowAbsolute, local);
    EXPECT_EQ(nowAbsolute.Seconds(), ToAbsoluteTime(nowCivilLocal, local).Seconds());
}

TEST(TimeZoneConversion, BeforeEpoch) {
    {
        //NOTE: This test will not work because NDatetime::Convert() with TInstant does not work properly for dates before 1/1/1970
        NDatetime::TCivilSecond civilTime = NDatetime::TCivilSecond{1969, 12, 1, 0, 0, 0};
        TInstant absTime = NDatetime::Convert(civilTime, NDatetime::GetUtcTimeZone());
        NDatetime::TCivilSecond civilTime2 = NDatetime::Convert(absTime, NDatetime::GetUtcTimeZone());
        EXPECT_NE(civilTime2, civilTime); // ERROR. Must be EXPECT_EQ, but Convert() functions with TInstant doesnot wotk properly for dates before EPOCH
    }

    // Right test
    NDatetime::TCivilSecond civilTime = NDatetime::TCivilSecond{1969, 12, 1, 0, 0, 0};
    NDatetime::TCivilSecond civilTime2 = Convert<NDatetime::TCivilSecond>(civilTime, NDatetime::GetUtcTimeZone(), NDatetime::GetUtcTimeZone());
    EXPECT_EQ(civilTime2, civilTime);

}

TEST(TimeZoneConversion, InvalidTimeZone) {
    EXPECT_THROW(GetTimeZone("Europe/Mscow"), yexception);
    EXPECT_THROW(GetTimeZone(""), yexception);
}

TEST(TimeZoneConversion, TestSaratov) {
    TTimeZone saratov = GetTimeZone("Europe/Saratov");

    CompareCivilTimes(
        ZonedTm(+4, false, 2016, 12, 5, 1, 55, 35),
        ToCivilTime(TInstant::Seconds(1480888535), saratov));

    CompareCivilTimes(
        ZonedTm(+3, false, 2016, 12, 1, 0, 55, 35),
        ToCivilTime(TInstant::Seconds(1480542935), saratov));
}

TEST(TimeZoneConversion, TestFutureDstChanges) {
    TTimeZone london = GetTimeZone("Europe/London");

    // This test assumes the British won't cancel DST before 2025.
    // I don't think they will, but then again, nobody really expected Brexit.

    // DST is still in effect in early October 2025, meaning we are in UTC+1.
    CHECK_ROUND_TRIP(
        london,
        1760124660,
        ZonedTm(+1, true, 2025, 10, 10, 20, 31, 0));

    // 31 days later we're back to UTC+0 again.
    CHECK_ROUND_TRIP(
        london,
        1760124660 + 31 * 24 * 60 * 60,
        ZonedTm(+0, false, 2025, 11, 10, 19, 31, 0));
}

TEST(TimeZoneConversion, TWDay) {
    TTimeZone nsk = GetTimeZone("Asia/Novosibirsk");

    for (time_t e = 1301167800, to = 1301167800 + 86400 * 7, dow = 0; e < to; e += 86400, ++dow) {
        EXPECT_EQ(dow, ToCivilTime(TInstant::Seconds(e), nsk).WDay);
    }
}

TEST(TimeZoneConversion, TestBaikonur) {
    // Yes, the Baikonur spaceport is located in Kyzylorda Region.
    const auto baikonur = GetTimeZone("Asia/Qyzylorda");

    CompareCivilTimes(
        ZonedTm(+5, false, 2019, 1, 11, 23, 55, 23),
        ToCivilTime(TInstant::Seconds(1547232923), baikonur));
}

TEST(TimeZoneConversion, DEVTOOLSSUPPORT_41537) {
    // https://mm.icann.org/pipermail/tz-announce/2024-February/000081.html:
    //   Kazakhstan unifies on UTC+5 beginning 2024-03-01
    //   Asia/Almaty and Asia/Qostanay [...] will transition from UTC+6
    //   on 2024-03-01 at 00:00 to join the western portion

    // > TZ=UTC date --date="2024-03-04 12:34:56" +%s
    // 1709555696
    const auto tmAfterTransition = TInstant::Seconds(1709555696);
    for (const auto* tzName : {"Asia/Almaty", "Asia/Qostanay"}) {
        const auto tz = GetTimeZone(tzName);
        CompareCivilTimes(
            ZonedTm(+5, false, 2024, 3, 4, 17, 34, 56),
            ToCivilTime(tmAfterTransition, tz));
    }
}
