#pragma once

#include <yql/essentials/public/udf/udf_value_builder.h>
#include <yql/essentials/minikql/mkql_type_ops.h>

#include <library/cpp/type_info/tz/tz.h>

#include <util/datetime/base.h>
#include <util/string/printf.h>

namespace NYql::DateTime {

constexpr size_t MAX_TIMEZONE_NAME_LEN = 64;

struct TTMStorage {
    unsigned int Year : 12;
    unsigned int DayOfYear : 9;
    unsigned int WeekOfYear : 6;
    unsigned int WeekOfYearIso8601 : 6;
    unsigned int DayOfWeek : 3;
    unsigned int Month : 4;
    unsigned int Day : 5;
    unsigned int Hour : 5;
    unsigned int Minute : 6;
    unsigned int Second : 6;
    unsigned int Microsecond : 20;
    unsigned int TimezoneId : 16;

    TTMStorage() {
        Zero(*this);
    }

    inline static bool IsUniversal(ui16 timezoneId) {
        return timezoneId == 0;
    }

    inline void MakeDefault() {
        Year = 1970;
        Month = 1;
        Day = 1;
        Hour = 0;
        Minute = 0;
        Second = 0;
        Microsecond = 0;
        TimezoneId = 0;
    }

    inline void FromDate(const NUdf::IDateBuilder& builder, ui16 value, ui16 timezoneId = 0) {
        ui32 year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek;

        if (!builder.FullSplitDate2(value, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, timezoneId)) {
            ythrow yexception() << "Error in FullSplitDate";
        }

        TimezoneId = timezoneId;

        Year = year;
        Month = month;
        Day = day;

        DayOfYear = dayOfYear;
        WeekOfYear = weekOfYear;
        WeekOfYearIso8601 = weekOfYearIso8601;
        DayOfWeek = dayOfWeek;
    }

    inline ui16 ToDate(const NUdf::IDateBuilder& builder, bool local) const {
        if (!IsUniversal(TimezoneId)) {
            ui32 datetime;
            if (!builder.MakeDatetime(Year, Month, Day, local ? 0 : Hour, local ? 0 : Minute, local ? 0 : Second, datetime, TimezoneId)) {
                ythrow yexception() << "Error in MakeDatetime";
            }
            return datetime / 86400u;
        } else {
            ui16 date;
            if (!builder.MakeDate(Year, Month, Day, date)) {
                ythrow yexception() << "Error in MakeDate";
            }
            return date;
        }
    }

    inline void FromDatetime(const NUdf::IDateBuilder& builder, ui32 value, ui16 timezoneId = 0) {
        ui32 year, month, day, hour, minute, second, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek;

        if (!builder.FullSplitDatetime2(value, year, month, day, hour, minute, second, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, timezoneId)) {
            ythrow yexception() << "Error in FullSplitDatetime";
        }

        TimezoneId = timezoneId;
        Year = year;
        Month = month;
        Day = day;
        Hour = hour;
        Minute = minute;
        Second = second;

        DayOfYear = dayOfYear;
        WeekOfYear = weekOfYear;
        WeekOfYearIso8601 = weekOfYearIso8601;
        DayOfWeek = dayOfWeek;
    }

    inline ui32 ToDatetime(const NUdf::IDateBuilder& builder) const {
        ui32 datetime = 0;
        if (!builder.MakeDatetime(Year, Month, Day, Hour, Minute, Second, datetime, TimezoneId)) {
            ythrow yexception() << "Error in MakeDatetime";
        }
        return datetime;
    }

    inline void FromTimestamp(const NUdf::IDateBuilder& builder, ui64 value, ui16 timezoneId = 0) {
        const ui32 seconds = value / 1000000ull;
        FromDatetime(builder, seconds, timezoneId);
        Microsecond = value - seconds * 1000000ull;
    }

    inline ui64 ToTimestamp(const NUdf::IDateBuilder& builder) const {
        return ToDatetime(builder) * 1000000ull + Microsecond;
    }

    inline bool Validate(const NUdf::IDateBuilder& builder) {
        ui32 datetime;
        if (!builder.MakeDatetime(Year, Month, Day, Hour, Minute, Second, datetime, TimezoneId)) {
            return false;
        }

        ui32 year, month, day, hour, minute, second, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek;
        if (!builder.FullSplitDatetime2(datetime, year, month, day, hour, minute, second, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, TimezoneId)) {
            ythrow yexception() << "Error in FullSplitDatetime.";
        }

        DayOfYear = dayOfYear;
        WeekOfYear = weekOfYear;
        WeekOfYearIso8601 = weekOfYearIso8601;
        DayOfWeek = dayOfWeek;

        return true;
    }

    inline void FromTimeOfDay(ui64 value) {
        Hour = value / 3600000000ull;
        value -= Hour * 3600000000ull;
        Minute = value / 60000000ull;
        value -= Minute * 60000000ull;
        Second = value / 1000000ull;
        Microsecond = value - Second * 1000000ull;
    }

    inline ui64 ToTimeOfDay() const {
        return ((Hour * 60ull + Minute) * 60ull + Second) * 1000000ull + Microsecond;
    }

    const TString ToString() const {
        const auto& tzName = NTi::GetTimezones()[TimezoneId];
        return Sprintf("%4d-%02d-%02dT%02d:%02d:%02d.%06d,%.*s",
                       Year, Month, Day, Hour, Minute, Second, Microsecond,
                       static_cast<int>(tzName.size()), tzName.data());
    }
};

static_assert(sizeof(TTMStorage) == 16, "TTMStorage size must be equal to TUnboxedValuePod size");

template<typename TStorage>
bool DoAddMonths(TStorage& storage, i64 months, const NUdf::IDateBuilder& builder) {
    i64 newMonth = months + storage.Month;
    storage.Year += (newMonth - 1) / 12;
    newMonth = 1 + (newMonth - 1) % 12;
    if (newMonth <= 0) {
        storage.Year--;
        newMonth += 12;
    }
    // The minimal year value for TTMStorage is 1970, but the
    // check below makes coverity happy.
    if constexpr (!std::is_same_v<TStorage, TTMStorage>) {
        if (storage.Year == 0) {
            storage.Year += months > 0 ? 1 : -1;
        }
    }
    storage.Month = newMonth;
    bool isLeap = NKikimr::NMiniKQL::IsLeapYear(storage.Year);
    ui32 monthLength = NKikimr::NMiniKQL::GetMonthLength(storage.Month, isLeap);
    storage.Day = std::min(monthLength, storage.Day);
    return storage.Validate(builder);
}

template<typename TStorage>
bool DoAddYears(TStorage& storage, i64 years, const NUdf::IDateBuilder& builder) {
    storage.Year += years;
    // The minimal year value for TTMStorage is 1970, but the
    // check below makes coverity happy.
    if constexpr (!std::is_same_v<TStorage, TTMStorage>) {
        if (storage.Year == 0) {
            storage.Year += years > 0 ? 1 : -1;
        }
    }
    if (storage.Month == 2 && storage.Day == 29) {
        bool isLeap = NKikimr::NMiniKQL::IsLeapYear(storage.Year);
        if (!isLeap) {
            storage.Day--;
        }
    }
    return storage.Validate(builder);
}

TInstant DoAddMonths(TInstant current, i64 months, const NUdf::IDateBuilder& builder);

TInstant DoAddYears(TInstant current, i64 years, const NUdf::IDateBuilder& builder);

}
