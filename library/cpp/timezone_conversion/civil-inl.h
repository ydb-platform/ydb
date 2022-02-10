#pragma once

#include "civil.h"

namespace NDatetime {
    namespace NDetail {
        template <typename T>
        struct TGetCivilUnit;

        template <>
        struct TGetCivilUnit<TCivilSecond> {
            static constexpr ECivilUnit Value = ECivilUnit::Second;
        };
        template <>
        struct TGetCivilUnit<TCivilMinute> {
            static constexpr ECivilUnit Value = ECivilUnit::Minute;
        };
        template <>
        struct TGetCivilUnit<TCivilHour> {
            static constexpr ECivilUnit Value = ECivilUnit::Hour;
        };
        template <>
        struct TGetCivilUnit<TCivilDay> {
            static constexpr ECivilUnit Value = ECivilUnit::Day;
        };
        template <>
        struct TGetCivilUnit<TCivilMonth> {
            static constexpr ECivilUnit Value = ECivilUnit::Month;
        };
        template <>
        struct TGetCivilUnit<TCivilYear> {
            static constexpr ECivilUnit Value = ECivilUnit::Year;
        };

        template <ECivilUnit Unit>
        struct TGetCivilTime;

        template <>
        struct TGetCivilTime<ECivilUnit::Second> {
            using TResult = TCivilSecond;
        };
        template <>
        struct TGetCivilTime<ECivilUnit::Minute> {
            using TResult = TCivilMinute;
        };
        template <>
        struct TGetCivilTime<ECivilUnit::Hour> {
            using TResult = TCivilHour;
        };
        template <>
        struct TGetCivilTime<ECivilUnit::Day> {
            using TResult = TCivilDay;
        };
        template <>
        struct TGetCivilTime<ECivilUnit::Month> {
            using TResult = TCivilMonth;
        };
        template <>
        struct TGetCivilTime<ECivilUnit::Year> {
            using TResult = TCivilYear;
        };
    }
}
