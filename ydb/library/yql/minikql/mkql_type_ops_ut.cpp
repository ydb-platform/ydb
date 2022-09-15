#include "mkql_alloc.h"
#include "mkql_type_ops.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/format.h>
#include <util/stream/str.h>
using namespace NYql;
using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

Y_UNIT_TEST_SUITE(TMiniKQLTypeOps) {
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
        UNIT_ASSERT_VALUES_EQUAL(count, 597);
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

    Y_UNIT_TEST(TimestampSeriailization) {
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
