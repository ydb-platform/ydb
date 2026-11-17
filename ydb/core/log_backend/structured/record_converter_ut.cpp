#include "record_converter.h"

#include <library/cpp/logger/record.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/charset/utf8.h>
#include <util/string/escape.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(StructuredLogConverter) {
    Y_UNIT_TEST(PreservesNativeFieldsAndTimestamp) {
        const TString data = "legacy (id=18446744073709551615)";
        TLogRecord record(TLOG_INFO, data.data(), data.size(), {
            {"_ydb_timestamp_us", "123456789"},
            {"_ydb_message", "brief"},
            {"_component", "TEST"},
            {"_priority", "INFO"},
            {"meta.message.id", "18446744073709551615"},
            {"labels.operation", "Read"},
        });
        const auto converted = MakeStructuredLogRecordConverter()->Convert(record);
        UNIT_ASSERT_VALUES_EQUAL(converted.Payload, "legacy (id=18446744073709551615)");
        UNIT_ASSERT(converted.Timestamp.Defined());
        UNIT_ASSERT_VALUES_EQUAL(converted.Timestamp->MicroSeconds(), 123456789);
        UNIT_ASSERT_VALUES_EQUAL(converted.Meta->at("_priority"), "INFO");
        UNIT_ASSERT_VALUES_EQUAL(converted.Meta->at("_ydb_message"), "brief");
        UNIT_ASSERT_VALUES_EQUAL(converted.Meta->at("meta.message.id"), "18446744073709551615");
        UNIT_ASSERT_VALUES_EQUAL(converted.Meta->at("labels.operation"), "Read");
        UNIT_ASSERT_VALUES_EQUAL(converted.Meta->at("_ydb_timestamp_us"), "123456789");
    }

    Y_UNIT_TEST(StripsOnlyMatchingLeadingTime) {
        const TString time = "1970-01-01T00:00:01.234567Z";
        const TString otherTime = "1970-01-01T00:00:01.234568Z :TEST INFO: hello";
        for (const auto& [data, expected] : TVector<std::pair<TString, TString>>{
            {time + " :TEST INFO: hello", ":TEST INFO: hello"},
            {otherTime, otherTime},
            {time, time},
        }) {
            TLogRecord record(TLOG_INFO, data.data(), data.size(), {{"_ydb_timestamp_us", "1234567"}});
            const auto converted = MakeStructuredLogRecordConverter()->Convert(record);
            UNIT_ASSERT_VALUES_EQUAL(converted.Payload, expected);
        }
    }

    Y_UNIT_TEST(PreservesEmptyBodyAndEpoch) {
        TLogRecord record(TLOG_INFO, "", 0, {
            {"_ydb_timestamp_us", "0"},
            {"_ydb_message", ""},
        });
        const auto converted = MakeStructuredLogRecordConverter()->Convert(record);
        UNIT_ASSERT(converted.Timestamp.Defined());
        UNIT_ASSERT_VALUES_EQUAL(converted.Timestamp->MicroSeconds(), 0);
        UNIT_ASSERT_VALUES_EQUAL(converted.Payload, "");
        UNIT_ASSERT_VALUES_EQUAL(converted.Meta->at("_ydb_message"), "");
    }

    Y_UNIT_TEST(EscapesInvalidUtf8Meta) {
        const TString bad = "bad\xff";
        const TString data = "legacy " + bad;
        TLogRecord record(TLOG_INFO, data.data(), data.size(), {
            {"_ydb_timestamp_us", "1"},
            {"_ydb_message", bad},
            {"meta.message.key", bad},
            {"meta.message.ok", "ok"},
        });
        const auto converted = MakeStructuredLogRecordConverter()->Convert(record);
        UNIT_ASSERT_VALUES_EQUAL(converted.Payload, data);
        for (const auto& [key, value] : *converted.Meta) {
            UNIT_ASSERT_C(IsUtf(value), key);
        }
        UNIT_ASSERT_VALUES_EQUAL(converted.Meta->at("_ydb_message"), EscapeC(bad));
        UNIT_ASSERT_VALUES_EQUAL(converted.Meta->at("meta.message.key"), EscapeC(bad));
        UNIT_ASSERT_VALUES_EQUAL(converted.Meta->at("meta.message.ok"), "ok");
    }

    Y_UNIT_TEST(OldAndIncompleteRecordsUseDefaultConverter) {
        const TString data = "original\n";
        const TLogRecord::TMetaFlags noTimestamp = {{"_ydb_message", "brief"}};
        const TLogRecord::TMetaFlags badTimestamp = {{"_ydb_timestamp_us", "invalid"}, {"_ydb_message", "brief"}};
        for (const auto& flags : {noTimestamp, badTimestamp}) {
            TLogRecord record(TLOG_INFO, data.data(), data.size(), flags);
            const auto expected = NUnifiedAgent::MakeDefaultRecordConverter()->Convert(record);
            const auto actual = MakeStructuredLogRecordConverter()->Convert(record);
            UNIT_ASSERT_VALUES_EQUAL(actual.Payload, expected.Payload);
            UNIT_ASSERT(actual.Meta == expected.Meta);
            UNIT_ASSERT(!actual.Timestamp.Defined());
        }
    }
}

} // namespace NKikimr
