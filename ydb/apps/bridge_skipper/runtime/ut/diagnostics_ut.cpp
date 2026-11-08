#include <ydb/apps/bridge_skipper/runtime/diagnostics.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/cast.h>

#include <iterator>

namespace NBridgeSkipper {

Y_UNIT_TEST_SUITE(TDiagnostics) {
    Y_UNIT_TEST(RetentionAndCursorsReportEvictionAndBootGaps) {
        TDiagnosticStore store("boot");
        for (ui64 n = 1; n <= 1005; ++n) {
            NJson::TJsonValue detail(NJson::JSON_MAP);
            detail["generation"] = ::ToString(n);
            store.History("view", std::move(detail), TInstant::Seconds(n));
        }
        auto history = store.ReadHistory("boot:1");
        UNIT_ASSERT(history["coverage_gap"].GetBoolean());
        UNIT_ASSERT_VALUES_EQUAL(history["rows"].GetArray().size(), 1000);
        UNIT_ASSERT_VALUES_EQUAL(history["rows"][0]["cursor"].GetString(), "boot:6");
        UNIT_ASSERT_VALUES_EQUAL(history["retained_count"].GetUInteger(), 1000);
        UNIT_ASSERT_VALUES_EQUAL(history["dropped_count"].GetUInteger(), 5);
        UNIT_ASSERT_VALUES_EQUAL(history["oldest_at"].GetString(), TInstant::Seconds(6).ToString());
        history = store.ReadHistory("boot:1004");
        UNIT_ASSERT(!history["coverage_gap"].GetBoolean());
        UNIT_ASSERT_VALUES_EQUAL(history["rows"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(history["next_cursor"].GetString(), "boot:1005");
        UNIT_ASSERT_VALUES_EQUAL(history["dropped_count"].GetUInteger(), 5);
        UNIT_ASSERT(!history["tail_clipped"].GetBoolean());
        UNIT_ASSERT(store.ReadHistory()["coverage_gap"].GetBoolean());
        UNIT_ASSERT(store.ReadHistory("old-boot:1005")["coverage_gap"].GetBoolean());
        for (ui64 n = 0; n < 5005; ++n) {
            store.Log("info", "retained message", TInstant::Seconds(n + 1));
        }
        const auto logs = store.ReadLogs("boot:1");
        UNIT_ASSERT_VALUES_EQUAL(logs["rows"].GetArray().size(), 5000);
        UNIT_ASSERT_VALUES_EQUAL(logs["retained_count"].GetUInteger(), 5000);
        UNIT_ASSERT_VALUES_EQUAL(logs["dropped_count"].GetUInteger(), 5);
        UNIT_ASSERT_VALUES_EQUAL(logs["oldest_at"].GetString(), TInstant::Seconds(6).ToString());
        UNIT_ASSERT(logs["coverage_gap"].GetBoolean());
        UNIT_ASSERT(store.ReadLogs()["coverage_gap"].GetBoolean());
        UNIT_ASSERT(!store.ReadLogs("boot:5004")["coverage_gap"].GetBoolean());
        UNIT_ASSERT(!logs["debug_available"].GetBoolean());
    }

    Y_UNIT_TEST(FiltersKeepSeverityThresholdAndCaseInsensitiveSearchAndPile) {
        TDiagnosticStore store("boot", "debug");
        store.Log("debug", "PileA preparing", TInstant::Seconds(1));
        store.Log("warn", "PileA lost quorum", TInstant::Seconds(2));
        store.Log("error", "PileB lost quorum", TInstant::Seconds(3));
        const auto filtered = store.ReadLogs({}, "info", "LOST", "pilea");
        UNIT_ASSERT_VALUES_EQUAL(filtered["rows"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(filtered["rows"][0]["level"].GetString(), "warn");
        UNIT_ASSERT_VALUES_EQUAL(filtered["next_cursor"].GetString(), "boot:3");
        UNIT_ASSERT(filtered["debug_available"].GetBoolean());
    }

    Y_UNIT_TEST(TailReadsPreserveMatchingOrderCursorsAndWholeStreamMetadata) {
        TDiagnosticStore store("boot", "debug");
        for (ui64 n = 1; n <= 6; ++n) {
            store.Log(n % 2 ? "debug" : "warn", n % 2 ? "pileB record" : "pileA record", TInstant::Seconds(n));
            store.History("view", NJson::TJsonValue(NJson::JSON_MAP), TInstant::Seconds(n));
        }
        auto logs = store.ReadLogs({}, "info", "RECORD", "pilea", 2);
        UNIT_ASSERT_VALUES_EQUAL(logs["rows"].GetArray().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(logs["rows"][0]["cursor"].GetString(), "boot:4");
        UNIT_ASSERT_VALUES_EQUAL(logs["rows"][1]["cursor"].GetString(), "boot:6");
        UNIT_ASSERT_VALUES_EQUAL(logs["retained_count"].GetUInteger(), 6);
        UNIT_ASSERT_VALUES_EQUAL(logs["total_count"].GetUInteger(), 6);
        UNIT_ASSERT_VALUES_EQUAL(logs["oldest_at"].GetString(), TInstant::Seconds(1).ToString());
        UNIT_ASSERT_VALUES_EQUAL(logs["next_cursor"].GetString(), "boot:6");
        UNIT_ASSERT(!logs["coverage_gap"].GetBoolean());
        UNIT_ASSERT(logs["tail_clipped"].GetBoolean());
        logs = store.ReadLogs("boot:2", "info", {}, "pilea", 1);
        UNIT_ASSERT_VALUES_EQUAL(logs["rows"][0]["cursor"].GetString(), "boot:6");
        UNIT_ASSERT(!logs["coverage_gap"].GetBoolean());
        UNIT_ASSERT(logs["tail_clipped"].GetBoolean());
        logs = store.ReadLogs("boot:4", "info", {}, "pilea", 1);
        UNIT_ASSERT_VALUES_EQUAL(logs["rows"].GetArray().size(), 1);
        UNIT_ASSERT(!logs["coverage_gap"].GetBoolean());
        UNIT_ASSERT(!logs["tail_clipped"].GetBoolean());
        logs = store.ReadLogs("boot:6", "info", {}, "pilea", 1);
        UNIT_ASSERT(logs["rows"].GetArray().empty());
        UNIT_ASSERT(!logs["coverage_gap"].GetBoolean());
        UNIT_ASSERT_VALUES_EQUAL(store.ReadLogs({}, "info", {}, "pilea")["rows"].GetArray().size(), 3);
        UNIT_ASSERT(store.ReadLogs({}, {}, "absent", {}, 2)["rows"].GetArray().empty());

        auto history = store.ReadHistory("boot:2", 2);
        UNIT_ASSERT_VALUES_EQUAL(history["rows"].GetArray().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(history["rows"][0]["cursor"].GetString(), "boot:5");
        UNIT_ASSERT_VALUES_EQUAL(history["rows"][1]["cursor"].GetString(), "boot:6");
        UNIT_ASSERT(!history["coverage_gap"].GetBoolean());
        UNIT_ASSERT(history["tail_clipped"].GetBoolean());
        UNIT_ASSERT_VALUES_EQUAL(history["retained_count"].GetUInteger(), 6);
        UNIT_ASSERT_VALUES_EQUAL(history["next_cursor"].GetString(), "boot:6");
        UNIT_ASSERT(!store.ReadHistory("boot:4", 2)["coverage_gap"].GetBoolean());
        UNIT_ASSERT(store.ReadHistory("old:6", 2)["coverage_gap"].GetBoolean());
        UNIT_ASSERT(store.ReadHistory({}, 0)["rows"].GetArray().empty());
        UNIT_ASSERT_VALUES_EQUAL(store.ReadHistory()["rows"].GetArray().size(), 6);
    }

    Y_UNIT_TEST(FreshStreamsDistinguishTailClippingFromCoverageLoss) {
        TDiagnosticStore store("fresh");
        UNIT_ASSERT(!store.ReadHistory()["coverage_gap"].GetBoolean());
        UNIT_ASSERT(!store.ReadLogs()["coverage_gap"].GetBoolean());
        for (ui64 n = 1; n <= 4; ++n) {
            store.History("view", NJson::TJsonValue(NJson::JSON_MAP), TInstant::Seconds(n));
            store.Log("info", "Fresh retained record", TInstant::Seconds(n));
        }
        for (const auto& excerpt : {store.ReadHistory({}, 2), store.ReadLogs({}, {}, {}, {}, 2)}) {
            UNIT_ASSERT_VALUES_EQUAL(excerpt["rows"].GetArray().size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(excerpt["retained_count"].GetUInteger(), 4);
            UNIT_ASSERT_VALUES_EQUAL(excerpt["dropped_count"].GetUInteger(), 0);
            UNIT_ASSERT(!excerpt["coverage_gap"].GetBoolean());
            UNIT_ASSERT(excerpt["tail_clipped"].GetBoolean());
        }
        for (const auto& complete : {store.ReadHistory(), store.ReadLogs()}) {
            UNIT_ASSERT_VALUES_EQUAL(complete["rows"].GetArray().size(), 4);
            UNIT_ASSERT(!complete["coverage_gap"].GetBoolean());
            UNIT_ASSERT(!complete["tail_clipped"].GetBoolean());
        }
        for (const TString cursor : {"fresh:999", "previous-boot:4"}) {
            UNIT_ASSERT(store.ReadHistory(cursor)["coverage_gap"].GetBoolean());
            UNIT_ASSERT(store.ReadLogs(cursor)["coverage_gap"].GetBoolean());
        }
    }

    Y_UNIT_TEST(HistoryPublishesCanonicalSummariesAndOriginLabels) {
        TDiagnosticStore store("history");
        const std::pair<TString, TString> origins[] = {
            {"baseline", "Baseline"}, {"inferred", "Inferred"}, {"external", "External/unknown"},
            {"manual", "Manual"}, {"auto", "Auto"}, {"operator", "Operator"},
            {"skipper", "Skipper"}, {"unrecognized", "External/unknown"}
        };
        for (const auto& [origin, label] : origins) {
            NJson::TJsonValue detail(NJson::JSON_MAP);
            detail["origin"] = origin;
            detail["message"] = "Observed pile state transition";
            store.History("view", std::move(detail), TInstant::Seconds(1));
        }
        const auto history = store.ReadHistory();
        for (size_t index = 0; index < std::size(origins); ++index) {
            const auto& row = history["rows"][index];
            UNIT_ASSERT_VALUES_EQUAL(row["origin"].GetString(), origins[index].first);
            UNIT_ASSERT_VALUES_EQUAL(row["origin_label"].GetString(), origins[index].second);
            UNIT_ASSERT_VALUES_EQUAL(row["summary"].GetString(), "Observed pile state transition");
        }
    }

    Y_UNIT_TEST(LogSummaryIsSharedForPlainAndStructuredRecords) {
        TDiagnosticStore store("boot");
        store.Log("info", "Plain log message", TInstant::Seconds(1));
        store.Log("info", R"({"component":"SKIPPER","event":"ready","message":""})", TInstant::Seconds(2));
        store.Log("info", R"({"component":"SKIPPER","event":"ready","message":"Controller ready"})", TInstant::Seconds(3));
        store.Log("info", "123", TInstant::Seconds(4));
        store.Log("info", "[]", TInstant::Seconds(5));
        const auto logs = store.ReadLogs();
        UNIT_ASSERT_VALUES_EQUAL(logs["rows"][0]["summary"].GetString(), "Plain log message");
        UNIT_ASSERT_VALUES_EQUAL(logs["rows"][1]["summary"].GetString(), "SKIPPER · ready");
        UNIT_ASSERT_VALUES_EQUAL(logs["rows"][2]["summary"].GetString(), "SKIPPER · ready · Controller ready");
        UNIT_ASSERT_VALUES_EQUAL(logs["rows"][3]["summary"].GetString(), "123");
        UNIT_ASSERT_VALUES_EQUAL(logs["rows"][4]["summary"].GetString(), "[]");
        UNIT_ASSERT_VALUES_EQUAL(store.ReadLogs({}, {}, "SKIPPER · ready")["rows"].GetArray().size(), 2);
    }

    Y_UNIT_TEST(CompletedCapacityNeverEvictsAnInFlightOperation) {
        TDiagnosticStore store("boot");
        store.Operation("inflight", NJson::TJsonValue(NJson::JSON_MAP), true);
        for (ui64 n = 0; n < 1002; ++n) {
            store.Operation("done-" + ::ToString(n), NJson::TJsonValue(NJson::JSON_MAP), false);
        }
        UNIT_ASSERT(store.ReadOperation("inflight"));
        UNIT_ASSERT(!store.ReadOperation("done-0"));
        UNIT_ASSERT(!store.ReadOperation("done-1"));
        UNIT_ASSERT(store.ReadOperation("done-2"));
        UNIT_ASSERT(store.ReadOperation("done-1001"));
    }
}

} // namespace NBridgeSkipper
