#include "common.h"

#include <ydb/library/testlib/solomon_helpers/solomon_emulator_helpers.h>
#include <library/cpp/json/json_reader.h>
#include <fmt/format.h>

namespace NKikimr::NKqp {
namespace {

const NTestUtils::TSolomonLocation ReplayMetrics{"history-replay", "tests", "custom", false};

class THistoryReplayFixture : public TStreamingTestFixture {
public:
    void Init(bool enabled = true, bool readFrom = false, bool compressedGraph = false) {
        auto& config = SetupAppConfig();
        if (compressedGraph) {
            config.MutableQueryServiceConfig()->SetQueryArtifactsCompressionMinSize(0);
            config.MutableQueryServiceConfig()->SetQueryArtifactsCompressionMethod("zstd_6");
        }
        config.MutableFeatureFlags()->SetEnableStreamingQueryStateRecompute(enabled);
        if (readFrom) {
            config.MutableFeatureFlags()->SetEnableStreamingQueryReadFrom(true);
            config.MutableFeatureFlags()->SetEnableStreamingQueryDisposition(false);
        }
        config.MutableTableServiceConfig()->SetEnableWatermarks(true);
        config.MutableTableServiceConfig()->SetEnableWatermarksAdvanced(true);
        CreateTopic("historyInput");
        CreatePqSource("historySource");
        CreateSolomonSource("historySink");
        NTestUtils::CleanupSolomon(ReplayMetrics);
    }

    std::string Body(ui32 window, const TString& sensor) {
        return fmt::format(R"(
            DO BEGIN
                $input = SELECT CAST(ts AS Timestamp) AS event_time, k FROM historySource.historyInput WITH (
                    FORMAT = json_each_row,
                    SCHEMA (ts String NOT NULL, k String NOT NULL),
                    WATERMARK = CAST(ts AS Timestamp) - Interval('PT5S'), WATERMARK_IDLE_TIMEOUT = "PT1H"
                );
                INSERT INTO historySink.`history-replay/tests/custom`
                SELECT HOP_END() AS ts, COUNT(*) AS value, "{}" AS sensor
                FROM $input WHERE k = "data"
                GROUP BY HoppingWindow(event_time, 'PT1S', 'PT{}S')
            END DO
        )", sensor, window);
    }

    void WaitCheckpoint() {
        const auto completed = GetCounters()->GetSubgroup("subsystem", "checkpoint_coordinator")
            ->GetCounter("CompletedCheckpoints", true);
        const auto initial = completed->Val();
        // One in-flight barrier may precede the last observed output. The next
        // two completed checkpoints guarantee that the input offset is durable.
        NTestUtils::WaitFor(TDuration::Seconds(15), "durable streaming checkpoint", [&](TString& error) {
            error = TStringBuilder() << "Completed " << completed->Val() << ", need " << initial + 2;
            return completed->Val() >= initial + 2;
        });
    }

    void WriteEvent(TInstant time, TStringBuf key = "data") {
        WriteTopicMessage("historyInput", fmt::format(R"({{"ts":"{}","k":"{}"}})", time.ToString(), key));
    }

    void WaitMetric(const TString& sensor, ui64 timestamp, ui64 value) {
        const auto deadline = TInstant::Now() + TDuration::Seconds(40);
        TString metrics;
        do {
            metrics = NTestUtils::GetSolomonMetrics(ReplayMetrics);
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(metrics, &json));
            for (const auto& metric : json.GetArraySafe()) {
                bool matches = false;
                for (const auto& label : metric["labels"].GetArraySafe()) {
                    matches |= label[0].GetStringSafe() == "sensor" && label[1].GetStringSafe() == sensor;
                }
                if (matches && metric["ts"].GetUIntegerRobust() == timestamp) {
                    UNIT_ASSERT_VALUES_EQUAL_C(metric["value"].GetDoubleRobust(), value, metrics);
                    return;
                }
            }
            Sleep(TDuration::MilliSeconds(100));
        } while (TInstant::Now() < deadline);
        UNIT_FAIL("Missing replay metric " << sensor << " at " << timestamp << ": " << metrics
            << "; query issues: " << GetStreamingQueryIssues("historyQuery"));
    }

    void AssertNoEarlierMetrics(const TString& sensor, ui64 firstOutputSecond) {
        NJson::TJsonValue json;
        const auto metrics = NTestUtils::GetSolomonMetrics(ReplayMetrics);
        UNIT_ASSERT(NJson::ReadJsonTree(metrics, &json));
        for (const auto& metric : json.GetArraySafe()) {
            for (const auto& label : metric["labels"].GetArraySafe()) {
                if (label[0].GetStringSafe() == "sensor" && label[1].GetStringSafe() == sensor) {
                    UNIT_ASSERT_C(metric["ts"].GetUIntegerRobust() >= firstOutputSecond, metrics);
                }
            }
        }
    }
};

} // namespace

Y_UNIT_TEST_SUITE(StreamingHistoryReplay) {
    Y_UNIT_TEST_TWIN_F(ExplicitOutputFromOnCreateAlterAndReplace, CompressedGraph, THistoryReplayFixture) {
        Init(/* enabled */ true, /* readFrom */ false, CompressedGraph);
        const auto base = TInstant::Seconds(TInstant::Now().Seconds());
        for (ui32 second = 0; second < 10; ++second) {
            WriteEvent(base + TDuration::Seconds(second));
        }
        WriteEvent(base + TDuration::Seconds(16), "watermark");
        // No previous query or checkpoint exists. Round an unaligned output
        // timestamp up, and warm up the first complete window from history.
        ExecQuery(fmt::format("CREATE STREAMING QUERY historyQuery WITH (OUTPUT_FROM = Timestamp(\"{}\")) AS {}",
            (base + TDuration::MilliSeconds(5500)).ToString(), Body(3, "explicit")));
        WaitMetric("explicit", base.Seconds() + 6, 3);
        AssertNoEarlierMetrics("explicit", base.Seconds() + 6);
        WaitCheckpoint();
        ExecQuery(fmt::format("ALTER STREAMING QUERY historyQuery SET (OUTPUT_FROM = Timestamp(\"{}\")) AS {}",
            (base + TDuration::Seconds(8)).ToString(), Body(2, "explicit-alter")));
        WaitMetric("explicit-alter", base.Seconds() + 8, 2);
        AssertNoEarlierMetrics("explicit-alter", base.Seconds() + 8);
        WaitCheckpoint();
        ExecQuery(fmt::format("CREATE OR REPLACE STREAMING QUERY historyQuery WITH (OUTPUT_FROM = Timestamp(\"{}\")) AS {}",
            (base + TDuration::Seconds(7)).ToString(), Body(3, "explicit-replace")));
        WaitMetric("explicit-replace", base.Seconds() + 7, 3);
        AssertNoEarlierMetrics("explicit-replace", base.Seconds() + 7);
        WaitCheckpoint();
        ExecQuery("ALTER STREAMING QUERY historyQuery SET (RUN = FALSE)");
        WriteEvent(base + TDuration::Seconds(17));
        WriteEvent(base + TDuration::Seconds(18));
        WriteEvent(base + TDuration::Seconds(19));
        WriteEvent(base + TDuration::Seconds(26), "watermark");
        ExecQuery("ALTER STREAMING QUERY historyQuery SET (RUN = TRUE)");
        WaitMetric("explicit-replace", base.Seconds() + 20, 3);
    }

    Y_UNIT_TEST_F(ExplicitOutputFromRepositionsWithoutTextChange, THistoryReplayFixture) {
        Init();
        const auto base = TInstant::Seconds(TInstant::Now().Seconds());
        ExecQuery(fmt::format("CREATE STREAMING QUERY historyQuery WITH (OUTPUT_FROM = Timestamp(\"{}\")) AS {}",
            (base + TDuration::Seconds(1000)).ToString(), Body(3, "reposition")));
        for (ui32 second = 0; second < 10; ++second) {
            WriteEvent(base + TDuration::Seconds(second));
        }
        WriteEvent(base + TDuration::Seconds(16), "watermark");
        WaitCheckpoint();
        ExecQuery(fmt::format("ALTER STREAMING QUERY historyQuery SET (OUTPUT_FROM = Timestamp(\"{}\") + Interval(\"PT1S\"))",
            (base + TDuration::Seconds(5)).ToString()));
        WaitMetric("reposition", base.Seconds() + 6, 3);
        AssertNoEarlierMetrics("reposition", base.Seconds() + 6);
    }

    Y_UNIT_TEST_F(ExplicitOutputFromRejectsMissingWatermarksEvenWithForce, THistoryReplayFixture) {
        Init();
        CreateTopic("historyOutput");
        ExecQuery(R"(
            CREATE STREAMING QUERY historyQuery WITH (
                FORCE = TRUE, OUTPUT_FROM = Timestamp("2025-05-04T11:30:34.336938Z")
            ) AS DO BEGIN
                INSERT INTO historySource.historyOutput SELECT Data FROM historySource.historyInput
            END DO
        )", NYdb::EStatus::BAD_REQUEST, "requires an input watermark generator");
    }

    Y_UNIT_TEST_F(OutputFromWithReadFromOnCreateAlterAndReplace, THistoryReplayFixture) {
        Init(/* enabled */ true, /* readFrom */ true);
        const auto base = TInstant::Seconds(TInstant::Now().Seconds());
        for (ui32 second = 3; second < 6; ++second) {
            WriteEvent(base + TDuration::Seconds(second));
        }
        Sleep(TDuration::Seconds(1));
        const auto readFrom = TInstant::Now();
        Sleep(TDuration::Seconds(1));
        for (ui32 second = 3; second < 6; ++second) {
            WriteEvent(base + TDuration::Seconds(second));
        }
        WriteEvent(base + TDuration::Seconds(16), "watermark");

        // Both batches have the same event times. Only READ_FROM can exclude the first one.
        ExecQuery(fmt::format(R"(
            $read = Timestamp("{}");
            $output = Timestamp("{}");
            CREATE STREAMING QUERY historyQuery WITH (
                READ_FROM = $read, OUTPUT_FROM = $output + Interval("PT1S")
            ) AS {})", readFrom.ToString(), (base + TDuration::Seconds(5)).ToString(), Body(3, "read-from-time")));
        WaitMetric("read-from-time", base.Seconds() + 6, 3);
        AssertNoEarlierMetrics("read-from-time", base.Seconds() + 6);
        WaitCheckpoint();

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY historyQuery SET (
                READ_FROM = EARLIEST, OUTPUT_FROM = Timestamp("{}")
            ) AS {})", (base + TDuration::Seconds(6)).ToString(), Body(3, "read-from-earliest")));
        WaitMetric("read-from-earliest", base.Seconds() + 6, 6);
        AssertNoEarlierMetrics("read-from-earliest", base.Seconds() + 6);
        WaitCheckpoint();

        ExecQuery(fmt::format(R"(
            CREATE OR REPLACE STREAMING QUERY historyQuery WITH (
                READ_FROM = LATEST, OUTPUT_FROM = Timestamp("{}")
            ) AS {})", (base + TDuration::Seconds(6)).ToString(), Body(3, "read-from-latest")));
        Sleep(TDuration::Seconds(1));
        for (ui32 second = 17; second < 20; ++second) {
            WriteEvent(base + TDuration::Seconds(second));
        }
        WriteEvent(base + TDuration::Seconds(26), "watermark");
        WaitMetric("read-from-latest", base.Seconds() + 20, 3);
        AssertNoEarlierMetrics("read-from-latest", base.Seconds() + 18);
    }

    Y_UNIT_TEST_TWIN_F(ChangedWindowReplaysConsumedHistory, Force, THistoryReplayFixture) {
        Init();
        ExecQuery("CREATE STREAMING QUERY historyQuery AS " + Body(3, "old"));
        const auto base = TInstant::Seconds(TInstant::Now().Seconds());
        for (ui32 second = 0; second < 10; ++second) {
            WriteEvent(base + TDuration::Seconds(second));
        }
        WriteEvent(base + TDuration::Seconds(10), "watermark");
        WaitMetric("old", base.Seconds() + 5, 3);
        WaitCheckpoint();

        ExecQuery(std::string("ALTER STREAMING QUERY historyQuery SET (FORCE = ") + (Force ? "TRUE" : "FALSE") + ") AS " + Body(2, "new"));
        // This window consists entirely of records consumed by the old query.
        // Restoring its source offsets would never produce this metric.
        WaitMetric("new", base.Seconds() + 3, 2);
        WriteEvent(base + TDuration::Seconds(16), "watermark");
        WaitMetric("new", base.Seconds() + 10, 2);
        WaitCheckpoint();
        ExecQuery("ALTER STREAMING QUERY historyQuery SET (RUN = FALSE)");
        WriteEvent(base + TDuration::Seconds(16));
        WriteEvent(base + TDuration::Seconds(17));
        WriteEvent(base + TDuration::Seconds(24), "watermark");
        ExecQuery("ALTER STREAMING QUERY historyQuery SET (RUN = TRUE)");
        WaitMetric("new", base.Seconds() + 18, 2);
        WaitCheckpoint();
        ExecQuery("CREATE OR REPLACE STREAMING QUERY historyQuery AS " + Body(2, "replaced"));
        WaitMetric("replaced", base.Seconds() + 18, 2);
    }

    Y_UNIT_TEST_TWIN_F(StatefulSinkRequiresForce, CompressedGraph, THistoryReplayFixture) {
        Init(true, false, CompressedGraph);
        CreateTopic("historyOutput");
        ExecQuery(R"(
            CREATE STREAMING QUERY historyQuery AS DO BEGIN
                INSERT INTO historySource.historyOutput SELECT Data FROM historySource.historyInput
            END DO
        )");
        WriteTopicMessage("historyInput", "old");
        ReadTopicMessages("historyOutput", {"old"});
        WaitCheckpoint();
        const std::string body = R"( AS DO BEGIN
            INSERT INTO historySource.historyOutput SELECT "new:" || Data FROM historySource.historyInput
        END DO)";
        ExecQuery("ALTER STREAMING QUERY historyQuery SET (FORCE = FALSE)" + body,
            NYdb::EStatus::BAD_REQUEST, "sink has state");
        ExecQuery("ALTER STREAMING QUERY historyQuery SET (FORCE = TRUE)" + body);
        WriteTopicMessage("historyInput", "live");
        ReadTopicMessages("historyOutput", {"old", "new:live"});
    }

    Y_UNIT_TEST_F(DisabledFlagPreservesExistingTextChangeRules, THistoryReplayFixture) {
        Init(false);
        CreateTopic("historyOutput");
        ExecQuery(R"(
            CREATE STREAMING QUERY historyQuery AS DO BEGIN
                INSERT INTO historySource.historyOutput SELECT Data FROM historySource.historyInput
            END DO
        )");
        WriteTopicMessage("historyInput", "old");
        ReadTopicMessages("historyOutput", {"old"});
        WaitCheckpoint();
        const std::string body = R"( AS DO BEGIN
            INSERT INTO historySource.historyOutput SELECT "new:" || Data FROM historySource.historyInput
        END DO)";
        ExecQuery("ALTER STREAMING QUERY historyQuery SET (FORCE = FALSE)" + body,
            NYdb::EStatus::PRECONDITION_FAILED, "Please use FORCE=true");
        ExecQuery("ALTER STREAMING QUERY historyQuery SET (FORCE = TRUE)" + body);
        WriteTopicMessage("historyInput", "live");
        ReadTopicMessages("historyOutput", {"old", "new:live"});
    }

    Y_UNIT_TEST_TWIN_F(ReadFromBypassesHistoryReplay, Replace, THistoryReplayFixture) {
        Init(/* enabled */ true, /* readFrom */ true);
        CreateTopic("historyOutput");
        ExecQuery(R"(
            CREATE STREAMING QUERY historyQuery AS DO BEGIN
                INSERT INTO historySource.historyOutput SELECT Data FROM historySource.historyInput
            END DO
        )");
        WriteTopicMessage("historyInput", "old");
        ReadTopicMessages("historyOutput", {"old"});
        WaitCheckpoint();
        // This sink cannot replay history. Explicit READ_FROM must still work without FORCE.
        ExecQuery(std::string(Replace
            ? "CREATE OR REPLACE STREAMING QUERY historyQuery WITH (READ_FROM = LATEST)"
            : "ALTER STREAMING QUERY historyQuery SET (READ_FROM = LATEST)") + R"( AS DO BEGIN
                INSERT INTO historySource.historyOutput SELECT "new:" || Data FROM historySource.historyInput
            END DO)");
        WaitCheckpoint();
        WriteTopicMessage("historyInput", "live");
        ReadTopicMessages("historyOutput", {"old", "new:live"});
    }

    Y_UNIT_TEST_TWIN_F(NestedWindowsReplayCompleteAggregates, ExplicitOutputFrom, THistoryReplayFixture) {
        Init();
        const auto body = [](ui32 innerWindow, TStringBuf sensor) {
            return fmt::format(R"( DO BEGIN
                $input = SELECT CAST(ts AS Timestamp) AS event_time, k FROM historySource.historyInput WITH (
                    FORMAT = json_each_row, SCHEMA(ts String NOT NULL, k String NOT NULL),
                    WATERMARK = CAST(ts AS Timestamp) - Interval('PT5S'), WATERMARK_IDLE_TIMEOUT = "PT1H"
                );
                $inner = SELECT HOP_END() AS event_time, COUNT(*) AS n FROM $input WHERE k = "data"
                    GROUP BY HoppingWindow(event_time, 'PT1S', 'PT{}S');
                INSERT INTO historySink.`history-replay/tests/custom`
                    SELECT HOP_END() AS ts, SUM(n) AS value, "{}" AS sensor FROM $inner
                    GROUP BY HoppingWindow(event_time, 'PT2S', 'PT4S')
                END DO)", innerWindow, sensor);
        };
        ExecQuery("CREATE STREAMING QUERY historyQuery AS " + body(3, "old-nested"));
        const auto base = TInstant::Seconds(TInstant::Now().Seconds() / 2 * 2);
        for (ui32 second = 0; second < 20; ++second) {
            WriteEvent(base + TDuration::Seconds(second));
        }
        WriteEvent(base + TDuration::Seconds(20), "watermark");
        WaitMetric("old-nested", base.Seconds() + 14, 12);
        WaitCheckpoint();
        if constexpr (ExplicitOutputFrom) {
            ExecQuery(fmt::format("ALTER STREAMING QUERY historyQuery SET (OUTPUT_FROM = Timestamp(\"{}\")) AS {}",
                (base + TDuration::Seconds(12)).ToString(), body(2, "new-nested")));
        } else {
            ExecQuery("ALTER STREAMING QUERY historyQuery SET (FORCE = FALSE) AS " + body(2, "new-nested"));
        }
        WaitMetric("new-nested", base.Seconds() + 12, 8);
        if constexpr (ExplicitOutputFrom) {
            AssertNoEarlierMetrics("new-nested", base.Seconds() + 12);
        }
    }
}

} // namespace NKikimr::NKqp
