#include "common.h"

#include <fmt/format.h>
#include <library/cpp/json/json_reader.h>
#include <ydb/library/testlib/solomon_helpers/solomon_emulator_helpers.h>

namespace NKikimr::NKqp {
namespace {

const NTestUtils::TSolomonLocation ReplayMetrics{"history-replay", "tests", "custom", false};

class THistoryReplayFixture : public TStreamingTestFixture {
public:
    void Init(bool enabled = true, bool readFrom = false, bool compressedGraph = false, bool sharedReading = false) {
        auto& config = SetupAppConfig();
        if (compressedGraph) {
            config.MutableQueryServiceConfig()->SetQueryArtifactsCompressionMinSize(0);
            config.MutableQueryServiceConfig()->SetQueryArtifactsCompressionMethod("zstd_6");
        }
        config.MutableFeatureFlags()->SetEnableStreamingQueryStateRecompute(enabled);
        config.MutableFeatureFlags()->SetEnableStreamingQueriesPqSinkDeduplication(true);
        config.MutableFeatureFlags()->SetEnableSharedReadingInStreamingQueries(sharedReading);
        config.MutableFeatureFlags()->SetEnableSharedReadingStructuredJsonParsing(sharedReading);
        if (readFrom) {
            config.MutableFeatureFlags()->SetEnableStreamingQueryReadFrom(true);
            config.MutableFeatureFlags()->SetEnableStreamingQueryDisposition(false);
        }
        config.MutableTableServiceConfig()->SetEnableWatermarks(true);
        config.MutableTableServiceConfig()->SetEnableWatermarksAdvanced(true);
        if (sharedReading) {
            config.MutableFeatureFlags()->SetEnableStreamingQueriesCounters(false);
        }
        CreateTopic("historyInput");
        if (sharedReading) {
            ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");
            ExecQuery(fmt::format(R"(
                CREATE EXTERNAL DATA SOURCE historySource WITH (
                    SOURCE_TYPE = "Ydb", LOCATION = "{}", DATABASE_NAME = "{}", AUTH_METHOD = "NONE",
                    SHARED_READING = "true", SHARED_READING_GROUP = "history-replay"
                );)", YDB_ENDPOINT, YDB_DATABASE));
        } else {
            CreatePqSource("historySource");
        }
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
    Y_UNIT_TEST_TWIN_F(SharedHoppingRequiresProgramWatermarkGenerator, Force, THistoryReplayFixture) {
        Init(/* enabled */ true, /* readFrom */ false, /* compressedGraph */ false, /* sharedReading */ true);
        ExecQuery("CREATE STREAMING QUERY historyQuery AS " + Body(3, "shared-old"));
        const auto base = TInstant::Seconds(TInstant::Now().Seconds());
        for (ui32 second = 0; second < 10; ++second) {
            WriteEvent(base + TDuration::Seconds(second));
        }
        WriteEvent(base + TDuration::Seconds(16), "watermark");
        WaitMetric("shared-old", base.Seconds() + 6, 3);
        WaitCheckpoint();
        const auto counters = GetCounters()->GetSubgroup("subsystem", "row_dispatcher");
        UNIT_ASSERT_GT(counters->GetCounter("SessionDataRate", true)->Val(), 0);
        ExecQuery(std::string("ALTER STREAMING QUERY historyQuery SET (FORCE = ") + (Force ? "TRUE" : "FALSE") + ") AS " + Body(2, "shared-new"),
            Force ? NYdb::EStatus::SUCCESS : NYdb::EStatus::BAD_REQUEST,
            Force ? "" : "requires a watermark generator before each hopping operator");
        if constexpr (Force) {
            WriteEvent(base + TDuration::Seconds(17));
            WriteEvent(base + TDuration::Seconds(18));
            WriteEvent(base + TDuration::Seconds(26), "watermark");
            WaitMetric("shared-new", base.Seconds() + 19, 2);
        }
        ExecQuery(fmt::format("CREATE STREAMING QUERY sharedExplicit WITH (OUTPUT_FROM = Timestamp(\"{}\")) AS {}",
            (base + TDuration::Seconds(6)).ToString(), Body(2, "shared-explicit")),
            NYdb::EStatus::BAD_REQUEST, "requires a watermark generator before each hopping operator");
    }

    Y_UNIT_TEST_F(SharedStatelessReaderContinuesWithoutForce, THistoryReplayFixture) {
        Init(/* enabled */ true, /* readFrom */ false, /* compressedGraph */ false, /* sharedReading */ true);
        const auto body = [](TStringBuf sensor) {
            return fmt::format(R"(DO BEGIN
                INSERT INTO historySink.`history-replay/tests/custom`
                SELECT CAST(ts AS Timestamp) AS ts, 1u AS value, "{}" AS sensor
                FROM historySource.historyInput WITH (
                    FORMAT = json_each_row, SCHEMA (ts String NOT NULL, k String NOT NULL)
                ) WHERE k = "data"
            END DO)", sensor);
        };
        // Keep the shared topic session alive while historyQuery is replaced.
        ExecQuery("CREATE STREAMING QUERY historyCompanion AS " + body("shared-companion"));
        ExecQuery("CREATE STREAMING QUERY historyQuery AS " + body("shared-old"));
        const auto base = TInstant::Seconds(TInstant::Now().Seconds());
        WriteEvent(base);
        WaitMetric("shared-companion", base.Seconds(), 1);
        WaitMetric("shared-old", base.Seconds(), 1);
        WaitCheckpoint();
        const auto counters = GetCounters()->GetSubgroup("subsystem", "row_dispatcher");
        UNIT_ASSERT_GT(counters->GetCounter("SessionDataRate", true)->Val(), 0);
        ExecQuery("ALTER STREAMING QUERY historyQuery SET (FORCE = FALSE) AS " + body("shared-new"));
        WriteEvent(base + TDuration::Seconds(1));
        WaitMetric("shared-new", base.Seconds() + 1, 1);
        AssertNoEarlierMetrics("shared-new", base.Seconds() + 1);
    }

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

    Y_UNIT_TEST_TWIN_F(StatelessOutputFromUsesRequestedReadPosition, ReadFrom, THistoryReplayFixture) {
        Init(/* enabled */ true, ReadFrom);
        CreateTopic("historyOutput");
        WriteTopicMessage("historyInput", "before-output-from");
        Sleep(TDuration::Seconds(1));
        const auto outputFrom = TInstant::Now();
        Sleep(TDuration::Seconds(1));
        WriteTopicMessage("historyInput", "retained");
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY historyQuery WITH (OUTPUT_FROM = Timestamp("{}"){}) AS DO BEGIN
                INSERT INTO historySource.historyOutput SELECT Data FROM historySource.historyInput
            END DO
        )", outputFrom.ToString(), ReadFrom ? ", READ_FROM = EARLIEST" : ""));
        // Without watermarks, OUTPUT_FROM sets the input position unless an
        // explicit READ_FROM overrides it. Both paths must read retained data.
        auto expected = ReadFrom
            ? std::vector<std::string>{"before-output-from", "retained"}
            : std::vector<std::string>{"retained"};
        ReadTopicMessages("historyOutput", expected);
        WriteTopicMessage("historyInput", "live");
        expected.push_back("live");
        ReadTopicMessages("historyOutput", expected);
    }

    Y_UNIT_TEST_TWIN_F(OutputFromRejectsDisabledCheckpoints, ReadFrom, THistoryReplayFixture) {
        Init(/* enabled */ true, ReadFrom);
        CreateTopic("historyOutput");
        const std::string body = R"( AS DO BEGIN
            PRAGMA ydb.DisableCheckpoints = "TRUE";
            INSERT INTO historySource.historyOutput SELECT Data FROM historySource.historyInput
        END DO)";
        const std::string outputFrom = "OUTPUT_FROM = Timestamp(\"2025-05-04T11:30:34Z\")"
            + std::string(ReadFrom ? ", READ_FROM = EARLIEST" : "");
        ExecQuery("CREATE STREAMING QUERY historyQuery WITH (RUN = FALSE)" + body);
        for (const auto prefix : {
            "CREATE STREAMING QUERY rejectedOutput WITH (",
            "ALTER STREAMING QUERY historyQuery SET (FORCE = TRUE, ",
            "CREATE OR REPLACE STREAMING QUERY historyQuery WITH (FORCE = TRUE, ",
        }) {
            ExecQuery(std::string(prefix) + "RUN = TRUE, " + outputFrom + ")" + body,
                NYdb::EStatus::GENERIC_ERROR, "Cannot use setting OUTPUT_FROM without checkpoints");
        }

        ExecQuery("CREATE STREAMING QUERY withoutCheckpoints" + body);
        WriteTopicMessage("historyInput", "live");
        ReadTopicMessages("historyOutput", {"live"});
        // Validate OUTPUT_FROM even when ALTER supplies no query text.
        ExecQuery("ALTER STREAMING QUERY withoutCheckpoints SET (FORCE = TRUE, " + outputFrom + ")",
            NYdb::EStatus::GENERIC_ERROR, "Cannot use setting OUTPUT_FROM without checkpoints");
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

    Y_UNIT_TEST_TWIN_F(StatelessOutputReplaysEarlyArrivingEvents, KeepWatermarks, THistoryReplayFixture) {
        Init();
        ExecQuery("CREATE STREAMING QUERY historyQuery AS " + Body(3, "early-old"));
        // These records are accepted by the watermark generator but their write
        // times precede the event-time frontier saved in the old checkpoint.
        const auto base = TInstant::Seconds(TInstant::Now().Seconds()) + TDuration::Minutes(2);
        for (ui32 second = 0; second < 10; ++second) {
            WriteEvent(base + TDuration::Seconds(second));
        }
        WriteEvent(base + TDuration::Seconds(10), "watermark");
        WaitMetric("early-old", base.Seconds() + 5, 3);
        WaitCheckpoint();

        ExecQuery(fmt::format(R"(ALTER STREAMING QUERY historyQuery SET (FORCE = FALSE) AS DO BEGIN
            INSERT INTO historySink.`history-replay/tests/custom`
            SELECT CAST(ts AS Timestamp) AS ts, 1u AS value, "early-new" AS sensor
            FROM historySource.historyInput WITH (
                FORMAT = json_each_row, SCHEMA (ts String NOT NULL, k String NOT NULL){}
            ) WHERE k = "data"
        END DO)", KeepWatermarks
            ? ", WATERMARK = CAST(ts AS Timestamp) - Interval('PT5S'), WATERMARK_IDLE_TIMEOUT = \"PT1H\""
            : ""));
        WaitMetric("early-new", base.Seconds() + 6, 1);
    }

    Y_UNIT_TEST_TWIN_F(DeduplicatingPqSinkRequiresForce, CompressedGraph, THistoryReplayFixture) {
        Init(true, false, CompressedGraph);
        CreateTopic("historyOutput");
        ExecQuery(R"(
            CREATE STREAMING QUERY historyQuery AS DO BEGIN
                PRAGMA pq.EnableDeduplication = "TRUE";
                INSERT INTO historySource.historyOutput SELECT Data FROM historySource.historyInput
            END DO
        )");
        WriteTopicMessage("historyInput", "old");
        ReadTopicMessages("historyOutput", {"old"});
        WaitCheckpoint();
        const std::string body = R"( AS DO BEGIN
            PRAGMA pq.EnableDeduplication = "FALSE";
            INSERT INTO historySource.historyOutput SELECT "new:" || Data FROM historySource.historyInput
        END DO)";
        ExecQuery("ALTER STREAMING QUERY historyQuery SET (FORCE = FALSE)" + body,
            NYdb::EStatus::BAD_REQUEST, "PQ sinks with deduplication");
        ExecQuery("ALTER STREAMING QUERY historyQuery SET (FORCE = TRUE)" + body);
        WriteTopicMessage("historyInput", "live");
        ReadTopicMessages("historyOutput", {"old", "new:live"});
    }

    Y_UNIT_TEST_TWIN_F(ConsumerAheadOfCheckpointIsRewoundOnTextChange, SharedReading, THistoryReplayFixture) {
        Init(/* enabled */ true, /* readFrom */ false, /* compressedGraph */ false, SharedReading);
        CreateTopic("historyOutput");
        const auto body = [](TStringBuf prefix) {
            return fmt::format(R"( AS DO BEGIN
                PRAGMA pq.Consumer = "test_consumer";
                PRAGMA pq.EnableDeduplication = "FALSE";
                INSERT INTO historySource.historyOutput SELECT "{}" || payload FROM historySource.historyInput
                    WITH (FORMAT = json_each_row, SCHEMA (payload String NOT NULL))
            END DO)", prefix);
        };
        const auto waitConsumer = [&](bool running) {
            NTestUtils::WaitFor(TDuration::Seconds(30), "consumer session", [&](TString& error) {
                const auto result = GetTopicClient()->DescribeConsumer("historyInput", "test_consumer",
                    NYdb::NTopic::TDescribeConsumerSettings().IncludeStats(true)).GetValue(TEST_OPERATION_TIMEOUT);
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                const auto& stats = result.GetConsumerDescription().GetPartitions().front().GetPartitionConsumerStats();
                UNIT_ASSERT(stats);
                error = TString(stats->GetReadSessionId());
                return error.empty() != running;
            });
        };
        ExecQuery("CREATE STREAMING QUERY historyQuery" + body(""));
        waitConsumer(true);
        WriteTopicMessage("historyInput", R"({"payload":"old"})");
        ReadTopicMessages("historyOutput", {"old"});
        WaitCheckpoint();
        ExecQuery("ALTER STREAMING QUERY historyQuery SET (RUN = FALSE)");
        waitConsumer(false);
        WriteTopicMessage("historyInput", R"({"payload":"queued"})");
        const auto committed = GetTopicClient()->CommitOffset("historyInput", 0, "test_consumer", 2).GetValue(TEST_OPERATION_TIMEOUT);
        UNIT_ASSERT_C(committed.IsSuccess(), committed.GetIssues().ToString());
        ExecQuery("ALTER STREAMING QUERY historyQuery SET (RUN = TRUE, FORCE = FALSE)" + body("new:"));
        waitConsumer(true);
        ReadTopicMessages("historyOutput", {"old", "new:queued"});
    }

    Y_UNIT_TEST_TWIN_F(StatelessQueryContinuesWithoutForce, NewDeduplication, THistoryReplayFixture) {
        Init();
        CreateTopic("historyOutput");
        ExecQuery(R"(
            CREATE STREAMING QUERY historyQuery AS DO BEGIN
                PRAGMA pq.EnableDeduplication = "FALSE";
                INSERT INTO historySource.historyOutput SELECT Data FROM historySource.historyInput
            END DO
        )");
        WriteTopicMessage("historyInput", "old");
        ReadTopicMessages("historyOutput", {"old"});
        WaitCheckpoint();
        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY historyQuery SET (FORCE = FALSE) AS DO BEGIN
                PRAGMA pq.EnableDeduplication = "{}";
                INSERT INTO historySource.historyOutput SELECT "new:" || Data FROM historySource.historyInput
            END DO
        )", NewDeduplication ? "TRUE" : "FALSE"));
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
                PRAGMA pq.EnableDeduplication = "TRUE";
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
                PRAGMA pq.EnableDeduplication = "TRUE";
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
