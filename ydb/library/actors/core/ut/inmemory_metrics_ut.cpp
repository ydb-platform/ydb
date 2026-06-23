#include "actor_benchmark_helper.h"
#include "subsystems/inmemory_metrics.h"

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <algorithm>
#include <chrono>
#include <random>
#include <thread>

using namespace NActors;
using namespace NActors::NTests;

Y_UNIT_TEST_SUITE(InMemoryMetrics) {

    using TActorBenchmark = ::NActors::NTests::TActorBenchmark<>;

    std::span<const TLabel> NoLabels() {
        return {};
    }

    bool IsSystemLine(const TLineSnapshot& line) {
        return line.Name.StartsWith("inmemory_metrics.");
    }

    TVector<TLineSnapshot> FilterUserLines(TVector<TLineSnapshot> lines) {
        lines.erase(std::remove_if(lines.begin(), lines.end(), [](const TLineSnapshot& line) {
            return IsSystemLine(line);
        }), lines.end());
        return lines;
    }

    const TLineSnapshot* FindLineByName(const TVector<TLineSnapshot>& lines, TStringBuf name) {
        for (const auto& line : lines) {
            if (line.Name == name) {
                return &line;
            }
        }
        return nullptr;
    }

    TVector<ui64> ReadValues(const TLineSnapshot& line) {
        TVector<ui64> values;
        line.ForEachRecord([&](const TRecordView& record) {
            values.push_back(record.Value);
        });
        return values;
    }

    TVector<ui64> ExpectedHeartbeatRead(ui64 first, ui64 second) {
        return {first, second};
    }

    Y_UNIT_TEST(SubsystemAccessor) {
        auto setup = TActorBenchmark::GetActorSystemSetup();
        TActorBenchmark::AddBasicPool(setup, 1, false, false);
        setup->RegisterSubSystem(MakeInMemoryMetricsRegistry({
            .MemoryBytes = 4096,
            .ChunkSizeBytes = 256,
            .MaxLines = 4,
        }));

        TActorSystem actorSystem(setup);
        UNIT_ASSERT_VALUES_EQUAL(GetInMemoryMetrics(actorSystem).GetConfig().MemoryBytes, 4096);
    }

    Y_UNIT_TEST(AppendAndSnapshot) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 1024,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
        });

        TLabel label{"kind", "basic"};
        auto writer = registry.CreateLine("line", std::span<const TLabel>(&label, 1));
        UNIT_ASSERT(writer);

        UNIT_ASSERT(writer.Append(10));
        UNIT_ASSERT(writer.Append(20));

        auto snapshot = registry.Snapshot();
        auto lines = FilterUserLines(snapshot.Lines());
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(lines[0].Name, "line");
        UNIT_ASSERT(!lines[0].Closed);
        UNIT_ASSERT_VALUES_EQUAL(lines[0].Chunks.size(), 1);

        TVector<ui64> values;
        TVector<TInstant> timestamps;
        lines[0].ForEachRecord([&](const TRecordView& record) {
            timestamps.push_back(record.Timestamp);
            values.push_back(record.Value);
        });

        UNIT_ASSERT_VALUES_EQUAL(values.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(values[0], 10);
        UNIT_ASSERT_VALUES_EQUAL(values[1], 20);
        UNIT_ASSERT(timestamps[0] <= timestamps[1]);
    }

    Y_UNIT_TEST(CommonLabelsAreStoredSeparately) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 1024,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
            .CommonLabels = {{
                TLabel{"node_id", "42"},
            }},
        });

        TLabel label{"kind", "basic"};
        auto writer = registry.CreateLine("line", std::span<const TLabel>(&label, 1));
        UNIT_ASSERT(writer);
        UNIT_ASSERT(writer.Append(10));

        auto snapshot = registry.Snapshot();
        auto lines = FilterUserLines(snapshot.Lines());
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.CommonLabels.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.CommonLabels[0].Name, "node_id");
        UNIT_ASSERT_VALUES_EQUAL(snapshot.CommonLabels[0].Value, "42");
        UNIT_ASSERT_VALUES_EQUAL(lines[0].Labels.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(lines[0].Labels[0].Name, "kind");
        UNIT_ASSERT_VALUES_EQUAL(lines[0].Labels[0].Value, "basic");
    }

    Y_UNIT_TEST(CommonLabelsCanBeUpdatedAfterLineCreation) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 1024,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
            .CommonLabels = {{
                TLabel{"node_id", "42"},
            }},
        });

        TLabel label{"kind", "basic"};
        auto writer = registry.CreateLine("line", std::span<const TLabel>(&label, 1));
        UNIT_ASSERT(writer);
        UNIT_ASSERT(writer.Append(10));

        const TVector<TLabel> updatedLabels = {
            {"node_id", "43"},
            {"host", "slot-1"},
        };
        registry.SetCommonLabels(updatedLabels);

        auto snapshot = registry.Snapshot();
        auto lines = FilterUserLines(snapshot.Lines());
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.CommonLabels.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.CommonLabels[0].Name, "node_id");
        UNIT_ASSERT_VALUES_EQUAL(snapshot.CommonLabels[0].Value, "43");
        UNIT_ASSERT_VALUES_EQUAL(snapshot.CommonLabels[1].Name, "host");
        UNIT_ASSERT_VALUES_EQUAL(snapshot.CommonLabels[1].Value, "slot-1");
        UNIT_ASSERT_VALUES_EQUAL(lines[0].Labels.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(lines[0].Labels[0].Name, "kind");
        UNIT_ASSERT_VALUES_EQUAL(lines[0].Labels[0].Value, "basic");

        auto duplicate = registry.CreateLine("line", std::span<const TLabel>(&label, 1));
        UNIT_ASSERT(!duplicate);
    }

    Y_UNIT_TEST(CreateLineStoresExplicitMetaInSnapshot) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 1024,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
        });

        const TLineMeta meta{
            .MetricKind = EMetricKind::Counter,
            .PublishPolicy = EPublishPolicy::OnChangeWithHeartbeat,
            .StorageEncoding = EStorageEncoding::RunLengthEncoded,
            .Heartbeat = TDuration::Seconds(15),
        };

        auto writer = registry.CreateLine("line", NoLabels(), meta);
        UNIT_ASSERT(writer);
        UNIT_ASSERT(writer.Append(10));

        auto snapshot = registry.Snapshot();
        auto lines = FilterUserLines(snapshot.Lines());
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);
        UNIT_ASSERT(lines[0].Meta.MetricKind == EMetricKind::Counter);
        UNIT_ASSERT(lines[0].Meta.PublishPolicy == EPublishPolicy::OnChangeWithHeartbeat);
        UNIT_ASSERT(lines[0].Meta.StorageEncoding == EStorageEncoding::RunLengthEncoded);
        UNIT_ASSERT_VALUES_EQUAL(lines[0].Meta.Heartbeat, TDuration::Seconds(15));
    }

    Y_UNIT_TEST(DifferentLinesCanUseDifferentMeta) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 1024,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
        });

        const TLineMeta rawMeta{
            .MetricKind = EMetricKind::Gauge,
            .PublishPolicy = EPublishPolicy::Raw,
            .StorageEncoding = EStorageEncoding::RawPoints,
        };
        const TLineMeta encodedMeta{
            .MetricKind = EMetricKind::Counter,
            .PublishPolicy = EPublishPolicy::OnChange,
            .StorageEncoding = EStorageEncoding::RunLengthEncoded,
            .Heartbeat = TDuration::Seconds(5),
        };

        auto rawWriter = registry.CreateLine("raw", NoLabels(), rawMeta);
        auto encodedWriter = registry.CreateLine("encoded", NoLabels(), encodedMeta);
        UNIT_ASSERT(rawWriter);
        UNIT_ASSERT(encodedWriter);
        UNIT_ASSERT(rawWriter.Append(1));
        UNIT_ASSERT(encodedWriter.Append(2));

        const auto lines = FilterUserLines(registry.Snapshot().Lines());
        const TLineSnapshot* rawLine = FindLineByName(lines, "raw");
        const TLineSnapshot* encodedLine = FindLineByName(lines, "encoded");
        UNIT_ASSERT(rawLine);
        UNIT_ASSERT(encodedLine);
        UNIT_ASSERT(rawLine->Meta.StorageEncoding == EStorageEncoding::RawPoints);
        UNIT_ASSERT(encodedLine->Meta.StorageEncoding == EStorageEncoding::RunLengthEncoded);
        UNIT_ASSERT(rawLine->Meta.PublishPolicy == EPublishPolicy::Raw);
        UNIT_ASSERT(encodedLine->Meta.PublishPolicy == EPublishPolicy::OnChange);
    }

    Y_UNIT_TEST(OnChangePolicySkipsRepeatedValues) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 1024,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
        });

        const TLineMeta meta{
            .PublishPolicy = EPublishPolicy::OnChange,
        };

        auto writer = registry.CreateLine("line", NoLabels(), meta);
        UNIT_ASSERT(writer);
        UNIT_ASSERT(writer.Append(10));
        UNIT_ASSERT(writer.Append(10));
        UNIT_ASSERT(writer.Append(10));
        UNIT_ASSERT(writer.Append(20));
        UNIT_ASSERT(writer.Append(20));

        const auto lines = FilterUserLines(registry.Snapshot().Lines());
        const TLineSnapshot* line = FindLineByName(lines, "line");
        UNIT_ASSERT(line);
        UNIT_ASSERT_VALUES_EQUAL(ReadValues(*line), TVector<ui64>({10, 20}));
    }

    Y_UNIT_TEST(OnChangeWithHeartbeatRepublishesAfterInterval) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 1024,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
        });

        const TLineMeta meta{
            .PublishPolicy = EPublishPolicy::OnChangeWithHeartbeat,
            .Heartbeat = TDuration::MilliSeconds(1),
        };

        auto writer = registry.CreateLine("line", NoLabels(), meta);
        UNIT_ASSERT(writer);
        UNIT_ASSERT(writer.Append(10));
        UNIT_ASSERT(writer.Append(10));
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        UNIT_ASSERT(writer.Append(10));

        const auto lines = FilterUserLines(registry.Snapshot().Lines());
        const TLineSnapshot* line = FindLineByName(lines, "line");
        UNIT_ASSERT(line);
        UNIT_ASSERT_VALUES_EQUAL(ReadValues(*line), TVector<ui64>({10, 10}));
    }

    Y_UNIT_TEST(OnChangeWithHeartbeatReadsRepeatedValueAsTwoPoints) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 1024,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
        });

        const TLineMeta meta{
            .PublishPolicy = EPublishPolicy::OnChangeWithHeartbeat,
            .Heartbeat = TDuration::Hours(1),
        };

        auto writer = registry.CreateLine("line", NoLabels(), meta);
        UNIT_ASSERT(writer);
        UNIT_ASSERT(writer.Append(10));
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        UNIT_ASSERT(writer.Append(10));

        const auto lines = FilterUserLines(registry.Snapshot().Lines());
        const TLineSnapshot* line = FindLineByName(lines, "line");
        UNIT_ASSERT(line);

        TVector<ui64> values;
        TVector<TInstant> timestamps;
        line->ForEachRecord([&](const TRecordView& record) {
            values.push_back(record.Value);
            timestamps.push_back(record.Timestamp);
        });

        UNIT_ASSERT_VALUES_EQUAL(values, TVector<ui64>({10, 10}));
        UNIT_ASSERT_VALUES_EQUAL(timestamps.size(), 2);
        UNIT_ASSERT(timestamps[0] < timestamps[1]);
    }

    Y_UNIT_TEST(OnChangeWithHeartbeatKeepsTwoPointsWhileObservedTailAdvances) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 1024,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
        });

        const TLineMeta meta{
            .PublishPolicy = EPublishPolicy::OnChangeWithHeartbeat,
            .Heartbeat = TDuration::Hours(1),
        };

        auto writer = registry.CreateLine("line", NoLabels(), meta);
        UNIT_ASSERT(writer);
        UNIT_ASSERT(writer.Append(10));
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        UNIT_ASSERT(writer.Append(10));

        auto snapshot = registry.Snapshot();
        auto lines = FilterUserLines(snapshot.Lines());
        const TLineSnapshot* line = FindLineByName(lines, "line");
        UNIT_ASSERT(line);

        TVector<TInstant> firstReadTimestamps;
        line->ForEachRecord([&](const TRecordView& record) {
            firstReadTimestamps.push_back(record.Timestamp);
        });
        UNIT_ASSERT_VALUES_EQUAL(firstReadTimestamps.size(), 2);

        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        UNIT_ASSERT(writer.Append(10));

        snapshot = registry.Snapshot();
        lines = FilterUserLines(snapshot.Lines());
        line = FindLineByName(lines, "line");
        UNIT_ASSERT(line);

        TVector<ui64> values;
        TVector<TInstant> secondReadTimestamps;
        line->ForEachRecord([&](const TRecordView& record) {
            values.push_back(record.Value);
            secondReadTimestamps.push_back(record.Timestamp);
        });

        UNIT_ASSERT_VALUES_EQUAL(values, TVector<ui64>({10, 10}));
        UNIT_ASSERT_VALUES_EQUAL(secondReadTimestamps.size(), 2);
        UNIT_ASSERT(secondReadTimestamps[0] == firstReadTimestamps[0]);
        UNIT_ASSERT(secondReadTimestamps[1] > firstReadTimestamps[1]);
    }

    Y_UNIT_TEST(OnChangeWithHeartbeatBackfillsPreviousValueOnChange) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 1024,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
        });

        const TLineMeta meta{
            .PublishPolicy = EPublishPolicy::OnChangeWithHeartbeat,
            .Heartbeat = TDuration::Seconds(1),
        };

        auto writer = registry.CreateLine("line", NoLabels(), meta);
        UNIT_ASSERT(writer);
        UNIT_ASSERT(writer.Append(10));
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        UNIT_ASSERT(writer.Append(10));
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        UNIT_ASSERT(writer.Append(20));

        const auto lines = FilterUserLines(registry.Snapshot().Lines());
        const TLineSnapshot* line = FindLineByName(lines, "line");
        UNIT_ASSERT(line);

        TVector<ui64> values;
        TVector<TInstant> timestamps;
        line->ForEachRecord([&](const TRecordView& record) {
            values.push_back(record.Value);
            timestamps.push_back(record.Timestamp);
        });

        UNIT_ASSERT_VALUES_EQUAL(values, TVector<ui64>({10, 10, 20}));
        UNIT_ASSERT_VALUES_EQUAL(timestamps.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(timestamps[0], timestamps[1]);
        UNIT_ASSERT(timestamps[1] < timestamps[2]);
    }

    Y_UNIT_TEST(OnChangeWithHeartbeatDoesNotEmitObservedTailAfterClose) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 1024,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
        });

        const TLineMeta meta{
            .PublishPolicy = EPublishPolicy::OnChangeWithHeartbeat,
            .Heartbeat = TDuration::Hours(1),
        };

        auto writer = registry.CreateLine("line", NoLabels(), meta);
        UNIT_ASSERT(writer);
        UNIT_ASSERT(writer.Append(10));
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        UNIT_ASSERT(writer.Append(10));
        writer.Close();

        const auto lines = FilterUserLines(registry.Snapshot().Lines());
        const TLineSnapshot* line = FindLineByName(lines, "line");
        UNIT_ASSERT(line);
        UNIT_ASSERT(line->Closed);
        UNIT_ASSERT_VALUES_EQUAL(ReadValues(*line), TVector<ui64>({10}));
    }

    Y_UNIT_TEST(SelfMetricsAreStoredAsRegularLinesWithHistory) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 4096,
            .ChunkSizeBytes = 64,
            .MaxLines = 32,
        });

        auto first = registry.CreateLine("first", NoLabels());
        auto second = registry.CreateLine("second", NoLabels());
        UNIT_ASSERT(first);
        UNIT_ASSERT(second);
        UNIT_ASSERT(first.Append(1));
        UNIT_ASSERT(second.Append(2));
        const auto beforeFirstUpdate = registry.GetStats();
        registry.UpdateSelfMetrics();
        second.Close();
        const auto beforeSecondUpdate = registry.GetStats();
        registry.UpdateSelfMetrics();

        const auto lines = registry.Snapshot().Lines();

        const TLineSnapshot* memoryUsed = FindLineByName(lines, "inmemory_metrics.memory_used_bytes");
        const TLineSnapshot* committedBytes = FindLineByName(lines, "inmemory_metrics.committed_bytes");
        const TLineSnapshot* freeChunks = FindLineByName(lines, "inmemory_metrics.free_chunks");
        const TLineSnapshot* usedChunks = FindLineByName(lines, "inmemory_metrics.used_chunks");
        const TLineSnapshot* sealedChunks = FindLineByName(lines, "inmemory_metrics.sealed_chunks");
        const TLineSnapshot* writableChunks = FindLineByName(lines, "inmemory_metrics.writable_chunks");
        const TLineSnapshot* retiringChunks = FindLineByName(lines, "inmemory_metrics.retiring_chunks");
        const TLineSnapshot* lineCount = FindLineByName(lines, "inmemory_metrics.lines");
        const TLineSnapshot* closedLines = FindLineByName(lines, "inmemory_metrics.closed_lines");
        const TLineSnapshot* reuseWatermark = FindLineByName(lines, "inmemory_metrics.reuse_watermark");
        const TLineSnapshot* appendFailures = FindLineByName(lines, "inmemory_metrics.append_failures_total");

        UNIT_ASSERT(memoryUsed);
        UNIT_ASSERT(committedBytes);
        UNIT_ASSERT(freeChunks);
        UNIT_ASSERT(usedChunks);
        UNIT_ASSERT(sealedChunks);
        UNIT_ASSERT(writableChunks);
        UNIT_ASSERT(retiringChunks);
        UNIT_ASSERT(lineCount);
        UNIT_ASSERT(closedLines);
        UNIT_ASSERT(reuseWatermark);
        UNIT_ASSERT(appendFailures);

        UNIT_ASSERT_VALUES_EQUAL(memoryUsed->Labels.size(), 0);
        UNIT_ASSERT(memoryUsed->Meta.PublishPolicy == EPublishPolicy::OnChangeWithHeartbeat);
        UNIT_ASSERT_VALUES_EQUAL(memoryUsed->Meta.Heartbeat, TDuration::Hours(1));
        UNIT_ASSERT_VALUES_EQUAL(ReadValues(*memoryUsed), ExpectedHeartbeatRead(beforeFirstUpdate.MemoryUsedBytes, beforeSecondUpdate.MemoryUsedBytes));
        UNIT_ASSERT_VALUES_EQUAL(ReadValues(*committedBytes), ExpectedHeartbeatRead(beforeFirstUpdate.CommittedBytes, beforeSecondUpdate.CommittedBytes));
        UNIT_ASSERT_VALUES_EQUAL(ReadValues(*freeChunks), ExpectedHeartbeatRead(beforeFirstUpdate.FreeChunks, beforeSecondUpdate.FreeChunks));
        UNIT_ASSERT_VALUES_EQUAL(ReadValues(*usedChunks), ExpectedHeartbeatRead(beforeFirstUpdate.UsedChunks, beforeSecondUpdate.UsedChunks));
        UNIT_ASSERT_VALUES_EQUAL(ReadValues(*sealedChunks), ExpectedHeartbeatRead(beforeFirstUpdate.SealedChunks, beforeSecondUpdate.SealedChunks));
        UNIT_ASSERT_VALUES_EQUAL(ReadValues(*writableChunks), ExpectedHeartbeatRead(beforeFirstUpdate.WritableChunks, beforeSecondUpdate.WritableChunks));
        UNIT_ASSERT_VALUES_EQUAL(ReadValues(*retiringChunks), ExpectedHeartbeatRead(beforeFirstUpdate.RetiringChunks, beforeSecondUpdate.RetiringChunks));
        UNIT_ASSERT_VALUES_EQUAL(ReadValues(*lineCount), ExpectedHeartbeatRead(beforeFirstUpdate.Lines, beforeSecondUpdate.Lines));
        UNIT_ASSERT_VALUES_EQUAL(ReadValues(*closedLines), ExpectedHeartbeatRead(beforeFirstUpdate.ClosedLines, beforeSecondUpdate.ClosedLines));
        UNIT_ASSERT_VALUES_EQUAL(ReadValues(*reuseWatermark), ExpectedHeartbeatRead(beforeFirstUpdate.ReuseWatermark, beforeSecondUpdate.ReuseWatermark));
        UNIT_ASSERT_VALUES_EQUAL(ReadValues(*appendFailures), ExpectedHeartbeatRead(beforeFirstUpdate.AppendFailuresTotal, beforeSecondUpdate.AppendFailuresTotal));
    }

    Y_UNIT_TEST(SelfMetricsAdvanceObservedTailForRepeatedStableValues) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 4096,
            .ChunkSizeBytes = 64,
            .MaxLines = 32,
        });

        auto writer = registry.CreateLine("line", NoLabels());
        UNIT_ASSERT(writer);
        UNIT_ASSERT(writer.Append(1));

        registry.UpdateSelfMetrics();
        auto lines = registry.Snapshot().Lines();
        const TLineSnapshot* appendFailures = FindLineByName(lines, "inmemory_metrics.append_failures_total");
        UNIT_ASSERT(appendFailures);

        TVector<TInstant> firstReadTimestamps;
        appendFailures->ForEachRecord([&](const TRecordView& record) {
            firstReadTimestamps.push_back(record.Timestamp);
        });
        UNIT_ASSERT_VALUES_EQUAL(firstReadTimestamps.size(), 1);

        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        registry.UpdateSelfMetrics();

        lines = registry.Snapshot().Lines();
        appendFailures = FindLineByName(lines, "inmemory_metrics.append_failures_total");
        UNIT_ASSERT(appendFailures);

        TVector<ui64> values;
        TVector<TInstant> secondReadTimestamps;
        appendFailures->ForEachRecord([&](const TRecordView& record) {
            values.push_back(record.Value);
            secondReadTimestamps.push_back(record.Timestamp);
        });

        UNIT_ASSERT_VALUES_EQUAL(values.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(values[0], values[1]);
        UNIT_ASSERT(secondReadTimestamps[0] < secondReadTimestamps[1]);
        UNIT_ASSERT(secondReadTimestamps[0] == firstReadTimestamps[0]);
    }

    Y_UNIT_TEST(AppendFailuresAreReportedInSelfMetricLine) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 4096,
            .ChunkSizeBytes = 64,
            .MaxLines = 32,
        });

        auto writer = registry.CreateLine("line", NoLabels());
        UNIT_ASSERT(writer);
        writer.Close();
        UNIT_ASSERT(!writer.Append(1));
        const auto stats = registry.GetStats();
        registry.UpdateSelfMetrics();

        const auto lines = registry.Snapshot().Lines();
        const TLineSnapshot* appendFailures = FindLineByName(lines, "inmemory_metrics.append_failures_total");
        UNIT_ASSERT(appendFailures);
        UNIT_ASSERT_VALUES_EQUAL(ReadValues(*appendFailures), TVector<ui64>({stats.AppendFailuresTotal}));
    }

    Y_UNIT_TEST(ClosePreservesHistory) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 1024,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
        });

        auto writer = registry.CreateLine("line", NoLabels());
        UNIT_ASSERT(writer.Append(42));
        writer.Close();
        UNIT_ASSERT(!writer.Append(43));

        auto snapshot = registry.Snapshot();
        auto lines = FilterUserLines(snapshot.Lines());
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);
        UNIT_ASSERT(lines[0].Closed);

        TVector<ui64> values;
        lines[0].ForEachRecord([&](const TRecordView& record) {
            values.push_back(record.Value);
        });
        UNIT_ASSERT_VALUES_EQUAL(values.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(values[0], 42);
    }

    Y_UNIT_TEST(LineLimitReturnsNoopWriter) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 128,
            .ChunkSizeBytes = 64,
            .MaxLines = 1,
        });

        auto first = registry.CreateLine("first", NoLabels());
        auto second = registry.CreateLine("second", NoLabels());

        UNIT_ASSERT(first);
        UNIT_ASSERT(!second);
        UNIT_ASSERT(!second.Append(1));
    }

    Y_UNIT_TEST(RolloverUsesFreeChunk) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 128,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
        });

        auto writer = registry.CreateLine("line", NoLabels());
        for (ui64 value = 1; value <= 5; ++value) {
            UNIT_ASSERT(writer.Append(value));
        }

        auto snapshot = registry.Snapshot();
        auto lines = FilterUserLines(snapshot.Lines());
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(lines[0].Chunks.size(), 2);

        TVector<ui64> values;
        lines[0].ForEachRecord([&](const TRecordView& record) {
            values.push_back(record.Value);
        });

        UNIT_ASSERT_VALUES_EQUAL(values.size(), 5);
        for (ui64 value = 1; value <= 5; ++value) {
            UNIT_ASSERT_VALUES_EQUAL(values[value - 1], value);
        }
    }

    Y_UNIT_TEST(DuplicateKeyReturnsNoopWriter) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 256,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
        });

        auto first = registry.CreateLine("line", NoLabels());
        auto duplicate = registry.CreateLine("line", NoLabels());

        UNIT_ASSERT(first);
        UNIT_ASSERT(!duplicate);
        UNIT_ASSERT(!duplicate.Append(20));
        UNIT_ASSERT(first.Append(10));

        auto snapshot = registry.Snapshot();
        auto lines = FilterUserLines(snapshot.Lines());
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);
        UNIT_ASSERT(!lines[0].Closed);

        TVector<ui64> values;
        lines[0].ForEachRecord([&](const TRecordView& record) {
            values.push_back(record.Value);
        });
        UNIT_ASSERT_VALUES_EQUAL(values.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(values[0], 10);
    }

    Y_UNIT_TEST(ZeroMemoryWriterDrops) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 0,
            .ChunkSizeBytes = 64,
            .MaxLines = 1,
        });

        auto writer = registry.CreateLine("line", NoLabels());
        UNIT_ASSERT(writer);
        UNIT_ASSERT(!writer.Append(1));
        UNIT_ASSERT_VALUES_EQUAL(FilterUserLines(registry.Snapshot().Lines()).size(), 0);
    }

    Y_UNIT_TEST(TooSmallChunkAlwaysDrops) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 8,
            .ChunkSizeBytes = 8,
            .MaxLines = 1,
        });

        auto writer = registry.CreateLine("line", NoLabels());
        UNIT_ASSERT(writer);
        UNIT_ASSERT(!writer.Append(1));
        UNIT_ASSERT_VALUES_EQUAL(FilterUserLines(registry.Snapshot().Lines()).size(), 0);
    }

    Y_UNIT_TEST(StealOldestSealedChunk) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 128,
            .ChunkSizeBytes = 64,
            .MaxLines = 2,
        });

        auto first = registry.CreateLine("first", NoLabels());
        for (ui64 value = 1; value <= 5; ++value) {
            UNIT_ASSERT(first.Append(value));
        }

        auto second = registry.CreateLine("second", NoLabels());
        UNIT_ASSERT(second.Append(100));
        UNIT_ASSERT(registry.GetReuseWatermark() > 0);

        auto snapshot = registry.Snapshot();
        auto lines = FilterUserLines(snapshot.Lines());
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 2);

        TVector<ui64> firstValues;
        TVector<ui64> secondValues;
        for (const auto& line : lines) {
            if (line.Name == "first") {
                line.ForEachRecord([&](const TRecordView& record) {
                    firstValues.push_back(record.Value);
                });
            } else if (line.Name == "second") {
                line.ForEachRecord([&](const TRecordView& record) {
                    secondValues.push_back(record.Value);
                });
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(firstValues.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(firstValues[0], 5);
        UNIT_ASSERT_VALUES_EQUAL(secondValues.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(secondValues[0], 100);
    }

    Y_UNIT_TEST(ClosedWritableChunkBecomesReusable) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 64,
            .ChunkSizeBytes = 64,
            .MaxLines = 2,
        });

        auto first = registry.CreateLine("first", NoLabels());
        UNIT_ASSERT(first);
        UNIT_ASSERT(first.Append(1));
        first.Close();

        auto second = registry.CreateLine("second", NoLabels());
        UNIT_ASSERT(second);
        UNIT_ASSERT(second.Append(2));
        UNIT_ASSERT(registry.GetReuseWatermark() > 0);

        auto lines = FilterUserLines(registry.Snapshot().Lines());
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(lines[0].Name, "second");

        TVector<ui64> values;
        lines[0].ForEachRecord([&](const TRecordView& record) {
            values.push_back(record.Value);
        });
        UNIT_ASSERT_VALUES_EQUAL(values.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(values[0], 2);
    }

    Y_UNIT_TEST(ConcurrentAppendAndSnapshot) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 32768,
            .ChunkSizeBytes = 256,
            .MaxLines = 4,
        });

        auto writer = registry.CreateLine("line", NoLabels());
        UNIT_ASSERT(writer);

        constexpr ui64 writes = 1000;
        std::atomic<bool> stop = false;
        std::atomic<bool> ok = true;

        std::thread snapshotter([&] {
            while (!stop.load(std::memory_order_acquire)) {
                auto snapshot = registry.Snapshot();
                for (const auto& line : FilterUserLines(snapshot.Lines())) {
                    line.ForEachRecord([&](const TRecordView& record) {
                        if (record.Value >= writes) {
                            ok.store(false, std::memory_order_release);
                        }
                    });
                }
                std::this_thread::yield();
            }
        });

        for (ui64 i = 0; i < writes; ++i) {
            if (!writer.Append(i)) {
                ok.store(false, std::memory_order_release);
                break;
            }
        }
        stop.store(true, std::memory_order_release);
        snapshotter.join();

        UNIT_ASSERT(ok.load(std::memory_order_acquire));

        auto snapshot = registry.Snapshot();
        auto lines = FilterUserLines(snapshot.Lines());
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);

        TVector<ui64> values;
        lines[0].ForEachRecord([&](const TRecordView& record) {
            values.push_back(record.Value);
        });
        UNIT_ASSERT_VALUES_EQUAL(values.size(), writes);
        for (ui64 i = 0; i < writes; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(values[i], i);
        }
    }

    Y_UNIT_TEST(ConcurrentWritersWithRandomSnapshots) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 16384,
            .ChunkSizeBytes = 256,
            .MaxLines = 8,
        });

        constexpr ui32 writersCount = 4;
        constexpr ui64 writesPerLine = 200;

        TVector<TLineWriter> writers;
        writers.reserve(writersCount);
        for (ui32 i = 0; i < writersCount; ++i) {
            writers.push_back(registry.CreateLine(Sprintf("line-%u", i), NoLabels()));
            UNIT_ASSERT(writers.back());
        }

        std::atomic<bool> stop = false;
        std::atomic<bool> ok = true;
        std::thread snapshotter([&] {
            std::mt19937_64 rng(42);
            while (!stop.load(std::memory_order_acquire)) {
                auto snapshot = registry.Snapshot();
                for (const auto& line : FilterUserLines(snapshot.Lines())) {
                    line.ForEachRecord([&](const TRecordView& record) {
                        const ui64 lineIndex = record.Value >> 32;
                        const ui64 seq = record.Value & 0xffffffffull;
                        if (lineIndex >= writersCount || seq >= writesPerLine) {
                            ok.store(false, std::memory_order_release);
                        }
                    });
                }
                for (ui64 spins = rng() % 16; spins; --spins) {
                    std::this_thread::yield();
                }
            }
        });

        TVector<std::thread> threads;
        threads.reserve(writersCount);
        for (ui32 i = 0; i < writersCount; ++i) {
            threads.emplace_back([&, i] {
                for (ui64 seq = 0; seq < writesPerLine; ++seq) {
                    const ui64 value = (static_cast<ui64>(i) << 32) | seq;
                    if (!writers[i].Append(value)) {
                        ok.store(false, std::memory_order_release);
                        return;
                    }
                    if ((seq & 7) == 0) {
                        std::this_thread::yield();
                    }
                }
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }
        stop.store(true, std::memory_order_release);
        snapshotter.join();

        UNIT_ASSERT(ok.load(std::memory_order_acquire));

        auto snapshot = registry.Snapshot();
        auto lines = FilterUserLines(snapshot.Lines());
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), writersCount);

        TVector<ui64> counts(writersCount, 0);
        for (const auto& line : lines) {
            line.ForEachRecord([&](const TRecordView& record) {
                ++counts[record.Value >> 32];
            });
        }

        for (ui32 i = 0; i < writersCount; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(counts[i], writesPerLine);
        }
    }
}
