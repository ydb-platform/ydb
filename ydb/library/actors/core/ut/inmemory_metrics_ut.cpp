#include "actor_benchmark_helper.h"
#include "subsystems/inmemory_metrics.h"

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <algorithm>
#include <chrono>
#include <cstdint>
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

    template<class TCallback>
    void ReadUserLines(const TInMemoryMetricsRegistry& registry, TCallback&& cb) {
        registry.ReadSnapshot([&](const TSnapshotView& snapshot) {
            TVector<TLabel> commonLabels;
            commonLabels.reserve(snapshot.CommonLabelsSize());
            snapshot.ForEachCommonLabel([&](const TLabel& label) {
                commonLabels.push_back(label);
            });

            TVector<const TLineSnapshot*> lines;
            lines.reserve(snapshot.LinesSize());
            snapshot.ForEachLine([&](const TLineSnapshot& line) {
                if (!IsSystemLine(line)) {
                    lines.push_back(&line);
                }
            });
            cb(commonLabels, lines);
        });
    }

    const TLineSnapshot* FindLineByName(const TVector<const TLineSnapshot*>& lines, TStringBuf name) {
        for (const auto* line : lines) {
            if (line->Name == name) {
                return line;
            }
        }
        return nullptr;
    }

    TVector<ui64> ReadValues(const TLineSnapshot& line) {
        auto deque = line.ReadValuesAs<ui64>();
        TVector<ui64> values(deque.begin(), deque.end());
        return values;
    }

    TVector<ui64> ExpectedOnChangeRead(ui64 first, ui64 second) {
        if (first == second) {
            return {first};
        }
        return {first, second};
    }

    Y_UNIT_TEST(GenericDoubleLineStoresDoubleValues) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 1024,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
        });

        auto writer = registry.CreateLine<TRawLineFrontend<double>>("double_line", NoLabels());
        UNIT_ASSERT(writer);

        UNIT_ASSERT(writer.Append(1.5));
        UNIT_ASSERT(writer.Append(2.25));

        ReadUserLines(registry, [&](const TVector<TLabel>&, const TVector<const TLineSnapshot*>& lines) {
            const TLineSnapshot* line = FindLineByName(lines, "double_line");
            UNIT_ASSERT(line);

            auto deque = line->ReadValuesAs<double>();
            TVector<double> values(deque.begin(), deque.end());

            UNIT_ASSERT_VALUES_EQUAL(values.size(), 2);
            UNIT_ASSERT_DOUBLES_EQUAL(values[0], 1.5, 1e-12);
            UNIT_ASSERT_DOUBLES_EQUAL(values[1], 2.25, 1e-12);
            UNIT_ASSERT_VALUES_EQUAL(line->Meta.FrontendName(), "raw");
        });
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
        auto* metrics = GetInMemoryMetrics(actorSystem);
        UNIT_ASSERT(metrics);
        UNIT_ASSERT_VALUES_EQUAL(metrics->GetConfig().MemoryBytes, 4096);
    }

    Y_UNIT_TEST(AccessorWithoutActivationContextReturnsNullptr) {
        UNIT_ASSERT_EQUAL(GetInMemoryMetrics(), nullptr);
    }

    Y_UNIT_TEST(AllowedMetricPrefixesFilterLineCreation) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 1024,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
            .AllowedMetricPrefixes = {
                "harmonizer.",
                "inmemory_metrics.",
            },
        });

        auto blocked = registry.CreateLine("line", NoLabels());
        auto allowed = registry.CreateLine("harmonizer.budget", NoLabels());

        UNIT_ASSERT(!blocked);
        UNIT_ASSERT(allowed);
        UNIT_ASSERT(allowed.Append(1));

        registry.ReadSnapshot([&](const TSnapshotView& snapshot) {
            TVector<const TLineSnapshot*> lines;
            snapshot.ForEachLine([&](const TLineSnapshot& line) {
                lines.push_back(&line);
            });
            UNIT_ASSERT(!FindLineByName(lines, "line"));
            UNIT_ASSERT(FindLineByName(lines, "harmonizer.budget"));
        });
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

        ReadUserLines(registry, [&](const TVector<TLabel>&, const TVector<const TLineSnapshot*>& lines) {
            UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(lines[0]->Name, "line");
            UNIT_ASSERT(!lines[0]->Closed);

            auto records = lines[0]->ReadRecordsAs<ui64>();
            TVector<ui64> values;
            TVector<TInstant> timestamps;
            values.reserve(records.size());
            timestamps.reserve(records.size());
            for (const auto& record : records) {
                timestamps.push_back(record.Timestamp);
                values.push_back(record.Value);
            }

            UNIT_ASSERT_VALUES_EQUAL(values.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(values[0], 10);
            UNIT_ASSERT_VALUES_EQUAL(values[1], 20);
            UNIT_ASSERT(timestamps[0] <= timestamps[1]);
        });
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

        ReadUserLines(registry, [&](const TVector<TLabel>& commonLabels, const TVector<const TLineSnapshot*>& lines) {
            UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(commonLabels.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(commonLabels[0].Name, "node_id");
            UNIT_ASSERT_VALUES_EQUAL(commonLabels[0].Value, "42");
            UNIT_ASSERT_VALUES_EQUAL(lines[0]->Labels.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(lines[0]->Labels[0].Name, "kind");
            UNIT_ASSERT_VALUES_EQUAL(lines[0]->Labels[0].Value, "basic");
        });
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

        ReadUserLines(registry, [&](const TVector<TLabel>& commonLabels, const TVector<const TLineSnapshot*>& lines) {
            UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(commonLabels.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(commonLabels[0].Name, "node_id");
            UNIT_ASSERT_VALUES_EQUAL(commonLabels[0].Value, "43");
            UNIT_ASSERT_VALUES_EQUAL(commonLabels[1].Name, "host");
            UNIT_ASSERT_VALUES_EQUAL(commonLabels[1].Value, "slot-1");
            UNIT_ASSERT_VALUES_EQUAL(lines[0]->Labels.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(lines[0]->Labels[0].Name, "kind");
            UNIT_ASSERT_VALUES_EQUAL(lines[0]->Labels[0].Value, "basic");
        });

        auto duplicate = registry.CreateLine("line", std::span<const TLabel>(&label, 1));
        UNIT_ASSERT(!duplicate);
    }

    Y_UNIT_TEST(CreateLineStoresExplicitMetaInSnapshot) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 1024,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
        });

        auto writer = registry.CreateLine<TOnChangeLineFrontend<>>("line", NoLabels());
        UNIT_ASSERT(writer);
        UNIT_ASSERT(writer.Append(10));

        ReadUserLines(registry, [&](const TVector<TLabel>&, const TVector<const TLineSnapshot*>& lines) {
            UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(lines[0]->Meta.FrontendName(), "on_change");
        });
    }

    Y_UNIT_TEST(DifferentLinesCanUseDifferentMeta) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 1024,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
        });

        auto rawWriter = registry.CreateLine("raw", NoLabels());
        auto encodedWriter = registry.CreateLine<TOnChangeLineFrontend<>>("encoded", NoLabels());
        UNIT_ASSERT(rawWriter);
        UNIT_ASSERT(encodedWriter);
        UNIT_ASSERT(rawWriter.Append(1));
        UNIT_ASSERT(encodedWriter.Append(2));

        ReadUserLines(registry, [&](const TVector<TLabel>&, const TVector<const TLineSnapshot*>& lines) {
            const TLineSnapshot* rawLine = FindLineByName(lines, "raw");
            const TLineSnapshot* encodedLine = FindLineByName(lines, "encoded");
            UNIT_ASSERT(rawLine);
            UNIT_ASSERT(encodedLine);
            UNIT_ASSERT_VALUES_EQUAL(rawLine->Meta.FrontendName(), "raw");
            UNIT_ASSERT_VALUES_EQUAL(encodedLine->Meta.FrontendName(), "on_change");
        });
    }

    Y_UNIT_TEST(OnChangeBackfillsPreviousValueAfterRepeatedValues) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 1024,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
        });

        auto writer = registry.CreateLine<TOnChangeLineFrontend<>>("line", NoLabels());
        UNIT_ASSERT(writer);
        UNIT_ASSERT(writer.Append(10));
        UNIT_ASSERT(writer.Append(10));
        UNIT_ASSERT(writer.Append(10));
        UNIT_ASSERT(writer.Append(20));
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        UNIT_ASSERT(writer.Append(20));

        ReadUserLines(registry, [&](const TVector<TLabel>&, const TVector<const TLineSnapshot*>& lines) {
            const TLineSnapshot* line = FindLineByName(lines, "line");
            UNIT_ASSERT(line);
            UNIT_ASSERT_VALUES_EQUAL(ReadValues(*line), TVector<ui64>({10, 20}));
        });
    }

    Y_UNIT_TEST(OnChangeDoesNotRepublishRepeatedValuesAfterInterval) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 1024,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
        });

        auto writer = registry.CreateLine<TOnChangeLineFrontend<>>("line", NoLabels());
        UNIT_ASSERT(writer);
        UNIT_ASSERT(writer.Append(10));
        UNIT_ASSERT(writer.Append(10));
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        UNIT_ASSERT(writer.Append(10));

        ReadUserLines(registry, [&](const TVector<TLabel>&, const TVector<const TLineSnapshot*>& lines) {
            const TLineSnapshot* line = FindLineByName(lines, "line");
            UNIT_ASSERT(line);
            UNIT_ASSERT_VALUES_EQUAL(ReadValues(*line), TVector<ui64>({10}));
        });
    }

    Y_UNIT_TEST(OnChangeDoesNotEmitObservedTailOnRepeatedValue) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 1024,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
        });

        auto writer = registry.CreateLine<TOnChangeLineFrontend<>>("line", NoLabels());
        UNIT_ASSERT(writer);
        UNIT_ASSERT(writer.Append(10));
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        UNIT_ASSERT(writer.Append(10));

        ReadUserLines(registry, [&](const TVector<TLabel>&, const TVector<const TLineSnapshot*>& lines) {
            const TLineSnapshot* line = FindLineByName(lines, "line");
            UNIT_ASSERT(line);

            auto records = line->ReadRecordsAs<ui64>();
            TVector<ui64> values;
            TVector<TInstant> timestamps;
            values.reserve(records.size());
            timestamps.reserve(records.size());
            for (const auto& record : records) {
                values.push_back(record.Value);
                timestamps.push_back(record.Timestamp);
            }

            UNIT_ASSERT_VALUES_EQUAL(values, TVector<ui64>({10}));
            UNIT_ASSERT_VALUES_EQUAL(timestamps.size(), 1);
        });
    }

    Y_UNIT_TEST(OnChangeBackfillsPreviousValueOnChange) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 1024,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
        });

        auto writer = registry.CreateLine<TOnChangeLineFrontend<>>("line", NoLabels());
        UNIT_ASSERT(writer);
        UNIT_ASSERT(writer.Append(10));
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        UNIT_ASSERT(writer.Append(10));
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        UNIT_ASSERT(writer.Append(20));

        ReadUserLines(registry, [&](const TVector<TLabel>&, const TVector<const TLineSnapshot*>& lines) {
            const TLineSnapshot* line = FindLineByName(lines, "line");
            UNIT_ASSERT(line);

            auto records = line->ReadRecordsAs<ui64>();
            TVector<ui64> values;
            TVector<TInstant> timestamps;
            values.reserve(records.size());
            timestamps.reserve(records.size());
            for (const auto& record : records) {
                values.push_back(record.Value);
                timestamps.push_back(record.Timestamp);
            }

            UNIT_ASSERT_VALUES_EQUAL(values, TVector<ui64>({10, 20}));
            UNIT_ASSERT_VALUES_EQUAL(timestamps.size(), 2);
            UNIT_ASSERT(timestamps[0] < timestamps[1]);
        });
    }

    Y_UNIT_TEST(OnChangeRangeIncludesPreviousValueAtBegin) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 1024,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
        });

        auto writer = registry.CreateLine<TOnChangeLineFrontend<>>("line", NoLabels());
        UNIT_ASSERT(writer);
        UNIT_ASSERT(writer.Append(10));
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        UNIT_ASSERT(writer.Append(20));

        ReadUserLines(registry, [&](const TVector<TLabel>&, const TVector<const TLineSnapshot*>& lines) {
            const TLineSnapshot* line = FindLineByName(lines, "line");
            UNIT_ASSERT(line);

            auto fullRecords = line->ReadRecordsAs<ui64>();
            UNIT_ASSERT_VALUES_EQUAL(fullRecords.size(), 2);

            const TInstant beginTs = fullRecords[0].Timestamp + TDuration::MilliSeconds(1);
            const TInstant endTs = beginTs;
            auto records = line->ReadRecordsAsInRange<ui64>(beginTs, endTs);

            UNIT_ASSERT_VALUES_EQUAL(records.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(records[0].Timestamp, beginTs);
            UNIT_ASSERT_VALUES_EQUAL(records[0].Value, 10);
        });
    }

    Y_UNIT_TEST(OnChangeRangeDoesNotSynthesizeBeforeFirstPoint) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 1024,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
        });

        auto writer = registry.CreateLine<TOnChangeLineFrontend<>>("line", NoLabels());
        UNIT_ASSERT(writer);
        UNIT_ASSERT(writer.Append(10));
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        UNIT_ASSERT(writer.Append(20));

        ReadUserLines(registry, [&](const TVector<TLabel>&, const TVector<const TLineSnapshot*>& lines) {
            const TLineSnapshot* line = FindLineByName(lines, "line");
            UNIT_ASSERT(line);

            auto fullRecords = line->ReadRecordsAs<ui64>();
            UNIT_ASSERT_VALUES_EQUAL(fullRecords.size(), 2);

            const TInstant endTs = fullRecords[0].Timestamp - TDuration::MilliSeconds(1);
            const TInstant beginTs = endTs;
            auto records = line->ReadRecordsAsInRange<ui64>(beginTs, endTs);

            UNIT_ASSERT_VALUES_EQUAL(records.size(), 0);
        });
    }

    Y_UNIT_TEST(OnChangeDoesNotBackfillOnCloseWithoutChange) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 1024,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
        });

        auto writer = registry.CreateLine<TOnChangeLineFrontend<>>("line", NoLabels());
        UNIT_ASSERT(writer);
        UNIT_ASSERT(writer.Append(10));
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        UNIT_ASSERT(writer.Append(10));
        writer.Close();

        ReadUserLines(registry, [&](const TVector<TLabel>&, const TVector<const TLineSnapshot*>& lines) {
            const TLineSnapshot* line = FindLineByName(lines, "line");
            UNIT_ASSERT(line);
            UNIT_ASSERT(line->Closed);
            UNIT_ASSERT_VALUES_EQUAL(ReadValues(*line), TVector<ui64>({10}));
        });
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

        registry.ReadSnapshot([&](const TSnapshotView& snapshot) {
            TVector<const TLineSnapshot*> lines;
            snapshot.ForEachLine([&](const TLineSnapshot& line) {
                lines.push_back(&line);
            });

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
            UNIT_ASSERT_VALUES_EQUAL(memoryUsed->Meta.FrontendName(), "on_change");
            UNIT_ASSERT_VALUES_EQUAL(ReadValues(*memoryUsed), ExpectedOnChangeRead(beforeFirstUpdate.MemoryUsedBytes, beforeSecondUpdate.MemoryUsedBytes));
            UNIT_ASSERT_VALUES_EQUAL(ReadValues(*committedBytes), ExpectedOnChangeRead(beforeFirstUpdate.CommittedBytes, beforeSecondUpdate.CommittedBytes));
            UNIT_ASSERT_VALUES_EQUAL(ReadValues(*freeChunks), ExpectedOnChangeRead(beforeFirstUpdate.FreeChunks, beforeSecondUpdate.FreeChunks));
            UNIT_ASSERT_VALUES_EQUAL(ReadValues(*usedChunks), ExpectedOnChangeRead(beforeFirstUpdate.UsedChunks, beforeSecondUpdate.UsedChunks));
            UNIT_ASSERT_VALUES_EQUAL(ReadValues(*sealedChunks), ExpectedOnChangeRead(beforeFirstUpdate.SealedChunks, beforeSecondUpdate.SealedChunks));
            UNIT_ASSERT_VALUES_EQUAL(ReadValues(*writableChunks), ExpectedOnChangeRead(beforeFirstUpdate.WritableChunks, beforeSecondUpdate.WritableChunks));
            UNIT_ASSERT_VALUES_EQUAL(ReadValues(*retiringChunks), ExpectedOnChangeRead(beforeFirstUpdate.RetiringChunks, beforeSecondUpdate.RetiringChunks));
            UNIT_ASSERT_VALUES_EQUAL(ReadValues(*lineCount), ExpectedOnChangeRead(beforeFirstUpdate.Lines, beforeSecondUpdate.Lines));
            UNIT_ASSERT_VALUES_EQUAL(ReadValues(*closedLines), ExpectedOnChangeRead(beforeFirstUpdate.ClosedLines, beforeSecondUpdate.ClosedLines));
            UNIT_ASSERT_VALUES_EQUAL(ReadValues(*reuseWatermark), ExpectedOnChangeRead(beforeFirstUpdate.ReuseWatermark, beforeSecondUpdate.ReuseWatermark));
            UNIT_ASSERT_VALUES_EQUAL(ReadValues(*appendFailures), ExpectedOnChangeRead(beforeFirstUpdate.AppendFailuresTotal, beforeSecondUpdate.AppendFailuresTotal));
        });
    }

    Y_UNIT_TEST(SelfMetricsKeepSinglePointForRepeatedStableValues) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 4096,
            .ChunkSizeBytes = 64,
            .MaxLines = 32,
        });

        auto writer = registry.CreateLine("line", NoLabels());
        UNIT_ASSERT(writer);
        UNIT_ASSERT(writer.Append(1));

        registry.UpdateSelfMetrics();
        TVector<TInstant> firstReadTimestamps;
        registry.ReadSnapshot([&](const TSnapshotView& snapshot) {
            TVector<const TLineSnapshot*> lines;
            snapshot.ForEachLine([&](const TLineSnapshot& line) {
                lines.push_back(&line);
            });
            const TLineSnapshot* appendFailures = FindLineByName(lines, "inmemory_metrics.append_failures_total");
            UNIT_ASSERT(appendFailures);
            for (const auto& record : appendFailures->ReadRecordsAs<ui64>()) {
                firstReadTimestamps.push_back(record.Timestamp);
            }
        });
        UNIT_ASSERT_VALUES_EQUAL(firstReadTimestamps.size(), 1);

        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        registry.UpdateSelfMetrics();

        TVector<ui64> values;
        TVector<TInstant> secondReadTimestamps;
        registry.ReadSnapshot([&](const TSnapshotView& snapshot) {
            TVector<const TLineSnapshot*> lines;
            snapshot.ForEachLine([&](const TLineSnapshot& line) {
                lines.push_back(&line);
            });
            const TLineSnapshot* appendFailures = FindLineByName(lines, "inmemory_metrics.append_failures_total");
            UNIT_ASSERT(appendFailures);
            for (const auto& record : appendFailures->ReadRecordsAs<ui64>()) {
                values.push_back(record.Value);
                secondReadTimestamps.push_back(record.Timestamp);
            }
        });

        UNIT_ASSERT_VALUES_EQUAL(values, TVector<ui64>({0}));
        UNIT_ASSERT_VALUES_EQUAL(secondReadTimestamps.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(secondReadTimestamps[0], firstReadTimestamps[0]);
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

        registry.ReadSnapshot([&](const TSnapshotView& snapshot) {
            TVector<const TLineSnapshot*> lines;
            snapshot.ForEachLine([&](const TLineSnapshot& line) {
                lines.push_back(&line);
            });
            const TLineSnapshot* appendFailures = FindLineByName(lines, "inmemory_metrics.append_failures_total");
            UNIT_ASSERT(appendFailures);
            UNIT_ASSERT_VALUES_EQUAL(ReadValues(*appendFailures), TVector<ui64>({stats.AppendFailuresTotal}));
        });
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

        ReadUserLines(registry, [&](const TVector<TLabel>&, const TVector<const TLineSnapshot*>& lines) {
            UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);
            UNIT_ASSERT(lines[0]->Closed);

            TVector<ui64> values = ReadValues(*lines[0]);
            UNIT_ASSERT_VALUES_EQUAL(values.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(values[0], 42);
        });
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

        ReadUserLines(registry, [&](const TVector<TLabel>&, const TVector<const TLineSnapshot*>& lines) {
            UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(registry.GetStats().UsedChunks, 2);

            TVector<ui64> values = ReadValues(*lines[0]);

            UNIT_ASSERT_VALUES_EQUAL(values.size(), 5);
            for (ui64 value = 1; value <= 5; ++value) {
                UNIT_ASSERT_VALUES_EQUAL(values[value - 1], value);
            }
        });
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

        ReadUserLines(registry, [&](const TVector<TLabel>&, const TVector<const TLineSnapshot*>& lines) {
            UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);
            UNIT_ASSERT(!lines[0]->Closed);

            TVector<ui64> values = ReadValues(*lines[0]);
            UNIT_ASSERT_VALUES_EQUAL(values.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(values[0], 10);
        });
    }

    Y_UNIT_TEST(DuplicateKeyIgnoresLabelOrder) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 256,
            .ChunkSizeBytes = 64,
            .MaxLines = 4,
        });

        const std::array<TLabel, 2> firstLabels{{
            TLabel{"a", "1"},
            TLabel{"b", "2"},
        }};
        const std::array<TLabel, 2> secondLabels{{
            TLabel{"b", "2"},
            TLabel{"a", "1"},
        }};

        auto first = registry.CreateLine("line", firstLabels);
        auto duplicate = registry.CreateLine("line", secondLabels);

        UNIT_ASSERT(first);
        UNIT_ASSERT(!duplicate);
        UNIT_ASSERT(first.Append(10));

        ReadUserLines(registry, [&](const TVector<TLabel>&, const TVector<const TLineSnapshot*>& lines) {
            UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);
        });
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
        ReadUserLines(registry, [&](const TVector<TLabel>&, const TVector<const TLineSnapshot*>& lines) {
            UNIT_ASSERT_VALUES_EQUAL(lines.size(), 0);
        });
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
        ReadUserLines(registry, [&](const TVector<TLabel>&, const TVector<const TLineSnapshot*>& lines) {
            UNIT_ASSERT_VALUES_EQUAL(lines.size(), 0);
        });
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

        ReadUserLines(registry, [&](const TVector<TLabel>&, const TVector<const TLineSnapshot*>& lines) {
            UNIT_ASSERT_VALUES_EQUAL(lines.size(), 2);

            TVector<ui64> firstValues;
            TVector<ui64> secondValues;
            for (const auto* line : lines) {
                if (line->Name == "first") {
                    for (const auto& value : line->ReadValuesAs<ui64>()) {
                        firstValues.push_back(value);
                    }
                } else if (line->Name == "second") {
                    for (const auto& value : line->ReadValuesAs<ui64>()) {
                        secondValues.push_back(value);
                    }
                }
            }

            UNIT_ASSERT_VALUES_EQUAL(firstValues, TVector<ui64>({4, 5}));
            UNIT_ASSERT_VALUES_EQUAL(secondValues.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(secondValues[0], 100);
        });
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

        ReadUserLines(registry, [&](const TVector<TLabel>&, const TVector<const TLineSnapshot*>& lines) {
            UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(lines[0]->Name, "second");

            TVector<ui64> values = ReadValues(*lines[0]);
            UNIT_ASSERT_VALUES_EQUAL(values.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(values[0], 2);
        });
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
                ReadUserLines(registry, [&](const TVector<TLabel>&, const TVector<const TLineSnapshot*>& lines) {
                    for (const auto* line : lines) {
                        for (const auto& value : line->ReadValuesAs<ui64>()) {
                            if (value >= writes) {
                                ok.store(false, std::memory_order_release);
                            }
                        }
                    }
                });
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

        ReadUserLines(registry, [&](const TVector<TLabel>&, const TVector<const TLineSnapshot*>& lines) {
            UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);

            TVector<ui64> values = ReadValues(*lines[0]);
            UNIT_ASSERT_VALUES_EQUAL(values.size(), writes);
            for (ui64 i = 0; i < writes; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(values[i], i);
            }
        });
    }

    Y_UNIT_TEST(ConcurrentWritersWithRandomSnapshots) {
        TInMemoryMetricsRegistry registry({
            .MemoryBytes = 16384,
            .ChunkSizeBytes = 256,
            .MaxLines = 8,
        });

        constexpr ui32 writersCount = 4;
        constexpr ui64 writesPerLine = 200;

        TVector<TLine<TRawLineFrontend<>>> writers;
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
                ReadUserLines(registry, [&](const TVector<TLabel>&, const TVector<const TLineSnapshot*>& lines) {
                    for (const auto* line : lines) {
                        for (const auto& value : line->ReadValuesAs<ui64>()) {
                            const ui64 lineIndex = value >> 32;
                            const ui64 seq = value & 0xffffffffull;
                            if (lineIndex >= writersCount || seq >= writesPerLine) {
                                ok.store(false, std::memory_order_release);
                            }
                        }
                    }
                });
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

        ReadUserLines(registry, [&](const TVector<TLabel>&, const TVector<const TLineSnapshot*>& lines) {
            UNIT_ASSERT_VALUES_EQUAL(lines.size(), writersCount);

            TVector<ui64> counts(writersCount, 0);
            for (const auto* line : lines) {
                for (const auto& value : line->ReadValuesAs<ui64>()) {
                    ++counts[value >> 32];
                }
            }

            for (ui32 i = 0; i < writersCount; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(counts[i], writesPerLine);
            }
        });
    }
}
