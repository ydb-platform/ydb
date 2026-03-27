#include "actor_benchmark_helper.h"
#include "subsystems/inmemory_metrics.h"

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <random>
#include <thread>

using namespace NActors;
using namespace NActors::NTests;

Y_UNIT_TEST_SUITE(InMemoryMetrics) {

    using TActorBenchmark = ::NActors::NTests::TActorBenchmark<>;

    std::span<const TLabel> NoLabels() {
        return {};
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
        auto lines = snapshot.Lines();
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
        auto lines = snapshot.Lines();
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
        auto lines = snapshot.Lines();
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
        auto lines = snapshot.Lines();
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
        UNIT_ASSERT_VALUES_EQUAL(registry.Snapshot().Lines().size(), 0);
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
        UNIT_ASSERT_VALUES_EQUAL(registry.Snapshot().Lines().size(), 0);
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
        auto lines = snapshot.Lines();
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

        auto lines = registry.Snapshot().Lines();
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
                for (const auto& line : snapshot.Lines()) {
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
        auto lines = snapshot.Lines();
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
                for (const auto& line : snapshot.Lines()) {
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
        auto lines = snapshot.Lines();
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
