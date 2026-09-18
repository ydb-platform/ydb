#include <ydb/library/actors/metrics/inmemory_backend.h>
#include <ydb/library/actors/metrics/lines/on_change_line_frontend.h>
#include <ydb/library/actors/metrics/lines/raw_line_frontend.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/thread/lfqueue.h>
#include <util/string/builder.h>

#include <array>
#include <atomic>
#include <thread>
#include <algorithm>

using namespace NActors;

Y_UNIT_TEST_SUITE(InMemoryMetrics) {
    void Pump(TInMemoryMetricsBackend* backend) {
        backend->BeginMaintenance();
        backend->ProcessMaintenance();
    }

    TVector<ui64> Values(const TInMemorySnapshot& snapshot, TStringBuf name) {
        TVector<ui64> result;
        snapshot.Read([&](const TSnapshotView& view) {
            view.ForEachLine([&](const TLineSnapshot& line) {
                if (line.Name == name) {
                    const auto values = line.ReadValuesAs<ui64>();
                    result.assign(values.begin(), values.end());
                }
            });
        });
        return result;
    }

    Y_UNIT_TEST(LineLimitObservesCloseBeforeMaintenance) {
        for (bool written : {false, true}) {
            TInMemoryMetricsBackend backend({.MemoryBytes = 4096, .ChunkSizeBytes = 64, .MaxLines = 1});
            auto first = backend.CreateLine("first", {});
            if (written) {
                Pump(&backend);
                UNIT_ASSERT(first.Append(1));
            }
            auto pinned = backend.CaptureSnapshot();
            first.Close();
            auto second = backend.CreateLine("second", {});
            UNIT_ASSERT(second.GetLineId());
            Pump(&backend);
            UNIT_ASSERT(second.Append(2));
            UNIT_ASSERT(Values(backend.CaptureSnapshot(), "second") == TVector<ui64>{2});
            if (written) {
                UNIT_ASSERT(Values(pinned, "first") == TVector<ui64>{1});
            }
        }
    }

    Y_UNIT_TEST(HistoryMovesBetweenOpenLines) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 64 * 32, .ChunkSizeBytes = 64, .MaxLines = 8});
        std::array<TLine<TRawLineFrontend<>>, 8> lines;
        for (ui32 i = 0; i < lines.size(); ++i) {
            lines[i] = backend.CreateLine(TStringBuilder() << "line" << i, {});
            Pump(&backend);
            for (ui64 value = 0; value < 120; ++value) {
                if (!lines[i].Append(value)) {
                    Pump(&backend);
                    UNIT_ASSERT(lines[i].Append(value));
                }
                Pump(&backend);
            }
            const auto values = Values(backend.CaptureSnapshot(), TStringBuilder() << "line" << i);
            UNIT_ASSERT(!values.empty());
            UNIT_ASSERT_VALUES_EQUAL(values.back(), 119);
            UNIT_ASSERT(std::is_sorted(values.begin(), values.end()));
            UNIT_ASSERT_VALUES_EQUAL(backend.GetStats().Lines, i + 1);
        }
    }

    Y_UNIT_TEST(ClosedKeyCanBeRegisteredAgainWithPinnedSnapshot) {
        for (bool maintenance : {false, true}) {
            TInMemoryMetricsBackend backend({.MemoryBytes = 4096, .ChunkSizeBytes = 64, .MaxLines = 4});
            auto line = backend.CreateLine("restart", {});
            Pump(&backend);
            UNIT_ASSERT(line.Append(1));
            const auto oldId = line.GetLineId();
            auto pinned = backend.CaptureSnapshot();
            line.Close();
            if (maintenance) {
                Pump(&backend);
            }
            auto replacement = backend.CreateLine("restart", {});
            UNIT_ASSERT(replacement.GetLineId());
            UNIT_ASSERT(replacement.GetLineId() != oldId);
            auto duplicate = backend.CreateLine("restart", {});
            UNIT_ASSERT(!duplicate.GetLineId());
            Pump(&backend);
            UNIT_ASSERT(replacement.Append(2));
            UNIT_ASSERT(Values(pinned, "restart") == TVector<ui64>{1});
            UNIT_ASSERT(Values(backend.CaptureSnapshot(), "restart") == TVector<ui64>{2});
        }
    }

    Y_UNIT_TEST(OnChangeRangeStopsAtCloseTime) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 4096, .ChunkSizeBytes = 64, .MaxLines = 4});
        auto line = backend.CreateLine<TOnChangeLineFrontend<>>("closed", {});
        Pump(&backend);
        UNIT_ASSERT(line.Append(7));
        line.Close();
        // Capture before maintenance: the writer publishes the actual close time.
        auto snapshot = backend.CaptureSnapshot();
        snapshot.Read([](const TSnapshotView& view) {
            view.ForEachLine([](const TLineSnapshot& captured) {
                UNIT_ASSERT(captured.Closed);
                const auto after = captured.ClosedAt + TDuration::MicroSeconds(1);
                UNIT_ASSERT(captured.ReadValuesAsInRange<ui64>(after, after + TDuration::Seconds(1)).empty());
                const auto boundary = captured.ReadValuesAsInRange<ui64>(captured.ClosedAt, after);
                UNIT_ASSERT_VALUES_EQUAL(boundary.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(boundary.front(), 7);
                UNIT_ASSERT_VALUES_EQUAL(captured.ReadValuesAs<ui64>().size(), 1);
            });
        });
    }

    Y_UNIT_TEST(ReserveRingConcurrentWraparound) {
        std::array<TChunk, 5> chunks = {TChunk(0, 64), TChunk(1, 64), TChunk(2, 64), TChunk(3, 64), TChunk(4, 64)};
        for (ui32 capacity : {1u, 3u, 8u}) {
            TChunkReserve reserve(capacity);
            bool correct = true;
            std::thread producer([&] {
                for (ui32 i = 0; i < 100000; ++i) {
                    while (!reserve.TryPush(&chunks[i % chunks.size()])) {
                        std::this_thread::yield();
                    }
                }
            });
            for (ui32 i = 0; i < 100000; ++i) {
                TChunk* chunk;
                while (!(chunk = reserve.TryPop())) {
                    std::this_thread::yield();
                }
                correct &= chunk == &chunks[i % chunks.size()];
            }
            producer.join();
            UNIT_ASSERT(correct);
            UNIT_ASSERT(!reserve.TryPop());
        }
    }

    Y_UNIT_TEST(RefillRequestedBeforeReserveEmpty) {
        ui32 notifications = 0;
        TInMemoryMetricsBackend backend({.MemoryBytes = 64 * 16, .ChunkSizeBytes = 64, .MaxLines = 1, .ReserveChunks = 3}, [&] { ++notifications; });
        auto line = backend.CreateLine("buffered", {});
        UNIT_ASSERT(!line.Append(0));
        UNIT_ASSERT_VALUES_EQUAL(notifications, 1);
        Pump(&backend);
        UNIT_ASSERT(line.Append(1));
        UNIT_ASSERT_VALUES_EQUAL(notifications, 2);
        Pump(&backend);
        UNIT_ASSERT_VALUES_EQUAL(backend.GetStats().UsedChunks, 4);
        for (ui64 i = 2; i <= 12; ++i) {
            UNIT_ASSERT(line.Append(i));
        }
        UNIT_ASSERT(!line.Append(13));
        UNIT_ASSERT_VALUES_EQUAL(notifications, 3);
        Pump(&backend);
        UNIT_ASSERT(line.Append(13));
        line.Close();
        Pump(&backend);
        const auto values = Values(backend.CaptureSnapshot(), "buffered");
        UNIT_ASSERT_VALUES_EQUAL(values.size(), 13);
        for (ui32 i = 0; i < values.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(values[i], i + 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(backend.GetStats().UsedChunks, 5);
        UNIT_ASSERT_VALUES_EQUAL(backend.GetStats().ClosedLines, 1);
    }

    Y_UNIT_TEST(PendingRegistrationAndCloseRace) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 1024, .ChunkSizeBytes = 64, .MaxLines = 1});
        for (ui32 i = 0; i < 200; ++i) {
            auto state = std::make_shared<TLineWriterState>();
            TLine<TRawLineFrontend<>> line(&backend, state);
            UNIT_ASSERT(line);
            UNIT_ASSERT(!line.Append(1));
            std::thread close([&] { line.Close(); });
            backend.RegisterLine(state, MakeLineKey("racing", {}), TRawLineFrontend<>::MakeMeta());
            close.join();
            Pump(&backend);
            UNIT_ASSERT_EQUAL(state->Status.load(), ELineWriterStatus::Closed);
            UNIT_ASSERT_VALUES_EQUAL(backend.GetStats().Lines, 0);
            UNIT_ASSERT_VALUES_EQUAL(backend.GetStats().UsedChunks, 0);
        }
    }

    Y_UNIT_TEST(RegistrationBeforeFirstWriteAndShutdown) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 1024, .ChunkSizeBytes = 64, .MaxLines = 1});
        auto state = std::make_shared<TLineWriterState>();
        TLine<TRawLineFrontend<>> line(&backend, state);
        UNIT_ASSERT(!line.Append(1));
        backend.RegisterLine(state, MakeLineKey("line", {}), TRawLineFrontend<>::MakeMeta());
        UNIT_ASSERT(!line.Append(1));
        Pump(&backend);
        UNIT_ASSERT(line.Append(42));
        backend.StopManagement();
        UNIT_ASSERT(!line.Append(43));
        line.Close();
        Pump(&backend);
        UNIT_ASSERT_VALUES_EQUAL(backend.GetStats().ClosedLines, 1);
        UNIT_ASSERT_VALUES_EQUAL(Values(backend.CaptureSnapshot(), "line"), TVector<ui64>{42});
    }

    Y_UNIT_TEST(CanonicalKeysFiltersAndLabels) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 4096, .ChunkSizeBytes = 64, .MaxLines = 4, .AllowedMetricPrefixes = {"allowed."}});
        const TVector<TLabel> labels = {{"b", "2"}, {"a", "1"}};
        const TVector<TLabel> reversed = {{"a", "1"}, {"b", "2"}};
        auto line = backend.CreateLine("allowed.value", labels);
        UNIT_ASSERT(line);
        UNIT_ASSERT(!backend.CreateLine("allowed.value", reversed));
        UNIT_ASSERT(!backend.CreateLine("denied", {}));
        Pump(&backend);
        UNIT_ASSERT(line.Append(1));
        const TVector<TLabel> common = {{"service", "old"}};
        backend.SetCommonLabels(common);
        auto old = backend.CaptureSnapshot();
        const TVector<TLabel> changed = {{"service", "new"}};
        backend.SetCommonLabels(changed);
        old.Read([&](const TSnapshotView& view) {
            UNIT_ASSERT_VALUES_EQUAL(view.CommonLabelsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(view.GetCommonLabel(0).Value, "old");
            UNIT_ASSERT(view.GetLine(0).Labels == reversed);
        });
        backend.ReadSnapshot([](const TSnapshotView& view) {
            UNIT_ASSERT_VALUES_EQUAL(view.GetCommonLabel(0).Value, "new");
        });
    }

    Y_UNIT_TEST(SnapshotCapturesPrefixForRawAndOnChange) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 4096, .ChunkSizeBytes = 256, .MaxLines = 4});
        auto raw = backend.CreateLine("raw", {});
        auto changed = backend.CreateLine<TOnChangeLineFrontend<>>("changed", {});
        Pump(&backend);
        UNIT_ASSERT(raw.Append(1));
        UNIT_ASSERT(changed.Append(1));
        auto pinned = backend.CaptureSnapshot();
        UNIT_ASSERT(raw.Append(2));
        UNIT_ASSERT(raw.Append(2));
        UNIT_ASSERT(changed.Append(1));
        UNIT_ASSERT(changed.Append(2));
        UNIT_ASSERT_VALUES_EQUAL(Values(pinned, "raw"), TVector<ui64>{1});
        UNIT_ASSERT_VALUES_EQUAL(Values(pinned, "changed"), TVector<ui64>{1});
        UNIT_ASSERT_VALUES_EQUAL(Values(backend.CaptureSnapshot(), "raw"), (TVector<ui64>{1, 2, 2}));
        UNIT_ASSERT_VALUES_EQUAL(Values(backend.CaptureSnapshot(), "changed"), (TVector<ui64>{1, 2}));
    }

    Y_UNIT_TEST(OnChangeRematerializesAfterPinnedEviction) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 64, .ChunkSizeBytes = 64, .MaxLines = 1});
        auto line = backend.CreateLine<TOnChangeLineFrontend<>>("pinned", {});
        Pump(&backend);
        for (ui64 i = 1; i <= 3; ++i) {
            UNIT_ASSERT(line.Append(i));
        }
        auto pinned = backend.CaptureSnapshot();
        UNIT_ASSERT(!line.Append(4));
        Pump(&backend);
        UNIT_ASSERT(!line.Append(3));
        UNIT_ASSERT_VALUES_EQUAL(Values(pinned, "pinned"), (TVector<ui64>{1, 2, 3}));
        UNIT_ASSERT_VALUES_EQUAL(backend.GetStats().RetiringChunks, 1);
        pinned = {};
        Pump(&backend);
        UNIT_ASSERT(line.Append(3));
        UNIT_ASSERT_VALUES_EQUAL(Values(backend.CaptureSnapshot(), "pinned"), TVector<ui64>{3});
    }

    Y_UNIT_TEST(OnChangeKeepsCacheWhenSealedHistoryRemains) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 128, .ChunkSizeBytes = 64, .MaxLines = 1});
        auto line = backend.CreateLine<TOnChangeLineFrontend<>>("line", {});
        Pump(&backend);
        for (ui64 i = 1; i <= 3; ++i) {
            UNIT_ASSERT(line.Append(i));
        }
        UNIT_ASSERT(!line.Append(4));
        Pump(&backend); // Seals first chunk; second is available without eviction.
        UNIT_ASSERT(line.Append(3));
        UNIT_ASSERT_VALUES_EQUAL(Values(backend.CaptureSnapshot(), "line"), (TVector<ui64>{1, 2, 3}));
    }

    Y_UNIT_TEST(LineLimitReclaimsClosedHistoryAndPreservesPins) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 256, .ChunkSizeBytes = 64, .MaxLines = 1});
        auto line = backend.CreateLine("old", {});
        Pump(&backend);
        UNIT_ASSERT(line.Append(42));
        auto pinned = backend.CaptureSnapshot();
        UNIT_ASSERT(!backend.CreateLine("overflow", {}));
        line.Close();
        Pump(&backend);
        for (ui32 i = 0; i < 10; ++i) {
            auto next = backend.CreateLine("new", {});
            UNIT_ASSERT(next);
            Pump(&backend);
            UNIT_ASSERT(next.Append(i));
            next.Close();
            Pump(&backend);
            // Admission will reclaim this key on the next iteration.
            auto replacement = backend.CreateLine("intermediate", {});
            UNIT_ASSERT(replacement);
            replacement.Close();
            Pump(&backend);
        }
        UNIT_ASSERT_VALUES_EQUAL(Values(pinned, "old"), TVector<ui64>{42});
    }

    Y_UNIT_TEST(EmptyPinnedChunkRetiresOnClose) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 8, .ChunkSizeBytes = 8, .MaxLines = 1});
        auto line = backend.CreateLine("small", {});
        Pump(&backend);
        UNIT_ASSERT(!line.Append(1));
        auto pinned = backend.CaptureSnapshot();
        line.Close();
        Pump(&backend);
        UNIT_ASSERT_VALUES_EQUAL(backend.GetStats().RetiringChunks, 1);
        UNIT_ASSERT_VALUES_EQUAL(backend.GetStats().FreeChunks, 0);
        pinned = {};
        UNIT_ASSERT_VALUES_EQUAL(backend.GetStats().FreeChunks, 1);
    }

    Y_UNIT_TEST(SnapshotOutlivesBackend) {
        TInMemorySnapshot pinned;
        {
            TInMemoryMetricsBackend backend({.MemoryBytes = 64, .ChunkSizeBytes = 64, .MaxLines = 1});
            auto line = backend.CreateLine("survivor", {});
            Pump(&backend);
            UNIT_ASSERT(line.Append(42));
            pinned = backend.CaptureSnapshot();
            line.Close();
            Pump(&backend);
        }
        std::thread reader([snapshot = std::move(pinned)] {
            UNIT_ASSERT_VALUES_EQUAL(Values(snapshot, "survivor"), TVector<ui64>{42});
        });
        reader.join();
    }

    Y_UNIT_TEST(PoolBitmapBoundariesAndReturn) {
        for (ui32 count : {1u, 64u, 65u, 129u}) {
            TChunkPool pool(count, 64);
            TVector<TChunk*> acquired;
            for (ui32 i = 0; i < count; ++i) {
                auto* chunk = pool.TryAcquire();
                UNIT_ASSERT(chunk);
                UNIT_ASSERT_VALUES_EQUAL(chunk->ChunkId, i);
                UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(chunk->Payload.data()) % TChunk::PayloadAlignment, 0);
                acquired.push_back(chunk);
            }
            UNIT_ASSERT(!pool.TryAcquire());
            for (auto* chunk : acquired) {
                pool.Return(chunk);
            }
            for (ui32 i = 0; i < count; ++i) {
                UNIT_ASSERT(pool.TryAcquire());
            }
            UNIT_ASSERT(!pool.TryAcquire());
        }
    }

    Y_UNIT_TEST(TypedValuesMoveAndTimeRange) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 4096, .ChunkSizeBytes = 256, .MaxLines = 4});
        auto original = backend.CreateLine<TRawLineFrontend<double>>("double", {});
        auto boolean = backend.CreateLine<TOnChangeLineFrontend<bool>>("bool", {});
        auto line = std::move(original);
        UNIT_ASSERT(!original);
        Pump(&backend);
        UNIT_ASSERT(line.Append(1.25));
        UNIT_ASSERT(boolean.Append(true));
        UNIT_ASSERT(boolean.Append(true));
        backend.ReadSnapshot([](const TSnapshotView& view) {
            view.ForEachLine([](const TLineSnapshot& item) {
                if (item.Name == "double") {
                    const auto records = item.ReadRecordsAs<double>();
                    UNIT_ASSERT_VALUES_EQUAL(records.size(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(records.front().Value, 1.25);
                    UNIT_ASSERT_VALUES_EQUAL(item.ReadValuesAsInRange<double>(records.front().Timestamp, records.front().Timestamp).size(), 1);
                    UNIT_ASSERT(item.ReadValuesAsInRange<double>(TInstant::Zero(), TInstant::Zero()).empty());
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(item.ReadValuesAs<bool>().size(), 1);
                }
            });
        });
    }

    Y_UNIT_TEST(ConcurrentWritersWithOwningSnapshots) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 64 * 8192, .ChunkSizeBytes = 64, .MaxLines = 2, .ReserveChunks = 3});
        std::array<TLine<TRawLineFrontend<>>, 2> lines = {backend.CreateLine("a", {}), backend.CreateLine("b", {})};
        Pump(&backend);
        std::array<TVector<ui64>, 2> accepted;
        std::atomic<ui32> done = 0;
        std::array<std::thread, 2> writers;
        for (ui32 id = 0; id < 2; ++id) {
            writers[id] = std::thread([&, id] {
                for (ui64 i = 0; i < 10000; ++i) {
                    if (lines[id].Append(i)) {
                        accepted[id].push_back(i);
                    }
                }
                lines[id].Close();
                done.fetch_add(1, std::memory_order_release);
            });
        }
        TInMemorySnapshot previous;
        do {
            Pump(&backend);
            previous = backend.CaptureSnapshot();
            previous.Read([](const TSnapshotView& view) {
                view.ForEachLine([](const TLineSnapshot& line) {
                    const auto values = line.ReadValuesAs<ui64>();
                    UNIT_ASSERT(std::is_sorted(values.begin(), values.end()));
                });
            });
        } while (done.load(std::memory_order_acquire) != 2);
        for (auto& writer : writers) {
            writer.join();
        }
        Pump(&backend);
        auto snapshot = backend.CaptureSnapshot();
        UNIT_ASSERT_VALUES_EQUAL(Values(snapshot, "a"), accepted[0]);
        UNIT_ASSERT_VALUES_EQUAL(Values(snapshot, "b"), accepted[1]);
        UNIT_ASSERT_VALUES_EQUAL(backend.GetStats().AppendFailuresTotal, 20000 - accepted[0].size() - accepted[1].size());
    }

    Y_UNIT_TEST(ConcurrentPinReleaseDuringEviction) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 64 * 8, .ChunkSizeBytes = 64, .MaxLines = 1, .ReserveChunks = 3});
        auto line = backend.CreateLine("reused", {});
        Pump(&backend);
        TLockFreeQueue<TInMemorySnapshot> snapshots;
        std::atomic<ui32> queued = 0;
        std::atomic<bool> writerDone = false;
        std::atomic<bool> managerDone = false;
        std::atomic<bool> consistent = true;
        ui32 accepted = 0;
        std::thread writer([&] {
            for (ui64 i = 0; i < 20000; ++i) {
                accepted += line.Append(i);
                std::this_thread::yield();
            }
            line.Close();
            writerDone.store(true, std::memory_order_release);
        });
        std::thread reader([&] {
            while (!managerDone.load(std::memory_order_acquire) || queued.load()) {
                TInMemorySnapshot snapshot;
                if (snapshots.Dequeue(&snapshot)) {
                    const auto first = Values(snapshot, "reused");
                    std::this_thread::yield();
                    const auto second = Values(snapshot, "reused");
                    if (first != second || !std::is_sorted(first.begin(), first.end())) {
                        consistent.store(false);
                    }
                    snapshot = {}; // Last pin may race RetireChunk on the owner.
                    queued.fetch_sub(1);
                } else {
                    std::this_thread::yield();
                }
            }
        });
        do {
            Pump(&backend);
            if (queued.load() < 4) {
                queued.fetch_add(1);
                snapshots.Enqueue(backend.CaptureSnapshot());
            }
        } while (!writerDone.load(std::memory_order_acquire));
        managerDone.store(true, std::memory_order_release);
        writer.join();
        reader.join();
        Pump(&backend);
        UNIT_ASSERT(consistent.load());
        UNIT_ASSERT(accepted > 0);
        UNIT_ASSERT_VALUES_EQUAL(backend.GetStats().AppendFailuresTotal, 20000 - accepted);
        UNIT_ASSERT(backend.GetStats().MemoryUsedBytes <= 64 * 8);
    }

    Y_UNIT_TEST(SelfMetricsAndDisabledStorage) {
        TInMemoryMetricsBackend disabled({});
        UNIT_ASSERT(!disabled.CreateLine("disabled", {}));
        TInMemoryMetricsBackend backend({.MemoryBytes = 65536, .ChunkSizeBytes = 256, .MaxLines = 32});
        backend.UpdateSelfMetrics();
        Pump(&backend);
        backend.UpdateSelfMetrics();
        UNIT_ASSERT(!Values(backend.CaptureSnapshot(), "inmemory_metrics.memory_used_bytes").empty());
        UNIT_ASSERT_VALUES_EQUAL(backend.GetStats().Lines, 0); // Self metrics excluded.
    }
}
