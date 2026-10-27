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
#include <limits>

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

    Y_UNIT_TEST(QueuedCloseSurvivesRemovalFromRegistry) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 64 * 8, .ChunkSizeBytes = 64, .MaxLines = 1});
        auto state = std::make_shared<TLineWriterState>(&backend);
        backend.RegisterLine(state, MakeLineKey("old", {}), TRawLineFrontend<>::MakeMeta());
        TLine<TRawLineFrontend<>> old(state);
        Pump(&backend);
        UNIT_ASSERT(old.Append(42));
        std::weak_ptr<TLineWriterState> weak = state;
        state.reset();
        UNIT_ASSERT(old.Append(43));
        UNIT_ASSERT(old.Append(44));
        UNIT_ASSERT(!old.Append(45)); // Leave a refill request owning the state.
        old.Close();
        auto next = backend.CreateLine("next", {});
        UNIT_ASSERT(next.GetLineId());
        UNIT_ASSERT(!weak.expired()); // The queued node owns its lifetime.
        Pump(&backend);
        UNIT_ASSERT(weak.expired());
        UNIT_ASSERT(next.Append(99));
    }

    Y_UNIT_TEST(BackendDestructionDrainsMoreThanOneBatchOfPendingCloses) {
        for (bool registered : {false, true}) {
            constexpr ui32 count = 160;
            std::array<std::weak_ptr<TLineWriterState>, count> states;
            {
                TInMemoryMetricsBackend backend({.MemoryBytes = 64, .ChunkSizeBytes = 64, .MaxLines = count});
                TVector<TLine<TRawLineFrontend<>>> lines;
                for (ui32 i = 0; i < count; ++i) {
                    auto state = std::make_shared<TLineWriterState>(&backend);
                    states[i] = state;
                    if (registered) {
                        backend.RegisterLine(state, MakeLineKey(TStringBuilder() << "line" << i, {}), TRawLineFrontend<>::MakeMeta());
                    }
                    lines.emplace_back(state);
                }
                for (auto& line : lines) {
                    line.Close();
                }
                if (registered) {
                    for (const auto& state : states) {
                        UNIT_ASSERT(!state.expired()); // Queued closes/refills retain ownership.
                    }
                }
            }
            for (const auto& state : states) {
                UNIT_ASSERT(state.expired());
            }
        }
    }

    Y_UNIT_TEST(WaitingRefillResumesAfterLastPinWithoutAnotherAppend) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 64, .ChunkSizeBytes = 64, .MaxLines = 1});
        auto line = backend.CreateLine("waiting", {});
        Pump(&backend);
        UNIT_ASSERT(line.Append(1));
        UNIT_ASSERT(line.Append(2));
        UNIT_ASSERT(line.Append(3));
        auto pinned = backend.CaptureSnapshot();
        UNIT_ASSERT(!line.Append(4));
        Pump(&backend);
        for (ui32 i = 0; i < 100; ++i) { UNIT_ASSERT(!line.Append(4)); }
        Pump(&backend); // Coalesced retries retain one waiting entry.
        pinned = {};
        Pump(&backend); // The return alone makes the waiting line runnable.
        UNIT_ASSERT(line.Append(4));
    }

    Y_UNIT_TEST(QueuedSealedChunksSurviveAdmissionEviction) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 64 * 100, .ChunkSizeBytes = 64,
            .MaxLines = 1, .ReserveChunks = 80, .FreeChunkReservePercent = 0});
        auto old = backend.CreateLine("old", {});
        Pump(&backend);
        Pump(&backend);
        for (ui64 i = 0; i < 240; ++i) {
            UNIT_ASSERT(old.Append(i));
        }
        auto pinned = backend.CaptureSnapshot();
        old.Close();
        // Retire queued nodes before the actor has consumed their seal messages.
        auto next = backend.CreateLine("next", {});
        UNIT_ASSERT(next.GetLineId());
        Pump(&backend);
        Pump(&backend);
        UNIT_ASSERT(next.Append(999));
        UNIT_ASSERT_VALUES_EQUAL(Values(pinned, "old").size(), 240);
        pinned = {};
        Pump(&backend);
        UNIT_ASSERT_VALUES_EQUAL(Values(backend.CaptureSnapshot(), "next"), TVector<ui64>{999});
    }

    Y_UNIT_TEST(RetiringSnapshotOutlivesNotificationEndpoint) {
        std::atomic<ui32> notifications = 0;
        TInMemorySnapshot pinned;
        {
            TInMemoryMetricsBackend backend({.MemoryBytes = 64 * 4, .ChunkSizeBytes = 64,
                .MaxLines = 1}, [&] { ++notifications; });
            auto first = backend.CreateLine("first", {});
            Pump(&backend);
            UNIT_ASSERT(first.Append(42));
            pinned = backend.CaptureSnapshot();
            first.Close();
            auto second = backend.CreateLine("second", {});
            UNIT_ASSERT(second.GetLineId());
            UNIT_ASSERT_VALUES_EQUAL(backend.GetStats().RetiringChunks, 1);
        }
        const auto before = notifications.load();
        UNIT_ASSERT_VALUES_EQUAL(Values(pinned, "first"), TVector<ui64>{42});
        std::thread lateRelease([snapshot = std::move(pinned)]() mutable { snapshot = {}; });
        lateRelease.join();
        UNIT_ASSERT_VALUES_EQUAL(notifications.load(), before);
    }

    Y_UNIT_TEST(ReleasedQueueHasMultipleProducersAndOwnerOnlyFreeDeque) {
        TChunkPool pool(128, 64);
        std::array<TChunk*, 128> chunks;
        for (auto& chunk : chunks) {
            chunk = pool.TryAcquire();
            chunk->State.store(EChunkState::Retiring);
            chunk->Readers.store(std::numeric_limits<i32>::min() + 1);
        }
        pool.RetiringCount = chunks.size();
        std::atomic<ui32> notified = 0;
        pool.SetNotify([&] { ++notified; });
        std::array<std::thread, 4> readers;
        for (ui32 i = 0; i < readers.size(); ++i) {
            readers[i] = std::thread([&, i] {
                for (ui32 j = i; j < chunks.size(); j += readers.size()) {
                    pool.ReleasePin(chunks[j]);
                }
            });
        }
        for (auto& reader : readers) { reader.join(); }
        UNIT_ASSERT_VALUES_EQUAL(pool.FreeCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(notified.load(), 128);
        while (pool.DrainReleased()) {}
        UNIT_ASSERT_VALUES_EQUAL(pool.FreeCount(), 128);
        UNIT_ASSERT_VALUES_EQUAL(pool.RetiringCount, 0);
        std::array<bool, 128> seen{};
        while (TChunk* chunk = pool.TryAcquire()) {
            UNIT_ASSERT(!seen[chunk->ChunkId]);
            seen[chunk->ChunkId] = true;
        }
        pool.StopNotify();
    }

    Y_UNIT_TEST(FreeReserveDoesNotRepeatedlyRetirePinnedHistory) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 64 * 20, .ChunkSizeBytes = 64,
            .MaxLines = 1, .ReserveChunks = 1, .FreeChunkReservePercent = 20});
        auto line = backend.CreateLine("history", {});
        Pump(&backend);
        for (ui64 i = 0; i < 51; ++i) {
            UNIT_ASSERT(line.Append(i));
            Pump(&backend);
        }
        auto pinned = backend.CaptureSnapshot();
        UNIT_ASSERT(line.Append(51));
        Pump(&backend);
        UNIT_ASSERT(line.Append(52));
        UNIT_ASSERT(line.Append(53));
        UNIT_ASSERT(line.Append(54));
        Pump(&backend);
        Pump(&backend);
        const auto retiring = backend.GetStats().RetiringChunks;
        UNIT_ASSERT(retiring > 0);
        for (ui32 i = 0; i < 10; ++i) { Pump(&backend); }
        UNIT_ASSERT_VALUES_EQUAL(backend.GetStats().RetiringChunks, retiring);
        UNIT_ASSERT_VALUES_EQUAL(Values(pinned, "history").size(), 51);
        pinned = {};
        Pump(&backend);
        UNIT_ASSERT_VALUES_EQUAL(backend.GetStats().RetiringChunks, 0);
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
            auto state = std::make_shared<TLineWriterState>(&backend);
            TLine<TRawLineFrontend<>> line(state);
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
        auto state = std::make_shared<TLineWriterState>(&backend);
        TLine<TRawLineFrontend<>> line(state);
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
        UNIT_ASSERT_VALUES_EQUAL(backend.GetStats().FreeChunks, 0);
        Pump(&backend);
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
    Y_UNIT_TEST(StatsFollowSealRetirementAndReturn) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 64 * 5, .ChunkSizeBytes = 64,
            .MaxLines = 1, .FreeChunkReservePercent = 0});
        auto line = backend.CreateLine("line", {});
        Pump(&backend);
        auto check = [&](ui64 bytes, ui64 used, ui64 writable, ui64 sealed, ui64 retiring, ui64 closed) {
            const auto stats = backend.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.CommittedBytes, bytes);
            UNIT_ASSERT_VALUES_EQUAL(stats.UsedChunks, used);
            UNIT_ASSERT_VALUES_EQUAL(stats.FreeChunks, 5 - used);
            UNIT_ASSERT_VALUES_EQUAL(stats.MemoryUsedBytes, used * 64);
            UNIT_ASSERT_VALUES_EQUAL(stats.WritableChunks, writable);
            UNIT_ASSERT_VALUES_EQUAL(stats.SealedChunks, sealed);
            UNIT_ASSERT_VALUES_EQUAL(stats.RetiringChunks, retiring);
            UNIT_ASSERT_VALUES_EQUAL(stats.Lines, 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.ClosedLines, closed);
        };
        check(0, 1, 0, 0, 0, 0);
        UNIT_ASSERT(line.Append(1));
        const ui64 firstBytes = backend.GetStats().CommittedBytes;
        UNIT_ASSERT(firstBytes > 0);
        UNIT_ASSERT(line.Append(2));
        UNIT_ASSERT(line.Append(3));
        const ui64 fullBytes = backend.GetStats().CommittedBytes;
        check(fullBytes, 1, 1, 0, 0, 0);
        UNIT_ASSERT(!line.Append(4));
        check(fullBytes, 1, 0, 1, 0, 0); // PendingSeal, before manager processing.
        Pump(&backend);
        check(fullBytes, 2, 0, 1, 0, 0);
        UNIT_ASSERT(line.Append(4));
        check(fullBytes + firstBytes, 2, 1, 1, 0, 0);
        auto pinned = backend.CaptureSnapshot();
        line.Close();
        check(fullBytes + firstBytes, 2, 1, 1, 0, 1);
        Pump(&backend);
        check(fullBytes + firstBytes, 2, 0, 2, 0, 1);
        line = backend.CreateLine("line", {});
        Pump(&backend);
        check(fullBytes + firstBytes, 3, 0, 0, 2, 0);
        pinned = {};
        Pump(&backend);
        check(0, 1, 0, 0, 0, 0);
    }

    Y_UNIT_TEST(LineSnapshotDoesNotPinOtherLines) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 64 * 2, .ChunkSizeBytes = 64,
            .MaxLines = 2, .FreeChunkReservePercent = 0});
        auto first = backend.CreateLine("first", {});
        auto second = backend.CreateLine("second", {});
        Pump(&backend);
        UNIT_ASSERT(first.Append(1));
        UNIT_ASSERT(second.Append(2));
        auto snapshot = backend.CaptureSnapshot(first.GetLineId());
        snapshot.Read([](const TSnapshotView& view) {
            UNIT_ASSERT_VALUES_EQUAL(view.LinesSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(view.GetLine(0).Name, "first");
        });
        first.Close(); // Do not let its pending refill consume the released chunk.
        second.Close();
        second = backend.CreateLine("second", {});
        Pump(&backend);
        UNIT_ASSERT(second.Append(3)); // The unrelated chunk was free to reuse.
        UNIT_ASSERT_VALUES_EQUAL(backend.GetStats().RetiringChunks, 0);
        UNIT_ASSERT_VALUES_EQUAL(Values(snapshot, "first"), TVector<ui64>{1});
    }

    Y_UNIT_TEST(LineSnapshotSelectionAndLifetime) {
        TInMemorySnapshot saved;
        {
            TInMemoryMetricsBackend backend({.MemoryBytes = 64 * 8, .ChunkSizeBytes = 64, .MaxLines = 2});
            const TVector<TLabel> labels = {{"b", "2"}, {"a", "1"}};
            const TVector<TLabel> reverse = {{"a", "1"}, {"b", "2"}};
            auto first = backend.CreateLine("same", labels);
            auto second = backend.CreateLine("same", {});
            Pump(&backend);
            UNIT_ASSERT(first.Append(42));
            UNIT_ASSERT(second.Append(99));
            const ui32 oldId = first.GetLineId();
            saved = backend.CaptureSnapshot("same", reverse);
            saved.Read([](const TSnapshotView& view) { UNIT_ASSERT_VALUES_EQUAL(view.LinesSize(), 1); });
            first.Close();
            first = backend.CreateLine("same", labels);
            backend.CaptureSnapshot(oldId).Read([](const TSnapshotView& view) { UNIT_ASSERT_VALUES_EQUAL(view.LinesSize(), 0); });
            backend.CaptureSnapshot("missing", {}).Read([](const TSnapshotView& view) { UNIT_ASSERT_VALUES_EQUAL(view.LinesSize(), 0); });
            backend.CaptureSnapshot(0).Read([](const TSnapshotView& view) { UNIT_ASSERT_VALUES_EQUAL(view.LinesSize(), 0); });
        }
        UNIT_ASSERT_VALUES_EQUAL(Values(saved, "same"), TVector<ui64>{42});
    }

    Y_UNIT_TEST(FirstTimestampSurvivesAppendsAndChunkReuse) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 64, .ChunkSizeBytes = 64,
            .MaxLines = 1, .FreeChunkReservePercent = 0});
        auto state = std::make_shared<TLineWriterState>(&backend);
        backend.RegisterLine(state, MakeLineKey("line", {}), TRawLineFrontend<>::MakeMeta());
        TLine<TRawLineFrontend<>> line(state);
        Pump(&backend);
        UNIT_ASSERT(line.Append(1));
        TChunk* chunk = state->Reader->Storage.Writable.load();
        const auto firstTs = chunk->FirstTs.load();
        UNIT_ASSERT(line.Append(2));
        UNIT_ASSERT(line.Append(3));
        UNIT_ASSERT_VALUES_EQUAL(chunk->FirstTs.load(), firstTs);
        UNIT_ASSERT(!line.Append(4));
        Pump(&backend);
        UNIT_ASSERT(line.Append(4));
        const auto reusedTs = chunk->FirstTs.load();
        UNIT_ASSERT(reusedTs > firstTs);
        UNIT_ASSERT(line.Append(5));
        UNIT_ASSERT_VALUES_EQUAL(chunk->FirstTs.load(), reusedTs);
    }

    Y_UNIT_TEST(AdmissionDrainsClosesBeyondOneBatch) {
        constexpr ui32 count = NInMemoryMetricsPrivate::MaintenanceBatchSize + 5;
        TInMemoryMetricsBackend backend({.MemoryBytes = 64 * count, .ChunkSizeBytes = 64,
            .MaxLines = count, .FreeChunkReservePercent = 0});
        TVector<TLine<TRawLineFrontend<>>> lines;
        for (ui32 i = 0; i < count; ++i) {
            lines.push_back(backend.CreateLine(TStringBuilder() << "line" << i, {}));
        }
        Pump(&backend);
        Pump(&backend);
        for (auto& line : lines) {
            UNIT_ASSERT(line.Append(1));
            line.Close();
        }
        for (ui32 i = 0; i < count; ++i) {
            auto replacement = backend.CreateLine(TStringBuilder() << "replacement" << i, {});
            UNIT_ASSERT(replacement.GetLineId());
        }
    }

    Y_UNIT_TEST(TargetedRemovalPreservesOldestChunkOrder) {
        constexpr ui32 count = 16;
        TInMemoryMetricsBackend backend({.MemoryBytes = 64 * count, .ChunkSizeBytes = 64,
            .MaxLines = count + 1, .FreeChunkReservePercent = 0});
        std::array<ui32, count> ids;
        auto writeClosed = [&](const TString& name, ui64 value) {
            auto line = backend.CreateLine(name, {});
            const ui32 id = line.GetLineId();
            UNIT_ASSERT(id);
            Pump(&backend);
            UNIT_ASSERT(line.Append(value));
            line.Close();
            Pump(&backend);
            return id;
        };
        for (ui32 i = 0; i < count; ++i) {
            ids[i] = writeClosed(TStringBuilder() << "line" << i, i);
        }
        // Remove arbitrary interior heap entries, then fill their freed chunks.
        for (ui32 i = 0; i < count; ++i) {
            const ui32 index = i * 7 % count;
            ids[index] = writeClosed(TStringBuilder() << "line" << index, 100 + index);
        }
        // Memory pressure must evict in the new timestamp order after all repairs.
        for (ui32 i = 0; i < count; ++i) {
            writeClosed(TStringBuilder() << "extra" << i, 200 + i);
            for (ui32 j = 0; j < count; ++j) {
                backend.CaptureSnapshot(ids[j * 7 % count]).Read([&](const TSnapshotView& view) {
                    UNIT_ASSERT_VALUES_EQUAL(view.LinesSize(), j <= i ? 0 : 1);
                });
            }
        }
    }

}
