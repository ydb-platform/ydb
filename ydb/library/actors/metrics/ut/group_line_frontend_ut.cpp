#include <ydb/library/actors/metrics/inmemory_backend.h>
#include <ydb/library/actors/metrics/lines/group_line_frontend.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

Y_UNIT_TEST_SUITE(GroupLineFrontend) {
    void Pump(TInMemoryMetricsBackend* backend) {
        backend->BeginMaintenance();
        backend->ProcessMaintenance();
    }

    Y_UNIT_TEST(SharedTimestampAndSnapshotIsolation) {
        using TFrontend = TGroupLineFrontend<5>;
        using TValues = TFrontend::TValueType;
        static_assert(sizeof(TFrontend::TStorageRecord) == 48);
        TInMemoryMetricsBackend backend({.MemoryBytes = 256 * 8, .ChunkSizeBytes = 256, .MaxLines = 1});
        auto line = backend.CreateLine<TFrontend>("resources", {});
        Pump(&backend);
        const TValues first = {1, 2, 3, 4, 5};
        const TValues second = {6, 7, 8, 9, 10};
        UNIT_ASSERT(line.Append(first));
        auto snapshot = backend.CaptureSnapshot(line.GetLineId());
        UNIT_ASSERT(line.Append(second));
        snapshot.Read([&](const TSnapshotView& view) {
            UNIT_ASSERT_VALUES_EQUAL(view.LinesSize(), 1);
            const auto records = view.GetLine(0).ReadRecordsAs<TValues>();
            UNIT_ASSERT_VALUES_EQUAL(records.size(), 1);
            UNIT_ASSERT(records[0].Value == first);
            const auto filtered = view.GetLine(0).ReadRecordsAsInRange<TValues>(records[0].Timestamp, records[0].Timestamp);
            UNIT_ASSERT_VALUES_EQUAL(filtered.size(), 1);
            UNIT_ASSERT(filtered[0].Value == first);
            UNIT_ASSERT(view.GetLine(0).ReadValuesAsInRange<TValues>(TInstant::Zero(), TInstant::Zero()).empty());
            UNIT_ASSERT(view.GetLine(0).ReadValuesAsInRange<TValues>(TInstant::Max(), TInstant::Zero()).empty());
        });
        backend.CaptureSnapshot(line.GetLineId()).Read([&](const TSnapshotView& view) {
            const auto values = view.GetLine(0).ReadValuesAs<TValues>();
            UNIT_ASSERT_VALUES_EQUAL(values.size(), 2);
            UNIT_ASSERT(values[0] == first);
            UNIT_ASSERT(values[1] == second);
        });
        UNIT_ASSERT_VALUES_EQUAL(backend.GetStats().Lines, 1);
    }

    Y_UNIT_TEST(ChunkBoundaryDoesNotPublishPartialGroup) {
        using TFrontend = TGroupLineFrontend<5, i64>;
        using TValues = TFrontend::TValueType;
        TInMemoryMetricsBackend backend({.MemoryBytes = 64 * 8, .ChunkSizeBytes = 64, .MaxLines = 1, .ReserveChunks = 1});
        auto line = backend.CreateLine<TFrontend>("signed", {});
        Pump(&backend);
        const TValues first = {-1, -2, 0, 4, 5};
        const TValues second = {-6, 7, -8, 9, 10};
        UNIT_ASSERT(line.Append(first));
        UNIT_ASSERT(!line.Append(second));
        backend.CaptureSnapshot(line.GetLineId()).Read([&](const TSnapshotView& view) {
            const auto values = view.GetLine(0).ReadValuesAs<TValues>();
            UNIT_ASSERT_VALUES_EQUAL(values.size(), 1);
            UNIT_ASSERT(values[0] == first);
        });
        Pump(&backend);
        UNIT_ASSERT(line.Append(second));
        line.Close();
        UNIT_ASSERT(!line.Append(first));
        Pump(&backend);
        backend.CaptureSnapshot().Read([&](const TSnapshotView& view) {
            UNIT_ASSERT(view.GetLine(0).Closed);
            const auto values = view.GetLine(0).ReadValuesAs<TValues>();
            UNIT_ASSERT_VALUES_EQUAL(values.size(), 2);
            UNIT_ASSERT(values[0] == first);
            UNIT_ASSERT(values[1] == second);
        });
    }

    Y_UNIT_TEST(RecordMustFitInEmptyChunk) {
        using TFrontend = TGroupLineFrontend<8>;
        using TValues = TFrontend::TValueType;
        TInMemoryMetricsBackend backend({.MemoryBytes = 64 * 8, .ChunkSizeBytes = 64, .MaxLines = 1});
        auto line = backend.CreateLine<TFrontend>("oversized", {});
        Pump(&backend);
        UNIT_ASSERT(!line.Append(TValues{}));
        // The first attempt obtains a writable chunk and replenishes its reserve.
        Pump(&backend);
        const auto before = backend.GetStats();
        for (size_t i = 0; i < 3; ++i) {
            UNIT_ASSERT(!line.Append(TValues{}));
            Pump(&backend);
        }
        const auto after = backend.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(after.CommittedBytes, before.CommittedBytes);
        UNIT_ASSERT_VALUES_EQUAL(after.UsedChunks, before.UsedChunks);
        UNIT_ASSERT_VALUES_EQUAL(after.AppendFailuresTotal, before.AppendFailuresTotal + 3);
        backend.CaptureSnapshot(line.GetLineId()).Read([&](const TSnapshotView& view) {
            UNIT_ASSERT_VALUES_EQUAL(view.LinesSize(), 1);
            UNIT_ASSERT(view.GetLine(0).ReadValuesAs<TValues>().empty());
        });
    }

    Y_UNIT_TEST(MultipleRecordsAndSnapshotOutlivesBackend) {
        using TFrontend = TGroupLineFrontend<2, double>;
        using TValues = TFrontend::TValueType;
        TInMemorySnapshot snapshot;
        {
            TInMemoryMetricsBackend backend({.MemoryBytes = 128 * 8, .ChunkSizeBytes = 128, .MaxLines = 1});
            auto line = backend.CreateLine<TFrontend>("floating", {});
            UNIT_ASSERT(!line.Append(TValues{}));
            Pump(&backend);
            for (size_t i = 0; i < 5; ++i) {
                UNIT_ASSERT(line.Append({double(i) + 0.5, -double(i) - 0.5}));
            }
            snapshot = backend.CaptureSnapshot(line.GetLineId());
            UNIT_ASSERT(!line.Append({5.5, -5.5}));
            Pump(&backend);
            UNIT_ASSERT(line.Append({5.5, -5.5}));
        }
        snapshot.Read([&](const TSnapshotView& view) {
            const auto records = view.GetLine(0).ReadRecordsAs<TValues>();
            UNIT_ASSERT_VALUES_EQUAL(records.size(), 5);
            for (size_t i = 0; i < records.size(); ++i) {
                UNIT_ASSERT((records[i].Value == TValues{double(i) + 0.5, -double(i) - 0.5}));
            }
            const auto filtered = view.GetLine(0).ReadValuesAsInRange<TValues>(records.front().Timestamp, records.back().Timestamp);
            UNIT_ASSERT_VALUES_EQUAL(filtered.size(), records.size());
        });
    }
}
