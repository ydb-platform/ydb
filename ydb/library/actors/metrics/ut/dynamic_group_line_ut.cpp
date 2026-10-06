#include <ydb/library/actors/metrics/inmemory_backend.h>
#include <ydb/library/actors/metrics/lines/dynamic_group_line.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;
namespace {
    void Pump(TInMemoryMetricsBackend* backend) { backend->BeginMaintenance(); backend->ProcessMaintenance(); }
    class TTimestampedLine final : public IMetricLine {
    public:
        TTimestampedLine(IMetricLine& line, NHPTimer::STime timestamp)
            : Line(line), Timestamp(timestamp) {}
        bool IsValid() const noexcept override { return Line.IsValid(); }
        void Close() noexcept override { Line.Close(); }
        ui32 GetLineId() const noexcept override { return Line.GetLineId(); }
        NHPTimer::STime CurrentTimestampTs() const noexcept override { return Timestamp; }
        bool AccessChunkMemory(void* opaque, TAccessChunkMemoryFn access) noexcept override {
            return Line.AccessChunkMemory(opaque, access);
        }
        std::optional<ui64> GetLastMaterializedValue() const noexcept override {
            return Line.GetLastMaterializedValue();
        }
        void MarkMaterialized(ui64 value) noexcept override { Line.MarkMaterialized(value); }
    private:
        IMetricLine& Line;
        NHPTimer::STime Timestamp;
    };

    struct TTimestampedFrontend : TDynamicGroupFrontend {
        struct TValueType {
            NHPTimer::STime Timestamp;
            TDynamicGroupFrontend::TValueType Sample;
        };
        static bool Append(IMetricLine& line, const TValueType& value) noexcept {
            TTimestampedLine timestamped(line, value.Timestamp);
            return TDynamicGroupFrontend::Append(timestamped, value.Sample);
        }
    };
    using TRows = TVector<TVector<TLineNumericValue>>;
    TRows Read(const TInMemorySnapshot& snapshot, TInstant begin = TInstant::Zero(), TInstant end = TInstant::Max()) {
        TRows rows;
        snapshot.Read([&](const TSnapshotView& view) {
            if (!view.LinesSize()) return;
            UNIT_ASSERT_VALUES_EQUAL(view.LinesSize(), 1);
            const auto& line = view.GetLine(0);
            line.Meta.Frontend->ReadNumericRange(line, begin, end, &rows,
                [](void* opaque, TInstant, std::span<const TLineNumericValue> values) {
                    static_cast<TRows*>(opaque)->emplace_back(values.begin(), values.end());
                });
        });
        return rows;
    }
}
Y_UNIT_TEST_SUITE(DynamicGroupLine) {
    Y_UNIT_TEST(BackwardTimestampPreservesRangeTail) {
        for (const auto mode : {EGroupUpdateMode::OnChangeAll, EGroupUpdateMode::OnChangePartial}) {
            TInMemoryMetricsBackend backend({.MemoryBytes = 8192, .ChunkSizeBytes = 256, .MaxLines = 1});
            auto schema = std::make_shared<TDynamicGroupSchema>(TVector<TDynamicGroupField>{
                {"value", {}, EGroupValueType::Unsigned},
            }, mode);
            auto line = backend.CreateLine<TTimestampedFrontend>("state", {}, {schema});
            Pump(&backend);
            TDynamicGroupFrontend::TCache cache;
            const auto step = static_cast<NHPTimer::STime>(TDynamicGroupFrontend::TCodec::TimestampStep());
            const auto origin = backend.CurrentTimestampTs() / step * step;
            for (const auto [offset, value] : {std::pair{10, ui64(1)}, {20, ui64(2)}, {5, ui64(3)}}) {
                const std::array<TLineNumericValue, 1> values = {value};
                UNIT_ASSERT(line.Append({origin + offset * step, {schema.get(), values, &cache}}));
            }
            backend.CaptureSnapshot().Read([&](const TSnapshotView& view) {
                const auto& snapshot = view.GetLine(0);
                const auto begin = NInMemoryMetricsPrivate::TLineSnapshotAccess::DecodeTimestampTs(snapshot, origin + 15 * step);
                const auto end = NInMemoryMetricsPrivate::TLineSnapshotAccess::DecodeTimestampTs(snapshot, origin + 25 * step);
                TVector<TGenericRecordView<ui64>> rows;
                schema->ReadNumericRange(snapshot, begin, end, &rows,
                    [](void* opaque, TInstant timestamp, std::span<const TLineNumericValue> values) {
                        static_cast<TVector<TGenericRecordView<ui64>>*>(opaque)->push_back({timestamp, std::get<ui64>(values[0])});
                    });
                UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);
                UNIT_ASSERT_VALUES_EQUAL(rows[0].Timestamp, begin);
                UNIT_ASSERT_VALUES_EQUAL(rows[0].Value, 1);
                UNIT_ASSERT_VALUES_EQUAL(rows[1].Value, 2);
                UNIT_ASSERT_VALUES_EQUAL(rows[2].Timestamp, end);
                UNIT_ASSERT_VALUES_EQUAL(rows[2].Value, 2);
            });
        }
    }
    Y_UNIT_TEST(ParticipantLabelsAndSnapshotOwnSchema) {
        TInMemorySnapshot snapshot;
        {
            TInMemoryMetricsBackend backend({.MemoryBytes = 8192, .ChunkSizeBytes = 256, .MaxLines = 2});
            auto group = TDynamicGroupLine::Create(&backend, "pools", {
                {"cpu", {{"pool", "System"}}, EGroupValueType::Decimal},
                {"cpu", {{"pool", "User"}}, EGroupValueType::Decimal},
                {"events", {}, EGroupValueType::Unsigned},
            });
            Pump(&backend);
            const std::array<TLineNumericValue, 3> values = {1.234, 0.567, ui64(-1)};
            UNIT_ASSERT(group.Append(values));
            snapshot = backend.CaptureSnapshot();
            group = {};
        }
        const auto rows = Read(snapshot);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        UNIT_ASSERT_DOUBLES_EQUAL(std::get<double>(rows[0][0]), 1.23, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(std::get<double>(rows[0][1]), 0.57, 1e-9);
        UNIT_ASSERT_VALUES_EQUAL(std::get<ui64>(rows[0][2]), ui64(-1));
        snapshot.Read([](const TSnapshotView& view) {
            const auto fields = view.GetLine(0).Meta.Frontend->Fields;
            UNIT_ASSERT_VALUES_EQUAL(fields[0].Labels[0].Value, "System");
            UNIT_ASSERT_VALUES_EQUAL(fields[1].Labels[0].Value, "User");
        });
    }
    Y_UNIT_TEST(PartialChangesRolloverAndFrozenPrefix) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 8192, .ChunkSizeBytes = 128, .MaxLines = 2});
        auto group = TDynamicGroupLine::Create(&backend, "state", {
            {"quota", {}, EGroupValueType::Decimal}, {"needy", {}, EGroupValueType::Bool},
            {"counter", {}, EGroupValueType::Unsigned},
        }, EGroupUpdateMode::OnChangePartial);
        Pump(&backend);
        std::array<TLineNumericValue, 3> values = {1.234, false, ui64(0)};
        UNIT_ASSERT(group.Append(values));
        const auto prefix = backend.CaptureSnapshot();
        const auto bytes = backend.GetStats().CommittedBytes;
        values[0] = 1.231;
        UNIT_ASSERT(group.Append(values));
        UNIT_ASSERT_VALUES_EQUAL(backend.GetStats().CommittedBytes, bytes);
        values[0] = std::numeric_limits<double>::quiet_NaN();
        UNIT_ASSERT(!group.Append(values));
        values[0] = 1.23;
        for (ui64 i = 1; i <= 80; ++i) {
            values[2] = i;
            if (!group.Append(values)) { Pump(&backend); UNIT_ASSERT(group.Append(values)); }
            Pump(&backend);
        }
        UNIT_ASSERT_VALUES_EQUAL(Read(prefix).size(), 1);
        const auto rows = Read(backend.CaptureSnapshot());
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 81);
        for (size_t i = 0; i < rows.size(); ++i) {
            UNIT_ASSERT_DOUBLES_EQUAL(std::get<double>(rows[i][0]), 1.23, 1e-9);
            UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(rows[i][1]), false);
            UNIT_ASSERT_VALUES_EQUAL(std::get<ui64>(rows[i][2]), i);
        }
        UNIT_ASSERT(backend.GetStats().CommittedBytes < 81 * 16);
    }
    Y_UNIT_TEST(EvictedStateIsMaterializedAgain) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 64 * 2, .ChunkSizeBytes = 64,
            .MaxLines = 1, .FreeChunkReservePercent = 50});
        auto group = TDynamicGroupLine::Create(&backend, "state", {
            {"a", {}, EGroupValueType::Bool}, {"b", {}, EGroupValueType::Bool},
        }, EGroupUpdateMode::OnChangePartial);
        Pump(&backend);
        const std::array<TLineNumericValue, 2> initial = {true, false}, changed = {false, false};
        UNIT_ASSERT(group.Append(initial));
        UNIT_ASSERT(!group.Append(changed)); // Full chunk is sealed; cache still holds initial.
        Pump(&backend);
        Pump(&backend); // Refill consumed the free reserve on the preceding turn.
        UNIT_ASSERT(Read(backend.CaptureSnapshot()).empty());
        UNIT_ASSERT(group.Append(initial)); // Even unchanged state must survive eviction.
        const auto rows = Read(backend.CaptureSnapshot());
        UNIT_ASSERT(!rows.empty());
        UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(rows.back()[0]), true);
    }
    Y_UNIT_TEST(OnChangeAllAndRangeBoundary) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 8192, .ChunkSizeBytes = 256, .MaxLines = 2});
        auto group = TDynamicGroupLine::Create(&backend, "flags", {
            {"a", {}, EGroupValueType::Bool}, {"b", {}, EGroupValueType::Bool},
        }, EGroupUpdateMode::OnChangeAll);
        Pump(&backend);
        const std::array<TLineNumericValue, 2> values = {true, false};
        UNIT_ASSERT(group.Append(values));
        UNIT_ASSERT(group.Append(values));
        const auto snapshot = backend.CaptureSnapshot();
        UNIT_ASSERT_VALUES_EQUAL(Read(snapshot).size(), 1);
        const auto end = TInstant::Now() + TDuration::Seconds(2), begin = end - TDuration::Seconds(1);
        const auto rows = Read(snapshot, begin, end);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(rows[0][0]), true);
        UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(rows[1][1]), false);
        group.Close();
        UNIT_ASSERT(!group.Append(values));
    }
}
