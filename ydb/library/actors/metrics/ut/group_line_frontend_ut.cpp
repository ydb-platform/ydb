#include <ydb/library/actors/metrics/inmemory_backend.h>
#include <ydb/library/actors/metrics/lines/group_line_frontend.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

namespace {
    constexpr TStringBuf FieldNames[] = {"a", "b", "c", "d", "e", "f", "g", "h"};

    template<size_t I, class TValue>
    struct TTestField {
        using TValueType = TValue;
        static constexpr TStringBuf Name = FieldNames[I];
        inline static constexpr std::array<TLineLabelView, 1> Labels = {{{"unit", "count"}}};
    };

    template<class TValue, size_t... I>
    auto TestFields(std::index_sequence<I...>) -> std::tuple<TTestField<I, TValue>...>;

    template<class TValue, size_t N>
    struct TTestDescriptor {
        using TFields = decltype(TestFields<TValue>(std::make_index_sequence<N>{}));
    };

    struct TMemoryBytes : TTestField<0, ui64> {};
    struct TDataBytes : TTestField<1, ui64> {};
    struct TChecksumBytes : TTestField<2, ui64> {};
    struct TPBBytes : TTestField<3, ui64> {};
    struct TReserveBytes : TTestField<4, ui64> {};

    struct TDDiskResources {
        using TFields = std::tuple<TMemoryBytes, TDataBytes, TChecksumBytes, TPBBytes, TReserveBytes>;
    };

    struct TReadBytes {
        using TValueType = ui64;
        static constexpr TStringBuf Name = "bytes";
        inline static constexpr std::array<TLineLabelView, 2> Labels = {{{"operation", "read"}, {"unit", "bytes"}}};
    };

    struct TWriteBytes {
        using TValueType = i64;
        static constexpr TStringBuf Name = "bytes";
        inline static constexpr std::array<TLineLabelView, 2> Labels = {{{"operation", "write"}, {"unit", "bytes"}}};
    };

    struct TLatency {
        using TValueType = double;
        static constexpr TStringBuf Name = "latency";
        inline static constexpr std::array<TLineLabelView, 1> Labels = {{{"unit", "seconds"}}};
    };

    struct TResources {
        using TFields = std::tuple<TReadBytes, TWriteBytes, TLatency>;
    };
}

Y_UNIT_TEST_SUITE(GroupLineFrontend) {
    void Pump(TInMemoryMetricsBackend* backend) {
        backend->BeginMaintenance();
        backend->ProcessMaintenance();
    }

    Y_UNIT_TEST(SharedTimestampAndSnapshotIsolation) {
        using TFrontend = TGroupLineFrontend<TDDiskResources>;
        using TValues = TFrontend::TValueType;
        static_assert(sizeof(TFrontend::TStorageRecord) == 48);
        TInMemoryMetricsBackend backend({.MemoryBytes = 256 * 8, .ChunkSizeBytes = 256, .MaxLines = 1});
        auto line = backend.CreateLine<TFrontend>("resources", {});
        Pump(&backend);
        const TValues first = {
            TFrontend::Value<TMemoryBytes>(1),
            TFrontend::Value<TDataBytes>(2),
            TFrontend::Value<TChecksumBytes>(3),
            TFrontend::Value<TPBBytes>(4),
            TFrontend::Value<TReserveBytes>(5),
        };
        // Named values may be supplied in any order.
        const TValues second = {
            TFrontend::Value<TReserveBytes>(10),
            TFrontend::Value<TPBBytes>(9),
            TFrontend::Value<TChecksumBytes>(8),
            TFrontend::Value<TDataBytes>(7),
            TFrontend::Value<TMemoryBytes>(6),
        };
        UNIT_ASSERT(line.Append(first));
        auto snapshot = backend.CaptureSnapshot(line.GetLineId());
        UNIT_ASSERT(line.Append(second));
        snapshot.Read([&](const TSnapshotView& view) {
            UNIT_ASSERT_VALUES_EQUAL(view.LinesSize(), 1);
            const auto records = TFrontend::ReadRecords(view.GetLine(0));
            UNIT_ASSERT_VALUES_EQUAL(records.size(), 1);
            UNIT_ASSERT(records[0].Value == first);
            const auto filtered = TFrontend::ReadRecords(view.GetLine(0), records[0].Timestamp, records[0].Timestamp);
            UNIT_ASSERT_VALUES_EQUAL(filtered.size(), 1);
            UNIT_ASSERT(filtered[0].Value == first);
            UNIT_ASSERT(TFrontend::ReadValues(view.GetLine(0), TInstant::Zero(), TInstant::Zero()).empty());
            UNIT_ASSERT(TFrontend::ReadValues(view.GetLine(0), TInstant::Max(), TInstant::Zero()).empty());
        });
        backend.CaptureSnapshot(line.GetLineId()).Read([&](const TSnapshotView& view) {
            const auto values = TFrontend::ReadValues(view.GetLine(0));
            UNIT_ASSERT_VALUES_EQUAL(values.size(), 2);
            UNIT_ASSERT(values[0] == first);
            UNIT_ASSERT(values[1] == second);
            UNIT_ASSERT_VALUES_EQUAL(values[1].Get<TMemoryBytes>(), 6);
            UNIT_ASSERT_VALUES_EQUAL(values[1].Get<TDataBytes>(), 7);
            UNIT_ASSERT_VALUES_EQUAL(values[1].Get<TChecksumBytes>(), 8);
            UNIT_ASSERT_VALUES_EQUAL(values[1].Get<TPBBytes>(), 9);
            UNIT_ASSERT_VALUES_EQUAL(values[1].Get<TReserveBytes>(), 10);
        });
        UNIT_ASSERT_VALUES_EQUAL(backend.GetStats().Lines, 1);
    }

    Y_UNIT_TEST(ChunkBoundaryDoesNotPublishPartialGroup) {
        using TFrontend = TGroupLineFrontend<TTestDescriptor<i64, 5>>;
        using TValues = TFrontend::TValueType;
        TInMemoryMetricsBackend backend({.MemoryBytes = 64 * 8, .ChunkSizeBytes = 64, .MaxLines = 1, .ReserveChunks = 1});
        auto line = backend.CreateLine<TFrontend>("signed", {});
        Pump(&backend);
        const TValues first = {
            TFrontend::Value<TTestField<0, i64>>(-1),
            TFrontend::Value<TTestField<1, i64>>(-2),
            TFrontend::Value<TTestField<2, i64>>(0),
            TFrontend::Value<TTestField<3, i64>>(4),
            TFrontend::Value<TTestField<4, i64>>(5),
        };
        const TValues second = {
            TFrontend::Value<TTestField<0, i64>>(-6),
            TFrontend::Value<TTestField<1, i64>>(7),
            TFrontend::Value<TTestField<2, i64>>(-8),
            TFrontend::Value<TTestField<3, i64>>(9),
            TFrontend::Value<TTestField<4, i64>>(10),
        };
        UNIT_ASSERT(line.Append(first));
        UNIT_ASSERT(!line.Append(second));
        backend.CaptureSnapshot(line.GetLineId()).Read([&](const TSnapshotView& view) {
            const auto values = TFrontend::ReadValues(view.GetLine(0));
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
            const auto values = TFrontend::ReadValues(view.GetLine(0));
            UNIT_ASSERT_VALUES_EQUAL(values.size(), 2);
            UNIT_ASSERT(values[0] == first);
            UNIT_ASSERT(values[1] == second);
        });
    }

    Y_UNIT_TEST(RecordMustFitInEmptyChunk) {
        using TFrontend = TGroupLineFrontend<TTestDescriptor<ui64, 8>>;
        using TValues = TFrontend::TValueType;
        TInMemoryMetricsBackend backend({.MemoryBytes = 64 * 8, .ChunkSizeBytes = 64, .MaxLines = 1});
        auto line = backend.CreateLine<TFrontend>("oversized", {});
        Pump(&backend);
        const TValues oversized = {
            TFrontend::Value<TTestField<0, ui64>>(0),
            TFrontend::Value<TTestField<1, ui64>>(0),
            TFrontend::Value<TTestField<2, ui64>>(0),
            TFrontend::Value<TTestField<3, ui64>>(0),
            TFrontend::Value<TTestField<4, ui64>>(0),
            TFrontend::Value<TTestField<5, ui64>>(0),
            TFrontend::Value<TTestField<6, ui64>>(0),
            TFrontend::Value<TTestField<7, ui64>>(0),
        };
        UNIT_ASSERT(!line.Append(oversized));
        // The first attempt obtains a writable chunk and replenishes its reserve.
        Pump(&backend);
        const auto before = backend.GetStats();
        for (size_t i = 0; i < 3; ++i) {
            UNIT_ASSERT(!line.Append(oversized));
            Pump(&backend);
        }
        const auto after = backend.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(after.CommittedBytes, before.CommittedBytes);
        UNIT_ASSERT_VALUES_EQUAL(after.UsedChunks, before.UsedChunks);
        UNIT_ASSERT_VALUES_EQUAL(after.AppendFailuresTotal, before.AppendFailuresTotal + 3);
        backend.CaptureSnapshot(line.GetLineId()).Read([&](const TSnapshotView& view) {
            UNIT_ASSERT_VALUES_EQUAL(view.LinesSize(), 1);
            UNIT_ASSERT(TFrontend::ReadValues(view.GetLine(0)).empty());
        });
    }

    Y_UNIT_TEST(MultipleRecordsAndSnapshotOutlivesBackend) {
        using TFrontend = TGroupLineFrontend<TTestDescriptor<double, 2>>;
        using TValues = TFrontend::TValueType;
        const auto sample = [](double value) {
            return TValues{
                TFrontend::Value<TTestField<0, double>>(value),
                TFrontend::Value<TTestField<1, double>>(-value),
            };
        };
        TInMemorySnapshot snapshot;
        {
            TInMemoryMetricsBackend backend({.MemoryBytes = 128 * 8, .ChunkSizeBytes = 128, .MaxLines = 1});
            auto line = backend.CreateLine<TFrontend>("floating", {});
            UNIT_ASSERT(!line.Append(sample(0.0)));
            Pump(&backend);
            for (size_t i = 0; i < 5; ++i) {
                UNIT_ASSERT(line.Append(sample(double(i) + 0.5)));
            }
            snapshot = backend.CaptureSnapshot(line.GetLineId());
            UNIT_ASSERT(!line.Append(sample(5.5)));
            Pump(&backend);
            UNIT_ASSERT(line.Append(sample(5.5)));
        }
        snapshot.Read([&](const TSnapshotView& view) {
            const auto records = TFrontend::ReadRecords(view.GetLine(0));
            UNIT_ASSERT_VALUES_EQUAL(records.size(), 5);
            for (size_t i = 0; i < records.size(); ++i) {
                UNIT_ASSERT((records[i].Value == sample(double(i) + 0.5)));
            }
            const auto filtered = TFrontend::ReadValues(view.GetLine(0), records.front().Timestamp, records.back().Timestamp);
            UNIT_ASSERT_VALUES_EQUAL(filtered.size(), records.size());
        });
    }

    Y_UNIT_TEST(DescriptorTypesKeysAndLabels) {
        using TFrontend = TGroupLineFrontend<TResources>;
        using TValues = TFrontend::TValueType;
        static_assert(std::is_same_v<decltype(std::declval<const TValues&>().Get<TReadBytes>()), const ui64&>);
        static_assert(std::is_same_v<decltype(std::declval<const TValues&>().Get<TWriteBytes>()), const i64&>);
        static_assert(std::is_same_v<decltype(std::declval<const TValues&>().Get<TLatency>()), const double&>);
        static_assert(TFrontend::Fields[0].Name == "bytes");
        static_assert(TFrontend::Fields[0].Labels[0].Name == "operation");
        static_assert(TFrontend::Fields[0].Labels[0].Value == "read");
        TInMemoryMetricsBackend backend({.MemoryBytes = 256 * 8, .ChunkSizeBytes = 256, .MaxLines = 1});
        const std::array<TLabel, 1> labels = {{{"device", "42"}}};
        auto line = backend.CreateLine<TFrontend>("resources", labels);
        Pump(&backend);
        const TValues values = {
            TFrontend::Value<TReadBytes>(std::numeric_limits<ui64>::max()),
            TFrontend::Value<TWriteBytes>(-17),
            TFrontend::Value<TLatency>(0.125),
        };
        UNIT_ASSERT(line.Append(values));
        auto snapshot = backend.CaptureSnapshot(line.GetLineId());
        snapshot.Read([&](const TSnapshotView& view) {
            const auto& captured = view.GetLine(0);
            UNIT_ASSERT_VALUES_EQUAL(captured.Name, "resources");
            UNIT_ASSERT_VALUES_EQUAL(captured.Labels[0].Name, "device");
            UNIT_ASSERT_VALUES_EQUAL(captured.Labels[0].Value, "42");
            const auto fields = captured.Meta.Frontend->Fields;
            UNIT_ASSERT_VALUES_EQUAL(fields.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(fields[1].Name, "bytes");
            UNIT_ASSERT_VALUES_EQUAL(fields[1].Labels[0].Name, "operation");
            UNIT_ASSERT_VALUES_EQUAL(fields[1].Labels[0].Value, "write");
            UNIT_ASSERT_VALUES_EQUAL(fields[2].Name, "latency");
            UNIT_ASSERT_VALUES_EQUAL(fields[2].Labels[0].Value, "seconds");
            const auto records = TFrontend::ReadRecords(captured);
            UNIT_ASSERT_VALUES_EQUAL(records.size(), 1);
            UNIT_ASSERT(records[0].Value == values);
            UNIT_ASSERT_VALUES_EQUAL(records[0].Value.Get<TReadBytes>(), std::numeric_limits<ui64>::max());
            UNIT_ASSERT_VALUES_EQUAL(records[0].Value.Get<TWriteBytes>(), -17);
            UNIT_ASSERT_VALUES_EQUAL(records[0].Value.Get<TLatency>(), 0.125);
            using TWrongFrontend = TGroupLineFrontend<TTestDescriptor<ui64, 3>>;
            UNIT_ASSERT_EXCEPTION(TWrongFrontend::ReadRecords(captured), yexception);
            UNIT_ASSERT_EXCEPTION(TWrongFrontend::ReadValues(captured), yexception);
        });
    }

    Y_UNIT_TEST(NamedValuesRequireEveryFieldExactlyOnce) {
        using TFrontend = TGroupLineFrontend<TResources>;
        using TValues = TFrontend::TValueType;
        using TRead = TFrontend::TFieldValue<TReadBytes>;
        using TWrite = TFrontend::TFieldValue<TWriteBytes>;
        using TTime = TFrontend::TFieldValue<TLatency>;
        using TForeign = TFrontend::TFieldValue<TMemoryBytes>;
        static_assert(std::is_constructible_v<TValues, TRead, TWrite, TTime>);
        static_assert(std::is_constructible_v<TValues, TTime, TRead, TWrite>);
        static_assert(!std::is_default_constructible_v<TValues>);
        static_assert(!std::is_constructible_v<TValues, ui64, i64, double>);
        static_assert(!std::is_constructible_v<TValues, std::tuple<ui64, i64, double>>);
        static_assert(!std::is_constructible_v<TValues, TRead, TWrite>);
        static_assert(!std::is_constructible_v<TValues, TRead, TRead, TTime>);
        static_assert(!std::is_constructible_v<TValues, TRead, TWrite, TForeign>);
        static_assert(!std::is_constructible_v<TValues, TRead, TWrite, TTime, TForeign>);
        constexpr TValues values = {
            TFrontend::Value<TLatency>(0.25),
            TFrontend::Value<TWriteBytes>(-2),
            TFrontend::Value<TReadBytes>(1),
        };
        static_assert(values.Get<TReadBytes>() == 1);
        static_assert(values.Get<TWriteBytes>() == -2);
        static_assert(values.Get<TLatency>() == 0.25);
    }
}
