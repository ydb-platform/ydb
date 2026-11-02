#include <ydb/library/actors/metrics/inmemory_backend.h>
#include <ydb/library/actors/metrics/lines/group_line_frontend.h>
#include <ydb/library/actors/metrics/lines/on_change_line_frontend.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

namespace {
    using TStorage = TCompressedLineStorage<0, TIntegerEncoding<>>;

    void Pump(TInMemoryMetricsBackend* backend) {
        backend->BeginMaintenance();
        backend->ProcessMaintenance();
    }

    struct TCpu {
        using TValueType = double;
        static constexpr TStringBuf Name = "cpu";
        inline static constexpr std::array<TLineLabelView, 0> Labels = {};
    };
    struct TEvents {
        using TValueType = ui64;
        static constexpr TStringBuf Name = "events_total";
        inline static constexpr std::array<TLineLabelView, 0> Labels = {};
    };
    struct TGroup {
        using TFields = std::tuple<TCpu, TEvents>;
    };
}

Y_UNIT_TEST_SUITE(CompressedLineStorage) {
    Y_UNIT_TEST(TimestampBeforeClockAnchor) {
        const TTimeAnchor anchor{static_cast<i64>(Us2Ts(1'000'000)), TInstant::Seconds(60)};
        const auto decoded = NInMemoryMetricsPrivate::DecodeTs(anchor, anchor.BaseCycles - Us2Ts(50'000));
        UNIT_ASSERT(decoded < anchor.BaseWallClock);
        UNIT_ASSERT_DOUBLES_EQUAL(static_cast<double>((anchor.BaseWallClock - decoded).MicroSeconds()), 50'000.0, 2.0);
    }

    Y_UNIT_TEST(TagsAndTruncatedInput) {
        for (const auto value : {ui64(0), ui64(63), ui64(64), ui64(16383), ui64(16384),
                (ui64(1) << 30) - 1, ui64(1) << 30, (ui64(1) << 62) - 1}) {
            std::array<char, 8> buffer;
            const size_t size = TStorage::Pack(value, buffer.data());
            const char* cursor = buffer.data();
            ui64 decoded;
            ui8 tag;
            UNIT_ASSERT(TStorage::Unpack(&cursor, buffer.data() + size, &decoded, &tag));
            UNIT_ASSERT_VALUES_EQUAL(decoded, value);
            UNIT_ASSERT_VALUES_EQUAL(cursor, buffer.data() + size);
            for (size_t length = 0; length < size; ++length) {
                cursor = buffer.data();
                UNIT_ASSERT(!TStorage::Unpack(&cursor, buffer.data() + length, &decoded, &tag));
            }
        }
        std::array<char, 8> buffer;
        UNIT_ASSERT_VALUES_EQUAL(TStorage::Pack(63, buffer.data()), 1);
        UNIT_ASSERT_VALUES_EQUAL(TStorage::Pack(64, buffer.data()), 2);
        UNIT_ASSERT_VALUES_EQUAL(TStorage::Pack(16384, buffer.data()), 4);
    }

    Y_UNIT_TEST(IntegerRangeAndReset) {
        using TUnsigned = TCompressedLineStorage<0, TIntegerEncoding<ELineValueEncoding::UnsignedDelta>>;
        for (const auto previous : {ui64(0), ui64(42), ui64(-1)}) {
            for (const auto value : {ui64(0), ui64(1), ui64(-1), ui64(1) << 63}) {
                std::array<char, 9> buffer;
                const size_t size = TUnsigned::PackValue<0>(value, previous, false, buffer.data());
                const char* cursor = buffer.data();
                ui64 decoded;
                UNIT_ASSERT(TUnsigned::UnpackValue<0>(&cursor, buffer.data() + size, previous, &decoded));
                UNIT_ASSERT_VALUES_EQUAL(decoded, value);
                const size_t signedSize = TStorage::PackValue<0>(value, previous, false, buffer.data());
                cursor = buffer.data();
                UNIT_ASSERT(TStorage::UnpackValue<0>(&cursor, buffer.data() + signedSize, previous, &decoded));
                UNIT_ASSERT_VALUES_EQUAL(decoded, value);
            }
        }
        std::array<char, 9> buffer;
        UNIT_ASSERT_VALUES_EQUAL(TUnsigned::PackValue<0>(1, 100, false, buffer.data()), 9);
        UNIT_ASSERT_VALUES_EQUAL(TStorage::PackValue<0>(99, 100, false, buffer.data()), 1);
    }

    Y_UNIT_TEST(FailedWritePreservesChunkAndTimestampQuantization) {
        using TQuantized = TCompressedLineStorage<100'000, TIntegerEncoding<>>;
        alignas(8) std::array<char, 128> payload = {};
        TWritableChunkMemory memory{.Payload = payload};
        const ui64 step = TQuantized::TimestampStep();
        TQuantized::TRecord<1> first{static_cast<i64>(step * 10 + step / 2), {100}};
        UNIT_ASSERT(TQuantized::WriteRecord<1>(&first, memory));
        TQuantized::TRecord<1> second{static_cast<i64>(step * 20 + step / 3), {101}};
        const auto firstBytes = memory.UsedPayloadBytes;
        UNIT_ASSERT(TQuantized::WriteRecord<1>(&second, memory));
        UNIT_ASSERT_VALUES_EQUAL(memory.UsedPayloadBytes - firstBytes, 2);
        const char* cursor = payload.data() + sizeof(TQuantized::THeader<1>);
        ui64 timestamp;
        ui8 tag;
        UNIT_ASSERT(TQuantized::Unpack(&cursor, payload.data() + memory.UsedPayloadBytes, &timestamp, &tag));
        UNIT_ASSERT_VALUES_EQUAL(timestamp, step * 10);
        ui64 value;
        UNIT_ASSERT(TQuantized::UnpackValue<0>(&cursor, payload.data() + memory.UsedPayloadBytes, 0, &value));
        UNIT_ASSERT_VALUES_EQUAL(value, 100);
        UNIT_ASSERT(TQuantized::Unpack(&cursor, payload.data() + memory.UsedPayloadBytes, &timestamp, &tag));
        UNIT_ASSERT_VALUES_EQUAL(timestamp, 10);
        const auto saved = payload;
        const auto savedBytes = memory.UsedPayloadBytes;
        memory.Payload = std::span<char>(payload.data(), savedBytes);
        UNIT_ASSERT(!TQuantized::WriteRecord<1>(&second, memory));
        UNIT_ASSERT(payload == saved);
        UNIT_ASSERT_VALUES_EQUAL(memory.UsedPayloadBytes, savedBytes);
        second.Timestamp = i64(1) << 62;
        UNIT_ASSERT(!TQuantized::WriteRecord<1>(&second, memory));
        UNIT_ASSERT(payload == saved);
    }

    Y_UNIT_TEST(SnapshotPrefixAndIndependentChunks) {
        using TFrontend = TRawLineFrontend<ui64, TStorage>;
        TInMemoryMetricsBackend backend({.MemoryBytes = 128 * 32, .ChunkSizeBytes = 128, .MaxLines = 1, .ReserveChunks = 2});
        auto line = backend.CreateLine<TFrontend>("counter", {});
        Pump(&backend);
        UNIT_ASSERT(line.Append(0));
        auto old = backend.CaptureSnapshot(line.GetLineId());
        for (ui64 i = 1; i < 60; ++i) {
            if (!line.Append(i)) {
                Pump(&backend);
                UNIT_ASSERT(line.Append(i));
            }
            Pump(&backend);
        }
        old.Read([&](const TSnapshotView& view) {
            const auto values = view.GetLine(0).ReadValuesAs<ui64>();
            UNIT_ASSERT_VALUES_EQUAL(values.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(values[0], 0);
        });
        backend.CaptureSnapshot(line.GetLineId()).Read([&](const TSnapshotView& view) {
            const auto& snapshot = view.GetLine(0);
            UNIT_ASSERT(snapshot.GetChunkCount() > 1);
            const auto values = snapshot.ReadValuesAs<ui64>();
            UNIT_ASSERT_VALUES_EQUAL(values.size(), 60);
            for (size_t i = 0; i < values.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(values[i], i);
            }
            size_t chunks = 0;
            NInMemoryMetricsPrivate::TLineSnapshotAccess::ForEachChunk(snapshot, [&](const auto& chunk) {
                bool first = true;
                TStorage::ReadChunk<1>(snapshot, chunk, [&](TInstant, const auto& decoded) {
                    if (first) {
                        UNIT_ASSERT_VALUES_EQUAL(decoded[0], values[chunks]);
                    }
                    first = false;
                    ++chunks;
                }, std::make_index_sequence<1>{});
                UNIT_ASSERT(!first);
            });
            UNIT_ASSERT_VALUES_EQUAL(chunks, 60);
        });
    }

    Y_UNIT_TEST(MixedGroupDecimalAndExactCounter) {
        using TPolicy = TCompressedLineStorage<0, TDecimalEncoding<2>, TIntegerEncoding<ELineValueEncoding::UnsignedDelta>>;
        using TFrontend = TGroupLineFrontend<TGroup, TPolicy>;
        TInMemoryMetricsBackend backend({.MemoryBytes = 256 * 8, .ChunkSizeBytes = 256, .MaxLines = 1});
        auto line = backend.CreateLine<TFrontend>("load", {});
        Pump(&backend);
        UNIT_ASSERT(line.Append({TFrontend::Value<TCpu>(1.234), TFrontend::Value<TEvents>(ui64(-1))}));
        UNIT_ASSERT(line.Append({TFrontend::Value<TCpu>(1.256), TFrontend::Value<TEvents>(0)}));
        UNIT_ASSERT(!line.Append({TFrontend::Value<TCpu>(std::numeric_limits<double>::infinity()), TFrontend::Value<TEvents>(1)}));
        backend.CaptureSnapshot(line.GetLineId()).Read([&](const TSnapshotView& view) {
            const auto values = TFrontend::ReadValues(view.GetLine(0));
            UNIT_ASSERT_VALUES_EQUAL(values.size(), 2);
            UNIT_ASSERT_DOUBLES_EQUAL(values[0].Get<TCpu>(), 1.23, 1e-12);
            UNIT_ASSERT_DOUBLES_EQUAL(values[1].Get<TCpu>(), 1.26, 1e-12);
            UNIT_ASSERT_VALUES_EQUAL(values[0].Get<TEvents>(), ui64(-1));
            UNIT_ASSERT_VALUES_EQUAL(values[1].Get<TEvents>(), 0);
        });
    }

    Y_UNIT_TEST(OnChangeComparesQuantizedValueAndExtendsRange) {
        using TFrontend = TOnChangeLineFrontend<double, TCompressedLineStorage<0, TDecimalEncoding<2>>>;
        TInMemoryMetricsBackend backend({.MemoryBytes = 128 * 8, .ChunkSizeBytes = 128, .MaxLines = 1});
        auto line = backend.CreateLine<TFrontend>("cpu", {});
        Pump(&backend);
        UNIT_ASSERT(line.Append(1.231));
        UNIT_ASSERT(line.Append(1.234));
        UNIT_ASSERT(line.Append(1.25));
        backend.CaptureSnapshot(line.GetLineId()).Read([&](const TSnapshotView& view) {
            const auto& snapshot = view.GetLine(0);
            const auto records = snapshot.ReadRecordsAs<double>();
            UNIT_ASSERT_VALUES_EQUAL(records.size(), 2);
            UNIT_ASSERT_DOUBLES_EQUAL(records[0].Value, 1.23, 1e-12);
            UNIT_ASSERT_DOUBLES_EQUAL(records[1].Value, 1.25, 1e-12);
            const auto begin = records.back().Timestamp + TDuration::Seconds(1);
            const auto extended = snapshot.ReadRecordsAsInRange<double>(begin, begin + TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(extended.size(), 2);
            UNIT_ASSERT_DOUBLES_EQUAL(extended.front().Value, 1.25, 1e-12);
        });
    }
}
