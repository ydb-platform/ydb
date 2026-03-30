#pragma once

#include <util/datetime/base.h>
#include <util/generic/function.h>
#include <util/system/hp_timer.h>
#include <util/system/types.h>

namespace NActors {

    template<class TFrontend>
    class TLine;
    class TInMemoryMetricsRegistry;
    class TLineSnapshot;
    class TLineWriterState;
    struct TLineFrontendOps;
    struct TLineMeta;
    struct TRecordView;

    template<class TValue = ui64>
    struct TRawLineFrontend {
        using TValueType = TValue;

        struct TConfig {};

        static TValue DecodeValue(ui64 value) noexcept {
            return NInMemoryMetricsPrivate::DecodeLineValue<TValue>(value);
        }

        static void ReadRange(const TLineSnapshot& snapshot, TInstant beginTs, TInstant endTs, const std::function<void(const TRecordView&)>& cb) {
            ForEachStoredRecordInRange(snapshot, beginTs, endTs, cb);
        }

        template<class TCallback>
        static void ForEachStoredRecordInRange(const TLineSnapshot& snapshot,
                                               TInstant beginTs,
                                               TInstant endTs,
                                               TCallback&& cb) {
            snapshot.ForEachChunk([&](const TChunkSnapshotView& chunk) {
                const auto* storedRecords = reinterpret_cast<const TStoredRecord*>(chunk.Payload.data());
                const size_t recordsCount = chunk.Payload.size() / sizeof(TStoredRecord);
                for (size_t i = 0; i < recordsCount; ++i) {
                    const TRecordView record{
                        .Timestamp = snapshot.DecodeTimestampTs(storedRecords[i].TimestampTs),
                        .Value = storedRecords[i].Value,
                    };
                    if (beginTs <= record.Timestamp && record.Timestamp <= endTs) {
                        cb(record);
                    }
                }
            });
        }

        static const TLineFrontendOps& Descriptor() noexcept {
            static const TLineFrontendOps descriptor{
                .Name = "raw",
                .ReadRange = &TRawLineFrontend<TValue>::ReadRange,
            };
            return descriptor;
        }

        static TLineMeta MakeMeta(const TConfig& = {}) noexcept {
            return TLineMeta(&Descriptor());
        }

    private:
        friend class TLine<TRawLineFrontend<TValue>>;

        static bool Append(TInMemoryMetricsRegistry& registry, TLineWriterState* writer, const TValueType& value) noexcept;
    };

} // namespace NActors
