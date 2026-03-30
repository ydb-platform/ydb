#pragma once

#include "../line_storage.h"

#include <util/datetime/base.h>
#include <util/generic/function.h>
#include <util/system/hp_timer.h>
#include <util/system/types.h>

namespace NActors {

    template<class TFrontend>
    class TLine;

    template<class TValue = ui64>
    struct TRawLineFrontend {
        using TValueType = TValue;

        struct TStorageRecord {
            NHPTimer::STime TimestampTs = 0;
            ui64 Value = 0;
        };

        struct TConfig {};

        static TValue DecodeValue(ui64 value) noexcept {
            return NInMemoryMetricsPrivate::DecodeLineValue<TValue>(value);
        }

        static void ReadRange(const TLineSnapshot& snapshot,
                              TInstant beginTs,
                              TInstant endTs,
                              void* opaque,
                              TLineFrontendOps::TInvokeValue invoke) {
            ForEachStoredRecordInRange(snapshot, beginTs, endTs, [&](TInstant timestamp, const TValue& value) {
                invoke(opaque, timestamp, &value);
            });
        }

        template<class TCallback>
        static void ForEachStoredRecordInRange(const TLineSnapshot& snapshot,
                                               TInstant beginTs,
                                               TInstant endTs,
                                               TCallback&& cb) {
            snapshot.ForEachChunk([&](const TChunkSnapshotView& chunk) {
                const auto* storedRecords = reinterpret_cast<const TStorageRecord*>(chunk.Payload.data());
                const size_t recordsCount = chunk.Payload.size() / sizeof(TStorageRecord);
                for (size_t i = 0; i < recordsCount; ++i) {
                    const TInstant timestamp = snapshot.DecodeTimestampTs(storedRecords[i].TimestampTs);
                    if (beginTs <= timestamp && timestamp <= endTs) {
                        cb(timestamp, DecodeValue(storedRecords[i].Value));
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

        static bool Append(TLineWriteBackend& backend, TLineWriterState* writer, const TValueType& value) noexcept;
    };

    template<class TValue>
    bool TRawLineFrontend<TValue>::Append(TLineWriteBackend& backend, TLineWriterState* writer, const TValue& value) noexcept {
        const ui64 encoded = NInMemoryMetricsPrivate::EncodeLineValue(value);
        const NHPTimer::STime nowTs = backend.CurrentTimestampTs();

        TStorageRecord record{
            .TimestampTs = nowTs,
            .Value = encoded,
        };
        const auto data = std::span<const char>(reinterpret_cast<const char*>(&record), sizeof(record));
        if (!backend.AppendChunkData(writer, data, record.TimestampTs, record.TimestampTs)) {
            return false;
        }
        backend.MarkPublished(writer, encoded, nowTs);
        return true;
    }

} // namespace NActors
