#pragma once

#include "../inmemory_backend.h"
#include "../line_storage.h"

#include <util/datetime/base.h>
#include <util/system/hp_timer.h>
#include <util/system/types.h>

#include <algorithm>

namespace NActors {

    template<class TFrontend>
    class TLine;
    template<class TValue>
    struct TOnChangeLineFrontend;

    template<class TValue = ui64>
    struct TRawLineFrontend {
        using TValueType = TValue;

        struct TStorageRecord {
            NHPTimer::STime TimestampTs = 0;
            ui64 Value = 0;
        };

        struct alignas(TStorageRecord) TChunkHeader {
            ui32 RecordsCount = 0;
            ui32 Reserved = 0;
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
            TLineSnapshotAccess::ForEachChunk(snapshot, [&](const TChunkSnapshotView& chunk) {
                if (chunk.Payload.size() < sizeof(TChunkHeader)) {
                    return;
                }

                const auto* header = reinterpret_cast<const TChunkHeader*>(chunk.Payload.data());
                const char* recordsBegin = chunk.Payload.data() + sizeof(TChunkHeader);
                const size_t recordsBytes = chunk.Payload.size() - sizeof(TChunkHeader);
                const size_t maxRecordsCount = recordsBytes / sizeof(TStorageRecord);
                const size_t recordsCount = std::min<size_t>(header->RecordsCount, maxRecordsCount);
                const auto* storedRecords = reinterpret_cast<const TStorageRecord*>(recordsBegin);
                for (size_t i = 0; i < recordsCount; ++i) {
                    const TInstant timestamp = TLineSnapshotAccess::DecodeTimestampTs(snapshot, storedRecords[i].TimestampTs);
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
        friend struct TOnChangeLineFrontend<TValue>;

        static bool Append(TInMemoryMetricsBackend& backend, TLineWriterState* state, const TValueType& value) noexcept;
        static bool WriteRecordToChunkMemory(void* opaque, TWritableChunkMemory& chunkMemory) noexcept;
    };

    template<class TValue>
    bool TRawLineFrontend<TValue>::Append(TInMemoryMetricsBackend& backend, TLineWriterState* state, const TValue& value) noexcept {
        const ui64 encoded = NInMemoryMetricsPrivate::EncodeLineValue(value);
        const NHPTimer::STime nowTs = backend.CurrentTimestampTs();

        TStorageRecord record{
            .TimestampTs = nowTs,
            .Value = encoded,
        };
        if (!backend.AccessChunkMemory(state, &record, &TRawLineFrontend<TValue>::WriteRecordToChunkMemory)) {
            return false;
        }
        backend.MarkMaterialized(state, encoded);
        return true;
    }

    template<class TValue>
    bool TRawLineFrontend<TValue>::WriteRecordToChunkMemory(void* opaque, TWritableChunkMemory& chunkMemory) noexcept {
        const auto& record = *static_cast<const TStorageRecord*>(opaque);
        const ui32 oldCommittedBytes = chunkMemory.UsedPayloadBytes;
        const ui32 requiredBytes = oldCommittedBytes == 0
            ? sizeof(TChunkHeader) + sizeof(TStorageRecord)
            : oldCommittedBytes + sizeof(TStorageRecord);
        if (requiredBytes > chunkMemory.Payload.size()) {
            return false;
        }

        auto* header = reinterpret_cast<TChunkHeader*>(chunkMemory.Payload.data());
        char* recordsBegin = chunkMemory.Payload.data() + sizeof(TChunkHeader);
        auto* storedRecords = reinterpret_cast<TStorageRecord*>(recordsBegin);
        if (oldCommittedBytes == 0) {
            *header = TChunkHeader{
                .RecordsCount = 1,
            };
            storedRecords[0] = record;
            chunkMemory.UsedPayloadBytes = sizeof(TChunkHeader) + sizeof(TStorageRecord);
            chunkMemory.FirstTs = record.TimestampTs;
            chunkMemory.LastTs = record.TimestampTs;
            return true;
        }

        const ui32 recordsCount = header->RecordsCount;
        storedRecords[recordsCount] = record;
        header->RecordsCount = recordsCount + 1;
        chunkMemory.UsedPayloadBytes = oldCommittedBytes + sizeof(TStorageRecord);
        chunkMemory.LastTs = record.TimestampTs;
        return true;
    }

} // namespace NActors
