#pragma once

#include "../line.h"
#include "../line_storage.h"

#include <util/datetime/base.h>
#include <util/system/hp_timer.h>
#include <util/system/types.h>

#include <limits>
#include <type_traits>
#include <array>

namespace NActors {

    template<class TFrontend>
    class TLine;

    // Fixed-order series share one timestamp and are published as one record.
    // Readers must use TValueType; column names belong to the caller's schema.
    template<size_t N, class TValue = ui64>
    struct TGroupLineFrontend {
        static_assert(N > 0);
        static_assert(std::is_trivially_copyable_v<TValue>);
        static_assert(sizeof(TValue) <= sizeof(ui64));
        using TValueType = std::array<TValue, N>;

        struct TStorageRecord {
            NHPTimer::STime TimestampTs = 0;
            std::array<ui64, N> Values = {};
        };

        struct alignas(TStorageRecord) TChunkHeader {
            ui32 RecordsCount = 0;
            ui32 Reserved = 0;
        };

        static_assert(sizeof(TChunkHeader) + sizeof(TStorageRecord) <= std::numeric_limits<ui32>::max());

        struct TConfig {};

        static TValueType DecodeValue(const std::array<ui64, N>& values) noexcept {
            TValueType result;
            for (size_t i = 0; i < N; ++i) {
                result[i] = NInMemoryMetricsPrivate::DecodeLineValue<TValue>(values[i]);
            }
            return result;
        }

        static void ReadRange(const TLineSnapshot& snapshot,
                              TInstant beginTs,
                              TInstant endTs,
                              void* opaque,
                              TLineFrontendOps::TInvokeValue invoke) {
            ForEachStoredRecordInRange(snapshot, beginTs, endTs, [&](TInstant timestamp, const TValueType& value) {
                invoke(opaque, timestamp, &value);
            });
        }

        template<class TCallback>
        static void ForEachStoredRecord(const TLineSnapshot& snapshot, TCallback&& cb) {
            NInMemoryMetricsPrivate::TLineSnapshotAccess::ForEachChunk(snapshot, [&](const NInMemoryMetricsPrivate::TChunkSnapshotView& chunk) {
                if (chunk.Payload.size() < sizeof(TChunkHeader)) {
                    return;
                }

                const char* recordsBegin = chunk.Payload.data() + sizeof(TChunkHeader);
                const size_t recordsBytes = chunk.Payload.size() - sizeof(TChunkHeader);
                const size_t recordsCount = recordsBytes / sizeof(TStorageRecord);
                const auto* storedRecords = reinterpret_cast<const TStorageRecord*>(recordsBegin);
                for (size_t i = 0; i < recordsCount; ++i) {
                    cb(
                        NInMemoryMetricsPrivate::TLineSnapshotAccess::DecodeTimestampTs(snapshot, storedRecords[i].TimestampTs),
                        DecodeValue(storedRecords[i].Values));
                }
            });
        }

        template<class TCallback>
        static void ForEachStoredRecordInRange(const TLineSnapshot& snapshot,
                                               TInstant beginTs,
                                               TInstant endTs,
                                               TCallback&& cb) {
            ForEachStoredRecord(snapshot, [&](TInstant timestamp, const TValueType& value) {
                if (beginTs <= timestamp && timestamp <= endTs) {
                    cb(timestamp, value);
                }
            });
        }

        static const TLineFrontendOps& Descriptor() noexcept {
            static const TLineFrontendOps descriptor{
                .Name = "group",
                .ReadRange = &TGroupLineFrontend<N, TValue>::ReadRange,
            };
            return descriptor;
        }

        static TLineMeta MakeMeta(const TConfig& = {}) noexcept {
            return TLineMeta(&Descriptor());
        }

    private:
        friend class TLine<TGroupLineFrontend<N, TValue>>;

        static bool Append(IMetricLine& line, const TValueType& value) noexcept;
        static bool WriteRecordToChunkMemory(void* opaque, TWritableChunkMemory& chunkMemory) noexcept;
    };

    template<size_t N, class TValue>
    bool TGroupLineFrontend<N, TValue>::Append(IMetricLine& line, const typename TGroupLineFrontend<N, TValue>::TValueType& value) noexcept {
        const NHPTimer::STime nowTs = line.CurrentTimestampTs();

        TStorageRecord record{
            .TimestampTs = nowTs,
        };
        for (size_t i = 0; i < N; ++i) {
            record.Values[i] = NInMemoryMetricsPrivate::EncodeLineValue(value[i]);
        }
        if (!line.AccessChunkMemory(&record, &TGroupLineFrontend<N, TValue>::WriteRecordToChunkMemory)) {
            return false;
        }
        return true;
    }

    template<size_t N, class TValue>
    bool TGroupLineFrontend<N, TValue>::WriteRecordToChunkMemory(void* opaque, TWritableChunkMemory& chunkMemory) noexcept {
        const auto& record = *static_cast<const TStorageRecord*>(opaque);
        const ui32 oldCommittedBytes = chunkMemory.UsedPayloadBytes;
        const size_t requiredBytes = oldCommittedBytes == 0
            ? sizeof(TChunkHeader) + sizeof(TStorageRecord)
            : size_t(oldCommittedBytes) + sizeof(TStorageRecord);
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
