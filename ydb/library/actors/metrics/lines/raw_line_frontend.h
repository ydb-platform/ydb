#pragma once

#include "../line.h"
#include "compressed_line_storage.h"
#include "../line_storage.h"

#include <util/datetime/base.h>
#include <util/system/hp_timer.h>
#include <util/system/types.h>

#include <algorithm>
#include <array>

namespace NActors {

    template<class TFrontend>
    class TLine;
    template<class TValue, class TStoragePolicy>
    struct TOnChangeLineFrontend;

    template<class TValue = ui64, class TStoragePolicy = TUncompressedLineStorage>
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
            if constexpr (TStoragePolicy::Enabled) {
                return TStoragePolicy::template Decode<0, TValue>(value);
            } else {
                return NInMemoryMetricsPrivate::DecodeLineValue<TValue>(value);
            }
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
        static void ForEachStoredRecord(const TLineSnapshot& snapshot, TCallback&& cb) {
            if constexpr (TStoragePolicy::Enabled) {
                TStoragePolicy::template ForEachRecord<1>(snapshot, [&](TInstant timestamp, const auto& values) {
                    cb(timestamp, DecodeValue(values[0]));
                });
                return;
            }
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
                        DecodeValue(storedRecords[i].Value));
                }
            });
        }

        template<class TCallback>
        static void ForEachStoredRecordInRange(const TLineSnapshot& snapshot,
                                               TInstant beginTs,
                                               TInstant endTs,
                                               TCallback&& cb) {
            ForEachStoredRecord(snapshot, [&](TInstant timestamp, const TValue& value) {
                if (beginTs <= timestamp && timestamp <= endTs) {
                    cb(timestamp, value);
                }
            });
        }

        static const TLineFrontendOps& Descriptor() noexcept {
            static const TLineFrontendOps descriptor{
                .Name = "raw",
                .ReadRange = &TRawLineFrontend<TValue, TStoragePolicy>::ReadRange,
                .ReadNumericRange = &ReadNumericRange,
            };
            return descriptor;
        }

        static TLineMeta MakeMeta(const TConfig& = {}) noexcept {
            return TLineMeta(&Descriptor());
        }

    private:
        static void ReadNumericRange(const TLineSnapshot& snapshot, TInstant beginTs, TInstant endTs,
                                     void* opaque, TLineFrontendOps::TInvokeNumericValues invoke) {
            ForEachStoredRecordInRange(snapshot, beginTs, endTs, [&](TInstant timestamp, const TValue& value) {
                const std::array<TLineNumericValue, 1> values = {MakeLineNumericValue(value)};
                invoke(opaque, timestamp, values);
            });
        }

        friend class TLine<TRawLineFrontend<TValue, TStoragePolicy>>;
        friend struct TOnChangeLineFrontend<TValue, TStoragePolicy>;

        static bool Append(IMetricLine& line, const TValueType& value) noexcept;
        static bool WriteRecordToChunkMemory(void* opaque, TWritableChunkMemory& chunkMemory) noexcept;
    };

    template<class TValue, class TStoragePolicy>
    bool TRawLineFrontend<TValue, TStoragePolicy>::Append(IMetricLine& line, const TValue& value) noexcept {
        ui64 encoded;
        if constexpr (TStoragePolicy::Enabled) {
            if (!TStoragePolicy::template Encode<0>(value, &encoded)) {
                return false;
            }
        } else {
            encoded = NInMemoryMetricsPrivate::EncodeLineValue(value);
        }
        const NHPTimer::STime nowTs = line.CurrentTimestampTs();

        TStorageRecord record{
            .TimestampTs = nowTs,
            .Value = encoded,
        };
        if (!line.AccessChunkMemory(&record, &TRawLineFrontend<TValue, TStoragePolicy>::WriteRecordToChunkMemory)) {
            return false;
        }
        line.MarkMaterialized(encoded);
        return true;
    }

    template<class TValue, class TStoragePolicy>
    bool TRawLineFrontend<TValue, TStoragePolicy>::WriteRecordToChunkMemory(void* opaque, TWritableChunkMemory& chunkMemory) noexcept {
        const auto& record = *static_cast<const TStorageRecord*>(opaque);
        if constexpr (TStoragePolicy::Enabled) {
            typename TStoragePolicy::template TRecord<1> packed{record.TimestampTs, {record.Value}};
            return TStoragePolicy::template WriteRecord<1>(&packed, chunkMemory);
        }
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
