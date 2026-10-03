#pragma once

#include "../line.h"
#include "../line_storage.h"

#include <util/datetime/base.h>
#include <util/system/hp_timer.h>
#include <util/system/types.h>

#include <util/generic/yexception.h>

#include <array>
#include <limits>
#include <tuple>
#include <type_traits>

namespace NActors {

    template<class TFrontend>
    class TLine;

    // TDescriptor::TFields is a tuple of field descriptors. Each field declares
    // TValueType, static constexpr TStringBuf Name and a static constexpr array
    // Labels of TLineLabelView (key=value). Snapshot labels identify the instance.
    // All fields share one timestamp and are published as one record.
    template<class TDescriptor>
    struct TGroupLineFrontend {
        using TFields = typename TDescriptor::TFields;
        static constexpr size_t FieldCount = std::tuple_size_v<TFields>;
        static_assert(FieldCount > 0);

    private:
        template<size_t I>
        using TField = std::tuple_element_t<I, TFields>;

        template<size_t... I>
        static constexpr auto FieldMetadata(std::index_sequence<I...>) {
            static_assert((std::is_trivially_copyable_v<typename TField<I>::TValueType> && ...));
            static_assert(((sizeof(typename TField<I>::TValueType) <= sizeof(ui64)) && ...));
            static_assert(((!TField<I>::Name.empty()) && ...));
            static_assert((HasField<TField<I>>(Indices) && ...), "Field descriptors must be unique");
            return std::array<TLineFieldMeta, FieldCount>{{
                {TField<I>::Name, TField<I>::Labels}...
            }};
        }

        static constexpr auto Indices = std::make_index_sequence<FieldCount>{};

    public:
        template<class TFieldDescriptor>
        struct TFieldValue {
            typename TFieldDescriptor::TValueType Value;

            bool operator==(const TFieldValue&) const = default;
        };

    private:
        template<class T, class... TArgs>
        static constexpr size_t TypeCount = (size_t(std::is_same_v<T, TArgs>) + ... + 0);

        template<class TFieldDescriptor, size_t... I>
        static constexpr bool HasField(std::index_sequence<I...>) {
            return TypeCount<TFieldDescriptor, TField<I>...> == 1;
        }

        template<class... TArgs, size_t... I>
        static constexpr bool CompleteValues(std::index_sequence<I...>) {
            return sizeof...(TArgs) == FieldCount
                && ((TypeCount<TFieldValue<TField<I>>, TArgs...> == 1) && ...);
        }

        template<size_t... I>
        static auto StorageType(std::index_sequence<I...>)
            -> std::tuple<TFieldValue<TField<I>>...>;

    public:
        // Bind each value to its field; argument order does not affect storage.
        template<class TFieldDescriptor>
            requires (HasField<TFieldDescriptor>(Indices))
        static constexpr TFieldValue<TFieldDescriptor> Value(const typename TFieldDescriptor::TValueType& value) noexcept {
            return {value};
        }

        class TValueType {
            using TStorage = decltype(StorageType(Indices));

            template<class... TArgs, size_t... I>
            static constexpr TStorage MakeStorage(std::index_sequence<I...>, const TArgs&... args) noexcept {
                const auto named = std::tuple{args...};
                return TStorage{std::get<TFieldValue<TField<I>>>(named)...};
            }

        public:
            // Missing, duplicate, foreign and positional values are rejected.
            template<class... TArgs>
                requires (CompleteValues<TArgs...>(Indices))
            constexpr TValueType(const TArgs&... args) noexcept
                : Values(MakeStorage(Indices, args...))
            {}

            template<class TFieldDescriptor>
                requires (HasField<TFieldDescriptor>(Indices))
            constexpr const typename TFieldDescriptor::TValueType& Get() const noexcept {
                return std::get<TFieldValue<TFieldDescriptor>>(Values).Value;
            }

            bool operator==(const TValueType&) const = default;

        private:
            TStorage Values;
        };

        inline static constexpr auto Fields = FieldMetadata(Indices);

        // Readers return named values and reject a different schema.
        static TDeque<TGenericRecordView<TValueType>> ReadRecords(
                const TLineSnapshot& snapshot,
                TInstant beginTs = TInstant::Zero(), TInstant endTs = TInstant::Max()) {
            Y_ENSURE(snapshot.Meta.Frontend == &Descriptor(), "Group line descriptor mismatch");
            return snapshot.ReadRecordsAsInRange<TValueType>(beginTs, endTs);
        }

        static TDeque<TValueType> ReadValues(
                const TLineSnapshot& snapshot,
                TInstant beginTs = TInstant::Zero(), TInstant endTs = TInstant::Max()) {
            Y_ENSURE(snapshot.Meta.Frontend == &Descriptor(), "Group line descriptor mismatch");
            return snapshot.ReadValuesAsInRange<TValueType>(beginTs, endTs);
        }

        struct TStorageRecord {
            NHPTimer::STime TimestampTs = 0;
            std::array<ui64, FieldCount> Values = {};
        };

        struct alignas(TStorageRecord) TChunkHeader {
            ui32 RecordsCount = 0;
            ui32 Reserved = 0;
        };

        static_assert(sizeof(TChunkHeader) + sizeof(TStorageRecord) <= std::numeric_limits<ui32>::max());

        struct TConfig {};

        template<size_t... I>
        static TValueType DecodeValue(const std::array<ui64, FieldCount>& values, std::index_sequence<I...>) noexcept {
            return TValueType{Value<TField<I>>(NInMemoryMetricsPrivate::DecodeLineValue<typename TField<I>::TValueType>(values[I]))...};
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
                        DecodeValue(storedRecords[i].Values, Indices));
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
                .ReadRange = &TGroupLineFrontend<TDescriptor>::ReadRange,
                .Fields = Fields,
            };
            return descriptor;
        }

        static TLineMeta MakeMeta(const TConfig& = {}) noexcept {
            return TLineMeta(&Descriptor());
        }

    private:
        friend class TLine<TGroupLineFrontend<TDescriptor>>;

        template<size_t... I>
        static auto EncodeValue(const TValueType& value, std::index_sequence<I...>) noexcept {
            return std::array<ui64, FieldCount>{NInMemoryMetricsPrivate::EncodeLineValue(value.template Get<TField<I>>())...};
        }

        static bool Append(IMetricLine& line, const TValueType& value) noexcept;
        static bool WriteRecordToChunkMemory(void* opaque, TWritableChunkMemory& chunkMemory) noexcept;
    };

    template<class TDescriptor>
    bool TGroupLineFrontend<TDescriptor>::Append(IMetricLine& line, const typename TGroupLineFrontend<TDescriptor>::TValueType& value) noexcept {
        const NHPTimer::STime nowTs = line.CurrentTimestampTs();

        TStorageRecord record{
            .TimestampTs = nowTs,
            .Values = EncodeValue(value, Indices),
        };
        return line.AccessChunkMemory(&record, &TGroupLineFrontend<TDescriptor>::WriteRecordToChunkMemory);
    }

    template<class TDescriptor>
    bool TGroupLineFrontend<TDescriptor>::WriteRecordToChunkMemory(void* opaque, TWritableChunkMemory& chunkMemory) noexcept {
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
