#pragma once

#include "../line_base.h"
#include "raw_line_frontend.h"

#include <util/datetime/base.h>
#include <util/generic/function.h>
#include <util/system/hp_timer.h>
#include <util/system/types.h>

namespace NActors {

    template<class TFrontend>
    class TLine;

    template<class TValue = ui64>
    struct TOnChangeLineFrontend {
        using TValueType = TValue;

        struct TConfig {};

        static TValue DecodeValue(ui64 value) noexcept {
            return NInMemoryMetricsPrivate::DecodeLineValue<TValue>(value);
        }

        static void ReadRange(const TLineSnapshot& snapshot, TInstant beginTs, TInstant endTs, const std::function<void(const TRecordView&)>& cb) {
            TRawLineFrontend<TValue>::ForEachStoredRecordInRange(snapshot, beginTs, endTs, cb);
        }

        static const TLineFrontendOps& Descriptor() noexcept {
            static const TLineFrontendOps descriptor{
                .Name = "on_change",
                .ReadRange = &TOnChangeLineFrontend<TValue>::ReadRange,
            };
            return descriptor;
        }

        static TLineMeta MakeMeta(const TConfig& = {}) noexcept {
            return TLineMeta(&Descriptor());
        }

    private:
        friend class TLine<TOnChangeLineFrontend<TValue>>;

        static bool Append(TLineWriteBackend& backend, TLineWriterState* writer, const TValueType& value) noexcept;
    };

    template<class TValue>
    bool TOnChangeLineFrontend<TValue>::Append(TLineWriteBackend& backend, TLineWriterState* writer, const TValue& value) noexcept {
        using TStorageRecord = typename TRawLineFrontend<TValue>::TStorageRecord;

        const ui64 encoded = NInMemoryMetricsPrivate::EncodeLineValue(value);
        const NHPTimer::STime nowTs = backend.CurrentTimestampTs();
        const TLinePublishState state = backend.GetPublishState(writer);

        if (state.HasLastPublished && state.LastPublishedValue == encoded) {
            backend.MarkObserved(writer, nowTs);
            return true;
        } else if (state.HasLastPublished && state.LastObservedTs > state.LastPublishedTs) {
            TStorageRecord previousRecord{
                .TimestampTs = state.LastPublishedTs,
                .Value = state.LastPublishedValue,
            };
            const auto previousData = std::span<const char>(reinterpret_cast<const char*>(&previousRecord), sizeof(previousRecord));
            if (!backend.AppendChunkData(writer, previousData, previousRecord.TimestampTs, previousRecord.TimestampTs)) {
                return false;
            }
            backend.MarkPublished(writer, state.LastPublishedValue, previousRecord.TimestampTs);
        }

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
