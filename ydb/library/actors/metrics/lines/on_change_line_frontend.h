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

        static void ReadRange(const TLineSnapshot& snapshot,
                              TInstant beginTs,
                              TInstant endTs,
                              void* opaque,
                              TLineFrontendOps::TInvokeValue invoke) {
            // on_change reuses the same physical chunk format as raw; only write
            // semantics differ.
            TRawLineFrontend<TValue>::ForEachStoredRecordInRange(snapshot, beginTs, endTs, [&](TInstant timestamp, const TValue& value) {
                invoke(opaque, timestamp, &value);
            });
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

        static bool Append(ILineWriteBackend& backend, TLineWriterState* state, const TValueType& value) noexcept;
    };

    template<class TValue>
    bool TOnChangeLineFrontend<TValue>::Append(ILineWriteBackend& backend, TLineWriterState* state, const TValue& value) noexcept {
        using TStorageRecord = typename TRawLineFrontend<TValue>::TStorageRecord;

        const ui64 encoded = NInMemoryMetricsPrivate::EncodeLineValue(value);
        const NHPTimer::STime nowTs = backend.CurrentTimestampTs();
        const TLinePublishState publishState = backend.GetPublishState(state);

        if (publishState.HasLastPublished && publishState.LastPublishedValue == encoded) {
            backend.MarkObserved(state, nowTs);
            return true;
        } else if (publishState.HasLastPublished && publishState.LastObservedTs > publishState.LastPublishedTs) {
            TStorageRecord previousRecord{
                .TimestampTs = publishState.LastPublishedTs,
                .Value = publishState.LastPublishedValue,
            };
            const auto previousData = std::span<const char>(reinterpret_cast<const char*>(&previousRecord), sizeof(previousRecord));
            if (!backend.AppendChunkData(state, previousData, previousRecord.TimestampTs, previousRecord.TimestampTs)) {
                return false;
            }
            backend.MarkPublished(state, publishState.LastPublishedValue, previousRecord.TimestampTs);
        }

        TStorageRecord record{
            .TimestampTs = nowTs,
            .Value = encoded,
        };
        const auto data = std::span<const char>(reinterpret_cast<const char*>(&record), sizeof(record));
        if (!backend.AppendChunkData(state, data, record.TimestampTs, record.TimestampTs)) {
            return false;
        }
        backend.MarkPublished(state, encoded, nowTs);
        return true;
    }

} // namespace NActors
