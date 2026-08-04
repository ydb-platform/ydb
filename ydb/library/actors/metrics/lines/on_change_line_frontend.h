#pragma once

#include "../line_read.h"
#include "../line_write.h"
#include "raw_line_frontend.h"

#include <util/datetime/base.h>
#include <util/system/hp_timer.h>
#include <util/system/types.h>

#include <algorithm>

namespace NActors {

    class TInMemoryMetricsBackend;

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
            // semantics differ. For explicit finite intervals reader may insert
            // a synthetic point at beginTs for the last value that started
            // before the interval, and appends a synthetic tail point for the
            // current value at interval end.
            bool hasPreviousValue = false;
            TValue previousValue{};
            bool hasLastValue = false;
            TInstant lastTimestamp;
            TValue lastValue{};
            bool hasPointInsideRange = false;

            TRawLineFrontend<TValue>::ForEachStoredRecord(snapshot, [&](TInstant timestamp, const TValue& value) {
                if (timestamp < beginTs) {
                    hasPreviousValue = true;
                    previousValue = value;
                    return;
                }
                if (timestamp > endTs) {
                    return;
                }

                if (!hasPointInsideRange && hasPreviousValue && beginTs < timestamp) {
                    hasLastValue = true;
                    lastTimestamp = beginTs;
                    lastValue = previousValue;
                    invoke(opaque, beginTs, &previousValue);
                }

                hasPointInsideRange = true;
                hasLastValue = true;
                lastTimestamp = timestamp;
                lastValue = value;
                invoke(opaque, timestamp, &value);
            });

            if (!hasPointInsideRange && hasPreviousValue && beginTs <= endTs) {
                hasLastValue = true;
                lastTimestamp = beginTs;
                lastValue = previousValue;
                invoke(opaque, beginTs, &previousValue);
            }

            if (!hasLastValue) {
                return;
            }

            if (snapshot.Closed || endTs == TInstant::Max()) {
                return;
            }

            if (endTs <= lastTimestamp || endTs < beginTs) {
                return;
            }

            invoke(opaque, endTs, &lastValue);
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
        friend class TInMemoryMetricsBackend;

        static bool Append(TInMemoryMetricsBackend& backend, TLineWriterState* state, const TValueType& value) noexcept;
    };

    template<class TValue>
    bool TOnChangeLineFrontend<TValue>::Append(TInMemoryMetricsBackend& backend, TLineWriterState* state, const TValue& value) noexcept {
        const ui64 encoded = NInMemoryMetricsPrivate::EncodeLineValue(value);
        const std::optional<ui64> lastMaterialized = backend.GetLastMaterializedValue(state);

        if (lastMaterialized && *lastMaterialized == encoded) {
            return true;
        }

        const NHPTimer::STime nowTs = backend.CurrentTimestampTs();

        typename TRawLineFrontend<TValue>::TStorageRecord record{
            .TimestampTs = nowTs,
            .Value = encoded,
        };
        if (!backend.AccessChunkMemory(state, &record, &TRawLineFrontend<TValue>::WriteRecordToChunkMemory)) {
            return false;
        }
        backend.MarkMaterialized(state, encoded);
        return true;
    }

} // namespace NActors
