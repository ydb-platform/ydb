#pragma once

#include "../line_read.h"
#include "../metric_line.h"
#include "raw_line_frontend.h"

#include <util/datetime/base.h>
#include <util/system/hp_timer.h>
#include <util/system/types.h>

#include <algorithm>

namespace NActors {

    template<class TFrontend>
    class TLine;

    template<class TValue = ui64, class TStoragePolicy = TUncompressedLineStorage>
    struct TOnChangeLineFrontend {
        using TValueType = TValue;

        struct TConfig {};

        static TValue DecodeValue(ui64 value) noexcept {
            return TRawLineFrontend<TValue, TStoragePolicy>::DecodeValue(value);
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
            const bool finiteEnd = endTs != TInstant::Max();
            if (snapshot.Closed) {
                endTs = std::min(endTs, snapshot.ClosedAt);
            }
            if (beginTs > endTs) {
                return;
            }
            bool hasPreviousValue = false;
            TValue previousValue{};
            bool hasLastValue = false;
            TInstant lastTimestamp;
            TValue lastValue{};
            bool hasPointInsideRange = false;

            TRawLineFrontend<TValue, TStoragePolicy>::ForEachStoredRecord(snapshot, [&](TInstant timestamp, const TValue& value) {
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

            if (!finiteEnd) {
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
                .ReadRange = &TOnChangeLineFrontend<TValue, TStoragePolicy>::ReadRange,
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
            struct TContext {
                void* Opaque;
                TLineFrontendOps::TInvokeNumericValues Invoke;
            } context{opaque, invoke};
            ReadRange(snapshot, beginTs, endTs, &context, [](void* opaque, TInstant timestamp, const void* value) {
                const auto& context = *static_cast<const TContext*>(opaque);
                const std::array<TLineNumericValue, 1> values = {MakeLineNumericValue(*static_cast<const TValue*>(value))};
                context.Invoke(context.Opaque, timestamp, values);
            });
        }

        friend class TLine<TOnChangeLineFrontend<TValue, TStoragePolicy>>;

        static bool Append(IMetricLine& line, const TValueType& value) noexcept;
    };

    template<class TValue, class TStoragePolicy>
    bool TOnChangeLineFrontend<TValue, TStoragePolicy>::Append(IMetricLine& line, const TValue& value) noexcept {
        ui64 encoded;
        if constexpr (TStoragePolicy::Enabled) {
            if (!TStoragePolicy::template Encode<0>(value, &encoded)) {
                return false;
            }
        } else {
            encoded = NInMemoryMetricsPrivate::EncodeLineValue(value);
        }
        const std::optional<ui64> lastMaterialized = line.GetLastMaterializedValue();

        if (lastMaterialized && *lastMaterialized == encoded) {
            return true;
        }

        const NHPTimer::STime nowTs = line.CurrentTimestampTs();

        typename TRawLineFrontend<TValue, TStoragePolicy>::TStorageRecord record{
            .TimestampTs = nowTs,
            .Value = encoded,
        };
        if (!line.AccessChunkMemory(&record, &TRawLineFrontend<TValue, TStoragePolicy>::WriteRecordToChunkMemory)) {
            return false;
        }
        line.MarkMaterialized(encoded);
        return true;
    }

} // namespace NActors
