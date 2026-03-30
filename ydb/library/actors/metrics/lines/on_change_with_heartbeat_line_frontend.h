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
    struct TOnChangeWithHeartbeatLineFrontend {
        using TValueType = TValue;

        struct TConfig {
            TDuration Heartbeat;
        };

        static TValue DecodeValue(ui64 value) noexcept {
            return NInMemoryMetricsPrivate::DecodeLineValue<TValue>(value);
        }

        static void ReadRange(const TLineSnapshot& snapshot, TInstant beginTs, TInstant endTs, const std::function<void(const TRecordView&)>& cb) {
            TRecordView lastRecord;
            bool hasLastRecord = false;
            TRawLineFrontend<TValue>::ForEachStoredRecordInRange(snapshot, beginTs, endTs, cb);
            TRawLineFrontend<TValue>::ForEachStoredRecordInRange(snapshot, TInstant::Zero(), TInstant::Max(), [&](const TRecordView& record) {
                lastRecord = record;
                hasLastRecord = true;
            });

            const TInstant lastObservedTimestamp = snapshot.GetLastObservedTimestamp();
            const TInstant lastPublishedTimestamp = snapshot.GetLastPublishedTimestamp();
            if (!snapshot.Closed
                && lastObservedTimestamp > lastPublishedTimestamp
                && hasLastRecord
                && lastRecord.Timestamp < lastObservedTimestamp
                && beginTs <= lastObservedTimestamp
                && lastObservedTimestamp <= endTs) {
                cb(TRecordView{
                    .Timestamp = lastObservedTimestamp,
                    .Value = lastRecord.Value,
                });
            }
        }

        static const TLineFrontendOps& Descriptor() noexcept {
            static const TLineFrontendOps descriptor{
                .Name = "on_change_with_heartbeat",
                .ReadRange = &TOnChangeWithHeartbeatLineFrontend<TValue>::ReadRange,
            };
            return descriptor;
        }

        static TLineMeta MakeMeta(const TConfig& config) noexcept {
            return TLineMeta(&Descriptor(), config.Heartbeat);
        }

    private:
        friend class TLine<TOnChangeWithHeartbeatLineFrontend<TValue>>;

        static bool Append(TInMemoryMetricsRegistry& registry, TLineWriterState* writer, const TValueType& value) noexcept;
    };

} // namespace NActors
