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
    struct TOnChangeLineFrontend {
        using TValueType = TValue;

        struct TConfig {};

        static TValue DecodeValue(ui64 value) noexcept {
            return NInMemoryMetricsPrivate::DecodeLineValue<TValue>(value);
        }

        static void ReadRange(const TLineSnapshot& snapshot, TInstant beginTs, TInstant endTs, const std::function<void(const TRecordView&)>& cb) {
            snapshot.ForEachMaterializedRecordInRange(beginTs, endTs, cb);
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

        static bool Append(TInMemoryMetricsRegistry& registry, TLineWriterState* writer, const TValueType& value) noexcept;
    };

} // namespace NActors
