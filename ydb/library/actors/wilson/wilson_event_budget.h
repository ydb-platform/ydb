#pragma once

#include "wilson_span.h"

#include <library/cpp/time_provider/monotonic.h>

#include <algorithm>
#include <utility>

namespace NWilson {

    // A producer-local budget. It does not establish completeness of a distributed
    // trace: sampling, propagation and other producers have independent coverage.
    // Keep terminal outcome in span attributes even if the terminal reserve fills.
    class TSpanEventBudget {
    public:
        enum class EKind {
            Regular,
            Terminal,
        };

        explicit TSpanEventBudget(ui32 limit = 256, ui32 terminalReserve = 8)
            : Limit(limit)
            , TerminalReserve(std::min(limit, terminalReserve))
        {
        }

        // Build attributes only after sampling, verbosity and budget checks. A
        // builder returns an owning TKeyValueList, never an initializer_list view.
        template <class TBuildAttributes>
        bool Event(TSpan& span, ui8 verbosity, const char* name,
                   TBuildAttributes&& buildAttributes, EKind kind = EKind::Regular) {
            if (!span || span.GetTraceId().GetVerbosity() < verbosity) {
                return false;
            }
            const ui64 sequence = ++Observed;
            const bool terminal = kind == EKind::Terminal;
            if ((terminal && TerminalRecorded >= TerminalReserve) || (!terminal && RegularRecorded >= Limit - TerminalReserve)) {
                ++Dropped;
                return false;
            }
            auto attributes = std::forward<TBuildAttributes>(buildAttributes)();
            attributes["ydb.event.sequence"] = static_cast<i64>(sequence);
            // Only compare these values within the same producer/process clock
            // domain. They are not remote delivery timestamps.
            attributes["ydb.event.monotonic_us"] = static_cast<i64>(TMonotonic::Now().MicroSeconds());
            span.Event(name, std::move(attributes));
            terminal ? ++TerminalRecorded : ++RegularRecorded;
            LastRecordedSequence = sequence;
            return true;
        }

        bool Event(TSpan& span, ui8 verbosity, const char* name, EKind kind = EKind::Regular) {
            return Event(span, verbosity, name, [] { return TKeyValueList{}; }, kind);
        }

        // Call before ending the owning span. Metadata is outside the event budget,
        // so truncation remains visible after both regular and terminal caps fill.
        void Annotate(TSpan& span) const {
            if (span) {
                span.Attribute("ydb.trace.event_budget.limit", static_cast<i64>(Limit));
                span.Attribute("ydb.trace.event_budget.terminal_reserve", static_cast<i64>(TerminalReserve));
                span.Attribute("ydb.trace.events_recorded", static_cast<i64>(RegularRecorded + TerminalRecorded));
                span.Attribute("ydb.trace.events_dropped", static_cast<i64>(Dropped));
                span.Attribute("ydb.trace.event_budget.exhausted", Dropped != 0);
                span.Attribute("ydb.trace.last_recorded_sequence", static_cast<i64>(LastRecordedSequence));
            }
        }

    private:
        const ui32 Limit;
        const ui32 TerminalReserve;
        ui32 RegularRecorded = 0;
        ui32 TerminalRecorded = 0;
        ui64 Observed = 0;
        ui64 Dropped = 0;
        ui64 LastRecordedSequence = 0;
    };

} // namespace NWilson
