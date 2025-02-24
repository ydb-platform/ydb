#include "retro_uploader.h"
#include "events.h"

#include <ydb/core/util/stlog.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/actors/wilson/wilson_trace.h>
#include <ydb/library/retro_tracing/retro_tracing.h>
#include <ydb/library/retro_tracing/span_circlebuf_stats.h>

using namespace NActors;

namespace NRetro {

class TRetroUploader : public TActorBootstrapped<TRetroUploader> {
public:
    TRetroUploader(TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
        : Counters(counters)
        , LastUploadTs(TMonotonic::Zero())
    {
        TracesDemanded = Counters->GetCounter("TracesDemanded", true);
        TracesUploaded = Counters->GetCounter("TracesUploaded", true);
        SpansUploaded = Counters->GetCounter("SpansUploaded", true);
    }

public:
    void Bootstrap(const TActorContext&/* ctx*/) {
        *TracesDemanded = 0;
        *SpansUploaded = 0;
        Become(&TThis::StateFunc);
    }

    void Handle(TEvDemandTrace::TPtr& ev) {
        ++*TracesDemanded;
        TMonotonic now = TActivationContext::Monotonic();
        if (now - LastUploadTs < MinUploadDelay) {
            return;
        }
        LastUploadTs = now;
        const ui8 verbosity = 1;
        NWilson::TTraceId wilsonTraceId = NWilson::TTraceId::NewTraceId(verbosity, Max<ui32>());
        NRetro::TTraceId retroTraceId = ev->Get()->TraceId;
        std::vector<TRetroSpan::TPtr> spans = ReadSpansOfTrace(retroTraceId);
        std::unordered_map<TSpanId, NWilson::TSpan> wilsonSpans;

        for (const TRetroSpan::TPtr& span : spans) {
            TSpanId spanId = span->GetId().SpanId;
            Y_DEBUG_ABORT_UNLESS(!wilsonSpans.contains(spanId));
            wilsonSpans[spanId] = NWilson::TSpan::CreateTerminated(span->GetName(),
                    wilsonTraceId.Span(verbosity), span->GetStart(), span->GetEnd());
        }

        for (const TRetroSpan::TPtr& span : spans) {
            std::optional<NRetro::TSpanId> parentId = span->GetParentSpanId();
            TSpanId spanId = span->GetId().SpanId;
            NWilson::TSpan& wilsonSpan = wilsonSpans[spanId];
            span->FillWilsonSpanAttributes(&wilsonSpan);
            if (parentId) {
                auto it = wilsonSpans.find(*parentId);
                if (it != wilsonSpans.end()) {
                    wilsonSpan.SetParentId(it->second.GetTraceId());
                    wilsonSpan.Attribute("parentRetroSpanId", ToString(parentId));
                }
            }
        }

        STLOG(PRI_NOTICE, RETRO_UPLOADER, RET01, "Trace demanded",
            (RetroTraceId, retroTraceId),
            (WilsonTraceId, wilsonTraceId.GetHexTraceId()),
            (SpansGenerated, wilsonSpans.size()),
        );

        ++*TracesUploaded;
        for (auto& [spanId, span] : wilsonSpans) {
            span
                .Attribute("retroTraceId", ToString(retroTraceId))
                .Attribute("retroSpanId", ToString(spanId));
            span.EndOk();
            ++*SpansUploaded;
        }
    }

    STRICT_STFUNC(StateFunc, {
        hFunc(TEvDemandTrace, Handle);
    })

private:

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr TracesDemanded;
    ::NMonitoring::TDynamicCounters::TCounterPtr TracesUploaded;
    ::NMonitoring::TDynamicCounters::TCounterPtr SpansUploaded;

    TDuration MinUploadDelay = TDuration::Seconds(10);
    TMonotonic LastUploadTs;
};

void DemandTrace(TTraceId traceId) {
    TActivationContext::Send(new IEventHandle(MakeRetroUploaderId(), {}, new TEvDemandTrace(traceId)));
}

IActor* CreateRetroUploader(TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
    return new TRetroUploader(counters);
}

} // namespace NRetro
