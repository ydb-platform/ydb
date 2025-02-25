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

class TRetroTraceCollector : public TActorBootstrapped<TRetroTraceCollector> {
public:
    TRetroTraceCollector(NRetro::TTraceId traceId, TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
        : TraceId(traceId)
        , Counters(counters)
    {
        SpansUploaded = Counters->GetCounter("SpansUploaded", true);
    }

public:
    void Bootstrap(const TActorContext&/* ctx*/) {
        Become(&TThis::StateFunc);

        AddSpans(ReadSpansOfTrace(TraceId));

        std::unordered_set<ui32> subrequestNodeIds;
        for (const NRetro::TRetroSpan::TPtr& span : RetroSpans) {
            for (const ui32 nodeId : span->GetSubrequestNodeIds()) {
                if (nodeId != SelfId().NodeId()) {
                    subrequestNodeIds.insert(nodeId);
                }
            }
        }

        SubrequestsWaiting = 0;
        for (const ui32 nodeId : subrequestNodeIds) {
            CollectSpansFromAnotherNode(nodeId);
        }

        STLOG(PRI_NOTICE, RETRO_UPLOADER, RET02, "Bootstrap collector",
            (RetroTraceId, TraceId),
            (Subrequests, SubrequestsWaiting),
        );

        if (SubrequestsWaiting > 0) {
            Schedule(WaitDeadline, new TEvents::TEvWakeup);
        } else {
            Finish(false);
        }
    }

    void CollectSpansFromAnotherNode(ui32 nodeId) {
        Send(MakeRetroUploaderId(nodeId), new TEvCollectSpansRequest(TraceId));
        ++SubrequestsWaiting;
    }

    void Handle(TEvCollectSpansResponse::TPtr& ev) {
        AddSpans(ev->Get()->GetSpans());
        if (--SubrequestsWaiting == 0) {
            Finish(false);
        }
    }

    void Finish(bool wakeup = true) {
        const ui8 verbosity = 1;
        NWilson::TTraceId wilsonTraceId = NWilson::TTraceId::NewTraceId(verbosity, Max<ui32>());

        for (const TRetroSpan::TPtr& span : RetroSpans) {
            TSpanId spanId = span->GetId().SpanId;
            if (WilsonSpans.contains(spanId)) {
                // span id collision
                continue;
            }
            WilsonSpans[spanId] = NWilson::TSpan::CreateTerminated(span->GetName(),
                    wilsonTraceId.Span(verbosity), span->GetStart(), span->GetEnd());
        }

        for (const TRetroSpan::TPtr& span : RetroSpans) {
            std::optional<NRetro::TSpanId> parentId = span->GetParentSpanId();
            TSpanId spanId = span->GetId().SpanId;
            NWilson::TSpan& wilsonSpan = WilsonSpans[spanId];
            span->FillWilsonSpanAttributes(&wilsonSpan);
            if (parentId) {
                auto it = WilsonSpans.find(*parentId);
                if (it != WilsonSpans.end()) {
                    wilsonSpan.SetParentId(it->second.GetTraceId());
                    wilsonSpan.Attribute("parentRetroSpanId", ToString(parentId));
                }
            }
        }

        STLOG(PRI_NOTICE, RETRO_UPLOADER, RET03, "Upload spans",
            (RetroTraceId, TraceId),
            (WilsonTraceId, wilsonTraceId.GetHexTraceId()),
            (SpansGenerated, WilsonSpans.size()),
            (FinishedByTimeout, wakeup),
        );

        for (auto& [spanId, span] : WilsonSpans) {
            span
                .Attribute("retroTraceId", ToString(TraceId))
                .Attribute("retroSpanId", ToString(spanId));
            span.EndOk();
            ++*SpansUploaded;
        }

        PassAway();
    }

    void AddSpans(std::vector<TRetroSpan::TPtr>&& spans) {
        RetroSpans.insert(RetroSpans.end(), std::make_move_iterator(spans.begin()),
                std::make_move_iterator(spans.end()));
    }

    STRICT_STFUNC(StateFunc, {
        hFunc(TEvCollectSpansResponse, Handle);
        cFunc(TEvents::TEvWakeup::EventType, Finish);
    })

private:
    NRetro::TTraceId TraceId;
    std::vector<TRetroSpan::TPtr> RetroSpans;
    std::unordered_map<TSpanId, NWilson::TSpan> WilsonSpans;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr SpansUploaded;

    ui32 SubrequestsWaiting;

    static constexpr TDuration WaitDeadline = TDuration::Seconds(60);
};

IActor* CreateRetroTraceCollector(TTraceId traceId, TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
    return new TRetroTraceCollector(traceId, counters);
}

class TRetroUploader : public TActorBootstrapped<TRetroUploader> {
public:
    TRetroUploader(TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
        : Counters(counters)
        , LastUploadTs(TMonotonic::Zero())
    {
        TracesDemanded = Counters->GetCounter("TracesDemanded", true);
        TracesUploaded = Counters->GetCounter("TracesUploaded", true);
        TraceUploadsRejected = Counters->GetCounter("TraceUploadsRejected", true);
    }

public:
    void Bootstrap(const TActorContext&/* ctx*/) {
        *TracesDemanded = 0;
        *TracesUploaded = 0;
        *TraceUploadsRejected = 0;
        Become(&TThis::StateFunc);
    }

    void Handle(TEvDemandTrace::TPtr& ev) {
        ++*TracesDemanded;
        TMonotonic now = TActivationContext::Monotonic();
        if (now - LastUploadTs < MinUploadDelay) {
            ++*TraceUploadsRejected;
            return;
        }
        LastUploadTs = now;

        TActivationContext::Register(CreateRetroTraceCollector(ev->Get()->TraceId, Counters));
        ++*TracesUploaded;
    }

    void Handle(TEvCollectSpansRequest::TPtr& ev) {
        TTraceId traceId = ev->Get()->Record.GetTraceId();
        std::vector<TRetroSpan::TPtr> spans = ReadSpansOfTrace(traceId);
        STLOG(PRI_NOTICE, RETRO_UPLOADER, RET04, "Collect spans",
            (RetroTraceId, traceId),
            (SpansCollected, spans.size()),
        );
        Send(ev->Sender, new TEvCollectSpansResponse(std::move(spans)));
    }


    STRICT_STFUNC(StateFunc, {
        hFunc(TEvCollectSpansRequest, Handle);
        hFunc(TEvDemandTrace, Handle);
    })

private:

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr TracesDemanded;
    ::NMonitoring::TDynamicCounters::TCounterPtr TracesUploaded;
    ::NMonitoring::TDynamicCounters::TCounterPtr TraceUploadsRejected;

    TDuration MinUploadDelay = TDuration::Seconds(10);
    TMonotonic LastUploadTs;
};

void DemandTrace(TTraceId traceId, NActors::TActorId sender) {
    TActivationContext::Send(new IEventHandle(MakeRetroUploaderId(sender.NodeId()),
            {}, new TEvDemandTrace(traceId)));
}

IActor* CreateRetroUploader(TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
    return new TRetroUploader(counters);
}

} // namespace NRetro
