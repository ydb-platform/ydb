#pragma once

#include <ydb/library/retro_tracing/retro_span.h>
#include <ydb/library/retro_tracing/retro_span_base.h>
#include <ydb/library/actors/retro_uploader/protos/events.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/event_pb.h>

namespace NRetro {

enum EEvents {
    EvDemandTrace = EventSpaceBegin(NActors::TEvents::ES_RETRO),
    EvCollectSpansRequest,
    EvCollectSpansResponse,
};

struct TEvDemandTrace : public NActors::TEventLocal<TEvDemandTrace, EvDemandTrace> {
public:
    TEvDemandTrace(TTraceId traceId)
        : TraceId(traceId)
    {}

public:
    TTraceId TraceId;
};

struct TEvCollectSpansRequest : public NActors::TEventPB<TEvCollectSpansRequest,
        NRetroUploaderProto::TEvCollectSpansRequest, EvCollectSpansRequest> {
public:
    TEvCollectSpansRequest() {
    }

    TEvCollectSpansRequest(TTraceId traceId) {
        Record.SetTraceId(traceId);
    }
};

struct TEvCollectSpansResponse : public NActors::TEventPB<TEvCollectSpansResponse,
        NRetroUploaderProto::TEvCollectSpansResponse, EvCollectSpansResponse> {
public:
    TEvCollectSpansResponse() {
    }

    TEvCollectSpansResponse(const std::vector<TRetroSpan::TPtr>& spans) {
        for (const TRetroSpan::TPtr& span : spans) {
            auto* spanRecord = Record.AddSpans();
            ERetroSpanType spanType = span->GetType();
            spanRecord->SetType((ui32)spanType);
            spanRecord->SetData(span.get(), SizeOfRetroSpan(spanType));
        }
    }

    std::vector<TRetroSpan::TPtr> GetSpans() const {
        std::vector<TRetroSpan::TPtr> res;
        for (const auto& serializedSpan : Record.GetSpans()) {
            ui32 spanType = serializedSpan.GetType();
            const char* spanData = serializedSpan.GetData().data();
            res.push_back(TRetroSpan::FromRawData(spanType, spanData));
        }
        return res;
    }
};

} // namespace NRetro
