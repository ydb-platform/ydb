#pragma once
#include "json_pipe_req.h"
#include "log.h"

namespace NKikimr::NViewer {

using namespace NActors;

class TViewerEventStreamCounter : public TViewerPipeClient {
public:
    using TThis = TViewerEventStreamCounter;
    using TBase = TViewerPipeClient;

    TViewerEventStreamCounter(IViewer* viewer, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev)
        : TBase(viewer, ev)
    {}

    ui32 MaxCounter = 10;
    ui32 Counter = 0;
    ui32 Period = 1000;
    ui32 KeepAlivePeriod = 0;
    ui32 FailChance = 0;
    TString ContentType = "text/event-stream";
    NHttp::THttpOutgoingResponsePtr HttpResponse;

    TString GetLogPrefix() const {
        return "EVENT_STREAM_COUNTER ";
    }

    void Bootstrap() override {
        if (TBase::NeedToRedirect()) {
            return;
        }
        NHttp::TUrlParameters params(HttpEvent->Get()->Request->GetParameters());
        MaxCounter = std::clamp<ui32>(FromStringWithDefault(params.Get("max_counter"), MaxCounter), 1, 100000);
        Period = std::clamp<ui32>(FromStringWithDefault(params.Get("period"), Period), 1, 100000);
        KeepAlivePeriod = std::clamp<ui32>(FromStringWithDefault(params.Get("keep_alive_period"), KeepAlivePeriod), 0, 100000);
        FailChance = std::clamp<ui32>(FromStringWithDefault(params.Get("fail_chance"), FailChance), 0, 100);
        if (params.Has("content_type")) {
            ContentType = params.Get("content_type");
        }
        BLOG_D("Started MaxCounter: " << MaxCounter << ", Period: " << Period << ", FailChance: " << FailChance << ", ContentType: " << ContentType);
        Send(HttpEvent->Sender, new NHttp::TEvHttpProxy::TEvSubscribeForCancel(), IEventHandle::FlagTrackDelivery);
        HttpResponse = HttpEvent->Get()->Request->CreateResponseString(Viewer->GetChunkedHTTPOK(GetRequest(), "text/event-stream"));
        Send(HttpEvent->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(HttpResponse));
        if (KeepAlivePeriod) {
            Schedule(TDuration::MilliSeconds(KeepAlivePeriod), new TEvents::TEvWakeup(1));
        }
        Become(&TThis::StateWork, TDuration::MilliSeconds(Period), new TEvents::TEvWakeup(0));
    }

    void HandleTimer(TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag == 0) {
            ++Counter;
            if (FailChance > 0 && ((ui32)NPrivate::TRandom() % 100) < FailChance) {
                BLOG_D("Simulate fail");
                Send(HttpEvent->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk("failed"));
                return ReplyAndPassAway();
            } else {
                BLOG_D("Counter: " << Counter);
                TStringBuilder content;
                content << "event: counter\n";
                content << "data: {\"Counter\":" << Counter << "}\n\n";
                auto dataChunk = HttpResponse->CreateDataChunk(content);
                Send(HttpEvent->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(dataChunk));
            }

            if (Counter >= MaxCounter) {
                {
                    auto dataChunk = HttpResponse->CreateDataChunk();
                    Send(HttpEvent->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(dataChunk));
                }
                return ReplyAndPassAway();
            } else {
                Schedule(TDuration::MilliSeconds(Period), new TEvents::TEvWakeup(0));
            }
        } else if (ev->Get()->Tag == 1) {
            TStringBuilder content;
            content << ": ping - " << NActors::TActivationContext::Now().ToIsoStringLocal() << "\n\n";
            auto dataChunk = HttpResponse->CreateDataChunk(content);
            Send(HttpEvent->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(dataChunk));
            Schedule(TDuration::MilliSeconds(KeepAlivePeriod), new TEvents::TEvWakeup(1));
        }
    }

    void Cancelled() {
        BLOG_D("Cancelled");
        ReplyAndPassAway();
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Get()->SourceType == NHttp::TEvHttpProxy::EvSubscribeForCancel) {
            Cancelled();
        }
    }

    void ReplyAndPassAway() override {
        BLOG_D("Done");
        HttpEvent.Reset(); // to avoid double reply
        TBase::ReplyAndPassAway("ok");
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvWakeup, HandleTimer);
            cFunc(NHttp::TEvHttpProxy::EvRequestCancelled, Cancelled);
            hFunc(TEvents::TEvUndelivered, Undelivered);
        }
    }
};

}
