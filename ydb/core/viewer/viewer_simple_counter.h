#pragma once
#include "json_pipe_req.h"
#include "log.h"

namespace NKikimr::NViewer {

using namespace NActors;

class TViewerSimpleCounter : public TViewerPipeClient {
public:
    using TThis = TViewerSimpleCounter;
    using TBase = TViewerPipeClient;

    TViewerSimpleCounter(IViewer* viewer, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev)
        : TBase(viewer, ev)
    {}

    ui32 MaxCounter = 10;
    ui32 Counter = 0;
    ui32 Period = 1000;
    ui32 FailChance = 0;
    NHttp::THttpOutgoingResponsePtr HttpResponse;

    TString GetLogPrefix() const {
        return "SIMPLE_COUNTER ";
    }

    void Bootstrap() override {
        if (TBase::NeedToRedirect()) {
            return;
        }
        NHttp::TUrlParameters params(HttpEvent->Get()->Request->GetParameters());
        MaxCounter = std::clamp<ui32>(FromStringWithDefault(params.Get("max_counter"), MaxCounter), 1, 100000);
        Period = std::clamp<ui32>(FromStringWithDefault(params.Get("period"), Period), 1, 100000);
        FailChance = std::clamp<ui32>(FromStringWithDefault(params.Get("fail_chance"), FailChance), 0, 100);
        BLOG_D("Started MaxCounter: " << MaxCounter << ", Period: " << Period << ", FailChance: " << FailChance);
        Send(HttpEvent->Sender, new NHttp::TEvHttpProxy::TEvSubscribeForCancel(), IEventHandle::FlagTrackDelivery);
        HttpResponse = HttpEvent->Get()->Request->CreateResponseString(Viewer->GetChunkedHTTPOK(GetRequest(), "text/plain"));
        Send(HttpEvent->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(HttpResponse));
        Become(&TThis::StateWork, TDuration::MilliSeconds(Period), new TEvents::TEvWakeup());
    }

    void HandleTimer() {
        ++Counter;
        if (FailChance > 0 && ((ui32)NPrivate::TRandom() % 100) < FailChance) {
            BLOG_D("Simulate fail");
            Send(HttpEvent->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk("failed"));
            return ReplyAndPassAway();
        }
        BLOG_D("Counter: " << Counter);
        auto dataChunk = HttpResponse->CreateDataChunk(TStringBuilder() << Counter << "\n");
        if (Counter >= MaxCounter) {
            dataChunk->SetEndOfData();
        }
        Send(HttpEvent->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(dataChunk));
        if (Counter >= MaxCounter) {
            return ReplyAndPassAway();
        } else {
            Schedule(TDuration::MilliSeconds(Period), new TEvents::TEvWakeup());
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
            cFunc(TEvents::TSystem::Wakeup, HandleTimer);
            cFunc(NHttp::TEvHttpProxy::EvRequestCancelled, Cancelled);
            hFunc(TEvents::TEvUndelivered, Undelivered);
        }
    }
};

}
