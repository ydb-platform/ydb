#include "http_proxy_gateway.h"

#include "mvp_log.h"
#include "mvp_log_context.h"

namespace NMVP {

namespace {

class THttpProxyGatewayRequestActor : public NActors::TActorBootstrapped<THttpProxyGatewayRequestActor> {
    using TBase = NActors::TActorBootstrapped<THttpProxyGatewayRequestActor>;

    const NActors::TActorId HttpProxyId;
    NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr Event;
    NHttp::TEvHttpProxy::TEvSubscribeForCancel::TPtr CancelSubscriber;
    bool RequestCancelled = false;
    bool StreamingResponse = false;

public:
    THttpProxyGatewayRequestActor(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event,
                                  const NActors::TActorId& httpProxyId)
        : HttpProxyId(httpProxyId)
        , Event(std::move(event))
    {
        EnsureRequestIdHeader(Event->Get()->Request);
    }

    void Bootstrap() {
        const auto& request = Event->Get()->Request;
        BLOG_D(GetLogPrefix(request) << "Incoming HTTP request: " << request->Method << ' ' << request->URL);

        Send(Event->Sender, new NHttp::TEvHttpProxy::TEvSubscribeForCancel(), NActors::IEventHandle::FlagTrackDelivery);

        auto forwardedEvent = std::make_unique<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(request);
        forwardedEvent->UserToken = Event->Get()->UserToken;
        Send(HttpProxyId, forwardedEvent.release(), 0, Event->Cookie);

        Become(&THttpProxyGatewayRequestActor::StateWork);
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpOutgoingResponse::TPtr event) {
        if (RequestCancelled) {
            PassAway();
            return;
        }

        auto response = std::move(event->Get()->Response);
        if (response) {
            BLOG_D(GetLogPrefix(Event->Get()->Request) << "Outgoing HTTP response: " << response->Status << ' ' << response->Message);

            TString requestId = GetRequestId(Event->Get()->Request);
            if (!requestId.empty()) {
                NHttp::THeadersBuilder extraHeaders;
                extraHeaders.Set(REQUEST_ID_HEADER, requestId);
                response = response->Duplicate(Event->Get()->Request, extraHeaders);
            }

            StreamingResponse = !response->IsDone();
        }

        auto forwarded = std::make_unique<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(std::move(response));
        forwarded->ProgressNotificationBytes = event->Get()->ProgressNotificationBytes;
        Send(Event->Sender, forwarded.release(), 0, Event->Cookie);

        if (!StreamingResponse) {
            PassAway();
        }
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk::TPtr event) {
        if (RequestCancelled) {
            PassAway();
            return;
        }

        bool isFinished = false;
        if (event->Get()->DataChunk) {
            isFinished = event->Get()->DataChunk->IsEndOfData();
            Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(std::move(event->Get()->DataChunk)), 0, Event->Cookie);
        } else {
            isFinished = true;
            Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(event->Get()->Error), 0, Event->Cookie);
        }
        if (isFinished) {
            PassAway();
        }
    }

    void Handle(NHttp::TEvHttpProxy::TEvSubscribeForCancel::TPtr event) {
        CancelSubscriber = std::move(event);
        if (RequestCancelled) {
            BLOG_D(GetLogPrefix(Event->Get()->Request) << "HTTP request cancelled");
            Send(CancelSubscriber->Sender, new NHttp::TEvHttpProxy::TEvRequestCancelled(), 0, CancelSubscriber->Cookie);
            PassAway();
        }
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr& event) {
        if (event->Get()->SourceType == NHttp::TEvHttpProxy::EvSubscribeForCancel) {
            PassAway();
        }
    }

    void Handle(NHttp::TEvHttpProxy::TEvRequestCancelled::TPtr&) {
        RequestCancelled = true;
        if (CancelSubscriber) {
            BLOG_D(GetLogPrefix(Event->Get()->Request) << "HTTP request cancelled");
            Send(CancelSubscriber->Sender, new NHttp::TEvHttpProxy::TEvRequestCancelled(), 0, CancelSubscriber->Cookie);
            PassAway();
        }
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvents::TEvUndelivered, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvHttpOutgoingResponse, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvSubscribeForCancel, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvRequestCancelled, Handle);
        }
    }
};

} // namespace

THttpProxyGateway::THttpProxyGateway(const NActors::TActorId& httpProxyId)
    : HttpProxyId(httpProxyId)
{}

void THttpProxyGateway::Bootstrap() {
    Become(&THttpProxyGateway::StateWork);
}

void THttpProxyGateway::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
    Register(new THttpProxyGatewayRequestActor(std::move(event), HttpProxyId));
}

} // namespace NMVP
