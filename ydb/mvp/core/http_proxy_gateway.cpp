#include "http_proxy_gateway.h"

#include "mvp_log.h"
#include "mvp_log_context.h"

namespace NMVP {

namespace {

class THttpProxyGatewayRequestActor : public NActors::TActorBootstrapped<THttpProxyGatewayRequestActor> {
    using TBase = NActors::TActorBootstrapped<THttpProxyGatewayRequestActor>;

    const NActors::TActorId HttpProxyId;
    const NHttp::THttpIncomingRequestPtr OriginalRequest;
    NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr Event;
    NHttp::TEvHttpProxy::TEvSubscribeForCancel::TPtr CancelSubscriber;
    NHttp::THttpOutgoingResponsePtr ForwardedResponse;
    bool RequestCancelled = false;
    bool StreamingResponse = false;

public:
    THttpProxyGatewayRequestActor(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event,
                                  const NActors::TActorId& httpProxyId)
        : HttpProxyId(httpProxyId)
        , OriginalRequest(event->Get()->Request)
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

            const bool responseWasDone = response->IsDone();
            NHttp::THeadersBuilder extraHeaders;
            TString requestId = GetRequestId(Event->Get()->Request);
            if (!requestId.empty()) {
                extraHeaders.Set(REQUEST_ID_HEADER, requestId);
            }

            if (responseWasDone) {
                response = response->Duplicate(OriginalRequest, extraHeaders);
                if (response && !response->IsDone()) {
                    response->Finish();
                }
            } else {
                NHttp::THeadersBuilder headers(response->Headers);
                for (const auto& [key, value] : extraHeaders.Headers) {
                    if (value) {
                        headers.Set(key, value);
                    } else {
                        headers.Erase(key);
                    }
                }

                auto forwardedResponse = OriginalRequest->CreateIncompleteResponse(response->Status, response->Message, headers);
                forwardedResponse->FinishHeader();
                response = std::move(forwardedResponse);
            }

            StreamingResponse = !response->IsDone();
            if (StreamingResponse) {
                ForwardedResponse = response;
            }
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
            NHttp::THttpOutgoingDataChunkPtr forwardedDataChunk;
            if (ForwardedResponse) {
                forwardedDataChunk = ForwardedResponse->CreateDataChunk(event->Get()->DataChunk->AsString());
                if (isFinished) {
                    forwardedDataChunk->SetEndOfData();
                }
            } else {
                forwardedDataChunk = std::move(event->Get()->DataChunk);
            }
            Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(std::move(forwardedDataChunk)), 0, Event->Cookie);
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
