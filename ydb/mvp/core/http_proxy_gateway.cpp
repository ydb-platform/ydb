#include "http_proxy_gateway.h"

#include "mvp_log.h"
#include "mvp_log_context.h"

namespace NMVP {

namespace {

class THttpProxyGatewayRequest : public NActors::TActorBootstrapped<THttpProxyGatewayRequest> {
    using TBase = NActors::TActorBootstrapped<THttpProxyGatewayRequest>;

    const NActors::TActorId OriginalSender;
    const NActors::TActorId NextProxyId;
    NHttp::THttpIncomingRequestPtr Request;
    TString UserToken;
    bool WaitForDataChunks = false;

public:
    THttpProxyGatewayRequest(const NActors::TActorId& originalSender,
                             const NActors::TActorId& nextProxyId,
                             const NHttp::THttpIncomingRequestPtr& request,
                             TString userToken)
        : OriginalSender(originalSender)
        , NextProxyId(nextProxyId)
        , Request(request)
        , UserToken(std::move(userToken))
    {
        EnsureRequestIdHeader(Request);
    }

    void Bootstrap() {
        BLOG_D(GetLogPrefix(Request) << "Incoming HTTP request: " << Request->Method << ' ' << Request->URL);

        auto event = std::make_unique<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(Request);
        event->UserToken = UserToken;
        Send(NextProxyId, event.release());

        Become(&THttpProxyGatewayRequest::StateWork);
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpOutgoingResponse::TPtr event) {
        auto response = std::move(event->Get()->Response);
        if (response) {
            BLOG_D(GetLogPrefix(Request) << "Outgoing HTTP response: " << response->Status << ' ' << response->Message);

            TString requestId = GetRequestId(Request);
            if (!requestId.empty()) {
                NHttp::THeadersBuilder extraHeaders;
                extraHeaders.Set(REQUEST_ID_HEADER, requestId);
                response = response->Duplicate(Request, extraHeaders);
            }

            WaitForDataChunks = !response->TransferEncoding.empty();
        }

        auto forwarded = std::make_unique<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(std::move(response));
        forwarded->ProgressNotificationBytes = event->Get()->ProgressNotificationBytes;
        Send(OriginalSender, forwarded.release());

        if (!WaitForDataChunks) {
            PassAway();
        }
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk::TPtr event) {
        bool isFinished = false;
        if (event->Get()->DataChunk) {
            isFinished = event->Get()->DataChunk->IsEndOfData();
            Send(OriginalSender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(std::move(event->Get()->DataChunk)));
        } else {
            isFinished = true;
            Send(OriginalSender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(event->Get()->Error));
        }
        if (isFinished) {
            PassAway();
        }
    }

    void Handle(NHttp::TEvHttpProxy::TEvSubscribeForCancel::TPtr event) {
        NActors::TActivationContext::Send(new NActors::IEventHandle(
            OriginalSender,
            event->Sender,
            new NHttp::TEvHttpProxy::TEvSubscribeForCancel(),
            event->Flags,
            event->Cookie
        ));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpOutgoingResponse, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvSubscribeForCancel, Handle);
        }
    }
};

} // namespace

THttpProxyGateway::THttpProxyGateway(const NActors::TActorId& nextProxyId)
    : NextProxyId(nextProxyId)
{}

void THttpProxyGateway::Bootstrap() {
    Become(&THttpProxyGateway::StateWork);
}

void THttpProxyGateway::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
    Register(new THttpProxyGatewayRequest(event->Sender, NextProxyId, event->Get()->Request, event->Get()->UserToken));
}

} // namespace NMVP
