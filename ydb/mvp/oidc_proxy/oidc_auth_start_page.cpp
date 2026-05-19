#include "oidc_auth_start_page.h"

#include "context.h"
#include "oidc_cookie.h"
#include "openid_connect.h"

namespace NMVP::NOIDC {

THandlerAuthStart::THandlerAuthStart(const NActors::TActorId& sender,
                                     const NHttp::THttpIncomingRequestPtr& request,
                                     const NActors::TActorId& httpProxyId,
                                     const TOpenIdConnectSettings& settings)
    : TMvpLogContextProvider(CreateMvpLogContext(request))
    , Sender(sender)
    , Request(request)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
{}

void THandlerAuthStart::Bootstrap() {
    NHttp::TUrlParameters urlParameters(Request->URL);
    const TString returnTo = urlParameters["return_to"];
    if (returnTo.empty()) {
        NHttp::THeadersBuilder responseHeaders;
        responseHeaders.Set("Content-Type", "text/plain");
        SetCORS(Request, &responseHeaders);
        SetRequestIdHeader(responseHeaders, GetRequestId());
        return ReplyAndPassAway(Request->CreateResponse("400", "Bad Request", responseHeaders, "`return_to` parameter is required"));
    }

    NHttp::THeaders headers(Request->Headers);
    NHttp::TCookies cookies(headers.Get("cookie"));
    TContext context({
        .State = GenerateRandomBase64(),
        .RequestedAddress = returnTo,
    });

    ReplyAndPassAway(GetHttpOutgoingResponsePtrForAuthStart(
        Request,
        Settings,
        context,
        GetCookie(cookies, CreateSharedOidcCookieName()),
        GetRequestId()
    ));
}

void THandlerAuthStart::ReplyAndPassAway(NHttp::THttpOutgoingResponsePtr httpResponse) {
    Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(std::move(httpResponse)));
    PassAway();
}

TAuthStartPageHandler::TAuthStartPageHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings)
    : TBase(&TAuthStartPageHandler::StateWork)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
{}

void TAuthStartPageHandler::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
    Register(new THandlerAuthStart(event->Sender, event->Get()->Request, HttpProxyId, Settings));
}

} // NMVP::NOIDC
