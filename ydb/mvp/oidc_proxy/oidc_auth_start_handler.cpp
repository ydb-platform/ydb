#include "oidc_auth_start_handler.h"

#include "context.h"
#include "openid_connect.h"

#include <ydb/mvp/core/cracked_page.h>

namespace NMVP::NOIDC {

namespace {

bool IsSafeReturnTo(const NHttp::THttpIncomingRequestPtr& request, TStringBuf returnTo) {
    if (returnTo.empty()) {
        return false;
    }
    if (returnTo.StartsWith("//")) {
        return false;
    }
    if (returnTo.StartsWith('/')) {
        return true;
    }

    const TCrackedPage crackedPage(returnTo);
    return crackedPage.IsParsed()
        && crackedPage.IsHttpSchemeAllowed()
        && crackedPage.Host == request->Host;
}

} // anonymous namespace

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
    if (!IsSafeReturnTo(Request, returnTo)) {
        NHttp::THeadersBuilder responseHeaders;
        responseHeaders.Set("Content-Type", "text/plain");
        SetCORS(Request, &responseHeaders);
        SetRequestIdHeader(responseHeaders, GetRequestId());
        return ReplyAndPassAway(Request->CreateResponse("400", "Bad Request", responseHeaders, "`return_to` parameter is invalid"));
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
        GetCookie(cookies, CreateNameYdbOidcCookie()),
        GetRequestId()
    ));
}

void THandlerAuthStart::ReplyAndPassAway(NHttp::THttpOutgoingResponsePtr httpResponse) {
    Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(std::move(httpResponse)));
    PassAway();
}

TAuthStartHandler::TAuthStartHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings)
    : TBase(&TAuthStartHandler::StateWork)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
{}

void TAuthStartHandler::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
    Register(new THandlerAuthStart(event->Sender, event->Get()->Request, HttpProxyId, Settings));
}

} // NMVP::NOIDC
