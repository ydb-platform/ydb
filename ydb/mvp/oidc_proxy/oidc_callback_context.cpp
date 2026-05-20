#include "oidc_callback_context.h"

#include "openid_connect.h"

#include <ydb/library/security/util.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/json/json_writer.h>

namespace NMVP::NOIDC {

THandlerAuthCallbackContext::THandlerAuthCallbackContext(const NActors::TActorId& sender,
                                                         const NHttp::THttpIncomingRequestPtr& request,
                                                         const TOpenIdConnectSettings& settings)
    : TMvpLogContextProvider(CreateMvpLogContext(request))
    , Sender(sender)
    , Request(request)
    , Settings(settings)
{}

void THandlerAuthCallbackContext::Bootstrap() {
    BLOG_D("Get oidc auth callback context");

    if (Request->Method != "GET") {
        BLOG_D("Get oidc auth callback context failed: only GET method is allowed");
        ReplyAndPassAway(CreateTextResponse("400", "Bad Request", "Only GET method is allowed"));
        return;
    }

    TCgiParameters queryParameters;
    const TStringBuf queryString = TStringBuf(Request->URL).After('?');
    if (!queryString.empty()) {
        queryParameters.Scan(queryString);
    }

    const TString state = queryParameters.Get("state");
    if (state.empty()) {
        BLOG_D("Get oidc auth callback context failed: state is empty");
        ReplyAndPassAway(CreateTextResponse("400", "Bad Request", "State is empty"));
        return;
    }

    const TDecodeStateResult decodedState = DecodeState(state);
    const TCheckStateResult checkStateResult = decodedState.Check(Settings.ClientSecret);
    if (!checkStateResult.Ok) {
        BLOG_D("Get oidc auth callback context failed: " << checkStateResult.ErrorMessage);
        ReplyAndPassAway(CreateTextResponse("403", "Forbidden", checkStateResult.ErrorMessage));
        return;
    }

    const TString flowId = decodedState.Payload.FlowId;
    if (flowId.empty()) {
        BLOG_D("Get oidc auth callback context failed: flow id is empty");
        ReplyAndPassAway(CreateTextResponse("400", "Bad Request", "Flow id is empty"));
        return;
    }

    const TRestoreOidcContextResult restoreContextResult = RestoreOidcContextFromStore(Settings.AuthCallbackContextStore, flowId);
    if (!restoreContextResult.IsSuccess()) {
        ReplyContextRestoreFailureAndPassAway(flowId, restoreContextResult);
        return;
    }

    BLOG_D("Get oidc auth callback context succeeded for flow " << NKikimr::MaskTicket(flowId));
    ReplyAndPassAway(CreateJsonResponse(restoreContextResult.Context.GetRequestedAddress()));
}

NHttp::THttpOutgoingResponsePtr THandlerAuthCallbackContext::CreateTextResponse(TStringBuf status, TStringBuf message, TStringBuf body) const {
    NHttp::THeadersBuilder responseHeaders;
    SetRequestIdHeader(responseHeaders, GetRequestId());
    if (!body.empty()) {
        responseHeaders.Set("Content-Type", "text/plain");
    }
    return Request->CreateResponse(status, message, responseHeaders, body);
}

NHttp::THttpOutgoingResponsePtr THandlerAuthCallbackContext::CreateJsonResponse(TStringBuf requestedAddress) const {
    NJson::TJsonValue json(NJson::JSON_MAP);
    json["requested_address"] = requestedAddress;

    NHttp::THeadersBuilder responseHeaders;
    SetRequestIdHeader(responseHeaders, GetRequestId());
    responseHeaders.Set("Content-Type", "application/json; charset=utf-8");
    return Request->CreateResponse("200", "OK", responseHeaders, NJson::WriteJson(json, false));
}

void THandlerAuthCallbackContext::ReplyContextRestoreFailureAndPassAway(TStringBuf flowId, const TRestoreOidcContextResult& restoreContextResult) {
    BLOG_D("Get oidc auth callback context failed for flow "
        << NKikimr::MaskTicket(flowId) << ": " << restoreContextResult.Status.ErrorMessage);
    if (!Settings.AuthCallbackContextStore) {
        ReplyAndPassAway(CreateTextResponse("503", "Service Unavailable", "Auth flow context store is not configured"));
        return;
    }
    ReplyAndPassAway(CreateTextResponse("404", "Not Found"));
}

void THandlerAuthCallbackContext::ReplyAndPassAway(NHttp::THttpOutgoingResponsePtr response) {
    Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(std::move(response)));
    PassAway();
}

TAuthCallbackContextHandler::TAuthCallbackContextHandler(const TOpenIdConnectSettings& settings)
    : TBase(&TAuthCallbackContextHandler::StateWork)
    , Settings(settings)
{}

void TAuthCallbackContextHandler::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
    Register(new THandlerAuthCallbackContext(event->Sender, event->Get()->Request, Settings));
}

} // NMVP::NOIDC
