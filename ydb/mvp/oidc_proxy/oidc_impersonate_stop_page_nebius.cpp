#include <library/cpp/json/json_reader.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/mvp/core/mvp_log.h>
#include "openid_connect.h"
#include "oidc_session_create.h"
#include "oidc_settings.h"

namespace NMVP {
namespace NOIDC {

THandlerImpersonateStop::THandlerSessionCreate(const NActors::TActorId& sender,
                                             const NHttp::THttpIncomingRequestPtr& request,
                                             const NActors::TActorId& httpProxyId,
                                             const TOpenIdConnectSettings& settings)
    : Sender(sender)
    , Request(request)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
{}

void THandlerImpersonateStop::Bootstrap(const NActors::TActorContext& ctx) {
    NHttp::THeadersBuilder responseHeaders;responseHeaders
    SetCORS(Request, &responseHeaders);
    responseHeaders.Set("Set-Cookie", CreateSecureCookie(Settings.ClientId, sessionToken));

    NHttp::THttpOutgoingResponsePtr httpResponse;
    httpResponse = Request->CreateResponse("200", "OK", responseHeaders);
    ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse));
    Die(ctx);
}

} // NOIDC
} // NMVP
