#include "extension.h"

namespace NMVP::NOIDC {

void TExtension::Bootstrap() {
    Become(&TExtension::StateWork);
}

void TExtension::ReplyAndPassAway(NHttp::THttpOutgoingResponsePtr httpResponse) {
    Send(Context->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(std::move(httpResponse)));
    PassAway();
}

void TExtension::ReplyAndPassAway() {
    auto& params = Context->Params;
    if (params->StatusOverride) {
        return ReplyAndPassAway(params->Request->CreateResponse(params->StatusOverride, params->MessageOverride, *params->HeadersOverride, params->BodyOverride));
    } else {
        static constexpr size_t MAX_LOGGED_SIZE = 1024;
        BLOG_D("Can not process request to protected resource:\n" << Context->Params->Request->GetObfuscatedData().substr(0, MAX_LOGGED_SIZE));
        return ReplyAndPassAway(CreateResponseForNotExistingResponseFromProtectedResource(Context->Params->Request, Context->Params->ResponseError));
    }
}

void TExtension::ContinueAndPassAway() {
    if (!Context->Route.empty()) {
        const auto route = Context->Route.Next();
        Send(route, new TEvPrivate::TEvExtensionRequest(std::move(Context)));
        PassAway();
    } else {
        ReplyAndPassAway();
    }
}

} // NMVP::NOIDC
