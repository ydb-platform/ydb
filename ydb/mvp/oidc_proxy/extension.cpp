#include "extension.h"

namespace NMVP::NOIDC {

void TExtensionContext::Reply(NHttp::THttpOutgoingResponsePtr httpResponse) {
    NActors::TActivationContext::Send(Sender, std::make_unique<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(std::move(httpResponse)));
}

void TExtensionContext::Reply() {
    if (Params->StatusOverride) {
        return Reply(Params->Request->CreateResponse(Params->StatusOverride, Params->MessageOverride, *Params->HeadersOverride, Params->BodyOverride));
    } else {
        static constexpr size_t MAX_LOGGED_SIZE = 1024;
        BLOG_D("Can not process request to protected resource:\n" << Params->Request->GetObfuscatedData().substr(0, MAX_LOGGED_SIZE));
        return Reply(CreateResponseForNotExistingResponseFromProtectedResource(Params->Request, Params->ResponseError));
    }
}

void TExtensionContext::Continue() {
    const auto step = Steps.Next();
    if (step) {
        step->Execute(this);
    } else {
        Reply();
    }
}

std::unique_ptr<IExtension> TExtensionsSteps::Next() {
    if (empty()) {
        return nullptr;
    }
    auto target = std::move(front());
    pop();
    return target;
}

} // NMVP::NOIDC
