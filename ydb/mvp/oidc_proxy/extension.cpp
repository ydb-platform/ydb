#include "extension.h"

namespace NMVP::NOIDC {

void TExtensionWorker::Reply(NHttp::THttpOutgoingResponsePtr httpResponse) {
    NActors::TActivationContext::ActorSystem()->Send(Context->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(std::move(httpResponse)));
}

void TExtensionWorker::Reply() {
    auto& params = Context->Params;
    if (params->StatusOverride) {
        return Reply(params->Request->CreateResponse(params->StatusOverride, params->MessageOverride, *params->HeadersOverride, params->BodyOverride));
    } else {
        static constexpr size_t MAX_LOGGED_SIZE = 1024;
        BLOG_D("Can not process request to protected resource:\n" << Context->Params->Request->GetObfuscatedData().substr(0, MAX_LOGGED_SIZE));
        return Reply(CreateResponseForNotExistingResponseFromProtectedResource(Context->Params->Request, Context->Params->ResponseError));
    }
}

void TExtensionWorker::Continue() {
    const auto step = Context->Steps.Next();
    if (step) {
        step->Execute(std::move(Context));
    } else {
        Reply();
    }
}

std::unique_ptr<TExtension> TExtensionsSteps::Next() {
    if (empty()) {
        return nullptr;
    }
    auto target = std::move(front());
    pop();
    return target;
}

} // NMVP::NOIDC
