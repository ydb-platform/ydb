#include "extension.h"

namespace NMVP::NOIDC {

void TExtensionContext::Reply(NHttp::THttpOutgoingResponsePtr httpResponse) {
    const auto request = httpResponse ? httpResponse->GetRequest() : nullptr;
    BLOG_D("TExtensionContext::Reply rid=" << GetRequestIdForLogs(request)
        << " status=" << (httpResponse ? httpResponse->Status : TStringBuf("-"))
        << " message=" << (httpResponse ? httpResponse->Message : TStringBuf("-"))
        << " is_done=" << (httpResponse && httpResponse->IsDone() ? 1 : 0)
        << " is_need_body=" << (httpResponse && httpResponse->IsNeedBody() ? 1 : 0));
    NActors::TActivationContext::Send(Sender, std::make_unique<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(std::move(httpResponse)));
}

void TExtensionContext::Reply() {
    if (!Params.StatusOverride.empty()) {
        BLOG_D("TExtensionContext::Reply override rid=" << GetRequestIdForLogs(Params.Request)
            << " status=" << Params.StatusOverride
            << " message=" << Params.MessageOverride
            << " body_size=" << Params.BodyOverride.size());
        return Reply(Params.Request->CreateResponse(Params.StatusOverride, Params.MessageOverride, *Params.HeadersOverride, Params.BodyOverride));
    } else {
        BLOG_D("TExtensionContext::Reply override_missing rid=" << GetRequestIdForLogs(Params.Request)
            << " error=" << Params.ResponseError);
        return Reply(CreateResponseForNotExistingResponseFromProtectedResource(Params.Request, Params.ResponseError));
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
