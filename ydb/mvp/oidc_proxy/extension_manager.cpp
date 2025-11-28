#include "extension_manager.h"

#include "extension_final.h"
#include "extension_whoami.h"

namespace NMVP::NOIDC {

using namespace NActors;

TExtensionManager::TExtensionManager(const TActorId sender,
                                     const TOpenIdConnectSettings& settings,
                                     const TCrackedPage& protectedPage,
                                     const TString authHeader)
    : ExtensionCtx(MakeIntrusive<TExtensionContext>())
    , Settings(settings)
    , AuthHeader(std::move(authHeader))
    , Timeout(settings.DefaultRequestTimeout)
{
    ExtensionCtx->Params.ProtectedPage = MakeHolder<TCrackedPage>(protectedPage);
    ExtensionCtx->Sender = sender;
}

void TExtensionManager::SetExtensionTimeout(TDuration timeout) {
    Timeout = timeout;
}

void TExtensionManager::SetRequest(NHttp::THttpIncomingRequestPtr request) {
    ExtensionCtx->Params.Request = std::move(request);
}

void TExtensionManager::SetOverrideResponse(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
    auto& params = ExtensionCtx->Params;
    params.HeadersOverride = MakeHolder<NHttp::THeadersBuilder>();
    params.ResponseError = event->Get()->GetError();

    auto response = std::move(event->Get()->Response);
    if (!response)
        return;

    params.StatusOverride = response->Status;
    auto headers = NHttp::THeaders(response->Headers);
    for (const auto& header : headers.Headers) {
        params.HeadersOverride->Set(header.first, header.second);
    }
    params.MessageOverride = response->Message;
    params.BodyOverride = response->Body;
}

void TExtensionManager::AddExtensionWhoami() {
    auto ext = std::make_unique<TExtensionWhoami>(Settings, AuthHeader, Timeout);
    AddExtension(std::move(ext));
}

void TExtensionManager::AddExtensionFinal() {
    auto ext = std::make_unique<TExtensionFinal>(Settings);
    AddExtension(std::move(ext));
}

void TExtensionManager::AddExtension(std::unique_ptr<IExtension> ext) {
    ExtensionCtx->Steps.push(std::move(ext));
}

bool TExtensionManager::NeedExtensionWhoami(const NHttp::THttpIncomingRequestPtr& request) const {
    if (!Settings.EnabledExtensionWhoami() || request->Method == "OPTIONS") {
        return false;
    }

    TCrackedPage page(request);
    auto path = TStringBuf(page.Url).Before('?');

    for (const auto& whoamiPath : Settings.WHOAMI_PATHS) {
        if (path.EndsWith(whoamiPath)) {
            return true;
        }
    }
    return false;
}

void TExtensionManager::ArrangeExtensions(const NHttp::THttpIncomingRequestPtr& request) {
    if (NeedExtensionWhoami(request)) {
        AddExtensionWhoami();
    }
    AddExtensionFinal();
}

void TExtensionManager::StartExtensionProcess(NHttp::THttpIncomingRequestPtr request,
                                              NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
    SetRequest(std::move(request));
    SetOverrideResponse(std::move(event));

    const auto step = ExtensionCtx->Steps.Next();
    step->Execute(std::move(ExtensionCtx));
}

} // NMVP::NOIDC
