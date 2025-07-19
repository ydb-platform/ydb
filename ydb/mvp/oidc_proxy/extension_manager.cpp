#include "extension_manager.h"

#include "extension_final.h"
#include "extension_whoami.h"

namespace NMVP::NOIDC {

TExtensionManager::TExtensionManager(const TActorId sender,
                                     const TOpenIdConnectSettings& settings,
                                     const TCrackedPage& protectedPage,
                                     const TString authHeader)
    : Settings(settings)
    , AuthHeader(std::move(authHeader))
{
    ExtensionCtx = MakeIntrusive<TExtensionContext>();
    ExtensionCtx->Params = MakeHolder<TProxiedResponseParams>();
    ExtensionCtx->Params->ProtectedPage = MakeHolder<TCrackedPage>(protectedPage);
    ExtensionCtx->Sender = sender;
}

void TExtensionManager::SetRequest(NHttp::THttpIncomingRequestPtr request) {
    ExtensionCtx->Params->Request = std::move(request);
}

void TExtensionManager::SetOverrideResponse(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
    ExtensionCtx->Params->HeadersOverride = MakeHolder<NHttp::THeadersBuilder>();
    ExtensionCtx->Params->ResponseError = event ? event->Get()->GetError() : "Timeout while waiting info";

    if (!event || !event->Get()->Response)
        return;

    auto& response = event->Get()->Response;
    ExtensionCtx->Params->StatusOverride = response->Status;
    auto headers = NHttp::THeaders(response->Headers);
    for (const auto& header : headers.Headers) {
        ExtensionCtx->Params->HeadersOverride->Set(header.first, header.second);
    }
    ExtensionCtx->Params->MessageOverride = response->Message;
    ExtensionCtx->Params->BodyOverride = response->Body;
}

void TExtensionManager::AddExtensionWhoami() {
    EnrichmentExtension = true;
    AddExtension(NActors::TActivationContext::ActorSystem()->Register(new TExtensionWhoami(Settings, AuthHeader)));
}

void TExtensionManager::AddExtensionFinal() {
    AddExtension(NActors::TActivationContext::ActorSystem()->Register(new TExtensionFinal(Settings)));
}

void TExtensionManager::AddExtension(const NActors::TActorId& stage) {
    ExtensionCtx->Route.push(stage);
}

bool TExtensionManager::NeedExtensionWhoami(const NHttp::THttpIncomingRequestPtr& request) const {
    if (Settings.AccessServiceType == NMvp::yandex_v2) {
        return false; // does not support whoami extension
    }

    if (request->Method == "OPTIONS" || Settings.WhoamiExtendedInfoEndpoint.empty()) {
        return false;
    }

    TCrackedPage page(request);
    auto path = TStringBuf(page.Url).Before('?');

    static constexpr TStringBuf WHOAMI_PATHS[] = { "/viewer/json/whoami", "/viewer/whoami" };
    for (const auto& whoamiPath : WHOAMI_PATHS) {
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

bool TExtensionManager::HasEnrichmentExtension() {
    return EnrichmentExtension;
}

void TExtensionManager::StartExtensionProcess(NHttp::THttpIncomingRequestPtr request,
                                              NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
    SetRequest(std::move(request));
    SetOverrideResponse(std::move(event));

    const auto route = ExtensionCtx->Route.Next();
    NActors::TActivationContext::ActorSystem()->Send(route, new TEvPrivate::TEvExtensionRequest(std::move(ExtensionCtx)));
}

} // NMVP::NOIDC
