#pragma once

#include "extension.h"

namespace NMVP::NOIDC {

struct TExtensionManager {
    TIntrusivePtr<TExtensionContext> ExtensionCtx;
    const TOpenIdConnectSettings Settings;
    TString AuthHeader;
    bool EnrichmentExtension = false;

public:
    TExtensionManager(const TActorId sender,
                      const TOpenIdConnectSettings& settings,
                      const TCrackedPage& protectedPage,
                      const TString authHeader);
    void ArrangeExtensions(const NHttp::THttpIncomingRequestPtr& request);
    bool HasEnrichmentExtension();
    void StartExtensionProcess(NHttp::THttpIncomingRequestPtr request,
                               NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event = nullptr);

private:
    void SetRequest(NHttp::THttpIncomingRequestPtr request);
    void SetOverrideResponse(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event);
    bool NeedExtensionWhoami(const NHttp::THttpIncomingRequestPtr& request) const;
    void AddExtensionWhoami();
    void AddExtensionFinal();
    void AddExtension(std::unique_ptr<IExtension> ext);
};

} // NMVP::NOIDC
