#pragma once

#include "extension.h"

namespace NMVP::NOIDC {

struct TExtensionManager {
    TIntrusivePtr<TExtensionContext> ExtensionCtx;
    const TOpenIdConnectSettings Settings;
    TString AuthHeader;
    TDuration Timeout;

public:
    TExtensionManager(const TActorId sender,
                      const TOpenIdConnectSettings& settings,
                      const TCrackedPage& protectedPage,
                      const TString authHeader);
    void SetExtensionTimeout(TDuration timeout);
    void ArrangeExtensions(const NHttp::THttpIncomingRequestPtr& request);
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
