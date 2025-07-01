#pragma once

#include "extension.h"

namespace NMVP::NOIDC {

class TExtensionFinal : public TExtension, public TExtensionWorker {
public:
    TExtensionFinal(const TOpenIdConnectSettings& settings)
        : TExtensionWorker(settings)
    {}
    void Execute(TIntrusivePtr<TExtensionContext> ctx) override;

private:
    TString FixReferenceInHtml(TStringBuf html, TStringBuf host, TStringBuf findStr);
    TString FixReferenceInHtml(TStringBuf html, TStringBuf host);
    void SetProxyResponseHeaders();
    void SetProxyResponseBody();
    TString GetFixedLocationHeader(TStringBuf location);
};
} // NMVP::NOIDC
