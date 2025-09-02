#pragma once

#include "extension.h"

namespace NMVP::NOIDC {

class TExtensionFinal : public IExtension {
private:
    const TOpenIdConnectSettings Settings;
    TIntrusivePtr<TExtensionContext> Context;

public:
    TExtensionFinal(const TOpenIdConnectSettings& settings)
        : Settings(settings)
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
