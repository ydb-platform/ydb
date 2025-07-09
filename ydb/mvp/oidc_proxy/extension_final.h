#pragma once

#include "extension.h"

namespace NMVP::NOIDC {

class TExtensionFinal : public TExtension {
private:
    using TBase = TExtension;

public:
    TExtensionFinal(const TOpenIdConnectSettings& settings)
        : TBase(settings)
    {}
    void Handle(TEvPrivate::TEvExtensionRequest::TPtr event) override;

private:
    TString FixReferenceInHtml(TStringBuf html, TStringBuf host, TStringBuf findStr);
    TString FixReferenceInHtml(TStringBuf html, TStringBuf host);
    void SetProxyResponseHeaders();
    void SetProxyResponseBody();
    TString GetFixedLocationHeader(TStringBuf location);
};
} // NMVP::NOIDC
