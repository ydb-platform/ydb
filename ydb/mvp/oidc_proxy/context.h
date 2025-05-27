#pragma once

#include <util/generic/string.h>
#include <util/generic/ptr.h>

namespace NHttp {

class THttpIncomingRequest;
using THttpIncomingRequestPtr = TIntrusivePtr<THttpIncomingRequest>;

}

namespace NMVP::NOIDC {

class TContext {
public:
    struct TInitializer {
        TString State;
        TString RequestedAddress;
        bool AjaxRequest = false;
    };

private:
    TString State;
    bool AjaxRequest = false;
    TString RequestedAddress;

public:
    TContext() = default;
    TContext(const TInitializer& initializer);
    TContext(const NHttp::THttpIncomingRequestPtr& request);

    TString GetState(const TString& key) const;
    bool IsAjaxRequest() const;
    TString GetRequestedAddress() const;

    TString CreateYdbOidcCookie(const TString& secret) const;

private:
    static TString GenerateState();
    static bool DetectAjaxRequest(const NHttp::THttpIncomingRequestPtr& request);
    static TStringBuf GetRequestedUrl(const NHttp::THttpIncomingRequestPtr& request, bool isAjaxRequest);

    TString GenerateCookie(const TString& key) const;
};

} // NMVP::NOIDC
