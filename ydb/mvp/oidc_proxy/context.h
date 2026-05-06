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
    };

private:
    TString State;
    bool NavigationRequest = true;
    TString RequestedAddress;

public:
    TContext() = default;
    TContext(const TInitializer& initializer);
    TContext(const NHttp::THttpIncomingRequestPtr& request);

    TString GetState(const TString& key) const;
    bool IsNavigationRequest() const;
    TString GetRequestedAddress() const;

    TString CreateAuthFlowCookie(const TString& secret) const;

private:
    static bool IsPageNavigationRequest(const NHttp::THttpIncomingRequestPtr& request);
    static TStringBuf GetRequestedUrl(const NHttp::THttpIncomingRequestPtr& request, bool isNavigationRequest);

    TString CreateAuthFlowCookieValue(const TString& key) const;
};

} // NMVP::NOIDC
