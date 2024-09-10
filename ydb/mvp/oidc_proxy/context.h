#pragma once

#include <util/generic/string.h>
#include <util/generic/ptr.h>
#include <util/datetime/base.h>

namespace NHttp {

class THttpIncomingRequest;
using THttpIncomingRequestPtr = TIntrusivePtr<THttpIncomingRequest>;

}

namespace NMVP {
namespace NOIDC {

class TContext {
public:
    static const TDuration STATE_LIFE_TIME;

private:
    TString State;
    bool IsAjaxRequest = false;
    TString RequestedAddress;

public:
    TContext(const TString& state = "", const TString& requestedAddress = "", bool isAjaxRequest = false);
    TContext(const NHttp::THttpIncomingRequestPtr& request);

    TString GetState() const;
    bool GetIsAjaxRequest() const;
    TString GetRequestedAddress() const;

    TString CreateYdbOidcCookie(const TString& secret) const;

private:
    static TString GenerateState();
    static bool DetectAjaxRequest(const NHttp::THttpIncomingRequestPtr& request);
    static TStringBuf GetRequestedUrl(const NHttp::THttpIncomingRequestPtr& request, bool isAjaxRequest);

    TString GenerateCookie(const TString& secret) const;
};

class TContextRecord {
private:
    TContext Context;
    TInstant ExpirationTime;

public:
    TContextRecord(const TContext& context);

    TContext GetContext() const;
    TInstant GetExpirationTime() const;
};

} // NOIDC
} // NMVP
