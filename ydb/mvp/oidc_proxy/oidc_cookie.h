#pragma once

#include <util/generic/string.h>

namespace NMVP::NOIDC {

struct TFindRequestedAddressInOidcCookieResult {
    bool IsSuccess = false;
    TString RequestedAddress;
    TString ErrorMessage;
};

TString CreateFlowId(TStringBuf key, TStringBuf requestedAddress);
TString CreateSharedOidcCookieName();
TString CreateSharedOidcCookie(const TString& key, TStringBuf currentCookieValue, TStringBuf requestedAddress);
bool HasSharedOidcCookieEntry(TStringBuf cookieValue, const TString& key, TStringBuf flowId, TStringBuf requestedAddress);
TString UpdateSharedOidcCookieValue(TStringBuf currentCookieValue, const TString& key, TStringBuf flowId, TStringBuf requestedAddress);
TFindRequestedAddressInOidcCookieResult FindRequestedAddressInSharedOidcCookieValue(TStringBuf cookieValue, const TString& key, TStringBuf flowId);

} // NMVP::NOIDC
