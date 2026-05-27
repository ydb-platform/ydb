#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NMVP::NOIDC {

TString CreateAuthFlowCookie(TStringBuf cookieValue);
TString ClearAuthFlowCookie();
TString UpdateAuthFlowCookieValue(TStringBuf currentCookieValue,
                                  const TString& key,
                                  TStringBuf antiForgeryToken,
                                  TStringBuf requestedAddress = "");
bool HasAuthFlowCookieNonce(TStringBuf cookieValue, const TString& key, TStringBuf antiForgeryToken);
TString RemoveAuthFlowCookieNonce(TStringBuf currentCookieValue, const TString& key, TStringBuf antiForgeryToken);
TString RestoreRequestedAddressFromAuthFlowCookie(TStringBuf cookieValue, const TString& key);

} // NMVP::NOIDC
