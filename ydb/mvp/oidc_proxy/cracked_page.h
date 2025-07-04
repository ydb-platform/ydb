#pragma once

#include "oidc_settings.h"
#include <ydb/library/actors/http/http_proxy.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NMVP::NOIDC {

struct TCrackedPage {
    TString Url;
    TString Scheme;
    TString Host;
    TString Uri;

    bool Parsed = false;
    bool SchemeValid = false;

    explicit TCrackedPage(TStringBuf url);
    TCrackedPage(const NHttp::THttpIncomingRequestPtr& url);
    bool IsSchemeValid() const;
    bool CheckRequestedHost(const TOpenIdConnectSettings& settings) const;
};

} // NMVP::NOIDC
