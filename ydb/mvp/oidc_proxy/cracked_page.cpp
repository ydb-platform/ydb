#include "cracked_page.h"

#include <ydb/core/util/wildcard.h>
#include <ydb/library/security/util.h>

namespace NMVP::NOIDC {

TCrackedPage::TCrackedPage(TStringBuf url)
    : Url(url)
{
    TStringBuf scheme;
    TStringBuf host;
    TStringBuf uri;
    Parsed = NHttp::CrackURL(Url, scheme, host, uri);
    if (Parsed) {
        SchemeValid = Scheme.empty() || Scheme == "http" || Scheme == "https";
        Scheme = scheme;
        Host = host;
        Uri = uri;
    }
}

TCrackedPage::TCrackedPage(const NHttp::THttpIncomingRequestPtr& request)
    : TCrackedPage(request->URL.SubStr(1))
{}

bool TCrackedPage::IsSchemeValid() const {
    return SchemeValid;
}

bool TCrackedPage::CheckRequestedHost(const TOpenIdConnectSettings& settings) const {
    if (!IsSchemeValid()) {
        return false;
    }
    return std::any_of(settings.AllowedProxyHosts.begin(), settings.AllowedProxyHosts.end(),
        [this](const TString& pattern) {
            return NKikimr::IsMatchesWildcard(Host, pattern);
        });
}

} // NMVP::NOIDC
