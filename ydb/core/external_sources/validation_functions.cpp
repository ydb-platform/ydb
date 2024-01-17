#include "external_source.h"
#include "validation_functions.h"

#include <ydb/library/actors/http/http.h>


namespace NKikimr::NExternalSource {

void ValidateHostname(const std::vector<TRegExMatch>& hostnamePatterns, const TString& url) {
    if (hostnamePatterns.empty()) {
        return;
    }

    TStringBuf scheme;
    TStringBuf address;
    TStringBuf uri;
    NHttp::CrackURL(url, scheme, address, uri);

    TString hostname;
    TIpPort port;
    NHttp::CrackAddress(TString(address), hostname, port);

    for (const TRegExMatch& pattern : hostnamePatterns) {
        if (pattern.Match(hostname.c_str())) {
            return;
        }
    }

    ythrow TExternalSourceException() << "It is not allowed to access hostname '" << hostname << "'";
}

}  // NKikimr::NExternalSource
