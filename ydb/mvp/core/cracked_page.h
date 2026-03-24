#pragma once

#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <vector>

namespace NMVP {

struct TCrackedPage {
    const TString Url;
    const TString Scheme;
    const TString Host;
    const TString Uri;

    explicit TCrackedPage(TStringBuf url);
    TCrackedPage(const NHttp::THttpIncomingRequestPtr& request);

    bool IsParsed() const;
    bool IsSecureScheme() const;
    bool IsGrpcSchemeAllowed() const;
    bool IsHttpSchemeAllowed() const;
    bool IsRequestedHostAllowed(const std::vector<TString>& allowedProxyHosts) const;
    bool TryGetHostAndPort(TStringBuf& scheme, TStringBuf& host, ui16& port) const;

private:
    enum class ESchemeKind {
        Empty,
        Http,
        Https,
        Grpc,
        Grpcs,
        Unknown,
    };

    const bool Parsed;
    const ESchemeKind SchemeKind;

    struct TParsedValues {
        TString Url;
        TString Scheme;
        TString Host;
        TString Uri;
        bool Parsed = false;
    };

    static TParsedValues Parse(TStringBuf url);
    static ESchemeKind ParseScheme(TStringBuf scheme);
    explicit TCrackedPage(TParsedValues&& parsedValues);
};

} // namespace NMVP
