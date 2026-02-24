#include "cracked_page.h"

#include <ydb/core/util/wildcard.h>
#include <ydb/library/actors/http/http.h>

#include <library/cpp/string_utils/url/url.h>

#include <util/string/ascii.h>

#include <algorithm>

namespace NMVP {
namespace {

enum class ESchemeKind {
    Empty,
    Http,
    Https,
    Grpc,
    Grpcs,
    Unknown,
};

ESchemeKind DetectSchemeKind(TStringBuf scheme) {
    const TString normalized = to_lower(TString(scheme));
    if (normalized.empty()) {
        return ESchemeKind::Empty;
    }
    if (normalized == "http") {
        return ESchemeKind::Http;
    }
    if (normalized == "https") {
        return ESchemeKind::Https;
    }
    if (normalized == "grpc") {
        return ESchemeKind::Grpc;
    }
    if (normalized == "grpcs") {
        return ESchemeKind::Grpcs;
    }
    return ESchemeKind::Unknown;
}

bool IsSecureSchemeKind(ESchemeKind kind) {
    return kind == ESchemeKind::Https || kind == ESchemeKind::Grpcs;
}

bool IsGrpcFamilyScheme(ESchemeKind kind) {
    return kind == ESchemeKind::Empty || kind == ESchemeKind::Grpc || kind == ESchemeKind::Grpcs;
}

bool IsHttpFamilyScheme(ESchemeKind kind) {
    return kind == ESchemeKind::Empty || kind == ESchemeKind::Http || kind == ESchemeKind::Https;
}

} // namespace

TCrackedPage::TParsedValues TCrackedPage::Parse(TStringBuf url) {
    TParsedValues parsedValues;
    parsedValues.Url = url;

    TStringBuf scheme;
    TStringBuf host;
    TStringBuf uri;
    parsedValues.Parsed = NHttp::CrackURL(parsedValues.Url, scheme, host, uri);
    if (parsedValues.Parsed) {
        parsedValues.Scheme = scheme;
        parsedValues.Host = host;
        parsedValues.Uri = uri;
    }

    return parsedValues;
}

TCrackedPage::TCrackedPage(TParsedValues&& parsedValues)
    : Url(std::move(parsedValues.Url))
    , Scheme(std::move(parsedValues.Scheme))
    , Host(std::move(parsedValues.Host))
    , Uri(std::move(parsedValues.Uri))
    , Parsed(parsedValues.Parsed)
{}

TCrackedPage::TCrackedPage(TStringBuf url)
    : TCrackedPage(Parse(url))
{}

TCrackedPage::TCrackedPage(const NHttp::THttpIncomingRequestPtr& request)
    : TCrackedPage(request->URL.SubStr(1))
{}

bool TCrackedPage::IsParsed() const {
    return Parsed;
}

bool TCrackedPage::IsSecureScheme() const {
    if (!Parsed) {
        return false;
    }
    return IsSecureSchemeKind(DetectSchemeKind(Scheme));
}

bool TCrackedPage::IsSchemeAllowed() const {
    return IsGrpcSchemeAllowed() || IsHttpSchemeAllowed();
}

bool TCrackedPage::IsGrpcSchemeAllowed() const {
    if (!Parsed) {
        return false;
    }
    return IsGrpcFamilyScheme(DetectSchemeKind(Scheme));
}

bool TCrackedPage::IsHttpSchemeAllowed() const {
    if (!Parsed) {
        return false;
    }
    return IsHttpFamilyScheme(DetectSchemeKind(Scheme));
}

bool TCrackedPage::IsRequestedHostAllowed(const std::vector<TString>& allowedProxyHosts) const {
    if (!IsParsed()) {
        return false;
    }
    return std::any_of(allowedProxyHosts.begin(), allowedProxyHosts.end(),
        [this](const TString& pattern) {
            return NKikimr::IsMatchesWildcard(Host, pattern);
        });
}

bool TCrackedPage::TryGetHostAndPort(TStringBuf& scheme, TStringBuf& host, ui16& port) const {
    return TryGetSchemeHostAndPort(Url, scheme, host, port);
}

} // namespace NMVP
