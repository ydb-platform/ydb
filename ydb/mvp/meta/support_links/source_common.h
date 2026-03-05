#pragma once

#include "types.h"

#include <ydb/mvp/meta/mvp.h>

#include <library/cpp/string_utils/quote/quote.h>

#include <util/generic/hash.h>

namespace NMVP::NSupportLinks {

struct TLinkResolveContext {
    size_t Place = 0;
    TString SourceName;
    TSupportLinkEntry LinkConfig;
    THashMap<TString, TString> ClusterColumns;
    TVector<std::pair<TString, TString>> QueryParams;
    NActors::TActorId Parent;
    NActors::TActorId HttpProxyId;
};

struct TResolverValidationContext {
    const TSupportLinkEntry& LinkConfig;
    const TGrafanaSupportConfig& GrafanaConfig;
};

inline bool HasQueryString(const TString& url) {
    return url.find('?') != TString::npos;
}

inline bool IsAbsoluteUrl(const TString& url) {
    return url.StartsWith("http://") || url.StartsWith("https://");
}

inline TString JoinUrl(TString endpoint, TString path) {
    while (endpoint.EndsWith('/')) {
        endpoint.pop_back();
    }
    if (!path.StartsWith('/')) {
        path = "/" + path;
    }
    return TStringBuilder() << endpoint << path;
}

inline TString AppendQueryParam(TString url, TStringBuf name, TStringBuf value) {
    TString escapedName(name);
    TString escapedValue(value);
    Quote(escapedName, "");
    Quote(escapedValue, "");

    return TStringBuilder()
        << url
        << (HasQueryString(url) ? "&" : "?")
        << escapedName
        << "="
        << escapedValue;
}

} // namespace NMVP::NSupportLinks
