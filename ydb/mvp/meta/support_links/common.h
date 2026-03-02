#pragma once

#include <ydb/mvp/meta/mvp.h>

#include <library/cpp/string_utils/quote/quote.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NMVP::NSupportLinks {

inline constexpr const char* SOURCE_GRAFANA_DASHBOARD = "grafana/dashboard";
inline constexpr const char* SOURCE_GRAFANA_DASHBOARD_SEARCH = "grafana/dashboard/search";
inline constexpr const char* SOURCE_META = "meta";
inline constexpr const char* INVALID_IDENTITY_PARAMS_MESSAGE =
    "Invalid identity parameters. Supported entities: cluster requires 'cluster'; database requires 'cluster' and 'database'.";

struct TResolvedLink {
    TString Title;
    TString Url;
};

struct TSupportError {
    TString Source;
    TMaybe<ui32> Status;
    TString Reason;
    TString Message;
};

struct TLinkResolveContext {
    size_t Place = 0;
    TString SourceName;
    TSupportLinkEntryConfig LinkConfig;
    THashMap<TString, TString> ClusterColumns;
    TVector<std::pair<TString, TString>> QueryParams;
    NActors::TActorId Parent;
    NActors::TActorId HttpProxyId;
};

struct TResolverValidationContext {
    const TSupportLinkEntryConfig& LinkConfig;
    const TGrafanaSupportConfig& GrafanaConfig;
    TStringBuf Where;
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
