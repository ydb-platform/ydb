#pragma once

#include <library/cpp/string_utils/quote/quote.h>

#include <util/string/builder.h>

namespace NMVP::NSupportLinks {

inline constexpr TStringBuf SOURCE_META = "meta";

inline bool IsAbsoluteUrl(TStringBuf url) {
    return url.StartsWith("http://") || url.StartsWith("https://");
}

inline TString JoinUrl(TStringBuf endpoint, TStringBuf path) {
    const bool endpointHasSlash = endpoint.EndsWith('/');
    const bool pathHasSlash = path.StartsWith('/');
    if (endpointHasSlash && pathHasSlash) {
        return TString(endpoint.SubStr(0, endpoint.size() - 1)) + TString(path);
    }
    if (!endpointHasSlash && !pathHasSlash) {
        return TString(endpoint) + "/" + TString(path);
    }
    return TString(endpoint) + TString(path);
}

inline TString AppendQueryParam(const TString& url, TStringBuf key, TStringBuf value) {
    TStringBuilder result;
    result << url << (url.Contains('?') ? '&' : '?') << key << "=" << CGIEscapeRet(value);
    return result;
}

} // namespace NMVP::NSupportLinks
