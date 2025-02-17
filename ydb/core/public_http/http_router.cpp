#include "http_router.h"

namespace NKikimr::NPublicHttp {

namespace {
    bool MatchPath(TStringBuf pathPattern, TStringBuf path, std::map<TString, TString>& pathParams) {
        const char delim = '/';
        pathPattern.SkipPrefix("/"sv);
        path.SkipPrefix("/"sv);

        TStringBuf pathPatternComponent = pathPattern.NextTok(delim);
        TStringBuf pathComponent = path.NextTok(delim);
        while (pathPatternComponent && pathComponent) {
            if (pathPatternComponent.StartsWith('{') && pathPatternComponent.EndsWith('}')) {
                TStringBuf paramName = pathPatternComponent.SubString(1, pathPatternComponent.size() - 2);
                pathParams.emplace(paramName, pathComponent);
            } else {
                if (pathPatternComponent != pathComponent) {
                    return false;
                }
            }
            pathPatternComponent = pathPattern.NextTok(delim);
            pathComponent = path.NextTok(delim);
        }

        return !pathPattern && !path && !pathPatternComponent && !pathComponent;
    }

    HTTP_METHOD ParseMethod(TStringBuf method) {
        if (method == "GET"sv) {
            return HTTP_METHOD_GET;
        }
        if (method == "POST"sv) {
            return HTTP_METHOD_POST;
        }
        if (method == "PUT"sv) {
            return HTTP_METHOD_PUT;
        }
        if (method == "DELETE"sv) {
            return HTTP_METHOD_DELETE;
        }
        if (method == "OPTIONS"sv) {
            return HTTP_METHOD_OPTIONS;
        }
        if (method == "HEAD"sv) {
            return HTTP_METHOD_HEAD;
        }
        return HTTP_METHOD_UNDEFINED;
    }
}

void THttpRequestRouter::RegisterHandler(TStringBuf method, const TString& pathPattern, THttpHandler handler) {
    RegisterHandler(ParseMethod(method), pathPattern, handler);
}

void THttpRequestRouter::RegisterHandler(HTTP_METHOD method, const TString& pathPattern, THttpHandler handler) {
    Data.emplace(std::make_pair(method, pathPattern), std::move(handler));
}

std::optional<THandlerWithParams> THttpRequestRouter::ResolveHandler(TStringBuf method, const TStringBuf& path) const {
    return ResolveHandler(ParseMethod(method), path);
}

std::optional<THandlerWithParams> THttpRequestRouter::ResolveHandler(HTTP_METHOD method, const TStringBuf& path) const {
    auto it = Data.find(std::pair<HTTP_METHOD, TString>(method, path));
    if (it != Data.end()) {
        return THandlerWithParams(it->first.second, it->second, {});
    }

    for (const auto& [k ,v] : Data) {
        if (k.first != method) {
            continue;
        }
        std::map<TString, TString> pathParams;
        if (MatchPath(k.second, path, pathParams)) {
            return THandlerWithParams(k.second, v, pathParams);
        }
    }
    return {};
}

size_t THttpRequestRouter::GetSize() const {
    return Data.size();
}

} // namespace NKikimr::NPublicHttp
