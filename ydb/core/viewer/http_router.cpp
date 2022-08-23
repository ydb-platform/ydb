#include "http_router.h"

namespace NKikimr {
namespace NViewer {

namespace {
    bool MatchPath(TStringBuf pathPattern, TStringBuf path, std::map<TString, TString>& pathParams) {
        const char delim = '/';
        pathPattern.SkipPrefix("/"sv);
        path.SkipPrefix("/"sv);

        TStringBuf pathPatternComponent = pathPattern.NextTok(delim);
        TStringBuf pathComponent = path.NextTok(delim);
        while (pathPatternComponent && pathComponent) {
            if (pathPatternComponent.StartsWith('{') && pathPatternComponent.EndsWith('}')) {
                TStringBuf paramName = pathPatternComponent.SubString(1, pathPatternComponent.Size() - 2);
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
}

void THttpRequestRouter::RegisterHandler(HTTP_METHOD method, const TString& pathPattern, TJsonHandlerBase::TPtr handler) {
    Data.emplace(std::make_pair(method, pathPattern), std::move(handler));
}

std::optional<THandlerWithParams> THttpRequestRouter::ResolveHandler(HTTP_METHOD method, const TStringBuf& path) const {
    auto it = Data.find(std::pair<HTTP_METHOD, TString>(method, path));
    if (it != Data.end()) {
        return THandlerWithParams(it->second, {});
    }

    for (const auto& [k ,v] : Data) {
        if (k.first != method) {
            continue;
        }
        std::map<TString, TString> pathParams;
        if (MatchPath(k.second, path, pathParams)) {
            return THandlerWithParams(v, pathParams);
        }
    }
    return {};
}

size_t THttpRequestRouter::GetSize() const {
    return Data.size();
}

}
}
