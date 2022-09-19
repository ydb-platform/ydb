#pragma once

#include "http_handler.h"

namespace NKikimr::NPublicHttp {

struct THandlerWithParams {
    THandlerWithParams(TString pathPattern, THttpHandler handler, std::map<TString, TString> pathParams)
      : PathPattern(std::move(pathPattern))
      , Handler(std::move(handler))
      , PathParams(std::move(pathParams))
    {
    }

    THandlerWithParams() = default;
    THandlerWithParams(THandlerWithParams&&) = default;
    THandlerWithParams& operator=(const THandlerWithParams&) = default;
    THandlerWithParams& operator=(const THandlerWithParams&&) = default;

    TString PathPattern;
    THttpHandler Handler;
    std::map<TString, TString> PathParams;
};

class THttpRequestRouter {
public:
    void RegisterHandler(TStringBuf method, const TString& pathPattern, THttpHandler handler);
    void RegisterHandler(HTTP_METHOD method, const TString& pathPattern, THttpHandler handler);
    void RegisterGetHandler(const TString& pathPattern, THttpHandler handler) {
        RegisterHandler(HTTP_METHOD_GET, pathPattern, handler);
    }

    std::optional<THandlerWithParams> ResolveHandler(HTTP_METHOD method, const TStringBuf& path) const;
    std::optional<THandlerWithParams> ResolveHandler(TStringBuf method, const TStringBuf& path) const;
    size_t GetSize() const;

    template <typename F>
    void ForEach(F f) const {
        for (const auto& [p, handler] : Data) {
            f(p.first, p.second, handler);
        }
    }

private:
    std::map<std::pair<HTTP_METHOD, TString>, THttpHandler> Data;
};

} // namespace NKikimr::NPublicHttp
