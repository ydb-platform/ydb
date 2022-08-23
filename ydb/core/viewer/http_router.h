#pragma once

#include "http_handler.h"

namespace NKikimr {
namespace NViewer {

struct THandlerWithParams {
    THandlerWithParams(TJsonHandlerBase::TPtr handler, std::map<TString, TString> pathParams)
      : Handler(std::move(handler))
      , PathParams(std::move(pathParams))
    {
    }

    THandlerWithParams() = default;
    THandlerWithParams(THandlerWithParams&&) = default;
    THandlerWithParams& operator=(const THandlerWithParams&) = default;
    THandlerWithParams& operator=(const THandlerWithParams&&) = default;

    TJsonHandlerBase::TPtr Handler;
    std::map<TString, TString> PathParams;
};

class THttpRequestRouter {
public:
    void RegisterHandler(HTTP_METHOD method, const TString& pathPattern, TJsonHandlerBase::TPtr handler);
    void RegisterGetHandler(const TString& pathPattern, TJsonHandlerBase::TPtr handler) {
        RegisterHandler(HTTP_METHOD_GET, pathPattern, handler);
    }

    std::optional<THandlerWithParams> ResolveHandler(HTTP_METHOD method, const TStringBuf& path) const;
    size_t GetSize() const;

    template <typename F>
    void ForEach(F f) const {
        for (const auto& [p, handler] : Data) {
            f(p.first, p.second, handler);
        }
    }

private:
    std::map<std::pair<HTTP_METHOD, TString>, TJsonHandlerBase::TPtr> Data;
};

}
}
