#pragma once

#include <ydb/mvp/core/mvp_log.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/security/util.h>
#include <library/cpp/string_utils/base64/base64.h>

namespace NMVP::NOIDC {

class TOidcHttpRequestHandlerBase : public IMvpLogContextProvider {
protected:
    NHttp::THttpIncomingRequestPtr Request;
    TMvpLogContext LogContext;

    explicit TOidcHttpRequestHandlerBase(const NHttp::THttpIncomingRequestPtr& request)
        : Request(request)
        , LogContext(CreateLogContext(Request))
    {}

    const TMvpLogContext* GetLogContext() const override {
        return &LogContext;
    }

    TStringBuf GetCookie(const NHttp::TCookies& cookies, const TString& cookieName) const {
        if (!cookies.Has(cookieName)) {
            return {};
        }
        TStringBuf cookieValue = cookies.Get(cookieName);
        if (!cookieValue.Empty()) {
            BLOG_D_CTX("Using cookie (" << cookieName << ": " << NKikimr::MaskTicket(cookieValue) << ")");
        }
        return cookieValue;
    }

    TString DecodeToken(const TStringBuf& cookie) const {
        TString token;
        try {
            Base64StrictDecode(cookie, token);
        } catch (std::exception& e) {
            BLOG_D_CTX("Base64Decode " << NKikimr::MaskTicket(cookie) << " cookie: " << e.what());
            token.clear();
        }
        return token;
    }
};

} // namespace NMVP::NOIDC
