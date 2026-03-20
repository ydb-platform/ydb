#include "auth_token_fetcher.h"

#include <ydb/library/actors/http/http.h>

namespace NKikimr::NSecurity {

////////////////////////////////////////////////////////////////////////////////

class TAuthTokenFetcherStub : public TAuthTokenFetcher {
public:
    TString GetAuthToken(const NHttp::THeaders& /* headers */) const override {
        return TString();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAuthTokenFetcherDefault : public TAuthTokenFetcher {
public:
    TString GetAuthToken(const NHttp::THeaders& headers) const override {
        const NHttp::TCookies cookies(headers["Cookie"]);

        TStringBuf authorization = headers["Authorization"];
        if (!authorization.empty()) {
            const TStringBuf scheme = authorization.NextTok(' ');
            if (scheme == "OAuth" || scheme == "Bearer") {
                return TString(authorization);
            }
        }

        return TString();
    }
};

////////////////////////////////////////////////////////////////////////////////

TAuthTokenFetcherPtr CreateAuthTokenFetcherStub() {
    return std::make_shared<TAuthTokenFetcherStub>();
}

TAuthTokenFetcherPtr CreateAuthTokenFetcherDefault() {
    return std::make_shared<TAuthTokenFetcherDefault>();
}

}  // namespace NKikimr::NSecurity