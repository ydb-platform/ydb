#include "auth_token_fetcher.h"

#include <ydb/library/actors/http/http.h>

/*
TString TRequest::GetAuthToken(const NHttp::THeaders& headers) const {
    NHttp::TCookies cookies(headers["Cookie"]);
    TStringBuf authorization = headers["Authorization"];
    if (!authorization.empty()) {
        TStringBuf scheme = authorization.NextTok(' ');
        if (scheme == "OAuth" || scheme == "Bearer") {
            return TString(authorization);
        }
    }
    TStringBuf subjectToken = headers["x-yacloud-subjecttoken"];
    if (!subjectToken.empty()) {
        return TString(subjectToken);
    }
    TStringBuf sessionId = cookies["Session_id"];
    if (!sessionId.empty()) {
        // TODO: check later
        // return BlackBoxTokenFromSessionId(sessionId);
        return TString();
    }
    return TString();
}
*/

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
        NHttp::TCookies cookies(headers["Cookie"]);

        if (TStringBuf authorization = headers["Authorization"]; !authorization.empty()) {
            TStringBuf scheme = authorization.NextTok(' ');
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