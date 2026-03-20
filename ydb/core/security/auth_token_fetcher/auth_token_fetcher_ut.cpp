#include "auth_token_fetcher.h"

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/actors/http/http.h>

namespace NKikimr::NSecurity {

Y_UNIT_TEST_SUITE(TAuthTokenFetcherStubTest) {
    Y_UNIT_TEST(ReturnsEmptyTokenForEmptyHeaders) {
        auto fetcher = CreateAuthTokenFetcherStub();
        NHttp::THeadersBuilder headers;
        UNIT_ASSERT_EQUAL(fetcher->GetAuthToken(headers), "");
    }

    Y_UNIT_TEST(ReturnsEmptyTokenForOAuthHeader) {
        auto fetcher = CreateAuthTokenFetcherStub();
        NHttp::THeadersBuilder headers(
            {{"Authorization", "OAuth mytoken123"}});
        UNIT_ASSERT_EQUAL(fetcher->GetAuthToken(headers), "");
    }

    Y_UNIT_TEST(ReturnsEmptyTokenForBearerHeader) {
        auto fetcher = CreateAuthTokenFetcherStub();
        NHttp::THeadersBuilder headers(
            {{"Authorization", "Bearer mytoken123"}});
        UNIT_ASSERT_EQUAL(fetcher->GetAuthToken(headers), "");
    }
}

Y_UNIT_TEST_SUITE(TAuthTokenFetcherDefaultTest) {
    Y_UNIT_TEST(ReturnsEmptyTokenForEmptyHeaders) {
        auto fetcher = CreateAuthTokenFetcherDefault();
        NHttp::THeadersBuilder headers;
        UNIT_ASSERT_EQUAL(fetcher->GetAuthToken(headers), "");
    }

    Y_UNIT_TEST(ExtractsTokenFromOAuthHeader) {
        auto fetcher = CreateAuthTokenFetcherDefault();
        NHttp::THeadersBuilder headers(
            {{"Authorization", "OAuth mytoken123"}});
        UNIT_ASSERT_EQUAL(fetcher->GetAuthToken(headers), "mytoken123");
    }

    Y_UNIT_TEST(ExtractsTokenFromBearerHeader) {
        auto fetcher = CreateAuthTokenFetcherDefault();
        NHttp::THeadersBuilder headers(
            {{"Authorization", "Bearer mytoken456"}});
        UNIT_ASSERT_EQUAL(fetcher->GetAuthToken(headers), "mytoken456");
    }

    Y_UNIT_TEST(ReturnsEmptyTokenForUnknownScheme) {
        auto fetcher = CreateAuthTokenFetcherDefault();
        NHttp::THeadersBuilder headers(
            {{"Authorization", "Basic dXNlcjpwYXNz"}});
        UNIT_ASSERT_EQUAL(fetcher->GetAuthToken(headers), "");
    }

    Y_UNIT_TEST(ReturnsEmptyTokenForEmptyAuthorizationHeader) {
        auto fetcher = CreateAuthTokenFetcherDefault();
        NHttp::THeadersBuilder headers(
            {{"Authorization", ""}});
        UNIT_ASSERT_EQUAL(fetcher->GetAuthToken(headers), "");
    }

    Y_UNIT_TEST(ReturnsEmptyTokenWhenNoAuthorizationHeader) {
        auto fetcher = CreateAuthTokenFetcherDefault();
        NHttp::THeadersBuilder headers(
            {{"Cookie", "session=abc123"}});
        UNIT_ASSERT_EQUAL(fetcher->GetAuthToken(headers), "");
    }

    Y_UNIT_TEST(ExtractsTokenWithSpecialCharacters) {
        auto fetcher = CreateAuthTokenFetcherDefault();
        NHttp::THeadersBuilder headers(
            {{"Authorization", "OAuth token-with.special_chars=123"}});
        UNIT_ASSERT_EQUAL(
            fetcher->GetAuthToken(headers),
            "token-with.special_chars=123");
    }

    Y_UNIT_TEST(OAuthSchemeIsCaseSensitive) {
        auto fetcher = CreateAuthTokenFetcherDefault();
        NHttp::THeadersBuilder headers(
            {{"Authorization", "oauth mytoken123"}});
        UNIT_ASSERT_EQUAL(fetcher->GetAuthToken(headers), "");
    }

    Y_UNIT_TEST(BearerSchemeIsCaseSensitive) {
        auto fetcher = CreateAuthTokenFetcherDefault();
        NHttp::THeadersBuilder headers(
            {{"Authorization", "bearer mytoken123"}});
        UNIT_ASSERT_EQUAL(fetcher->GetAuthToken(headers), "");
    }
}

}  // namespace NKikimr::NSecurity
