#include "oidc_cookie.h"
#include "oidc_settings.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

using namespace NMVP::NOIDC;

namespace {

const TString TEST_SECRET = "0123456789abcdef";

TString MakeToken(size_t index, size_t paddingSize = 0) {
    return TStringBuilder()
        << "token-" << index
        << "-" << TString(paddingSize, 'x');
}

} // namespace

Y_UNIT_TEST_SUITE(OidcCookie) {
    Y_UNIT_TEST(AuthFlowCookieRestoresExactToken) {
        const TString token = MakeToken(1);
        const TString cookieValue = UpdateAuthFlowCookieValue("", TEST_SECRET, token);

        UNIT_ASSERT(HasAuthFlowCookieNonce(cookieValue, TEST_SECRET, token));
    }

    Y_UNIT_TEST(AuthFlowCookieKeepsParallelTokens) {
        const TString firstToken = MakeToken(1);
        const TString secondToken = MakeToken(2);

        TString cookieValue = UpdateAuthFlowCookieValue("", TEST_SECRET, firstToken);
        cookieValue = UpdateAuthFlowCookieValue(cookieValue, TEST_SECRET, secondToken);

        UNIT_ASSERT(HasAuthFlowCookieNonce(cookieValue, TEST_SECRET, firstToken));
        UNIT_ASSERT(HasAuthFlowCookieNonce(cookieValue, TEST_SECRET, secondToken));
    }

    Y_UNIT_TEST(AuthFlowCookieRemovesUsedTokenOnly) {
        const TString firstToken = MakeToken(1);
        const TString secondToken = MakeToken(2);

        TString cookieValue = UpdateAuthFlowCookieValue("", TEST_SECRET, firstToken);
        cookieValue = UpdateAuthFlowCookieValue(cookieValue, TEST_SECRET, secondToken);
        cookieValue = RemoveAuthFlowCookieNonce(cookieValue, TEST_SECRET, firstToken);

        UNIT_ASSERT(!HasAuthFlowCookieNonce(cookieValue, TEST_SECRET, firstToken));
        UNIT_ASSERT(HasAuthFlowCookieNonce(cookieValue, TEST_SECRET, secondToken));
    }

    Y_UNIT_TEST(AuthFlowCookieEvictsOldEntriesToFitSizeLimit) {
        TString cookieValue;
        TString lastToken;

        for (size_t index = 0; index < 24; ++index) {
            lastToken = MakeToken(index, 300);
            cookieValue = UpdateAuthFlowCookieValue(cookieValue, TEST_SECRET, lastToken);
        }

        UNIT_ASSERT(cookieValue.size() <= TOpenIdConnectSettings::MAX_AUTH_START_COOKIE_VALUE_SIZE);
        UNIT_ASSERT(HasAuthFlowCookieNonce(cookieValue, TEST_SECRET, lastToken));
    }
}
