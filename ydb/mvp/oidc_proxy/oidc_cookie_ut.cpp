#include "oidc_cookie.h"
#include "oidc_settings.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

using namespace NMVP::NOIDC;

namespace {

const TString TEST_SECRET = "0123456789abcdef";

TString MakeRequestedAddress(size_t index, size_t paddingSize = 0) {
    return TStringBuilder()
        << "https://site.example.com/path?index=" << index
        << "&padding=" << TString(paddingSize, 'x');
}

} // namespace

Y_UNIT_TEST_SUITE(OidcCookie) {
    Y_UNIT_TEST(CreateFlowIdIsDeterministicForSameAddress) {
        const TString requestedAddress = "https://site.example.com/path?x=1";
        const TString flowId1 = CreateFlowId(TEST_SECRET, requestedAddress);
        const TString flowId2 = CreateFlowId(TEST_SECRET, requestedAddress);

        UNIT_ASSERT_STRINGS_EQUAL(flowId1, flowId2);
        UNIT_ASSERT_VALUES_EQUAL(flowId1.size(), 10);
    }

    Y_UNIT_TEST(CreateFlowIdDiffersForDifferentAddresses) {
        const TString flowId1 = CreateFlowId(TEST_SECRET, "https://site.example.com/path?x=1");
        const TString flowId2 = CreateFlowId(TEST_SECRET, "https://site.example.com/path?x=2");

        UNIT_ASSERT_UNEQUAL(flowId1, flowId2);
    }

    Y_UNIT_TEST(SharedOidcCookieRestoresExactRequestedAddress) {
        const TString requestedAddress = MakeRequestedAddress(1);
        const TString flowId = CreateFlowId(TEST_SECRET, requestedAddress);
        const TString cookieValue = UpdateSharedOidcCookieValue("", TEST_SECRET, flowId, requestedAddress);

        const TFindRequestedAddressInOidcCookieResult result =
            FindRequestedAddressInSharedOidcCookieValue(cookieValue, TEST_SECRET, flowId);

        UNIT_ASSERT(result.IsSuccess);
        UNIT_ASSERT_STRINGS_EQUAL(result.RequestedAddress, requestedAddress);
    }

    Y_UNIT_TEST(SharedOidcCookieFallsBackToMostRecentRequestedAddress) {
        const TString firstRequestedAddress = MakeRequestedAddress(1);
        const TString secondRequestedAddress = MakeRequestedAddress(2);
        const TString firstFlowId = CreateFlowId(TEST_SECRET, firstRequestedAddress);
        const TString secondFlowId = CreateFlowId(TEST_SECRET, secondRequestedAddress);

        TString cookieValue = UpdateSharedOidcCookieValue("", TEST_SECRET, firstFlowId, firstRequestedAddress);
        cookieValue = UpdateSharedOidcCookieValue(cookieValue, TEST_SECRET, secondFlowId, secondRequestedAddress);

        const TFindRequestedAddressInOidcCookieResult result =
            FindRequestedAddressInSharedOidcCookieValue(cookieValue, TEST_SECRET, "missing-flow-id");

        UNIT_ASSERT(result.IsSuccess);
        UNIT_ASSERT_STRINGS_EQUAL(result.RequestedAddress, secondRequestedAddress);
    }

    Y_UNIT_TEST(SharedOidcCookieEvictsOldEntriesToFitSizeLimit) {
        TString cookieValue;
        TString lastFlowId;
        TString lastRequestedAddress;

        for (size_t index = 0; index < 12; ++index) {
            lastRequestedAddress = MakeRequestedAddress(index, 500);
            lastFlowId = CreateFlowId(TEST_SECRET, lastRequestedAddress);
            cookieValue = UpdateSharedOidcCookieValue(cookieValue, TEST_SECRET, lastFlowId, lastRequestedAddress);
        }

        UNIT_ASSERT(cookieValue.size() <= TOpenIdConnectSettings::MAX_AUTH_FLOW_COOKIE_VALUE_SIZE);

        const TFindRequestedAddressInOidcCookieResult result =
            FindRequestedAddressInSharedOidcCookieValue(cookieValue, TEST_SECRET, lastFlowId);

        UNIT_ASSERT(result.IsSuccess);
        UNIT_ASSERT_STRINGS_EQUAL(result.RequestedAddress, lastRequestedAddress);
    }
}
