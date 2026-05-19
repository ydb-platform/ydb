#include "auth_flow_context.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/defaults.h>

namespace NMVP::NOIDC {

namespace {

void AssertRequestedAddress(TAuthFlowContextStore& store, const TString& flowId, TStringBuf expectedRequestedAddress) {
    const auto requestedAddress = store.Find(flowId);
    UNIT_ASSERT(requestedAddress);
    UNIT_ASSERT_VALUES_EQUAL(*requestedAddress, expectedRequestedAddress);
}

} // namespace

Y_UNIT_TEST_SUITE(TAuthFlowContextStore) {
    Y_UNIT_TEST(SaveReturnsFlowIdForRequestedAddress) {
        TAuthFlowContextStore store(TDuration::Minutes(1));
        const TString flowId = store.Save("/viewer");

        UNIT_ASSERT(!flowId.empty());
        AssertRequestedAddress(store, flowId, "/viewer");
    }

    Y_UNIT_TEST(SaveReturnsDifferentFlowIdsForDifferentRequestedAddresses) {
        TAuthFlowContextStore store(TDuration::Minutes(1));
        const TString firstFlowId = store.Save("/viewer/first");
        const TString secondFlowId = store.Save("/viewer/second");

        UNIT_ASSERT(!firstFlowId.empty());
        UNIT_ASSERT(!secondFlowId.empty());
        UNIT_ASSERT_UNEQUAL(firstFlowId, secondFlowId);
        AssertRequestedAddress(store, firstFlowId, "/viewer/first");
        AssertRequestedAddress(store, secondFlowId, "/viewer/second");
    }

    Y_UNIT_TEST(SaveRenewsExpirationForSameRequestedAddress) {
        TAuthFlowContextStore store(TDuration::MilliSeconds(100));
        const TString firstFlowId = store.Save("/viewer");

        Sleep(TDuration::MilliSeconds(50));

        const TString secondFlowId = store.Save("/viewer");
        UNIT_ASSERT_VALUES_EQUAL(firstFlowId, secondFlowId);

        Sleep(TDuration::MilliSeconds(60));
        AssertRequestedAddress(store, firstFlowId, "/viewer");
    }

    Y_UNIT_TEST(FindReturnsNothingForExpiredFlow) {
        TAuthFlowContextStore store(TDuration::Seconds(0));
        const TString flowId = store.Save("/viewer");

        UNIT_ASSERT(!flowId.empty());
        UNIT_ASSERT(!store.Find(flowId));
    }

    Y_UNIT_TEST(SaveReturnsNewFlowIdForExpiredRequestedAddress) {
        TAuthFlowContextStore store(TDuration::MilliSeconds(10));
        const TString firstFlowId = store.Save("/viewer");

        Sleep(TDuration::MilliSeconds(15));

        const TString secondFlowId = store.Save("/viewer");

        UNIT_ASSERT(!firstFlowId.empty());
        UNIT_ASSERT(!secondFlowId.empty());
        UNIT_ASSERT_UNEQUAL(firstFlowId, secondFlowId);
        UNIT_ASSERT(!store.Find(firstFlowId));
        AssertRequestedAddress(store, secondFlowId, "/viewer");
    }
}

} // NMVP::NOIDC
