#include "auth_callback_context_store.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/defaults.h>

namespace NMVP::NOIDC {

static constexpr size_t DEFAULT_MAX_ENTRIES = 100000;

class TAuthCallbackContextStoreTestAccessor {
public:
    static size_t GetFlowCount(const TAuthCallbackContextStore& store) {
        return store.FlowRecordsById.size();
    }

    static size_t GetScheduledExpirationCount(const TAuthCallbackContextStore& store) {
        return store.FlowIdsByExpirationTime.size();
    }
};

namespace {

void AssertRequestedAddress(TAuthCallbackContextStore& store, const TString& flowId, TStringBuf expectedRequestedAddress) {
    const auto requestedAddress = store.Find(flowId);
    UNIT_ASSERT(requestedAddress);
    UNIT_ASSERT_VALUES_EQUAL(*requestedAddress, expectedRequestedAddress);
}

} // namespace

Y_UNIT_TEST_SUITE(TAuthCallbackContextStore) {
    Y_UNIT_TEST(SaveReturnsFlowIdForRequestedAddress) {
        TAuthCallbackContextStore store(TDuration::Minutes(1), DEFAULT_MAX_ENTRIES);
        const TString flowId = store.Save("/viewer");

        UNIT_ASSERT(!flowId.empty());
        AssertRequestedAddress(store, flowId, "/viewer");
    }

    Y_UNIT_TEST(SaveReturnsDifferentFlowIdsForDifferentRequestedAddresses) {
        TAuthCallbackContextStore store(TDuration::Minutes(1), DEFAULT_MAX_ENTRIES);
        const TString firstFlowId = store.Save("/viewer/first");
        const TString secondFlowId = store.Save("/viewer/second");

        UNIT_ASSERT(!firstFlowId.empty());
        UNIT_ASSERT(!secondFlowId.empty());
        UNIT_ASSERT_UNEQUAL(firstFlowId, secondFlowId);
        AssertRequestedAddress(store, firstFlowId, "/viewer/first");
        AssertRequestedAddress(store, secondFlowId, "/viewer/second");
    }

    Y_UNIT_TEST(SaveRenewsExpirationForSameRequestedAddress) {
        TAuthCallbackContextStore store(TDuration::MilliSeconds(300), DEFAULT_MAX_ENTRIES);
        const TString firstFlowId = store.Save("/viewer");

        Sleep(TDuration::MilliSeconds(100));

        const TString secondFlowId = store.Save("/viewer");
        UNIT_ASSERT_VALUES_EQUAL(firstFlowId, secondFlowId);

        Sleep(TDuration::MilliSeconds(150));
        AssertRequestedAddress(store, firstFlowId, "/viewer");
    }

    Y_UNIT_TEST(SaveKeepsSingleScheduledExpirationForSameRequestedAddress) {
        TAuthCallbackContextStore store(TDuration::Minutes(1), DEFAULT_MAX_ENTRIES);
        const TString flowId = store.Save("/viewer");

        for (ui32 i = 0; i < 10; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(store.Save("/viewer"), flowId);
        }

        UNIT_ASSERT_VALUES_EQUAL(TAuthCallbackContextStoreTestAccessor::GetScheduledExpirationCount(store), 1);
    }

    Y_UNIT_TEST(FindReturnsNothingForExpiredFlow) {
        TAuthCallbackContextStore store(TDuration::Seconds(0), DEFAULT_MAX_ENTRIES);
        const TString flowId = store.Save("/viewer");

        UNIT_ASSERT(!flowId.empty());
        UNIT_ASSERT(!store.Find(flowId));
    }

    Y_UNIT_TEST(SaveReturnsNewFlowIdForExpiredRequestedAddress) {
        TAuthCallbackContextStore store(TDuration::MilliSeconds(50), DEFAULT_MAX_ENTRIES);
        const TString firstFlowId = store.Save("/viewer");

        Sleep(TDuration::MilliSeconds(100));

        const TString secondFlowId = store.Save("/viewer");

        UNIT_ASSERT(!firstFlowId.empty());
        UNIT_ASSERT(!secondFlowId.empty());
        UNIT_ASSERT_UNEQUAL(firstFlowId, secondFlowId);
        UNIT_ASSERT(!store.Find(firstFlowId));
        AssertRequestedAddress(store, secondFlowId, "/viewer");
    }

    Y_UNIT_TEST(SaveEvictsOldestFlowWhenMaxEntriesExceeded) {
        TAuthCallbackContextStore store(TDuration::Minutes(1), 2);
        const TString firstFlowId = store.Save("/viewer/first");
        const TString secondFlowId = store.Save("/viewer/second");
        const TString thirdFlowId = store.Save("/viewer/third");

        UNIT_ASSERT(!store.Find(firstFlowId));
        AssertRequestedAddress(store, secondFlowId, "/viewer/second");
        AssertRequestedAddress(store, thirdFlowId, "/viewer/third");
        UNIT_ASSERT_VALUES_EQUAL(TAuthCallbackContextStoreTestAccessor::GetFlowCount(store), 2);
        UNIT_ASSERT_VALUES_EQUAL(TAuthCallbackContextStoreTestAccessor::GetScheduledExpirationCount(store), 2);
    }
}

} // NMVP::NOIDC
