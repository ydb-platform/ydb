#include "counters.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSecurity {

Y_UNIT_TEST_SUITE(TCountersUtilTest) {

    Y_UNIT_TEST(GetCountersForTicketParserReturnsNonNull) {
        const auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
        const auto counters = GetCountersForTicketParser(root);
        UNIT_ASSERT(counters);
    }

    Y_UNIT_TEST(GetCountersForTicketParserCreatesTicketParserSubgroup) {
        const auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
        GetCountersForTicketParser(root);

        const auto authCounters = root->FindSubgroup("counters", "auth");
        UNIT_ASSERT_C(authCounters, "Expected 'counters/auth' subgroup to be created");

        const auto ticketParserCounters = authCounters->FindSubgroup("subsystem", "TicketParser");
        UNIT_ASSERT_C(ticketParserCounters, "Expected 'subsystem/TicketParser' subgroup to be created");
    }

    Y_UNIT_TEST(GetCountersForTicketParserCanBeUsedToCreateMetrics) {
        const auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
        const auto ticketParser = GetCountersForTicketParser(root);

        const auto counter = ticketParser->GetCounter("TestCounter", true);
        UNIT_ASSERT(counter);

        counter->Set(42);
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), 42);
    }

    Y_UNIT_TEST(GetCountersForExternalIdpProviderReturnsNonNull) {
        const auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
        const auto counters = GetCountersForExternalIdpProvider(root);
        UNIT_ASSERT(counters);
    }

    Y_UNIT_TEST(GetCountersForExternalIdpProviderCreatesFullHierarchy) {
        const auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
        GetCountersForExternalIdpProvider(root);

        const auto authCounters = root->FindSubgroup("counters", "auth");
        UNIT_ASSERT_C(authCounters, "Expected 'counters/auth' subgroup to be created");

        const auto ticketParserCounters = authCounters->FindSubgroup("subsystem", "TicketParser");
        UNIT_ASSERT_C(ticketParserCounters, "Expected 'subsystem/TicketParser' subgroup to be created");

        const auto externalIdpCounters = ticketParserCounters->FindSubgroup("component", "ExternalIdpProvider");
        UNIT_ASSERT_C(externalIdpCounters, "Expected 'component/ExternalIdpProvider' subgroup to be created");
    }

    Y_UNIT_TEST(GetCountersForExternalIdpProviderCanBeUsedToCreateMetrics) {
        const auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
        const auto externalIdp = GetCountersForExternalIdpProvider(root);

        auto counter = externalIdp->GetCounter("TestCounter", true);
        UNIT_ASSERT(counter);
        counter->Set(42);
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), 42);
    }

}

} // namespace NKikimr::NSecurity
