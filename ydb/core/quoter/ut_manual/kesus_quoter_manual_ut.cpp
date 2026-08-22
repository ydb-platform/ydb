#include "quoter_service.h"
#include "kesus_quoter_proxy.h"
#include "ut_helpers.h"

namespace NKikimr {

Y_UNIT_TEST_SUITE(QuoterWithKesusTest) {
    Y_UNIT_TEST(PrefetchCoefficient) {
        TKesusQuoterTestSetup setup;
        NKikimrKesus::THierarchicalDRRResourceConfig cfg;
        double rate = 100.0;
        double prefetch = 1000.0;
        cfg.SetMaxUnitsPerSecond(rate);
        cfg.SetPrefetchCoefficient(prefetch);
        setup.CreateKesusResource(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, "root", cfg);

        cfg.ClearMaxUnitsPerSecond();
        cfg.ClearPrefetchCoefficient(); // should be inherited from root
        setup.CreateKesusResource(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, "root/leaf", cfg);

        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, "root/leaf"); // stabilization
        // Consume exactly both ticks' worth: rate * prefetch * 2 = 200,000
        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, "root/leaf", rate * prefetch * 2, TDuration::Seconds(10));
        // Channel is now exhausted and Balance = 0. This must timeout.
        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, "root/leaf", 1, TDuration::MilliSeconds(500), TEvQuota::TEvClearance::EResult::Deadline);
    }
}

} // namespace NKikimr
