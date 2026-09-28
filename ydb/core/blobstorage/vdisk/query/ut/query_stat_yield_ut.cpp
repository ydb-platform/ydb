#include <ydb/core/blobstorage/vdisk/query/query_stat_yield.h>

#include "query_stat_test_utils.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace {

    using NDbStatTest::TManualMonotonicTimeProvider;

    Y_UNIT_TEST_SUITE(TDbStatYieldCheckerTest) {
        Y_UNIT_TEST(ChecksTimeOnlyAtConfiguredSteps) {
            auto timeProvider = MakeIntrusive<TManualMonotonicTimeProvider>();
            TDbStatYieldChecker checker(
                TDbStatYieldPolicy{
                    .StepsBeforeMeasures = 3,
                    .QuantumDuration = TDuration::MilliSeconds(10),
                    .DelayBetweenQuanta = TDuration::Zero(),
                },
                timeProvider);

            timeProvider->Advance(TDuration::MilliSeconds(11));
            UNIT_ASSERT(!checker.StepAndCheckForYield());
            UNIT_ASSERT(!checker.StepAndCheckForYield());
            UNIT_ASSERT(checker.StepAndCheckForYield());
            UNIT_ASSERT_VALUES_EQUAL(timeProvider->GetCalls(), 2);

            timeProvider->Advance(TDuration::MilliSeconds(10));
            UNIT_ASSERT(!checker.StepAndCheckForYield());
            UNIT_ASSERT(!checker.StepAndCheckForYield());
            UNIT_ASSERT(!checker.StepAndCheckForYield());
            UNIT_ASSERT_VALUES_EQUAL(timeProvider->GetCalls(), 3);
        }

        Y_UNIT_TEST(DisabledPolicyDoesNotNeedTimeProvider) {
            TDbStatYieldChecker checker(std::nullopt);
            UNIT_ASSERT(!checker.StepAndCheckForYield());
        }
    }

} // anonymous namespace
} // namespace NKikimr
