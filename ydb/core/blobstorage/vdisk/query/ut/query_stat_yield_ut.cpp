#include <ydb/core/blobstorage/vdisk/query/query_stat_yield.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace {

    class TManualMonotonicTimeProvider : public NMonotonic::IMonotonicTimeProvider {
    public:
        TMonotonic Now() override {
            ++Calls;
            return CurrentTime;
        }

        void Advance(TDuration duration) {
            CurrentTime += duration;
        }

        ui32 GetCalls() const {
            return Calls;
        }

    private:
        TMonotonic CurrentTime = TMonotonic::Zero();
        ui32 Calls = 0;
    };

    Y_UNIT_TEST_SUITE(TDbStatYieldCheckerTest) {
        Y_UNIT_TEST(YieldPolicy) {
            auto timeProvider = MakeIntrusive<TManualMonotonicTimeProvider>();
            TDbStatYieldChecker checker(
                TDbStatYieldPolicy{
                    .StepsBeforeMeasures = 3,
                    .QuantumDuration = TDuration::MilliSeconds(10),
                    .DelayBetweenQuanta = TDuration::MilliSeconds(100),
                },
                timeProvider);

            timeProvider->Advance(TDuration::MilliSeconds(11));
            UNIT_ASSERT(!checker.StepAndCheckForYield());
            UNIT_ASSERT(!checker.StepAndCheckForYield());
            UNIT_ASSERT(checker.StepAndCheckForYield());

            // The elapsed time must exceed QuantumDuration, and the clock is
            // checked only after another StepsBeforeMeasures records.
            timeProvider->Advance(TDuration::MilliSeconds(10));
            UNIT_ASSERT(!checker.StepAndCheckForYield());
            UNIT_ASSERT(!checker.StepAndCheckForYield());
            UNIT_ASSERT(!checker.StepAndCheckForYield());

            timeProvider->Advance(TDuration::MilliSeconds(1));
            UNIT_ASSERT(!checker.StepAndCheckForYield());
            UNIT_ASSERT(!checker.StepAndCheckForYield());
            UNIT_ASSERT(checker.StepAndCheckForYield());
        }

        Y_UNIT_TEST(UsesInjectedMonotonicTimeProvider) {
            auto timeProvider = MakeIntrusive<TManualMonotonicTimeProvider>();
            TDbStatYieldChecker checker(
                TDbStatYieldPolicy{
                    .StepsBeforeMeasures = 2,
                    .QuantumDuration = TDuration::MilliSeconds(50),
                    .DelayBetweenQuanta = TDuration::MilliSeconds(100),
                },
                timeProvider);

            UNIT_ASSERT_VALUES_EQUAL(timeProvider->GetCalls(), 1);
            UNIT_ASSERT(!checker.StepAndCheckForYield());

            timeProvider->Advance(TDuration::MilliSeconds(51));
            UNIT_ASSERT(checker.StepAndCheckForYield());
            UNIT_ASSERT_VALUES_EQUAL(timeProvider->GetCalls(), 2);

            UNIT_ASSERT(!checker.StepAndCheckForYield());
            UNIT_ASSERT(!checker.StepAndCheckForYield());
            UNIT_ASSERT_VALUES_EQUAL(timeProvider->GetCalls(), 3);
        }

        Y_UNIT_TEST(DoesNotNeedTimeProviderWhenYieldingIsDisabled) {
            TDbStatYieldChecker checker(std::nullopt, {});

            UNIT_ASSERT(!checker.StepAndCheckForYield());
        }
    }

} // anonymous namespace
} // NKikimr
