#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/core/harmonizer/types.h>
#include <ydb/library/actors/core/harmonizer/harmonizer.h>
#include <ydb/library/actors/core/executor_pool.h>
#include <ydb/library/actors/core/executor_pool_shared.h>
#include <ydb/library/actors/core/executor_thread_ctx.h>
#include <ydb/library/actors/helpers/pool_stats_collector.h>

using namespace NActors;

Y_UNIT_TEST_SUITE(THarmonizerIterationTests) {

    Y_UNIT_TEST(TestHarmonizerCreationAndInvokeReadHistory) {
        ui64 currentTs = 1000000;
        std::unique_ptr<IHarmonizer> harmonizer(MakeHarmonizer(currentTs));
        UNIT_ASSERT(harmonizer != nullptr);
        harmonizer->InvokeReadHistory([](const TIterableDoubleRange<THarmonizerIterationState>& history) {
            ui16 count = 0;
            for (const auto& _ : history) {
                ++count;
            }
            UNIT_ASSERT_VALUES_EQUAL(count, 0);
        });

        currentTs += 10000000;
        harmonizer->Harmonize(currentTs);
        harmonizer->InvokeReadHistory([](const TIterableDoubleRange<THarmonizerIterationState>& history) {
            ui16 count = 0;
            for (const auto& _ : history) {
                ++count;
            }
            UNIT_ASSERT_VALUES_EQUAL(count, 1);
        });
    }

    Y_UNIT_TEST(TestHarmonizerIterationStatsToNeedyNextToHoggish) {
        ui64 currentTs = 1000000;
        std::unique_ptr<IHarmonizer> harmonizer(MakeHarmonizer(currentTs));
        UNIT_ASSERT(harmonizer != nullptr);
        currentTs += 10000000;
        harmonizer->Harmonize(currentTs);
        harmonizer->InvokeReadHistory([](const TIterableDoubleRange<THarmonizerIterationState>& history) {
            ui16 count = 0;
            for (const auto& _ : history) {
                ++count;
            }
            UNIT_ASSERT_VALUES_EQUAL(count, 1);
        });
    }

}
