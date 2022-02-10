#include "counters.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/set.h>
#include <util/thread/pool.h>

using namespace NMonitoring;

Y_UNIT_TEST_SUITE(TDeprecatedCountersTest) {
    Y_UNIT_TEST(CounterGroupsAreThreadSafe) { 
        const static ui32 GROUPS_COUNT = 1000;
        const static ui32 THREADS_COUNT = 10;

        TDeprecatedCounterGroups<ui32, ui32> groups;

        auto adder = [&groups]() {
            for (ui32 id = 0; id < GROUPS_COUNT; id++) {
                groups.Get(id);

                // adds contention
                ::NanoSleep(42);
            }
        };

        TThreadPool q;
        q.Start(THREADS_COUNT);
        for (ui32 i = 0; i < THREADS_COUNT; i++) {
            q.SafeAddFunc(adder);
        }
        q.Stop();

        // each group id is present
        for (ui32 id = 0; id < GROUPS_COUNT; id++) {
            UNIT_ASSERT(groups.Has(id));
        }

        // group names contains only appropriate ids
        auto ids = groups.GetGroupsNames();
        for (ui32 id : *ids) {
            UNIT_ASSERT(id < GROUPS_COUNT);
        }

        // no duplication in group names
        TSet<ui32> uniqueIds(ids->begin(), ids->end());
        UNIT_ASSERT_EQUAL(ids->size(), uniqueIds.size());
        UNIT_ASSERT_EQUAL(ids->size(), GROUPS_COUNT);
    }
}
