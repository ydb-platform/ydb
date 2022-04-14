#include "queue_pool.h"

#include <library/cpp/testing/unittest/registar.h>

#include <unordered_map>

namespace NPersQueue {
Y_UNIT_TEST_SUITE(TQueuePoolTest) {
    Y_UNIT_TEST(QueuesDistribution) {
        TQueuePool pool;
        pool.Start(10);
        size_t addresses[10000] = {};
        std::unordered_map<IThreadPool*, size_t> queueAddresses;
        for (size_t& address : addresses) {
            IThreadPool* q = &pool.GetQueue(&address);
            ++queueAddresses[q];
            UNIT_ASSERT_EQUAL(q, &pool.GetQueue(&address)); // one address always leads to one queue
        }
        UNIT_ASSERT_VALUES_EQUAL(queueAddresses.size(), 10);
        for (const auto& queueToCount : queueAddresses) {
            UNIT_ASSERT_C(queueToCount.second >= 850, "Count: " << queueToCount.second);
            UNIT_ASSERT_C(queueToCount.second <= 1150, "Count: " << queueToCount.second);
        }
    }
}
} // namespace NPersQueue
