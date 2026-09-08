#include <ydb/library/yql/dq/common/dq_resource_quoter.h>

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <thread>
#include <vector>

namespace NYql::NDq {

Y_UNIT_TEST_SUITE(TDqResourceQuoterTest) {

    Y_UNIT_TEST(Totals) {
        TResourceQuoter quoter(1000);
        UNIT_ASSERT_VALUES_EQUAL(quoter.GetLimit(), 1000);
        UNIT_ASSERT_VALUES_EQUAL(quoter.GetAllocatedTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(quoter.GetFreeTotal(), 1000);

        UNIT_ASSERT(quoter.Allocate(TTxId{ui64(1)}, 1, 300));
        UNIT_ASSERT_VALUES_EQUAL(quoter.GetAllocatedTotal(), 300);
        UNIT_ASSERT_VALUES_EQUAL(quoter.GetFreeTotal(), 700);
        UNIT_ASSERT_VALUES_EQUAL(quoter.GetAllocated(TTxId{ui64(1)}, 1), 300);

        UNIT_ASSERT(!quoter.Allocate(TTxId{ui64(1)}, 1, 701)); // over the limit, nothing changes
        UNIT_ASSERT_VALUES_EQUAL(quoter.GetFreeTotal(), 700);
        UNIT_ASSERT(quoter.Allocate(TTxId{ui64(2)}, 7, 700));
        UNIT_ASSERT_VALUES_EQUAL(quoter.GetFreeTotal(), 0);

        quoter.Free(TTxId{ui64(1)}, 1, 300);
        UNIT_ASSERT_VALUES_EQUAL(quoter.GetFreeTotal(), 300);
        quoter.Free(TTxId{ui64(2)}, 7); // the whole task
        UNIT_ASSERT_VALUES_EQUAL(quoter.GetAllocatedTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(quoter.GetFreeTotal(), 1000);
    }

    // Limit == 0 never refuses; the free total reads as 0 then and the callers treat that limit as unlimited
    Y_UNIT_TEST(UnlimitedQuoter) {
        TResourceQuoter quoter(0);
        UNIT_ASSERT(quoter.Allocate(TTxId{ui64(1)}, 1, ui64(1) << 40));
        UNIT_ASSERT_VALUES_EQUAL(quoter.GetAllocatedTotal(), ui64(1) << 40);
        UNIT_ASSERT_VALUES_EQUAL(quoter.GetFreeTotal(), 0);
    }

    // The totals are read without the lock: concurrent allocations and releases must never make them
    // exceed the limit and must leave them exact at the end
    Y_UNIT_TEST(ConcurrentTotals) {
        constexpr ui64 limit = 1000;
        constexpr size_t threadCount = 8;
        constexpr size_t iterations = 20000;
        TResourceQuoter quoter(limit);
        std::atomic<bool> inconsistent{false};

        std::vector<std::thread> threads;
        for (size_t i = 0; i < threadCount; ++i) {
            threads.emplace_back([&, txId = ui64(i + 1)] {
                for (size_t j = 0; j < iterations; ++j) {
                    const ui64 size = 1 + j % 7;
                    if (quoter.Allocate(TTxId{txId}, 0, size)) {
                        if (quoter.GetAllocatedTotal() > limit || quoter.GetFreeTotal() > limit) {
                            inconsistent = true;
                        }
                        quoter.Free(TTxId{txId}, 0, size);
                    }
                }
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }

        UNIT_ASSERT(!inconsistent);
        UNIT_ASSERT_VALUES_EQUAL(quoter.GetAllocatedTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(quoter.GetFreeTotal(), limit);
    }
}

} // namespace NYql::NDq
