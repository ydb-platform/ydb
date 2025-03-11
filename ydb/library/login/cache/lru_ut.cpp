#include <library/cpp/testing/unittest/registar.h>
#include "lru.h"

Y_UNIT_TEST_SUITE(TCacheTest) {
    Y_UNIT_TEST(TestLruCache) {
        NLogin::TLruCache cache(2);
        const auto user1 = std::make_pair("user1", "pass1");
        const auto user2 = std::make_pair("user2", "pass2");
        UNIT_ASSERT_EQUAL(cache.Size(), 0);
        {
            const auto insertResult = cache.Insert(user1, true);
            UNIT_ASSERT_EQUAL(insertResult.second, true);
            UNIT_ASSERT(insertResult.first != cache.End());
        }
        {
            const auto insertResult = cache.Insert(user2, true);
            UNIT_ASSERT_EQUAL(insertResult.second, true);
            UNIT_ASSERT(insertResult.first != cache.End());
        }
        UNIT_ASSERT_EQUAL(cache.Size(), 2);
        {
            const auto it = cache.Find(user1);
            UNIT_ASSERT(it != cache.End());
            UNIT_ASSERT_EQUAL(it->second, true);
        }
        const auto user3 = std::make_pair("user3", "pass3");
        // user2 must be evicted
        {
            const auto insertResult = cache.Insert(user3, true);
            UNIT_ASSERT_EQUAL(insertResult.second, true);
        }
        UNIT_ASSERT_EQUAL(cache.Size(), 2);
        {
            const auto it = cache.Find(user2);
            UNIT_ASSERT(it == cache.End());
        }
        {
            const auto it = cache.Find(user3);
            UNIT_ASSERT(it != cache.End());
            UNIT_ASSERT_EQUAL(it->second, true);
        }
        const auto user4 = std::make_pair("user4", "pass4");
        // user1 must be evicted
        {
            const auto insertResult = cache.Insert(user4, true);
            UNIT_ASSERT_EQUAL(insertResult.second, true);
        }
        UNIT_ASSERT_EQUAL(cache.Size(), 2);
        {
            const auto it = cache.Find(user1);
            UNIT_ASSERT(it == cache.End());
        }
        {
            const auto it = cache.Find(user4);
            UNIT_ASSERT(it != cache.End());
            UNIT_ASSERT_EQUAL(it->second, true);
        }
        cache.Clear();
        UNIT_ASSERT_EQUAL(cache.Size(), 0);
    }

    Y_UNIT_TEST(TestLruCacheWithEmptyCapacity) {
        NLogin::TLruCache cache(0);
        const auto user1 = std::make_pair("user1", "pass1");
        const auto user2 = std::make_pair("user2", "pass2");
        UNIT_ASSERT_EQUAL(cache.Size(), 0);
        {
            const auto insertResult = cache.Insert(user1, true);
            UNIT_ASSERT_EQUAL(insertResult.second, false);
            UNIT_ASSERT(insertResult.first == cache.End());
        }
        {
            const auto insertResult = cache.Insert(user2, true);
            UNIT_ASSERT_EQUAL(insertResult.second, false);
            UNIT_ASSERT(insertResult.first == cache.End());
        }
    }
}
