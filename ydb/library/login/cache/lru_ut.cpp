#include <library/cpp/testing/unittest/registar.h>
#include "lru.h"

Y_UNIT_TEST_SUITE(TCacheTest) {
    Y_UNIT_TEST(TestLruCache) {
        NLogin::TLruCache cache(2);
        const NLogin::TLruCache::TKey user1 = {.User = "user1", .Password = "pass1", .Hash = "12345"};
        const NLogin::TLruCache::TKey user2 = {.User = "user2", .Password = "pass2", .Hash = "67890"};
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
        const NLogin::TLruCache::TKey user3 = {.User = "user3", .Password = "pass3", .Hash = "abcd"};
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
        const NLogin::TLruCache::TKey user4 = {.User = "user4", .Password = "pass4", .Hash = "fedcba"};
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
        const NLogin::TLruCache::TKey user1 = {.User = "user1", .Password = "pass1", .Hash = "12345"};
        const NLogin::TLruCache::TKey user2 = {.User = "user2", .Password = "pass2", .Hash = "67890"};
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

    Y_UNIT_TEST(TestLruCacheWithSameUserAndPassword) {
        NLogin::TLruCache cache(2);
        const NLogin::TLruCache::TKey key1 = {.User = "user1", .Password = "pass1", .Hash = "12345"};
        const NLogin::TLruCache::TKey key2 = {.User = "user1", .Password = "pass1", .Hash = "67890"};
        UNIT_ASSERT_EQUAL(cache.Size(), 0);
        {
            const auto insertResult = cache.Insert(key1, false); // Bad password
            UNIT_ASSERT_EQUAL(insertResult.second, true);
            UNIT_ASSERT(insertResult.first != cache.End());
        }
        {
            const auto insertResult = cache.Insert(key2, true); // Good password
            UNIT_ASSERT_EQUAL(insertResult.second, true);
            UNIT_ASSERT(insertResult.first != cache.End());
        }
        UNIT_ASSERT_EQUAL(cache.Size(), 2);
    }

    Y_UNIT_TEST(TestLruCacheWithDifferentPasswords) {
        NLogin::TLruCache cache(2);
        const NLogin::TLruCache::TKey key1 = {.User = "user1", .Password = "pass1", .Hash = "12345"};
        const NLogin::TLruCache::TKey key2 = {.User = "user1", .Password = "pass2", .Hash = "12345"};
        UNIT_ASSERT_EQUAL(cache.Size(), 0);
        {
            const auto insertResult = cache.Insert(key1, false); // Bad password
            UNIT_ASSERT_EQUAL(insertResult.second, true);
            UNIT_ASSERT(insertResult.first != cache.End());
        }
        {
            const auto insertResult = cache.Insert(key2, true); // Good password
            UNIT_ASSERT_EQUAL(insertResult.second, true);
            UNIT_ASSERT(insertResult.first != cache.End());
        }
        UNIT_ASSERT_EQUAL(cache.Size(), 2);
    }

    Y_UNIT_TEST(TestLruCacheResize) {
        NLogin::TLruCache cache(2);
        const NLogin::TLruCache::TKey user1 = {.User = "user1", .Password = "pass1", .Hash = "12345"};
        const NLogin::TLruCache::TKey user2 = {.User = "user2", .Password = "pass2", .Hash = "67890"};
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
        const NLogin::TLruCache::TKey user3 = {.User = "user3", .Password = "pass3", .Hash = "abcdef"};
        {
            const auto insertResult = cache.Insert(user3, true);
            UNIT_ASSERT_EQUAL(insertResult.second, true);
            UNIT_ASSERT(insertResult.first != cache.End());
        }
        UNIT_ASSERT_EQUAL(cache.Size(), 2);

        // Resize
        cache.Resize(5);
        {
            // return user1 to cache
            const auto insertResult = cache.Insert(user1, true);
            UNIT_ASSERT_EQUAL(insertResult.second, true);
            UNIT_ASSERT(insertResult.first != cache.End());
        }
        const NLogin::TLruCache::TKey user4 = {.User = "user4", .Password = "pass4", .Hash = "abcde4"};
        {
            const auto insertResult = cache.Insert(user4, true);
            UNIT_ASSERT_EQUAL(insertResult.second, true);
            UNIT_ASSERT(insertResult.first != cache.End());
        }
        const NLogin::TLruCache::TKey user5 = {.User = "user5", .Password = "pass5", .Hash = "abcde5"};
        {
            const auto insertResult = cache.Insert(user5, true);
            UNIT_ASSERT_EQUAL(insertResult.second, true);
            UNIT_ASSERT(insertResult.first != cache.End());
        }
        UNIT_ASSERT_EQUAL(cache.Size(), 5);

        // Resize
        cache.Resize(3);
        UNIT_ASSERT_EQUAL(cache.Size(), 3);
        {
            const auto it = cache.Find(user3);
            UNIT_ASSERT(it == cache.End());
        }
        {
            const auto it = cache.Find(user2);
            UNIT_ASSERT(it == cache.End());
        }
    }
}
