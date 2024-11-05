#include "cache.h"

#include <ydb/services/ydb/ydb_common_ut.h>

namespace NFq {

using TCache = TTtlCache<int,int>;

Y_UNIT_TEST_SUITE(Cache) {
    Y_UNIT_TEST(Test1) {
        TCache cache(TTtlCacheSettings().SetTtl(TDuration::Minutes(10)).SetMaxSize(100));
        TMaybe<int> t;
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cache.Get(100, &t), false);
        cache.Put(100, 10);
        UNIT_ASSERT_VALUES_EQUAL(cache.Get(100, &t), true);
        UNIT_ASSERT_VALUES_EQUAL(*t, 10);
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 1);
        cache.Put(100, TMaybe<int>());
        UNIT_ASSERT_VALUES_EQUAL(cache.Get(100, &t), true);
        UNIT_ASSERT_VALUES_EQUAL(t, TMaybe<int>());
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 1);
        cache.Put(101, 10);
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 2);
    }


    Y_UNIT_TEST(Test2) {
        TCache cache(TTtlCacheSettings().SetTtl(TDuration::Minutes(10)).SetMaxSize(3));
        TMaybe<int> t;
        cache.Put(100, 10);
        cache.Put(101, 11);
        cache.Put(102, 12);
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 3);
        cache.Put(103, 13);
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(cache.Get(100, &t), false);
    }


    Y_UNIT_TEST(Test3) {
        TCache cache(TTtlCacheSettings().SetTtl(TDuration::Minutes(0)).SetMaxSize(3));
        cache.Put(100, 10);
        cache.Put(101, 11);
        cache.Put(102, 12);
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 0);
    }

    Y_UNIT_TEST(Test4) {
        TCache cache(TTtlCacheSettings().SetErrorTtl(TDuration::Minutes(0)).SetMaxSize(3));
        cache.Put(100, 10);
        cache.Put(101, 11);
        cache.Put(102, 12);
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 3);
        cache.Put(102, TMaybe<int>());
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 2);
    }

    Y_UNIT_TEST(Test5) {
        TCache cache(TTtlCacheSettings().SetTtl(TDuration::Seconds(1)).SetMaxSize(3));
        cache.Put(100, 10);
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 1);
        Sleep(TDuration::Seconds(1));
        TMaybe<int> t;
        UNIT_ASSERT_VALUES_EQUAL(cache.Get(100, &t), false);
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 0);
    }
}

}  // namespace NFq
