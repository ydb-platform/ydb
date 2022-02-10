#include "cache.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NCache {

struct TDtorTracer {
    TDtorTracer() {
        ++InstanceCount;
    }

    TDtorTracer(const TDtorTracer&) {
        ++InstanceCount;
    }

    ~TDtorTracer() {
        --InstanceCount;
    }

    static ui64 InstanceCount;
};

ui64 TDtorTracer::InstanceCount = 0;

Y_UNIT_TEST_SUITE(TCacheTest) {
    Y_UNIT_TEST(TestUnboundedMapCache) {
        TUnboundedCacheOnMap<ui32, TString> cache;
        TString* tmp;
        UNIT_ASSERT_EQUAL(cache.GetCount(), 0);
        UNIT_ASSERT(!cache.Find(1, tmp));
        UNIT_ASSERT(!cache.FindWithoutPromote(1, tmp));
        UNIT_ASSERT(cache.Insert(1, "a", tmp));
        UNIT_ASSERT_EQUAL(cache.GetCount(), 1);
        UNIT_ASSERT(cache.Find(1, tmp));
        UNIT_ASSERT_EQUAL(*tmp, "a");
        UNIT_ASSERT(cache.FindWithoutPromote(1, tmp));
        UNIT_ASSERT_EQUAL(*tmp, "a");
        UNIT_ASSERT(cache.Pop());
        UNIT_ASSERT_EQUAL(cache.GetCount(), 0);
        UNIT_ASSERT(!cache.Pop());
        UNIT_ASSERT_EQUAL(cache.GetCount(), 0);
        UNIT_ASSERT(cache.Insert(2, "b", tmp));
        UNIT_ASSERT_EQUAL(cache.GetCount(), 1);
        UNIT_ASSERT(!cache.Insert(2, "c", tmp));
        UNIT_ASSERT_EQUAL(cache.GetCount(), 1);
        UNIT_ASSERT(cache.Find(2, tmp));
        UNIT_ASSERT_EQUAL(*tmp, "b");
        UNIT_ASSERT(cache.Erase(2));
        UNIT_ASSERT_EQUAL(cache.GetCount(), 0);
        UNIT_ASSERT(!cache.Erase(2));
        UNIT_ASSERT_EQUAL(cache.GetCount(), 0);
        const auto& stat = cache.GetStatistics();
        UNIT_ASSERT_EQUAL(stat.EraseCount, 2);
        UNIT_ASSERT_EQUAL(stat.EraseMissingCount, 1);
        UNIT_ASSERT_EQUAL(stat.EvictionCount, 2);
        UNIT_ASSERT_EQUAL(stat.FindCount, 3);
        UNIT_ASSERT_EQUAL(stat.FindHitCount, 2);
        UNIT_ASSERT_EQUAL(stat.FindWithoutPromoteCount, 2);
        UNIT_ASSERT_EQUAL(stat.FindWithoutPromoteHitCount, 1);
        UNIT_ASSERT_EQUAL(stat.InsertCount, 3);
        UNIT_ASSERT_EQUAL(stat.InsertExistingCount, 1);
        UNIT_ASSERT_EQUAL(stat.PopCount, 2);
        UNIT_ASSERT_EQUAL(stat.PopEmptyCount, 1);
        cache.ClearStatistics();
        UNIT_ASSERT_EQUAL(stat.EraseCount, 0);
        UNIT_ASSERT(!cache.IsOverflow());
    }

    Y_UNIT_TEST(EnsureNoLeakAfterUnboundedCacheOnMapDtor) {
        {
            TUnboundedCacheOnMap<ui32, TDtorTracer> cache;
            TDtorTracer* tmp;
            cache.Insert(1, TDtorTracer(), tmp);
            UNIT_ASSERT_EQUAL(TDtorTracer::InstanceCount, 1);
        }

        UNIT_ASSERT_EQUAL(TDtorTracer::InstanceCount, 0);
    }

    Y_UNIT_TEST(TestSizeBasedOverflowCallback) {
        TUnboundedCacheOnMap<ui32, TString> cache;
        TSizeBasedOverflowCallback<ui32, TString> overflowCallback(2);
        cache.SetOverflowCallback([&](const ICache<ui32, TString>& cache) { return overflowCallback(cache); });
        TString* tmp;
        cache.Insert(1, "a", tmp);
        UNIT_ASSERT(!cache.IsOverflow());
        cache.Insert(2, "b", tmp);
        UNIT_ASSERT(cache.IsOverflow());
        overflowCallback.SetMaxSize(3);
        UNIT_ASSERT(!cache.IsOverflow());
        cache.Insert(3, "c", tmp);
        UNIT_ASSERT(cache.IsOverflow());
        cache.Erase(1);
        UNIT_ASSERT(!cache.IsOverflow());
    }

    Y_UNIT_TEST(TestLruCache) {
        TLruCache<ui32, TString> cache;
        TString* tmp;
        TSizeBasedOverflowCallback<ui32, TString> overflowCallback(2);
        cache.SetOverflowCallback([&](const ICache<ui32, TString>& cache) { return overflowCallback(cache); });
        UNIT_ASSERT(cache.Insert(1, "a", tmp));
        UNIT_ASSERT(cache.Insert(2, "b", tmp));
        UNIT_ASSERT_EQUAL(cache.GetUsedSize(), 2);
        UNIT_ASSERT(cache.IsOverflow());
        UNIT_ASSERT(cache.Find(1, tmp));
        UNIT_ASSERT_EQUAL(*tmp, "a");
        UNIT_ASSERT(cache.Insert(3, "c", tmp));
        // 2 must be evicted
        UNIT_ASSERT_EQUAL(cache.GetUsedSize(), 2);
        UNIT_ASSERT(!cache.Find(2, tmp));
        UNIT_ASSERT(cache.Find(3, tmp));
        UNIT_ASSERT_EQUAL(*tmp, "c");
        UNIT_ASSERT(cache.FindWithoutPromote(1, tmp));
        // 1 must be evicted
        UNIT_ASSERT(cache.Insert(4, "d", tmp));
        UNIT_ASSERT_EQUAL(cache.GetUsedSize(), 2);
        UNIT_ASSERT(!cache.Find(1, tmp));
        UNIT_ASSERT(cache.Find(4, tmp));
        UNIT_ASSERT_EQUAL(*tmp, "d");
        UNIT_ASSERT_EQUAL(cache.GetStatistics().EvictionCount, 2);
        UNIT_ASSERT(cache.Pop());
        UNIT_ASSERT_EQUAL(cache.GetUsedSize(), 1);
        // 3 must be evicted
        UNIT_ASSERT(!cache.Find(3, tmp));
        UNIT_ASSERT(cache.Find(4, tmp));
        UNIT_ASSERT_EQUAL(*tmp, "d");
        UNIT_ASSERT(!cache.Erase(3));
        UNIT_ASSERT(cache.Erase(4));
        UNIT_ASSERT_EQUAL(cache.GetUsedSize(), 0);
        UNIT_ASSERT(!cache.Pop());
        UNIT_ASSERT_EQUAL(cache.GetUsedSize(), 0);
    }

    Y_UNIT_TEST(EnsureNoLeakAfterLruCacheDtor) {
        {
            TLruCache<ui32, TDtorTracer> cache;
            TDtorTracer* tmp;
            cache.Insert(1, TDtorTracer(), tmp);
            UNIT_ASSERT_EQUAL(TDtorTracer::InstanceCount, 1);
        }

        UNIT_ASSERT_EQUAL(TDtorTracer::InstanceCount, 0);
    }

    Y_UNIT_TEST(Test2QCache) {
        TIntrusivePtr<T2QCacheConfig> config(new T2QCacheConfig);
        T2QCache<ui32, TString> cache(config);
        TString lastEvicted;
        cache.SetEvictionCallback([&](const ui32& key, TString& value, ui64 size){
            Y_UNUSED(key);
            Y_UNUSED(size);
            lastEvicted = value;
        });

        ui32 lastEvictedKey = 0;
        cache.SetKeyEvictionCallback([&](const ui32& key){
            lastEvictedKey = key;
        });

        TString* tmp;
        TSizeBasedOverflowCallback<ui32, TString> overflowCallback(4);
        cache.SetOverflowCallback([&](const ICache<ui32, TString>& cache) { return overflowCallback(cache); });
        UNIT_ASSERT(cache.Insert(1, "a", tmp));
        UNIT_ASSERT(!cache.Insert(1, "a", tmp));
        UNIT_ASSERT(cache.Insert(2, "b", tmp));
        UNIT_ASSERT(cache.Insert(3, "c", tmp));
        UNIT_ASSERT(cache.Insert(4, "d", tmp));
        UNIT_ASSERT_EQUAL(cache.GetUsedSize(), 4);
        UNIT_ASSERT(cache.IsOverflow());
        UNIT_ASSERT(cache.Find(1, tmp));
        UNIT_ASSERT_EQUAL(*tmp, "a");
        UNIT_ASSERT(cache.Insert(5, "e", tmp));
        // will evict 1
        UNIT_ASSERT_EQUAL(lastEvicted, "a");
        UNIT_ASSERT(!cache.Find(1, tmp));
        // will evict 2, 1 will be placed to main cache
        UNIT_ASSERT(cache.Insert(1, "a", tmp));
        UNIT_ASSERT_EQUAL(lastEvicted, "b");
        UNIT_ASSERT_EQUAL(lastEvictedKey, 1);
        UNIT_ASSERT(!cache.Find(2, tmp));
        UNIT_ASSERT(cache.Insert(6, "f", tmp)); // in cache
        UNIT_ASSERT_EQUAL(lastEvicted, "c");
        UNIT_ASSERT_EQUAL(lastEvictedKey, 2);
        UNIT_ASSERT(cache.Insert(7, "g", tmp)); // in cache
        UNIT_ASSERT_EQUAL(lastEvicted, "d");
        UNIT_ASSERT_EQUAL(lastEvictedKey, 3);
        UNIT_ASSERT(cache.Insert(8, "h", tmp)); // in cache
        UNIT_ASSERT_EQUAL(lastEvicted, "e");
        UNIT_ASSERT_EQUAL(lastEvictedKey, 4);
        UNIT_ASSERT(cache.Insert(9, "i", tmp)); // in cache
        UNIT_ASSERT_EQUAL(lastEvicted, "f");
        UNIT_ASSERT_EQUAL(lastEvictedKey, 5);
        UNIT_ASSERT(!cache.Find(5, tmp));
        UNIT_ASSERT(!cache.Find(4, tmp));
        UNIT_ASSERT(!cache.Find(3, tmp));
        UNIT_ASSERT(cache.Find(1, tmp));
        UNIT_ASSERT_EQUAL(*tmp, "a");
        UNIT_ASSERT(cache.Insert(6, "f", tmp)); // go to main cache
        UNIT_ASSERT_EQUAL(lastEvicted, "g");
        UNIT_ASSERT_EQUAL(lastEvictedKey, 6);
        UNIT_ASSERT(cache.FindWithoutPromote(1, tmp));
        UNIT_ASSERT_EQUAL(*tmp, "a");
        UNIT_ASSERT(!cache.FindWithoutPromote(99, tmp));
        UNIT_ASSERT(cache.Erase(1));
        UNIT_ASSERT_EQUAL(lastEvicted, "a");
        UNIT_ASSERT(!cache.Erase(1));
        UNIT_ASSERT(cache.Erase(9));
        UNIT_ASSERT_EQUAL(lastEvicted, "i");
        UNIT_ASSERT(!cache.Erase(9));
        UNIT_ASSERT_EQUAL(cache.GetUsedSize(), 2);
        UNIT_ASSERT(cache.Pop());
        UNIT_ASSERT_EQUAL(lastEvicted, "h");
        UNIT_ASSERT_EQUAL(lastEvictedKey, 7);
        UNIT_ASSERT(cache.Pop());
        UNIT_ASSERT_EQUAL(lastEvicted, "f");
        UNIT_ASSERT(!cache.Pop());
        UNIT_ASSERT_EQUAL(cache.GetUsedSize(), 0);

        UNIT_ASSERT(cache.Insert(8, "h", tmp)); // go to main cache
        UNIT_ASSERT(!cache.Insert(8, "h", tmp));
        UNIT_ASSERT(cache.Erase(8));
        UNIT_ASSERT_EQUAL(lastEvicted, "h");
        UNIT_ASSERT_EQUAL(lastEvictedKey, 8);
    }

    Y_UNIT_TEST(EnsureNoLeakAfterQ2CacheDtor) {
        {
            TIntrusivePtr<T2QCacheConfig> config(new T2QCacheConfig);
            T2QCache<ui32, TDtorTracer> cache(config);
            TSizeBasedOverflowCallback<ui32, TDtorTracer> overflowCallback(4);
            cache.SetOverflowCallback([&](const ICache<ui32, TDtorTracer>& cache) { return overflowCallback(cache); });
            TDtorTracer* tmp;
            cache.Insert(1, TDtorTracer(), tmp);
            cache.Insert(2, TDtorTracer(), tmp);
            cache.Insert(3, TDtorTracer(), tmp);
            cache.Insert(4, TDtorTracer(), tmp);
            UNIT_ASSERT_EQUAL(TDtorTracer::InstanceCount, 4);
            cache.Insert(5, TDtorTracer(), tmp);
            cache.Insert(1, TDtorTracer(), tmp); // 1 will go to main cache
            cache.Insert(10, TDtorTracer(), tmp);
            cache.Insert(11, TDtorTracer(), tmp);
            cache.Insert(12, TDtorTracer(), tmp);
            cache.Insert(13, TDtorTracer(), tmp);
            UNIT_ASSERT(cache.Find(1, tmp)); // 1 will be found in the main cache
            UNIT_ASSERT(cache.Find(11, tmp)); // 11 will be found in the in cache
        }

        UNIT_ASSERT_EQUAL(TDtorTracer::InstanceCount, 0);
    }

    Y_UNIT_TEST(TestUpdateItemSize) {
        TUnboundedCacheOnMap<ui32, TString> cache([](const ui32& key, const TString& value) {
            Y_UNUSED(key);
            return value.size();
        });

        TString* tmp;
        ui32 key1 = 1;
        ui32 key2 = 2;
        UNIT_ASSERT_EQUAL(cache.GetCount(), 0);
        UNIT_ASSERT(cache.Insert(key1, "aaa", tmp));
        UNIT_ASSERT(cache.Insert(key2, "bb", tmp));
        UNIT_ASSERT_EQUAL(cache.GetUsedSize(), 5);
        ui64 oldSize = cache.Measure(key2, *tmp);
        UNIT_ASSERT_EQUAL(oldSize, 2);
        *tmp = "ccc";
        ui64 newSize = cache.UpdateUsedSize(key2, *tmp, oldSize);
        UNIT_ASSERT_EQUAL(newSize, 3);
        UNIT_ASSERT_EQUAL(cache.GetUsedSize(), 6);
    }
}

}
}
