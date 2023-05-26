#include "blobstorage_pdisk_log_cache.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NPDisk {

Y_UNIT_TEST_SUITE(TLogCache) {
    TLogCache::TCacheRecord MakeRecord(ui64 offset, TString str) {
        return TLogCache::TCacheRecord(
            offset,
            TRcBuf(TString(str.c_str(), str.c_str() + str.size() + 1)),
            {});
    }

    Y_UNIT_TEST(Simple) {
        TLogCache cache;

        UNIT_ASSERT(cache.Insert(MakeRecord(1, "a")));
        UNIT_ASSERT(cache.Insert(MakeRecord(2, "b")));
        UNIT_ASSERT_EQUAL(cache.Size(), 2);
        UNIT_ASSERT_STRINGS_EQUAL(cache.Find(1)->Data.GetData(), "a");

        UNIT_ASSERT(cache.Insert(MakeRecord(3, "c")));
        UNIT_ASSERT(cache.Pop()); // 2 must be evicted
        UNIT_ASSERT_EQUAL(cache.Size(), 2);
        UNIT_ASSERT_EQUAL(nullptr, cache.Find(2));
        UNIT_ASSERT_STRINGS_EQUAL(cache.Find(3)->Data.GetData(), "c");

        UNIT_ASSERT(cache.Pop()); // 1 must be evicted
        UNIT_ASSERT(cache.Insert(MakeRecord(4, "d")));

        UNIT_ASSERT_EQUAL(cache.Size(), 2);
        UNIT_ASSERT_EQUAL(nullptr, cache.Find(1));
        UNIT_ASSERT_STRINGS_EQUAL(cache.Find(4)->Data.GetData(), "d");

        UNIT_ASSERT(cache.Pop()); // 3 must be evicted
        UNIT_ASSERT_EQUAL(cache.Size(), 1);
        UNIT_ASSERT_EQUAL(nullptr, cache.Find(3));
        UNIT_ASSERT_STRINGS_EQUAL(cache.Find(4)->Data.GetData(), "d");

        UNIT_ASSERT_EQUAL(0, cache.Erase(3));
        UNIT_ASSERT_EQUAL(1, cache.Erase(4));
        UNIT_ASSERT_EQUAL(cache.Size(), 0);
        UNIT_ASSERT(!cache.Pop());
        UNIT_ASSERT_EQUAL(cache.Size(), 0);
    }

    TLogCache SetupCache(const TVector<std::pair<ui64, TString>>& content = {{5, "x"}, {1, "y"}, {10, "z"}}) {
        TLogCache cache;
        for (auto pair : content) {
            cache.Insert(MakeRecord(pair.first, pair.second));
        }
        return cache;
    };

    void AssertCacheContains(TLogCache& cache, const TVector<std::pair<ui64, TString>>& content = {{5, "x"}, {1, "y"}, {10, "z"}}) {
        UNIT_ASSERT_VALUES_EQUAL(content.size(), cache.Size());
        for (auto pair : content) {
            UNIT_ASSERT_STRINGS_EQUAL(
                pair.second,
                cache.FindWithoutPromote(pair.first)->Data.GetData());
        }
        for (auto pair : content) {
            UNIT_ASSERT(cache.Pop());
            UNIT_ASSERT_EQUAL(nullptr, cache.FindWithoutPromote(pair.first));
        }
    }

    Y_UNIT_TEST(EraseRangeOnEmpty) {
        TLogCache cache;
        UNIT_ASSERT_EQUAL(0, cache.EraseRange(0, 0));
        UNIT_ASSERT_EQUAL(0, cache.EraseRange(0, 10));
        UNIT_ASSERT_EQUAL(0, cache.EraseRange(10, 10));
    }

    Y_UNIT_TEST(EraseRangeOutsideOfData) {
        TLogCache cache = SetupCache();
        UNIT_ASSERT_EQUAL(3, cache.Size());
        UNIT_ASSERT_EQUAL(0, cache.EraseRange(0, 1));
        UNIT_ASSERT_EQUAL(3, cache.Size());
        UNIT_ASSERT_EQUAL(0, cache.EraseRange(11, 12));
        UNIT_ASSERT_EQUAL(3, cache.Size());
        UNIT_ASSERT_EQUAL(0, cache.EraseRange(11, 100));
        UNIT_ASSERT_EQUAL(3, cache.Size());
    }

    Y_UNIT_TEST(EraseRangeSingleMinElement) {
        TLogCache cache = SetupCache();
        UNIT_ASSERT_EQUAL(1, cache.EraseRange(1, 2));
        AssertCacheContains(cache, {{5, "x"}, {10, "z"}});
    }

    Y_UNIT_TEST(EraseRangeSingleMidElement) {
        TLogCache cache = SetupCache();
        UNIT_ASSERT_EQUAL(1, cache.EraseRange(5, 6));
        AssertCacheContains(cache, {{1, "y"}, {10, "z"}});
    }

    Y_UNIT_TEST(EraseRangeSingleMaxElement) {
        TLogCache cache = SetupCache();
        UNIT_ASSERT_EQUAL(1, cache.EraseRange(10, 11));
        AssertCacheContains(cache, {{5, "x"}, {1, "y"}});
    }

    Y_UNIT_TEST(EraseRangeSample) {
        TLogCache cache = SetupCache();
        UNIT_ASSERT_EQUAL(2, cache.EraseRange(2, 100));
        AssertCacheContains(cache, {{1, "y"}});
    }

    Y_UNIT_TEST(EraseRangeAllExact) {
        TLogCache cache = SetupCache();
        UNIT_ASSERT_EQUAL(3, cache.EraseRange(1, 11));
        UNIT_ASSERT_EQUAL(0, cache.Size());
    }

    Y_UNIT_TEST(EraseRangeAllAmple) {
        TLogCache cache = SetupCache();
        UNIT_ASSERT_EQUAL(3, cache.EraseRange(0, 100));
        UNIT_ASSERT_EQUAL(0, cache.Size());
    }
}

} // NPDisk
} // NKikimr
