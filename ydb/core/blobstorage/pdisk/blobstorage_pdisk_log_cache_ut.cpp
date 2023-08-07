#include "blobstorage_pdisk_log_cache.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NPDisk {

Y_UNIT_TEST_SUITE(TLogCache) {
    Y_UNIT_TEST(Simple) {
        TLogCache cache;
        char buf[2] = {};

        UNIT_ASSERT(cache.Insert("a", 1, 1));
        UNIT_ASSERT(cache.Insert("b", 2, 1));
        UNIT_ASSERT_EQUAL(cache.Size(), 2);
        UNIT_ASSERT(cache.Find(1, 1, buf));
        UNIT_ASSERT_STRINGS_EQUAL(buf, "a");

        UNIT_ASSERT(cache.Insert("c", 3, 1));
        UNIT_ASSERT(cache.Pop()); // 2 must be evicted
        UNIT_ASSERT_EQUAL(cache.Size(), 2);
        UNIT_ASSERT(!cache.Find(2, 1, buf));
        UNIT_ASSERT(cache.Find(3, 1, buf));
        UNIT_ASSERT_STRINGS_EQUAL(buf, "c");

        UNIT_ASSERT(cache.Pop()); // 1 must be evicted
        UNIT_ASSERT(cache.Insert("d", 4, 1));

        UNIT_ASSERT_EQUAL(cache.Size(), 2);
        UNIT_ASSERT(!cache.Find(1, 1, buf));
        UNIT_ASSERT(cache.Find(4, 1, buf));
        UNIT_ASSERT_STRINGS_EQUAL(buf, "d");

        UNIT_ASSERT(cache.Pop()); // 3 must be evicted
        UNIT_ASSERT_EQUAL(cache.Size(), 1);
        UNIT_ASSERT(!cache.Find(3, 1, buf));
        UNIT_ASSERT(cache.Find(4, 1, buf));
        UNIT_ASSERT_STRINGS_EQUAL(buf, "d");


        UNIT_ASSERT_EQUAL(1, cache.EraseRange(3, 5));
        UNIT_ASSERT_EQUAL(cache.Size(), 0);
        UNIT_ASSERT(!cache.Pop());
        UNIT_ASSERT_EQUAL(cache.Size(), 0);
    }

    Y_UNIT_TEST(DoubleInsertion) {
        TLogCache cache;
        
        char buf[5] = {};

        auto checkFn = [&]() {
            UNIT_ASSERT_EQUAL(25, cache.Size());
            
            for (int i = 0; i < 100; i += 4) {
                UNIT_ASSERT(cache.Find(i, 4, buf));
                UNIT_ASSERT_STRINGS_EQUAL(buf, "abcd");
            }
        };

        for (int i = 0; i < 100; i += 4) {
            UNIT_ASSERT(cache.Insert("abcd", i, 4));
        }

        checkFn();
        
        for (int i = 0; i < 100; i += 4) {
            UNIT_ASSERT(!cache.Insert("abcd", i, 4));
        }

        checkFn();
    }

    Y_UNIT_TEST(FullyOverlapping) {
        TLogCache cache;
        
        cache.Insert("abcd", 0, 4);
        UNIT_ASSERT_EQUAL(1, cache.Size());

        UNIT_ASSERT(!cache.Insert("bc", 1, 2));
        UNIT_ASSERT_EQUAL(1, cache.Size());

        char buf[2] = {};
        UNIT_ASSERT(cache.Find(3, 1, buf));
        UNIT_ASSERT_STRINGS_EQUAL(buf, "d");
    }

    Y_UNIT_TEST(BetweenTwoEntries) {
        {
            TLogCache cache;
            
            UNIT_ASSERT(cache.Insert("abcd", 0, 4));
            UNIT_ASSERT_EQUAL(1, cache.Size());
            UNIT_ASSERT(cache.Insert("ijkl", 8, 4));
            UNIT_ASSERT_EQUAL(2, cache.Size());
            UNIT_ASSERT(cache.Insert("efgh", 4, 4));
            UNIT_ASSERT_EQUAL(3, cache.Size());

            char buf[5] = {};
            UNIT_ASSERT(cache.Find(4, 4, buf));
            UNIT_ASSERT_STRINGS_EQUAL(buf, "efgh");
        }

        {
            TLogCache cache;
            
            UNIT_ASSERT(cache.Insert("abcd", 0, 4));
            UNIT_ASSERT_EQUAL(1, cache.Size());
            UNIT_ASSERT(cache.Insert("ijkl", 8, 4));
            UNIT_ASSERT_EQUAL(2, cache.Size());
            UNIT_ASSERT(cache.Insert("defghi", 3, 6));
            UNIT_ASSERT_EQUAL(3, cache.Size());

            char buf[5] = {};
            UNIT_ASSERT(cache.Find(4, 4, buf));
            UNIT_ASSERT_STRINGS_EQUAL(buf, "efgh");
        }

        {
            TLogCache cache;
            
            UNIT_ASSERT(cache.Insert("abcd", 0, 4));
            UNIT_ASSERT_EQUAL(1, cache.Size());
            UNIT_ASSERT(cache.Insert("ijkl", 8, 4));
            UNIT_ASSERT_EQUAL(2, cache.Size());
            UNIT_ASSERT(cache.Insert("efgh", 4, 4));
            UNIT_ASSERT_EQUAL(3, cache.Size());

            char buf[7] = {};
            UNIT_ASSERT(cache.Find(3, 6, buf));
            UNIT_ASSERT_STRINGS_EQUAL(buf, "defghi");
        }

        {
            TLogCache cache;
            
            UNIT_ASSERT(cache.Insert("abcd", 0, 4));
            UNIT_ASSERT_EQUAL(1, cache.Size());
            UNIT_ASSERT(cache.Insert("ijkl", 8, 4));
            UNIT_ASSERT_EQUAL(2, cache.Size());
            UNIT_ASSERT(cache.Insert("defghi", 3, 6));
            UNIT_ASSERT_EQUAL(3, cache.Size());

            char buf[7] = {};
            UNIT_ASSERT(cache.Find(3, 6, buf));
            UNIT_ASSERT_STRINGS_EQUAL(buf, "defghi");
        }
    }

    Y_UNIT_TEST(NoDuplicates) {
        {
            TLogCache cache;
            
            UNIT_ASSERT(cache.Insert("abcd", 0, 4));
            UNIT_ASSERT_EQUAL(1, cache.Size());
            UNIT_ASSERT(cache.Insert("def", 3, 3));
            UNIT_ASSERT_EQUAL(2, cache.Size());

            char buf[2] = {};
            UNIT_ASSERT(cache.Find(3, 1, buf));
            UNIT_ASSERT_STRINGS_EQUAL(buf, "d");

            char buf2[3] = {};
            UNIT_ASSERT(cache.Find(3, 2, buf2));
            UNIT_ASSERT_STRINGS_EQUAL(buf2, "de");

            char buf3[11] = {};
            UNIT_ASSERT(!cache.Find(3, 10, buf3));
            UNIT_ASSERT_STRINGS_EQUAL(buf3, "");
        }

        {
            TLogCache cache;
            
            UNIT_ASSERT(cache.Insert("def", 3, 3));
            UNIT_ASSERT_EQUAL(1, cache.Size());
            UNIT_ASSERT(cache.Insert("abcd", 0, 4));
            UNIT_ASSERT_EQUAL(2, cache.Size());

            char buf[2] = {};
            UNIT_ASSERT(cache.Find(3, 1, buf));
            UNIT_ASSERT_STRINGS_EQUAL(buf, "d");

            char buf2[5] = {};
            UNIT_ASSERT(cache.Find(0, 4, buf2));
            UNIT_ASSERT_STRINGS_EQUAL(buf2, "abcd");

            char buf3[11] = {};
            UNIT_ASSERT(!cache.Find(3, 10, buf3));
            UNIT_ASSERT_STRINGS_EQUAL(buf3, "");
        }
    }

    TLogCache SetupCache(const TVector<std::pair<ui64, TString>>& content = {{5, "x"}, {1, "y"}, {10, "z"}}) {
        TLogCache cache;
        for (auto pair : content) {
            auto& data = pair.second;

            cache.Insert(data.c_str(), pair.first, data.Size());
        }
        return cache;
    };

    void AssertCacheContains(TLogCache& cache, const TVector<std::pair<ui64, TString>>& content = {{5, "x"}, {1, "y"}, {10, "z"}}) {
        UNIT_ASSERT_VALUES_EQUAL(content.size(), cache.Size());

        char buf[2] = {};

        for (auto pair : content) {
            UNIT_ASSERT(cache.FindWithoutPromote(pair.first, 1, buf));

            UNIT_ASSERT_STRINGS_EQUAL(pair.second, buf);
        }

        for (auto pair : content) {
            UNIT_ASSERT(cache.Pop());

            UNIT_ASSERT(!cache.FindWithoutPromote(pair.first, 1, buf));
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
