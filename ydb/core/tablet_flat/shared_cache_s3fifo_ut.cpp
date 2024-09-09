#include <library/cpp/testing/unittest/registar.h>
#include "shared_cache_s3fifo.h"

namespace NKikimr::NCache {

namespace {

    struct TPage {
        ui32 Id;
        size_t Size;

        ui32 CacheFlags1 : 4 = 0;
        ui32 CacheFlags2 : 4 = 0;
    };

    struct TKey {
        ui32 Id;

        TKey(ui32 id)
            : Id(id)
        {}

        TString ToString() const {
            return std::to_string(Id);
        }
    };

    struct TKeyHash {
        inline size_t operator()(const TKey& key) const {
            return std::hash<ui32>()(key.Id);
        }
    };

    struct TKeyEqual {
        inline bool operator()(const TKey& left, const TKey& right) const {
            return left.Id == right.Id;
        }
    };

    struct TSize {
        static ui64 Get(const TPage *x) {
            return x->Size;
        }
    };

    struct TCacheFlags1 {
        static ui32 Get(const TPage *x) {
            return x->CacheFlags1;
        }
        static void Set(TPage *x, ui32 flags) {
            Y_ABORT_UNLESS(flags < (1 << 4));
            x->CacheFlags1 = flags;
        }
    };

    struct TCacheFlags2 {
        static ui32 Get(const TPage *x) {
            return x->CacheFlags2;
        }
        static void Set(TPage *x, ui32 flags) {
            Y_ABORT_UNLESS(flags < (1 << 4));
            x->CacheFlags2 = flags;
        }
    };

}

Y_UNIT_TEST_SUITE(TGhostQueue) {
    
    Y_UNIT_TEST(Add) {
        TGhostQueue<TKey, TKeyHash, TKeyEqual> queue(100);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "");

        queue.Add(1, 10);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{1 10b}");

        queue.Add(2, 30);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{1 10b}, {2 30b}");

        queue.Add(3, 60);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{1 10b}, {2 30b}, {3 60b}");

        queue.Add(4, 1);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{2 30b}, {3 60b}, {4 1b}");

        queue.Add(1, 3);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{2 30b}, {3 60b}, {4 1b}, {1 3b}");
    }

    Y_UNIT_TEST(Erase) {
        TGhostQueue<TKey, TKeyHash, TKeyEqual> queue(100);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "");

        queue.Add(1, 10);
        queue.Add(2, 30);
        queue.Add(3, 60);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{1 10b}, {2 30b}, {3 60b}");

        UNIT_ASSERT_VALUES_EQUAL(queue.Erase(5, 42), false);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{1 10b}, {2 30b}, {3 60b}");

        UNIT_ASSERT_VALUES_EQUAL(queue.Erase(2, 30), true);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{1 10b}, {3 60b}");

        queue.Add(4, 30);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{1 10b}, {3 60b}, {4 30b}");

        queue.Add(5, 1);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{3 60b}, {4 30b}, {5 1b}");
    }

    Y_UNIT_TEST(Erase_Add) {
        TGhostQueue<TKey, TKeyHash, TKeyEqual> queue(100);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "");

        queue.Add(1, 10);
        queue.Add(2, 30);
        queue.Add(3, 60);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{1 10b}, {2 30b}, {3 60b}");

        UNIT_ASSERT_VALUES_EQUAL(queue.Erase(2, 30), true);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{1 10b}, {3 60b}");

        queue.Add(2, 30);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{1 10b}, {3 60b}, {2 30b}");

        queue.Add(4, 70);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{2 30b}, {4 70b}");
    }

    Y_UNIT_TEST(Add_Big) {
        TGhostQueue<TKey, TKeyHash, TKeyEqual> queue(100);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "");

        queue.Add(1, 101);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "");
    }

    Y_UNIT_TEST(UpdateLimit) {
        TGhostQueue<TKey, TKeyHash, TKeyEqual> queue(100);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "");

        queue.Add(1, 10);
        queue.Add(2, 20);
        queue.Add(3, 30);
        queue.Add(4, 40);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{1 10b}, {2 20b}, {3 30b}, {4 40b}");

        queue.UpdateLimit(80);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{3 30b}, {4 40b}");
    }
}

}
