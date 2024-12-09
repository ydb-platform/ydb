#include "sized_cache.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;

struct TTestCacheObj: public TSizedCache::ICacheObj {
    TTestCacheObj(const TString& name, ui64 size)
        : Name(name)
        , Size(size)
        , Dismissed(false)
    {
    }

    TString GetName() override {
        return Name;
    }
    ui64 GetSize() override {
        return Size;
    }
    void Dismiss() override {
        Dismissed = true;
    }

    TString Name;
    ui64 Size;
    bool Dismissed;
};

Y_UNIT_TEST_SUITE(TSizedCacheTests) {
    Y_UNIT_TEST(Count) {
        TSizedCache cache(100, 100);
        TIntrusivePtr<TTestCacheObj> o1 = MakeIntrusive<TTestCacheObj>("o1", 10ull);
        TIntrusivePtr<TTestCacheObj> o2 = MakeIntrusive<TTestCacheObj>("o2", 20ull);
        TIntrusivePtr<TTestCacheObj> o3 = MakeIntrusive<TTestCacheObj>("o3", 15ull);

        UNIT_ASSERT_VALUES_EQUAL(cache.GetCount(), 0);
        cache.Put(o1, false);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetCount(), 1);
        cache.Put(o2, false);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetCount(), 2);
        cache.Put(o3, false);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetCount(), 3);
        cache.Put(o1, false); // Equal object
        UNIT_ASSERT_VALUES_EQUAL(cache.GetCount(), 3);
        cache.Put(o3, false); // Equal object
        UNIT_ASSERT_VALUES_EQUAL(cache.GetCount(), 3);
    }

    Y_UNIT_TEST(GetOccupiedSize) {
        TSizedCache cache(100, 100);
        TIntrusivePtr<TTestCacheObj> o1 = MakeIntrusive<TTestCacheObj>("o1", 10ull);
        TIntrusivePtr<TTestCacheObj> o2 = MakeIntrusive<TTestCacheObj>("o2", 20ull);
        TIntrusivePtr<TTestCacheObj> o3 = MakeIntrusive<TTestCacheObj>("o3", 15ull);

        UNIT_ASSERT_VALUES_EQUAL(cache.GetOccupiedSize(), 0);
        cache.Put(o1, false);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetOccupiedSize(), 10);
        cache.Put(o2, false);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetOccupiedSize(), 30);
        cache.Put(o3, false);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetOccupiedSize(), 45);
        cache.Put(o2, false); // Equla object
        UNIT_ASSERT_VALUES_EQUAL(cache.GetOccupiedSize(), 45);
        cache.Put(o3, false); // Equla object
        UNIT_ASSERT_VALUES_EQUAL(cache.GetOccupiedSize(), 45);
    }

    Y_UNIT_TEST(DisplaceByCount) {
        TSizedCache cache(2, 100);
        TIntrusivePtr<TTestCacheObj> o1 = MakeIntrusive<TTestCacheObj>("o1", 10ull);
        TIntrusivePtr<TTestCacheObj> o2 = MakeIntrusive<TTestCacheObj>("o2", 20ull);
        TIntrusivePtr<TTestCacheObj> o3 = MakeIntrusive<TTestCacheObj>("o3", 15ull);

        cache.Put(o1, false);
        cache.Put(o2, false);
        cache.Put(o3, false);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetOccupiedSize(), 35);
        UNIT_ASSERT(o1->Dismissed);
        UNIT_ASSERT(!o2->Dismissed);
        UNIT_ASSERT(!o3->Dismissed);
    }

    Y_UNIT_TEST(DisplaceBySize) {
        TSizedCache cache(100, 35);
        TIntrusivePtr<TTestCacheObj> o1 = MakeIntrusive<TTestCacheObj>("o1", 10ull);
        TIntrusivePtr<TTestCacheObj> o2 = MakeIntrusive<TTestCacheObj>("o2", 20ull);
        TIntrusivePtr<TTestCacheObj> o3 = MakeIntrusive<TTestCacheObj>("o3", 15ull);

        cache.Put(o1, false);
        cache.Put(o2, false);
        cache.Put(o3, false);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetOccupiedSize(), 35);
        UNIT_ASSERT(o1->Dismissed);
        UNIT_ASSERT(!o2->Dismissed);
        UNIT_ASSERT(!o3->Dismissed);
    }

    Y_UNIT_TEST(Lock) {
        TSizedCache cache(2, 30);
        TIntrusivePtr<TTestCacheObj> o1 = MakeIntrusive<TTestCacheObj>("o1", 10ull);
        TIntrusivePtr<TTestCacheObj> o2 = MakeIntrusive<TTestCacheObj>("o2", 20ull);
        TIntrusivePtr<TTestCacheObj> o3 = MakeIntrusive<TTestCacheObj>("o3", 15ull);

        UNIT_ASSERT(cache.GetLocks("o1").Empty());
        cache.Put(o1, true);
        UNIT_ASSERT_VALUES_EQUAL(*cache.GetLocks("o1"), 1);
        cache.Put(o2, true);
        cache.Put(o3, true);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetCount(), 3);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetOccupiedSize(), 45);
        UNIT_ASSERT(!o1->Dismissed);
        UNIT_ASSERT(!o2->Dismissed);
        UNIT_ASSERT(!o3->Dismissed);
        cache.Release("o1"); // Unlock object. It will be removed immediately
        UNIT_ASSERT_VALUES_EQUAL(cache.GetCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetOccupiedSize(), 35);
        UNIT_ASSERT(o1->Dismissed);
        UNIT_ASSERT(!o2->Dismissed);
        UNIT_ASSERT(!o3->Dismissed);
    }

    Y_UNIT_TEST(MultiLock) {
        TSizedCache cache(2, 30);
        TIntrusivePtr<TTestCacheObj> o1 = MakeIntrusive<TTestCacheObj>("o1", 10ull);
        TIntrusivePtr<TTestCacheObj> o2 = MakeIntrusive<TTestCacheObj>("o2", 20ull);
        TIntrusivePtr<TTestCacheObj> o3 = MakeIntrusive<TTestCacheObj>("o3", 15ull);

        cache.Put(o1, true);
        UNIT_ASSERT_VALUES_EQUAL(*cache.GetLocks("o1"), 1);
        cache.Put(o1, true);
        UNIT_ASSERT_VALUES_EQUAL(*cache.GetLocks("o1"), 2);
        cache.Put(o1, true);
        UNIT_ASSERT_VALUES_EQUAL(*cache.GetLocks("o1"), 3);
        cache.Put(o2, true);
        cache.Put(o3, true);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetCount(), 3);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetOccupiedSize(), 45);
        UNIT_ASSERT(!o1->Dismissed);
        UNIT_ASSERT(!o2->Dismissed);
        UNIT_ASSERT(!o3->Dismissed);
        cache.Release("o1");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetCount(), 3);
        UNIT_ASSERT_VALUES_EQUAL(*cache.GetLocks("o1"), 2);
        cache.Release("o1");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetCount(), 3);
        UNIT_ASSERT_VALUES_EQUAL(*cache.GetLocks("o1"), 1);
        cache.Release("o1"); // Unlock object. It will be removed immediately
        UNIT_ASSERT(cache.GetLocks("o1").Empty());
        UNIT_ASSERT_VALUES_EQUAL(cache.GetCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetOccupiedSize(), 35);
        UNIT_ASSERT(o1->Dismissed);
        UNIT_ASSERT(!o2->Dismissed);
        UNIT_ASSERT(!o3->Dismissed);
    }
}
