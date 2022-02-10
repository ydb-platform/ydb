#include "intrusive_fixed_hash_set.h"

#include <util/generic/deque.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TIntrusiveFixedHashSetTest) {
    struct TSimpleItem {
        TSimpleItem *Next = nullptr;
        ui64 X = 0;
        ui64 Y = 0;

        static ui64 Hash(const TSimpleItem &value) {
            return value.X;
        }

        static bool Equals(const TSimpleItem &a, const TSimpleItem &b) {
            return (a.X == b.X && a.Y == b.Y);
        }

        TSimpleItem() {}

        TSimpleItem(ui64 x, ui64 y)
            : X(x)
            , Y(y)
        {}
    };

    Y_UNIT_TEST(TestEmptyFind) {
        TIntrusiveFixedHashSet<TSimpleItem, &TSimpleItem::Next, &TSimpleItem::Hash, &TSimpleItem::Equals> set(100);
        TSimpleItem item;
        TSimpleItem *res = set.Find(&item);
        UNIT_ASSERT_EQUAL(res, nullptr);
    }

    Y_UNIT_TEST(TestPushFindClear) {
        TIntrusiveFixedHashSet<TSimpleItem, &TSimpleItem::Next, &TSimpleItem::Hash, &TSimpleItem::Equals> set(100);
        TDeque<TSimpleItem> items;
        for (ui64 i = 0; i < 100; ++i) {
            items.emplace_back(i /2, i);
            set.Push(&items.back());
            items.emplace_back(i, i + 100);
            set.Push(&items.back());
        }
        for (ui64 i = 0; i < 100; ++i) {
            TSimpleItem A(i/2, i);
            TSimpleItem *res = set.Find(&A);
            UNIT_ASSERT_EQUAL(res, &items[i * 2]);
            TSimpleItem B(i, i + 100);
            res = set.Find(&B);
            UNIT_ASSERT_EQUAL(res, &items[i * 2 + 1]);
            TSimpleItem C(i, i + 101);
            res = set.Find(&C);
            UNIT_ASSERT_EQUAL(res, nullptr);
            TSimpleItem D(i + 100, i);
            res = set.Find(&D);
            UNIT_ASSERT_EQUAL(res, nullptr);
        }
        set.Clear();
        for (ui64 i = 0; i < 100; ++i) {
            TSimpleItem A(i/2, i);
            TSimpleItem *res = set.Find(&A);
            UNIT_ASSERT_EQUAL(res, nullptr);
            TSimpleItem B(i, i + 100);
            res = set.Find(&B);
            UNIT_ASSERT_EQUAL(res, nullptr);
            TSimpleItem C(i, i + 101);
            res = set.Find(&C);
            UNIT_ASSERT_EQUAL(res, nullptr);
            TSimpleItem D(i + 100, i);
            res = set.Find(&D);
            UNIT_ASSERT_EQUAL(res, nullptr);
        }
    }
}

} // NKikimr
