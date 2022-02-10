#include "intrusive_stack.h"

#include <util/random/random.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TIntrusiveStackTest) {
    struct TSimpleItem {
        TSimpleItem *Next = nullptr;
    };

    Y_UNIT_TEST(TestEmptyPop) {
        TIntrusiveStack<TSimpleItem, &TSimpleItem::Next> stack;
        TSimpleItem *res = stack.Pop();
        UNIT_ASSERT_EQUAL(res, nullptr);
        ui64 size = stack.Size();
        UNIT_ASSERT_EQUAL(size, 0);
    }

    Y_UNIT_TEST(TestPushPop) {
        TIntrusiveStack<TSimpleItem, &TSimpleItem::Next> stack;
        TVector<TSimpleItem> items;
        items.resize(100);
        for (i64 i = 0; i < 100; ++i) {
            stack.Push(&items[i]);
        }
        ui64 size = stack.Size();
        UNIT_ASSERT_EQUAL(size, 100);
        for (i64 i = 99; i > 50; --i) {
            TSimpleItem *res = stack.Pop();
            UNIT_ASSERT_EQUAL(res, &items[i]);
        }
        size = stack.Size();
        UNIT_ASSERT_EQUAL(size, 51);
        for (i64 i = 51; i < 75; ++i) {
            stack.Push(&items[i]);
        }
        size = stack.Size();
        UNIT_ASSERT_EQUAL(size, 75);
        for (i64 i = 74; i >= 0; --i) {
            TSimpleItem *res = stack.Pop();
            UNIT_ASSERT_EQUAL(res, &items[i]);
        }
        size = stack.Size();
        UNIT_ASSERT_EQUAL(size, 0);
        TSimpleItem *res = stack.Pop();
        UNIT_ASSERT_EQUAL(res, nullptr);
    }
}

} // NKikimr
