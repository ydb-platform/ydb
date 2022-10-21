#include "mkql_computation_node_list.h"

#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NMiniKQL {
Y_UNIT_TEST_SUITE(TestListRepresentation) {
    Y_UNIT_TEST(Test) {
        TMemoryUsageInfo memInfo(TStringBuf("test"));
        TScopedAlloc alloc(__LOCATION__);
        using TListType = TListRepresentation<ui32, 256>;
        TListType list1;
        auto it1 = list1.GetIterator();
        UNIT_ASSERT(it1.AtEnd());

        TListType list2(list1, 0);
        auto it2 = list2.GetIterator();
        UNIT_ASSERT(!it2.AtEnd());
        UNIT_ASSERT_VALUES_EQUAL(it2.Current(), 0);
        it2.Next();
        UNIT_ASSERT(it2.AtEnd());

        TVector<TListType> lists;
        lists.push_back(list1);
        const ui32 n = 100;
        for (ui32 i = 0; i < n; ++i) {
            auto c = i;
            lists.push_back(TListType(lists.back(), std::move(c)));
        }

        for (ui32 i = 0; i < n; ++i) {
            auto list = lists[i];
            auto it = list.GetIterator();
            ui32 expected = 0;
            while (!it.AtEnd()) {
                UNIT_ASSERT_VALUES_EQUAL(it.Current(), expected);
                ++expected;
                it.Next();
            }

            UNIT_ASSERT_VALUES_EQUAL(expected, i);
        }

        for (ui32 i = 0; i < n; ++i) {
            auto list = lists[i];
            auto it = list.GetReverseIterator();
            ui32 expected = i;
            while (!it.AtEnd()) {
                UNIT_ASSERT_VALUES_EQUAL(it.Current(), --expected);
                it.Next();
            }

            UNIT_ASSERT_VALUES_EQUAL(expected, 0);
        }

        TVector<TListType> rlists;
        rlists.push_back(list1);
        for (ui32 i = 0; i < n; ++i) {
            auto c = i;
            rlists.push_back(TListType(std::move(c), rlists.back()));
        }

        for (ui32 i = 0; i < n; ++i) {
            auto list = rlists[i];
            auto it = list.GetIterator();
            ui32 expected = i;
            while (!it.AtEnd()) {
                UNIT_ASSERT_VALUES_EQUAL(it.Current(), --expected);
                it.Next();
            }

            UNIT_ASSERT_VALUES_EQUAL(expected, 0);
        }

        for (ui32 i = 0; i < n; ++i) {
            auto list = rlists[i];
            auto it = list.GetReverseIterator();
            ui32 expected = 0;
            while (!it.AtEnd()) {
                UNIT_ASSERT_VALUES_EQUAL(it.Current(), expected);
                ++expected;
                it.Next();
            }

            UNIT_ASSERT_VALUES_EQUAL(expected, i);
        }

        TVector<TListType> zlists;
        zlists.push_back(list1);
        for (ui32 i = 0; i < n; ++i) {
            zlists.push_back(zlists.back().Append(2 * i));
            zlists.push_back(zlists.back().Prepend(2 * i + 1));
        }

        for (ui32 i = 0; i < 2 * n; ++i) {
            ui32 k = (i + 1) / 2;
            auto list = zlists[i];
            auto it = list.GetIterator();
            ui32 expected = 2 * k + 1;
            if ((i % 2))
                expected -= 2;

            if (i >= 2) {
                while (!it.AtEnd()) {
                    expected -= 2;
                    UNIT_ASSERT_VALUES_EQUAL(it.Current(), expected);
                    it.Next();
                    if (expected == 1)
                        break;
                }
            }

            expected = 0;
            while (!it.AtEnd()) {
                UNIT_ASSERT_VALUES_EQUAL(it.Current(), expected);
                expected += 2;
                it.Next();
            }

            UNIT_ASSERT_VALUES_EQUAL(expected, 2 * k);
        }
    }
}
}
}
