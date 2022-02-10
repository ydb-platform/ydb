#include "string_pool.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NMonitoring;

Y_UNIT_TEST_SUITE(TStringPoolTest) {
    Y_UNIT_TEST(PutIfAbsent) {
        TStringPoolBuilder strPool;
        strPool.SetSorted(true);

        auto* h1 = strPool.PutIfAbsent("one");
        auto* h2 = strPool.PutIfAbsent("two");
        auto* h3 = strPool.PutIfAbsent("two");
        UNIT_ASSERT(h1 != h2);
        UNIT_ASSERT(h2 == h3);

        UNIT_ASSERT_VALUES_EQUAL(h1->Frequency, 1);
        UNIT_ASSERT_VALUES_EQUAL(h1->Index, 0);

        UNIT_ASSERT_VALUES_EQUAL(h2->Frequency, 2);
        UNIT_ASSERT_VALUES_EQUAL(h2->Index, 1);

        UNIT_ASSERT_VALUES_EQUAL(strPool.BytesSize(), 6);
        UNIT_ASSERT_VALUES_EQUAL(strPool.Count(), 2);
    }

    Y_UNIT_TEST(SortByFrequency) {
        TStringPoolBuilder strPool;
        strPool.SetSorted(true);

        auto* h1 = strPool.PutIfAbsent("one");
        auto* h2 = strPool.PutIfAbsent("two");
        auto* h3 = strPool.PutIfAbsent("two");
        UNIT_ASSERT(h1 != h2);
        UNIT_ASSERT(h2 == h3);

        strPool.Build();

        UNIT_ASSERT_VALUES_EQUAL(h1->Frequency, 1);
        UNIT_ASSERT_VALUES_EQUAL(h1->Index, 1);

        UNIT_ASSERT_VALUES_EQUAL(h2->Frequency, 2);
        UNIT_ASSERT_VALUES_EQUAL(h2->Index, 0);

        UNIT_ASSERT_VALUES_EQUAL(strPool.BytesSize(), 6);
        UNIT_ASSERT_VALUES_EQUAL(strPool.Count(), 2);
    }

    Y_UNIT_TEST(ForEach) {
        TStringPoolBuilder strPool;
        strPool.SetSorted(true);

        strPool.PutIfAbsent("one");
        strPool.PutIfAbsent("two");
        strPool.PutIfAbsent("two");
        strPool.PutIfAbsent("three");
        strPool.PutIfAbsent("three");
        strPool.PutIfAbsent("three");

        UNIT_ASSERT_VALUES_EQUAL(strPool.BytesSize(), 11);
        UNIT_ASSERT_VALUES_EQUAL(strPool.Count(), 3);

        strPool.Build();

        TVector<TString> strings;
        TVector<ui32> indexes;
        TVector<ui32> frequences;
        strPool.ForEach([&](TStringBuf str, ui32 index, ui32 freq) {
            strings.emplace_back(str);
            indexes.push_back(index);
            frequences.push_back(freq);
        });

        TVector<TString> expectedStrings = {"three", "two", "one"};
        UNIT_ASSERT_EQUAL(strings, expectedStrings);

        TVector<ui32> expectedIndexes = {0, 1, 2};
        UNIT_ASSERT_EQUAL(indexes, expectedIndexes);

        TVector<ui32> expectedFrequences = {3, 2, 1};
        UNIT_ASSERT_EQUAL(frequences, expectedFrequences);
    }
}
