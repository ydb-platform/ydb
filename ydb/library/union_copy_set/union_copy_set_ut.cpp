#include "union_copy_set.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/shuffle.h>

#include <unordered_set>

namespace NKikimr {

Y_UNIT_TEST_SUITE(UnionCopySet) {

    struct TMyValue : public TUnionCopySet<TMyValue>::TItem {
        // nothing
    };

    std::unordered_set<TMyValue*> Enumerate(const TUnionCopySet<TMyValue>& s) {
        std::unordered_set<TMyValue*> values;
        s.ForEachValue([&](TMyValue* value) {
            values.insert(value);
            return true;
        });
        return values;
    }

    size_t Count(const TUnionCopySet<TMyValue>& s) {
        size_t count = 0;
        s.ForEachValue([&](TMyValue*) {
            ++count;
            return true;
        });
        return count;
    }

    std::unordered_set<TMyValue*> MakeSet(std::initializer_list<TMyValue*> l) {
        return std::unordered_set<TMyValue*>(l);
    }

    Y_UNIT_TEST(Simple) {
        TMyValue a, b, c;
        TUnionCopySet<TMyValue> set1, set2, set3;
        set1.Add(&a);
        set2.Add(&b);
        set3.Add(set2);
        set3.Add(&c);
        UNIT_ASSERT(Enumerate(set1) == MakeSet({&a}));
        UNIT_ASSERT(Enumerate(set2) == MakeSet({&b}));
        UNIT_ASSERT(Enumerate(set3) == MakeSet({&b, &c}));
        set2.Clear();
        UNIT_ASSERT(Enumerate(set2) == MakeSet({}));
        UNIT_ASSERT(Enumerate(set3) == MakeSet({&b, &c}));
        auto set4 = set3;
        UNIT_ASSERT(Enumerate(set4) == MakeSet({&b, &c}));
        set4.Add(&a);
        UNIT_ASSERT(Enumerate(set4) == MakeSet({&a, &b, &c}));
        auto set5 = std::move(set4);
        UNIT_ASSERT(Enumerate(set4) == MakeSet({}));
        UNIT_ASSERT(Enumerate(set5) == MakeSet({&a, &b, &c}));
        set4.Add(&a);
        set4 = set5;
        UNIT_ASSERT(Enumerate(set4) == MakeSet({&a, &b, &c}));
        set4.Clear();
        set4.Add(&a);
        set4 = std::move(set5);
        UNIT_ASSERT(Enumerate(set4) == MakeSet({&a, &b, &c}));
        UNIT_ASSERT(Enumerate(set5) == MakeSet({}));
    }

    Y_UNIT_TEST(ItemAddedToManySets) {
        TMyValue value;
        TUnionCopySet<TMyValue> set1, set2, set3;
        set1.Add(&value);
        set2.Add(&value);
        set3.Add(&value);
        UNIT_ASSERT(Enumerate(set1) == MakeSet({&value}));
        UNIT_ASSERT(Enumerate(set2) == MakeSet({&value}));
        UNIT_ASSERT(Enumerate(set3) == MakeSet({&value}));
    }

    Y_UNIT_TEST(SetAddedToManySets) {
        TMyValue value;
        TUnionCopySet<TMyValue> set1, set2, set3;
        set1.Add(&value);
        set2.Add(set1);
        set3.Add(set1);
        UNIT_ASSERT(Enumerate(set1) == MakeSet({&value}));
        UNIT_ASSERT(Enumerate(set2) == MakeSet({&value}));
        UNIT_ASSERT(Enumerate(set3) == MakeSet({&value}));
    }

    Y_UNIT_TEST(NotDisjoint) {
        TVector<TMyValue> values;
        values.emplace_back();
        values.emplace_back();
        TUnionCopySet<TMyValue> set;
        set.Add(&values[0]);
        set.Add(&values[0]);
        set.Add(&values[0]);
        set.Add(&values[1]);
        set.Add(&values[1]);
        set.Add(&values[1]);
        UNIT_ASSERT_VALUES_EQUAL(Count(set), 6u);
        values.pop_back();
        UNIT_ASSERT_VALUES_EQUAL(Count(set), 3u);
        set.Clear();
    }

    Y_UNIT_TEST(NotDisjointOptimizeCopyPairWhileDestroying) {
        TVector<TMyValue> values;
        values.emplace_back();
        TUnionCopySet<TMyValue> set;
        set.Add(&values[0]);
        set.Add(&values[0]);
        TUnionCopySet<TMyValue> set1, set2, set3;
        set1.Add(set);
        set2.Add(set);
        set3.Add(set);
        // We should now have: (4 sets) <-> copy node <-> union node <-> (2 links) <-> copy node <-> value
        // Destroying the value will trigger redundant union node optimization,
        // which has links to a copy node pair, and where the smaller node is
        // under destruction.
        values.pop_back();
    }

    Y_UNIT_TEST(NotDisjointOptimizeUnionPairWhileDestroying) {
        TVector<TMyValue> values;
        values.resize(4);
        TUnionCopySet<TMyValue> set;
        set.Add(&values[0]);
        set.Add(&values[1]);
        set.Add(&values[2]);
        set.Add(&values[3]);
        // We have: set <-> union node <-> 4 values
        // Making a copy produces: 2 sets <-> copy node <-> union node <-> 4 values
        TUnionCopySet<TMyValue> copy = set;
        // When we add the set again a union node is added with a second link to union node
        copy.Add(set);
        // Clearning the original set will leave:
        // copy <-> union node <-> (2 links) <-> copy node <-> union node <-> 4 values
        set.Clear();
        // Clearning the copy will trigger redundant copy node optimization,
        // which has links to a union node pair, and where the smaller node is
        // under destruction.
        copy.Clear();
    }

    Y_UNIT_TEST(MoveAdd) {
        TVector<TMyValue> values;
        TUnionCopySet<TMyValue> set1;
        TUnionCopySet<TMyValue> set2;
        TUnionCopySet<TMyValue> set3;

        // Adding empty set to an empty set
        set1.Add(std::move(set2));
        UNIT_ASSERT_VALUES_EQUAL(Count(set1), 0u);
        UNIT_ASSERT_VALUES_EQUAL(Count(set2), 0u);

        // Test adding existing values to an empty set
        set2.Add(&values.emplace_back());
        set1.Add(std::move(set2));
        UNIT_ASSERT_VALUES_EQUAL(Count(set1), 1u);
        UNIT_ASSERT_VALUES_EQUAL(Count(set2), 0u);

        // Testing when moving makes a set into a union
        set3.Add(&values.emplace_back());
        set1.Add(std::move(set3));
        UNIT_ASSERT_VALUES_EQUAL(Count(set1), 2u);
        UNIT_ASSERT_VALUES_EQUAL(Count(set3), 0u);

        // Test adding a union to a non-union
        set2.Add(&values.emplace_back());
        set2.Add(std::move(set1));
        UNIT_ASSERT_VALUES_EQUAL(Count(set2), 3u);
        UNIT_ASSERT_VALUES_EQUAL(Count(set1), 0u);

        // Testing adding union to a union
        set1.Add(&values.emplace_back());
        set1.Add(&values.emplace_back());
        set1.Add(std::move(set2));
        UNIT_ASSERT_VALUES_EQUAL(Count(set1), 5u);
        UNIT_ASSERT_VALUES_EQUAL(Count(set2), 0u);
    }

    Y_UNIT_TEST(StressDestroyUp) {
        size_t n = 1'000'000;
        TVector<TMyValue> items;
        items.resize(n);
        TVector<TUnionCopySet<TMyValue>> sets;
        sets.resize(n);
        TVector<size_t> indexes;
        indexes.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            indexes.push_back(i);
        }
        ShuffleRange(indexes);
        for (size_t i = 0; i < n; ++i) {
            if (i > 0) {
                sets[i].Add(sets[i-1]);
            }
            sets[i].Add(&items[indexes[i]]);
        }
        // The last set must contain all values
        UNIT_ASSERT_VALUES_EQUAL(Count(sets.back()), n);
        while (!items.empty()) {
            items.pop_back();
        }
    }

    Y_UNIT_TEST(StressDestroyDown) {
        size_t n = 1'000'000;
        TVector<TMyValue> items;
        items.resize(n);
        TVector<TUnionCopySet<TMyValue>> sets;
        sets.resize(n);
        for (size_t i = 0; i < n; ++i) {
            sets[i].Add(&items[i]);
            if (i > 0) {
                sets[i].Add(sets[i-1]);
            }
        }
        // The last set must contain all values
        UNIT_ASSERT_VALUES_EQUAL(Count(sets.back()), n);
        TVector<size_t> indexes;
        indexes.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            indexes.push_back(i);
        }
        ShuffleRange(indexes);
        for (size_t i : indexes) {
            sets[i].Clear();
        }
    }

    Y_UNIT_TEST(MovingSetsAndValues) {
        TVector<TMyValue> values;
        TVector<TUnionCopySet<TMyValue>> sets;
        for (size_t i = 0; i < 1024; ++i) {
            // Sets and values will be move constructed when vector capacity changes
            sets.emplace_back().Add(&values.emplace_back());
            // Make sets and values more complex after some simple moves
            if (i >= 128) {
                sets.back().Add(sets[i - 128]);
            }
        }
    }
}

} // namespace NKikimr
