#include "checkpoint_map.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>

namespace NYql {

Y_UNIT_TEST_SUITE(CheckpointHashMapTests) {
Y_UNIT_TEST(BasicOperations) {
    TCheckpointHashMap<ui32, TString> map;

    // Check empty map
    UNIT_ASSERT(map.empty());
    UNIT_ASSERT_VALUES_EQUAL(0, map.size());

    // Add elements
    map.Set(1, "one");
    map.Set(2, "two");

    UNIT_ASSERT(!map.empty());
    UNIT_ASSERT_VALUES_EQUAL(2, map.size());

    // Check values using find
    auto it1 = map.find(1);
    UNIT_ASSERT(it1 != map.end());
    UNIT_ASSERT_VALUES_EQUAL("one", it1->second);

    auto it2 = map.find(2);
    UNIT_ASSERT(it2 != map.end());
    UNIT_ASSERT_VALUES_EQUAL("two", it2->second);
}

Y_UNIT_TEST(CheckpointAndRollback) {
    TCheckpointHashMap<ui32, TString> map;

    // Add initial data
    map.Set(1, "one");
    map.Set(2, "two");

    // Make checkpoint
    map.Checkpoint();
    UNIT_ASSERT_VALUES_EQUAL(0, map.GetCommandCount());

    // Modify data after checkpoint
    map.Set(1, "ONE");
    map.Set(3, "three");
    map.erase(2);

    UNIT_ASSERT_VALUES_EQUAL(3, map.GetCommandCount());

    auto it1 = map.find(1);
    UNIT_ASSERT(it1 != map.end());
    UNIT_ASSERT_VALUES_EQUAL("ONE", it1->second);

    auto it3 = map.find(3);
    UNIT_ASSERT(it3 != map.end());
    UNIT_ASSERT_VALUES_EQUAL("three", it3->second);

    UNIT_ASSERT_VALUES_EQUAL(2, map.size());
    UNIT_ASSERT(map.find(2) == map.end());

    // Rollback
    map.Rollback();

    UNIT_ASSERT_VALUES_EQUAL(0, map.GetCommandCount());

    it1 = map.find(1);
    UNIT_ASSERT(it1 != map.end());
    UNIT_ASSERT_VALUES_EQUAL("one", it1->second);

    auto it2 = map.find(2);
    UNIT_ASSERT(it2 != map.end());
    UNIT_ASSERT_VALUES_EQUAL("two", it2->second);

    UNIT_ASSERT_VALUES_EQUAL(2, map.size());
    UNIT_ASSERT(map.find(3) == map.end());
}

Y_UNIT_TEST(SetOperations) {
    TCheckpointHashMap<ui32, TString> map;

    map.Checkpoint();

    // Test Set
    map.Set(1, "one");
    auto it1 = map.find(1);
    UNIT_ASSERT(it1 != map.end());
    UNIT_ASSERT_VALUES_EQUAL("one", it1->second);

    // Overwrite existing key
    map.Set(1, "ONE");
    it1 = map.find(1);
    UNIT_ASSERT(it1 != map.end());
    UNIT_ASSERT_VALUES_EQUAL("ONE", it1->second);

    // Test Set with another key
    map.Set(2, "two");
    auto it2 = map.find(2);
    UNIT_ASSERT(it2 != map.end());
    UNIT_ASSERT_VALUES_EQUAL("two", it2->second);

    UNIT_ASSERT_VALUES_EQUAL(2, map.size());

    // Rollback
    map.Rollback();
    UNIT_ASSERT(map.empty());
}

Y_UNIT_TEST(EraseOperations) {
    TCheckpointHashMap<ui32, TString> map;

    // Prepare data
    map.Set(1, "one");
    map.Set(2, "two");
    map.Set(3, "three");

    map.Checkpoint();

    // Erase by key
    UNIT_ASSERT_VALUES_EQUAL(1, map.erase(2));
    UNIT_ASSERT_VALUES_EQUAL(0, map.erase(10)); // non-existent key

    // Erase by key
    UNIT_ASSERT_VALUES_EQUAL(1, map.erase(3));

    UNIT_ASSERT_VALUES_EQUAL(1, map.size());
    auto it1 = map.find(1);
    UNIT_ASSERT(it1 != map.end());
    UNIT_ASSERT_VALUES_EQUAL("one", it1->second);

    // Rollback
    map.Rollback();

    UNIT_ASSERT_VALUES_EQUAL(3, map.size());
    it1 = map.find(1);
    UNIT_ASSERT(it1 != map.end());
    UNIT_ASSERT_VALUES_EQUAL("one", it1->second);

    auto it2 = map.find(2);
    UNIT_ASSERT(it2 != map.end());
    UNIT_ASSERT_VALUES_EQUAL("two", it2->second);

    auto it3 = map.find(3);
    UNIT_ASSERT(it3 != map.end());
    UNIT_ASSERT_VALUES_EQUAL("three", it3->second);
}

Y_UNIT_TEST(SearchOperations) {
    TCheckpointHashMap<ui32, TString> map;

    map.Set(1, "one");
    map.Set(2, "two");

    // Test find
    auto it1 = map.find(1);
    UNIT_ASSERT(it1 != map.end());
    UNIT_ASSERT_VALUES_EQUAL("one", it1->second);

    auto it2 = map.find(10);
    UNIT_ASSERT(it2 == map.end());

    // Test count
    UNIT_ASSERT_VALUES_EQUAL(1, map.count(1));
    UNIT_ASSERT_VALUES_EQUAL(0, map.count(10));

    // Test contains
    UNIT_ASSERT(map.contains(1));
    UNIT_ASSERT(!map.contains(10));
}

Y_UNIT_TEST(Iterators) {
    TCheckpointHashMap<ui32, TString> map;

    map.Set(1, "one");
    map.Set(2, "two");
    map.Set(3, "three");

    // Test iterators
    ui32 count = 0;
    for (auto it = map.cbegin(); it != map.cend(); ++it) {
        count++;
        UNIT_ASSERT(it->first >= 1 && it->first <= 3);
    }
    UNIT_ASSERT_VALUES_EQUAL(3, count);

    // Test const iterators
    const auto& constMap = map;
    count = 0;
    for (auto it = constMap.cbegin(); it != constMap.cend(); ++it) {
        count++;
    }
    UNIT_ASSERT_VALUES_EQUAL(3, count);

    // Test range-based for
    count = 0;
    for (const auto& pair : map) {
        count++;
        UNIT_ASSERT(pair.first >= 1 && pair.first <= 3);
    }
    UNIT_ASSERT_VALUES_EQUAL(3, count);
}

Y_UNIT_TEST(ComparisonOperators) {
    TCheckpointHashMap<ui32, TString> map1;
    TCheckpointHashMap<ui32, TString> map2;

    // Empty maps are equal
    UNIT_ASSERT(map1 == map2);
    UNIT_ASSERT(!(map1 != map2));

    // Add data to first map
    map1.Set(1, "one");
    UNIT_ASSERT(!(map1 == map2));
    UNIT_ASSERT(map1 != map2);

    // Add same data to second map
    map2.Set(1, "one");
    UNIT_ASSERT(map1 == map2);
    UNIT_ASSERT(!(map1 != map2));
}

Y_UNIT_TEST(MultipleCheckpointsAndRollbacks) {
    TCheckpointHashMap<ui32, TString> map;

    // State 1
    map.Set(1, "one");
    map.Checkpoint();

    // State 2
    map.Set(2, "two");
    map.Checkpoint();

    // State 3
    map.Set(3, "three");
    map.Set(1, "ONE");

    UNIT_ASSERT_VALUES_EQUAL(3, map.size());
    auto it1 = map.find(1);
    UNIT_ASSERT(it1 != map.end());
    UNIT_ASSERT_VALUES_EQUAL("ONE", it1->second);

    // Rollback to state 2
    map.Rollback();
    UNIT_ASSERT_VALUES_EQUAL(2, map.size());
    it1 = map.find(1);
    UNIT_ASSERT(it1 != map.end());
    UNIT_ASSERT_VALUES_EQUAL("one", it1->second);

    auto it2 = map.find(2);
    UNIT_ASSERT(it2 != map.end());
    UNIT_ASSERT_VALUES_EQUAL("two", it2->second);
    UNIT_ASSERT(map.find(3) == map.end());

    // New changes
    map.Set(4, "four");
    map.erase(1);

    // Verify state after changes
    UNIT_ASSERT_VALUES_EQUAL(2, map.size());
    it2 = map.find(2);
    UNIT_ASSERT(it2 != map.end());
    UNIT_ASSERT_VALUES_EQUAL("two", it2->second);

    auto it4 = map.find(4);
    UNIT_ASSERT(it4 != map.end());
    UNIT_ASSERT_VALUES_EQUAL("four", it4->second);
    UNIT_ASSERT(map.find(1) == map.end());

    // Rollback to state 1
    map.Rollback();
    UNIT_ASSERT_VALUES_EQUAL(1, map.size());
    it1 = map.find(1);
    UNIT_ASSERT(it1 != map.end());
    UNIT_ASSERT_VALUES_EQUAL("one", it1->second);
    UNIT_ASSERT(map.find(2) == map.end());
    UNIT_ASSERT(map.find(4) == map.end());
}

Y_UNIT_TEST(ComplexScenario) {
    TCheckpointHashMap<TString, ui32> map;

    // Initial state
    map.Set("a", 1);
    map.Set("b", 2);
    map.Set("c", 3);
    map.Checkpoint();

    // Complex changes
    map.Set("a", 10); // update
    map.Set("d", 4);  // new element
    map.erase("b");   // deletion
    map.Set("e", 5);  // another new element
    map.Set("c", 30); // another update

    UNIT_ASSERT_VALUES_EQUAL(4, map.size());
    auto ita = map.find("a");
    UNIT_ASSERT(ita != map.end());
    UNIT_ASSERT_VALUES_EQUAL(10, ita->second);

    auto itc = map.find("c");
    UNIT_ASSERT(itc != map.end());
    UNIT_ASSERT_VALUES_EQUAL(30, itc->second);

    auto itd = map.find("d");
    UNIT_ASSERT(itd != map.end());
    UNIT_ASSERT_VALUES_EQUAL(4, itd->second);

    auto ite = map.find("e");
    UNIT_ASSERT(ite != map.end());
    UNIT_ASSERT_VALUES_EQUAL(5, ite->second);
    UNIT_ASSERT(map.find("b") == map.end());

    // Rollback
    map.Rollback();

    UNIT_ASSERT_VALUES_EQUAL(3, map.size());
    ita = map.find("a");
    UNIT_ASSERT(ita != map.end());
    UNIT_ASSERT_VALUES_EQUAL(1, ita->second);

    auto itb = map.find("b");
    UNIT_ASSERT(itb != map.end());
    UNIT_ASSERT_VALUES_EQUAL(2, itb->second);

    itc = map.find("c");
    UNIT_ASSERT(itc != map.end());
    UNIT_ASSERT_VALUES_EQUAL(3, itc->second);
    UNIT_ASSERT(map.find("d") == map.end());
    UNIT_ASSERT(map.find("e") == map.end());
}

Y_UNIT_TEST(EmptyMapOperations) {
    TCheckpointHashMap<ui32, TString> map;

    map.Checkpoint();

    // Operations on empty map
    UNIT_ASSERT_VALUES_EQUAL(0, map.erase(1));
    UNIT_ASSERT(map.find(1) == map.end());
    UNIT_ASSERT_VALUES_EQUAL(0, map.count(1));
    UNIT_ASSERT(!map.contains(1));

    map.Rollback();
    UNIT_ASSERT(map.empty());
}

Y_UNIT_TEST(RollbackWithNoChanges) {
    TCheckpointHashMap<ui32, TString> map;

    map.Checkpoint();
    map.Checkpoint();
    map.Set(1, "string");
    map.Checkpoint();
    UNIT_ASSERT_VALUES_EQUAL(1, map.size());
    map.Rollback();
    UNIT_ASSERT_VALUES_EQUAL(1, map.size());
    map.Rollback();
    UNIT_ASSERT_VALUES_EQUAL(0, map.size());
    map.Rollback();
    UNIT_ASSERT(map.empty());
}

Y_UNIT_TEST(RandomOperationsTest) {
    // Test constants
    constexpr ui32 initialSize = 20;
    constexpr ui32 numOperations = 100;
    constexpr ui32 keyRange = 50;
    constexpr ui32 valueRange = 1000;
    constexpr ui32 initialValueMultiplier = 10;

    enum class EOperation {
        Insert = 0,
        Erase = 1,
        Update = 2,
        Count = 3
    };

    TCheckpointHashMap<ui32, ui32> map;
    THashMap<ui32, ui32> referenceMap;

    // Fill initial data
    for (ui32 i = 0; i < initialSize; ++i) {
        ui32 value = i * initialValueMultiplier;
        map.Set(i, value);
        referenceMap[i] = value;
    }

    // Save reference state before checkpoint
    auto savedState = referenceMap;
    map.Checkpoint();

    // Perform random operations
    for (ui32 op = 0; op < numOperations; ++op) {
        auto operation = static_cast<EOperation>(RandomNumber<ui32>() % static_cast<ui32>(EOperation::Count));
        ui32 key = RandomNumber<ui32>() % keyRange;

        switch (operation) {
            case EOperation::Insert: {
                ui32 value = RandomNumber<ui32>() % valueRange;
                map.Set(key, value);
                referenceMap[key] = value;
                break;
            }
            case EOperation::Erase: {
                map.erase(key);
                referenceMap.erase(key);
                break;
            }
            case EOperation::Update: {
                auto it = referenceMap.find(key);
                if (it != referenceMap.end()) {
                    ui32 newValue = RandomNumber<ui32>() % valueRange;
                    map.Set(key, newValue);
                    referenceMap[key] = newValue;
                }
                break;
            }
            case EOperation::Count:
                // Should not reach here
                break;
        }
    }

    // Verify current state matches reference
    UNIT_ASSERT_VALUES_EQUAL(map.size(), referenceMap.size());
    for (const auto& pair : referenceMap) {
        UNIT_ASSERT(map.contains(pair.first));
        auto it = map.find(pair.first);
        UNIT_ASSERT(it != map.end());
        UNIT_ASSERT_VALUES_EQUAL(it->second, pair.second);
    }

    // Rollback and verify state matches saved state
    map.Rollback();

    UNIT_ASSERT_VALUES_EQUAL(map.size(), savedState.size());
    for (const auto& pair : savedState) {
        UNIT_ASSERT(map.contains(pair.first));
        auto it = map.find(pair.first);
        UNIT_ASSERT(it != map.end());
        UNIT_ASSERT_VALUES_EQUAL(it->second, pair.second);
    }

    // Verify no extra elements exist
    for (const auto& pair : map) {
        UNIT_ASSERT(savedState.find(pair.first) != savedState.end());
    }
}

Y_UNIT_TEST(MultipleCheckpointsAndRollbacksTest) {
    TCheckpointHashMap<ui32, TString> map;

    // Initial state
    map.Set(1, "initial1");
    map.Set(2, "initial2");

    // First checkpoint
    map.Checkpoint();
    map.Set(1, "checkpoint1_1");
    map.Set(3, "checkpoint1_3");

    // Second checkpoint
    map.Checkpoint();
    map.Set(2, "checkpoint2_2");
    map.Set(4, "checkpoint2_4");

    // Third checkpoint
    map.Checkpoint();
    map.Set(1, "checkpoint3_1");
    map.erase(3);

    // Verify current state
    UNIT_ASSERT_VALUES_EQUAL(3, map.size());
    UNIT_ASSERT_VALUES_EQUAL("checkpoint3_1", map.Get(1));
    UNIT_ASSERT_VALUES_EQUAL("checkpoint2_2", map.Get(2));
    UNIT_ASSERT_VALUES_EQUAL("checkpoint2_4", map.Get(4));
    UNIT_ASSERT(map.find(3) == map.end());

    // First rollback (undo checkpoint 3)
    map.Rollback();
    UNIT_ASSERT_VALUES_EQUAL(4, map.size());
    UNIT_ASSERT_VALUES_EQUAL("checkpoint1_1", map.Get(1));
    UNIT_ASSERT_VALUES_EQUAL("checkpoint2_2", map.Get(2));
    UNIT_ASSERT_VALUES_EQUAL("checkpoint1_3", map.Get(3));
    UNIT_ASSERT_VALUES_EQUAL("checkpoint2_4", map.Get(4));

    // Second rollback (undo checkpoint 2)
    map.Rollback();
    UNIT_ASSERT_VALUES_EQUAL(3, map.size());
    UNIT_ASSERT_VALUES_EQUAL("checkpoint1_1", map.Get(1));
    UNIT_ASSERT_VALUES_EQUAL("initial2", map.Get(2));
    UNIT_ASSERT_VALUES_EQUAL("checkpoint1_3", map.Get(3));
    UNIT_ASSERT(map.find(4) == map.end());

    // Third rollback (undo checkpoint 1)
    map.Rollback();
    UNIT_ASSERT_VALUES_EQUAL(2, map.size());
    UNIT_ASSERT_VALUES_EQUAL("initial1", map.Get(1));
    UNIT_ASSERT_VALUES_EQUAL("initial2", map.Get(2));
    UNIT_ASSERT(map.find(3) == map.end());
    UNIT_ASSERT(map.find(4) == map.end());

    // Test multiple checkpoints without rollback
    map.Checkpoint();
    map.Set(5, "test5");
    map.Checkpoint();
    map.Set(6, "test6");
    map.Checkpoint();
    map.Set(7, "test7");

    UNIT_ASSERT_VALUES_EQUAL(5, map.size());

    // Multiple rollbacks
    map.Rollback(); // Remove 7
    UNIT_ASSERT_VALUES_EQUAL(4, map.size());
    UNIT_ASSERT(map.find(7) == map.end());

    map.Rollback(); // Remove 6
    UNIT_ASSERT_VALUES_EQUAL(3, map.size());
    UNIT_ASSERT(map.find(6) == map.end());

    map.Rollback(); // Remove 5
    UNIT_ASSERT_VALUES_EQUAL(2, map.size());
    UNIT_ASSERT(map.find(5) == map.end());
}

Y_UNIT_TEST(RollbackWithoutCheckpointTest) {
    TCheckpointHashMap<ui32, TString> map;

    // Try to rollback without checkpoint - should throw
    UNIT_ASSERT_EXCEPTION_CONTAINS(map.Rollback(), yexception, "Cannot rollback without checkpoint");

    // Add some data and try rollback again - should still throw
    map.Set(1, "test");
    UNIT_ASSERT_EXCEPTION_CONTAINS(map.Rollback(), yexception, "Cannot rollback without checkpoint");

    // Now make checkpoint and rollback should work
    map.Checkpoint();
    map.Set(2, "test2");
    map.Rollback(); // Should not throw

    // After rollback, another rollback should throw
    UNIT_ASSERT_EXCEPTION_CONTAINS(map.Rollback(), yexception, "Cannot rollback without checkpoint");
}

Y_UNIT_TEST(GetUnderlyingMapTest) {
    TCheckpointHashMap<ui32, TString> map;

    map.Set(1, "one");
    map.Set(2, "two");

    const auto& underlying = map.GetUnderlyingMap();
    UNIT_ASSERT_VALUES_EQUAL(2, underlying.size());
    UNIT_ASSERT(underlying.find(1) != underlying.end());
    UNIT_ASSERT(underlying.find(2) != underlying.end());
    UNIT_ASSERT_VALUES_EQUAL("one", underlying.at(1));
    UNIT_ASSERT_VALUES_EQUAL("two", underlying.at(2));
}

Y_UNIT_TEST(GetCommandCountTest) {
    TCheckpointHashMap<ui32, TString> map;

    // No checkpoint - command count should be 0
    UNIT_ASSERT_VALUES_EQUAL(0, map.GetCommandCount());

    map.Set(1, "one");
    UNIT_ASSERT_VALUES_EQUAL(0, map.GetCommandCount()); // Still no checkpoint

    map.Checkpoint();
    UNIT_ASSERT_VALUES_EQUAL(0, map.GetCommandCount()); // Just after checkpoint

    map.Set(2, "two");
    UNIT_ASSERT_VALUES_EQUAL(1, map.GetCommandCount());

    map.Set(3, "three");
    UNIT_ASSERT_VALUES_EQUAL(2, map.GetCommandCount());

    map.erase(1);
    UNIT_ASSERT_VALUES_EQUAL(3, map.GetCommandCount());

    map.Rollback();
    UNIT_ASSERT_VALUES_EQUAL(0, map.GetCommandCount()); // After rollback
}

Y_UNIT_TEST(OperationsWithoutCheckpointTest) {
    TCheckpointHashMap<ui32, TString> map;

    // Operations without checkpoint should not record commands
    map.Set(1, "one");
    map.Set(2, "two");
    map.erase(1);
    map.Set(3, "three");

    UNIT_ASSERT_VALUES_EQUAL(0, map.GetCommandCount());
    UNIT_ASSERT_VALUES_EQUAL(2, map.size());
    UNIT_ASSERT_VALUES_EQUAL("two", map.Get(2));
    UNIT_ASSERT_VALUES_EQUAL("three", map.Get(3));

    // After checkpoint, operations should record commands
    map.Checkpoint();
    UNIT_ASSERT_VALUES_EQUAL(0, map.GetCommandCount());

    map.Set(4, "four");
    UNIT_ASSERT_VALUES_EQUAL(1, map.GetCommandCount());

    map.Set(2, "TWO");
    UNIT_ASSERT_VALUES_EQUAL(2, map.GetCommandCount());

    // Rollback should work
    map.Rollback();
    UNIT_ASSERT_VALUES_EQUAL(0, map.GetCommandCount());
    UNIT_ASSERT_VALUES_EQUAL(2, map.size());
    UNIT_ASSERT_VALUES_EQUAL("two", map.Get(2));
    UNIT_ASSERT(map.find(4) == map.end());
}

Y_UNIT_TEST(RollbackThenModifyWithoutCheckpointTest) {
    TCheckpointHashMap<ui32, TString> map;

    // Initial state
    map.Set(1, "one");
    map.Set(2, "two");
    map.Set(3, "three");

    // Make checkpoint
    map.Checkpoint();
    map.Set(1, "ONE");
    map.Set(4, "four");
    map.erase(2);

    UNIT_ASSERT_VALUES_EQUAL(3, map.size());
    UNIT_ASSERT_VALUES_EQUAL("ONE", map.Get(1));
    UNIT_ASSERT_VALUES_EQUAL("three", map.Get(3));
    UNIT_ASSERT_VALUES_EQUAL("four", map.Get(4));
    UNIT_ASSERT(map.find(2) == map.end());

    // Rollback
    map.Rollback();
    UNIT_ASSERT_VALUES_EQUAL(3, map.size());
    UNIT_ASSERT_VALUES_EQUAL("one", map.Get(1));
    UNIT_ASSERT_VALUES_EQUAL("two", map.Get(2));
    UNIT_ASSERT_VALUES_EQUAL("three", map.Get(3));
    UNIT_ASSERT(map.find(4) == map.end());

    // Make changes AFTER rollback but WITHOUT new checkpoint
    map.Set(2, "TWO");
    map.Set(5, "five");
    map.erase(3);

    UNIT_ASSERT_VALUES_EQUAL(3, map.size());
    UNIT_ASSERT_VALUES_EQUAL("one", map.Get(1));
    UNIT_ASSERT_VALUES_EQUAL("TWO", map.Get(2));
    UNIT_ASSERT_VALUES_EQUAL("five", map.Get(5));
    UNIT_ASSERT(map.find(3) == map.end());

    // Try to rollback again - should throw because no active checkpoint
    UNIT_ASSERT_EXCEPTION_CONTAINS(map.Rollback(), yexception, "Cannot rollback without checkpoint");

    // State should remain unchanged after failed rollback
    UNIT_ASSERT_VALUES_EQUAL(3, map.size());
    UNIT_ASSERT_VALUES_EQUAL("one", map.Get(1));
    UNIT_ASSERT_VALUES_EQUAL("TWO", map.Get(2));
    UNIT_ASSERT_VALUES_EQUAL("five", map.Get(5));
    UNIT_ASSERT(map.find(3) == map.end());
}

Y_UNIT_TEST(RollbackThenCheckpointThenModifyThenRollbackTest) {
    TCheckpointHashMap<ui32, TString> map;

    // Initial state
    map.Set(1, "one");
    map.Set(2, "two");

    // First checkpoint
    map.Checkpoint();
    map.Set(1, "ONE");
    map.Set(3, "three");

    // First rollback
    map.Rollback();
    UNIT_ASSERT_VALUES_EQUAL(2, map.size());
    UNIT_ASSERT_VALUES_EQUAL("one", map.Get(1));
    UNIT_ASSERT_VALUES_EQUAL("two", map.Get(2));
    UNIT_ASSERT(map.find(3) == map.end());

    // Second checkpoint AFTER rollback
    map.Checkpoint();
    map.Set(2, "TWO");
    map.Set(4, "four");
    map.erase(1);

    UNIT_ASSERT_VALUES_EQUAL(2, map.size());
    UNIT_ASSERT_VALUES_EQUAL("TWO", map.Get(2));
    UNIT_ASSERT_VALUES_EQUAL("four", map.Get(4));
    UNIT_ASSERT(map.find(1) == map.end());

    // Second rollback
    map.Rollback();
    UNIT_ASSERT_VALUES_EQUAL(2, map.size());
    UNIT_ASSERT_VALUES_EQUAL("one", map.Get(1));
    UNIT_ASSERT_VALUES_EQUAL("two", map.Get(2));
    UNIT_ASSERT(map.find(3) == map.end());
    UNIT_ASSERT(map.find(4) == map.end());
}

Y_UNIT_TEST(CheckpointGuardTest) {
    TCheckpointHashMap<ui32, TString> map;

    // Initial state
    map.Set(1, "one");
    map.Set(2, "two");

    {
        // Guard automatically creates checkpoint
        TCheckpointGuard<ui32, TString> guard(map);

        // Make changes
        map.Set(1, "ONE");
        map.Set(3, "three");
        map.erase(2);

        UNIT_ASSERT_VALUES_EQUAL(2, map.size());
        UNIT_ASSERT_VALUES_EQUAL("ONE", map.Get(1));
        UNIT_ASSERT_VALUES_EQUAL("three", map.Get(3));
        UNIT_ASSERT(map.find(2) == map.end());

        // Guard destructor will automatically rollback
    }

    // After guard destruction, changes should be rolled back
    UNIT_ASSERT_VALUES_EQUAL(2, map.size());
    UNIT_ASSERT_VALUES_EQUAL("one", map.Get(1));
    UNIT_ASSERT_VALUES_EQUAL("two", map.Get(2));
    UNIT_ASSERT(map.find(3) == map.end());
}

} // Y_UNIT_TEST_SUITE(CheckpointHashMapTests)

} // namespace NYql
