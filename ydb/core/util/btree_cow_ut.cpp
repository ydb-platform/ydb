#include "btree_cow.h"

#include "ut_common.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>
#include <util/random/shuffle.h>
#include <util/system/mutex.h>
#include <util/system/event.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TCowBTreeTest) {

    Y_UNIT_TEST(Empty) {
        using TTree = TCowBTree<ui64, ui64>;

        TTree tree;

        UNIT_ASSERT(tree.Empty());
        UNIT_ASSERT_VALUES_EQUAL(tree.Size(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(tree.Height(), 0u);

        auto snapshot = tree.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Size(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Height(), 0u);

        auto it = snapshot.Iterator();
        UNIT_ASSERT_VALUES_EQUAL(it.Size(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(it.Height(), 0u);

        UNIT_ASSERT(!it.SeekFirst());
        UNIT_ASSERT(!it.SeekLast());
        UNIT_ASSERT(!it.SeekLowerBound(42));
        UNIT_ASSERT(!it.SeekUpperBound(42));
    }

    Y_UNIT_TEST(Basics) {
        using TTree = TCowBTree<ui64, ui64>;

        TTree tree;

        UNIT_ASSERT_VALUES_EQUAL(tree.Height(), 0u);
        UNIT_ASSERT(tree.Find(42) == nullptr);
        tree.EmplaceUnsafe(42, 123);
        UNIT_ASSERT_VALUES_EQUAL(tree.Height(), 1u);
        UNIT_ASSERT(tree.Find(42) != nullptr);

        // Tree now has a single item, it should be visible to iteration
        auto it = tree.Snapshot().Iterator();
        it.SeekFirst();
        UNIT_ASSERT(it.IsValid());
        UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), 42);
        UNIT_ASSERT_VALUES_EQUAL(it.GetValue(), 123);
        it.Next();
        UNIT_ASSERT(!it.IsValid());

        // New keys added to page tail must not be visible
        it = tree.Snapshot().Iterator();
        it.SeekFirst();
        UNIT_ASSERT(it.IsValid());
        UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), 42);
        UNIT_ASSERT_VALUES_EQUAL(it.GetValue(), 123);
        UNIT_ASSERT(tree.Find(51) == nullptr);
        tree.EmplaceUnsafe(51, 555);
        it.Next();
        UNIT_ASSERT(!it.IsValid());

        // New keys added to the tree must not be visible
        it = tree.Snapshot().Iterator();
        it.SeekFirst();
        UNIT_ASSERT(it.IsValid());
        UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), 42);
        UNIT_ASSERT_VALUES_EQUAL(it.GetValue(), 123);
        UNIT_ASSERT(tree.Find(45) == nullptr);
        tree.EmplaceUnsafe(45, 444);
        it.Next();
        UNIT_ASSERT(it.IsValid());
        UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), 51);
        UNIT_ASSERT_VALUES_EQUAL(it.GetValue(), 555);
        it.Next();
        UNIT_ASSERT(!it.IsValid());

        // New snapshot should grab new page version that has the new key
        it = tree.Snapshot().Iterator();
        it.SeekFirst();
        UNIT_ASSERT(it.IsValid());
        UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), 42);
        UNIT_ASSERT_VALUES_EQUAL(it.GetValue(), 123);
        it.Next();
        UNIT_ASSERT(it.IsValid());
        UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), 45);
        UNIT_ASSERT_VALUES_EQUAL(it.GetValue(), 444);
        it.Next();
        UNIT_ASSERT(it.IsValid());
        UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), 51);
        UNIT_ASSERT_VALUES_EQUAL(it.GetValue(), 555);
        it.Next();
        UNIT_ASSERT(!it.IsValid());

        // Duplicate keys should be rejected in Emplace
        UNIT_ASSERT(!tree.Emplace(42, 555));

        // We currently have keys 42, 45 and 51, add more keys until the first split
        ui64 lastKey = -1;
        for (ui64 key = 100; tree.Height() < 2; key += 2) {
            UNIT_ASSERT_C(tree.Emplace(key, ~key), "Failed to insert " << key);
            lastKey = key;
        }

        it = tree.Snapshot().Iterator();
        it.SeekFirst();
        UNIT_ASSERT(it.IsValid());
        UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), 42);
        UNIT_ASSERT_VALUES_EQUAL(it.GetValue(), 123);

        // We shouldn't see new keys added to non-current page
        UNIT_ASSERT(tree.Emplace(lastKey - 1, 12345));
        UNIT_ASSERT(tree.Emplace(lastKey + 1, 54321));

        TMap<ui64, ui64> seen;
        while (it.IsValid()) {
            seen[it.GetKey()] = it.GetValue();
            it.Next();
        }

        UNIT_ASSERT(!seen.contains(lastKey - 1));
        UNIT_ASSERT(!seen.contains(lastKey + 1));

        // Creating a new snapshot should make those values visible
        it = tree.Snapshot().Iterator();
        it.SeekFirst();

        seen.clear();
        while (it.IsValid()) {
            seen[it.GetKey()] = it.GetValue();
            it.Next();
        }

        UNIT_ASSERT(seen.contains(lastKey - 1));
        UNIT_ASSERT(seen.contains(lastKey + 1));
        UNIT_ASSERT_VALUES_EQUAL(seen[lastKey - 1], 12345);
        UNIT_ASSERT_VALUES_EQUAL(seen[lastKey + 1], 54321);
    }

    Y_UNIT_TEST(ClearAndReuse) {
        using TTree = TCowBTree<ui64, ui64>;

        TTree tree;

        auto it = tree.Snapshot().Iterator();
        UNIT_ASSERT(!it.SeekFirst());
        UNIT_ASSERT(!it.SeekLast());
        UNIT_ASSERT(!it.SeekLowerBound(42));
        UNIT_ASSERT(!it.SeekUpperBound(42));
        it = { };

        for (size_t i = 0; i < 10000; ++i) {
            tree.Emplace(i, i);
        }
        UNIT_ASSERT_VALUES_EQUAL(tree.Size(), 10000u);

        it = tree.Snapshot().Iterator();
        it.SeekFirst();
        for (size_t i = 0; i < 10000; ++i) {
            UNIT_ASSERT_C(it.IsValid(), "Expected " << i);
            UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), i);
            it.Next();
        }
        UNIT_ASSERT_C(!it.IsValid(), "Expected EOF");
        it = { };

        tree.Clear();
        UNIT_ASSERT_VALUES_EQUAL(tree.Size(), 0u);

        it = tree.Snapshot().Iterator();
        it.SeekFirst();
        UNIT_ASSERT_C(!it.IsValid(), "Expected empty tree");
        it.SeekLast();
        UNIT_ASSERT_C(!it.IsValid(), "Expected empty tree");
        it.SeekLowerBound(0);
        UNIT_ASSERT_C(!it.IsValid(), "Expected empty tree");

        for (size_t i = 100000; i < 110000; ++i) {
            tree.Emplace(i, i);
        }
        UNIT_ASSERT_VALUES_EQUAL(tree.Size(), 10000u);

        it = tree.Snapshot().Iterator();
        it.SeekFirst();
        for (size_t i = 100000; i < 110000; ++i) {
            UNIT_ASSERT_C(it.IsValid(), "Expected " << i);
            UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), i);
            it.Next();
        }
        UNIT_ASSERT_C(!it.IsValid(), "Expected EOF");
    }

    void DoSeekForwardPermutations(bool threadSafe) {
        using TTree = TCowBTree<ui64, ui64>;

        TTree tree;

        TTree::TSnapshot snapshot;

        for (ui64 key = 1u; key <= 10001u; key += 2) {
            if (threadSafe) {
                snapshot = tree.Snapshot(); // force copy-on-write
            }
            UNIT_ASSERT_C(
                tree.Emplace(key, ~key),
                "Failed to insert key " << key);
        }

        // ~5000 keys with ~32 keys per page should produce height 3 tree
        UNIT_ASSERT_VALUES_EQUAL(tree.Height(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(tree.Size(), 5001u);

        auto it = threadSafe ? tree.Snapshot().Iterator() : tree.UnsafeSnapshot().Iterator();
        for (ui64 i = 0; i <= 10010u; i += 2) {
            it.SeekLowerBound(i);
            UNIT_ASSERT_C(
                i <= 10001u ? (it.IsValid() && it.GetKey() == i + 1) : !it.IsValid(),
                "Found " << (it.IsValid() ? it.GetKey() : 0)
                << " while seeking lower bound to key " << i);

            it.SeekLowerBound(i + 1);
            UNIT_ASSERT_C(
                i + 1 <= 10001u ? (it.IsValid() && it.GetKey() == i + 1) : !it.IsValid(),
                "Found " << (it.IsValid() ? it.GetKey() : 0)
                << " while seeking lower bound to key " << (i + 1));

            it.SeekUpperBound(i);
            UNIT_ASSERT_C(
                i <= 10000u ? (it.IsValid() && it.GetKey() == i + 1) : !it.IsValid(),
                "Found " << (it.IsValid() ? it.GetKey() : 0)
                << " while seeking upper bound to key " << i);

            it.SeekUpperBound(i + 1);
            UNIT_ASSERT_C(
                i + 1 <= 10000u ? (it.IsValid() && it.GetKey() == i + 3) : !it.IsValid(),
                "Found " << (it.IsValid() ? it.GetKey() : 0)
                << " while seeking upper bound to key " << (i + 1));

            it.SeekExactFirst(i);
            UNIT_ASSERT_C(
                !it.IsValid(),
                "Found " << it.GetKey()
                << " while seeking exact to key " << i);

            it.SeekExactFirst(i + 1);
            UNIT_ASSERT_C(
                i + 1 <= 10001u ? (it.IsValid() && it.GetKey() == i + 1) : !it.IsValid(),
                "Found " << (it.IsValid() ? it.GetKey() : 0)
                << " while seeking exact to key " << (i + 1));
        }
    }

    Y_UNIT_TEST(SeekForwardPermutationsInplace) {
        DoSeekForwardPermutations(false);
    }

    Y_UNIT_TEST(SeekForwardPermutationsThreadSafe) {
        DoSeekForwardPermutations(true);
    }

    void DoSeekBackwardPermutations(bool threadSafe) {
        using TTree = TCowBTree<ui64, ui64>;

        TTree tree;

        TTree::TSnapshot snapshot;

        for (ui64 key = 1u; key <= 10001u; key += 2) {
            if (threadSafe) {
                snapshot = tree.Snapshot(); // force copy-on-write
            }
            UNIT_ASSERT_C(
                tree.Emplace(key, ~key),
                "Failed to insert key " << key);
        }

        // ~5000 keys with ~32 keys per page should produce height 3 tree
        UNIT_ASSERT_VALUES_EQUAL(tree.Height(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(tree.Size(), 5001u);

        auto it = threadSafe ? tree.Snapshot().Iterator() : tree.UnsafeSnapshot().Iterator();
        for (ui64 i = 0; i <= 10010u; i += 2) {
            it.SeekLowerBound(i, true);
            UNIT_ASSERT_C(
                i >= 2u ? (it.IsValid() && it.GetKey() == (i <= 10001u ? i - 1 : 10001u)) : !it.IsValid(),
                "Found " << (it.IsValid() ? it.GetKey() : 0)
                << " while seeking lower bound (backwards) to key " << i);

            it.SeekLowerBound(i + 1, true);
            UNIT_ASSERT_C(
                i + 1 >= 3u ? (it.IsValid() && it.GetKey() == (i + 1 <= 10001u ? i - 1 : 10001u)) : !it.IsValid(),
                "Found " << (it.IsValid() ? it.GetKey() : 0)
                << " while seeking lower bound (backwards) to key " << (i + 1));

            it.SeekUpperBound(i, true);
            UNIT_ASSERT_C(
                i >= 2u ? (it.IsValid() && it.GetKey() == (i <= 10001u ? i - 1 : 10001u)) : !it.IsValid(),
                "Found " << (it.IsValid() ? it.GetKey() : 0)
                << " while seeking upper bound (backwards) to key " << i);

            it.SeekUpperBound(i + 1, true);
            UNIT_ASSERT_C(
                i + 1 >= 1 ? (it.IsValid() && it.GetKey() == (i + 1 <= 10001u ? i + 1 : 10001u)) : !it.IsValid(),
                "Found " << (it.IsValid() ? it.GetKey() : 0)
                << " while seeking upper bound (backwards) to key " << (i + 1));

            it.SeekExact(i);
            UNIT_ASSERT_C(
                !it.IsValid(),
                "Found " << it.GetKey()
                << " while seeking exact (backwards) to key " << i);

            it.SeekExact(i + 1);
            UNIT_ASSERT_C(
                i + 1 <= 10001u ? (it.IsValid() && it.GetKey() == i + 1) : !it.IsValid(),
                "Found " << (it.IsValid() ? it.GetKey() : 0)
                << " while seeking exact (backwards) to key " << (i + 1));
        }
    }

    Y_UNIT_TEST(SeekBackwardPermutationsInplace) {
        DoSeekBackwardPermutations(false);
    }

    Y_UNIT_TEST(SeekBackwardPermutationsThreadSafe) {
        DoSeekBackwardPermutations(true);
    }

    void DoRandomInsert(bool threadSafe) {
        using TTree = TCowBTree<ui64, ui64>;

        TTree tree;

        TTree::TSnapshot snapshot;

        std::map<ui64, ui64> keys;
        size_t count = NSan::PlainOrUnderSanitizer(1000000u, 200000u);

        for (size_t i = 0; i < count; ++i) {
            ui64 key = RandomNumber<ui64>();
            ui64 value = RandomNumber<ui64>();
            if (keys.emplace(key, value).second) {
                if (threadSafe) {
                    snapshot = tree.Snapshot(); // force copy-on-write
                }
                UNIT_ASSERT_C(
                    tree.Emplace(key, value),
                    "Unexpected failure to insert key " << key);
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(tree.Size(), keys.size());

        for (const auto& kv : keys) {
            const ui64* value = tree.Find(kv.first);
            UNIT_ASSERT_C(value, "Unexpected failure to find key " << kv.first);
            UNIT_ASSERT_VALUES_EQUAL(*value, kv.second);
        }

        // Verify forward iteration
        auto it = threadSafe ? tree.Snapshot().Iterator() : tree.UnsafeSnapshot().Iterator();
        it.SeekFirst();
        for (auto p = keys.begin(); p != keys.end(); ++p) {
            UNIT_ASSERT_C(it.IsValid(), "Invalid iterator, expected key " << p->first);
            UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), p->first);
            UNIT_ASSERT_VALUES_EQUAL(it.GetValue(), p->second);
            it.Next();
        }
        UNIT_ASSERT_C(!it.IsValid(), "Unexpected key " << it.GetKey() << ", expected end of iteration");

        // Verify backward iteration
        it.SeekLast();
        for (auto p = keys.rbegin(); p != keys.rend(); ++p) {
            UNIT_ASSERT_C(it.IsValid(), "Invalid iterator, expected key " << p->first);
            UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), p->first);
            UNIT_ASSERT_VALUES_EQUAL(it.GetValue(), p->second);
            it.Prev();
        }
        UNIT_ASSERT_C(!it.IsValid(), "Unexpected key " << it.GetKey() << ", expected end of iteration");
    }

    Y_UNIT_TEST(RandomInsertInplace) {
        DoRandomInsert(false);
    }

    Y_UNIT_TEST(RandomInsertThreadSafe) {
        DoRandomInsert(true);
    }

    void DoMultipleSnapshots(bool withClear, bool withGc) {
        using TTree = TCowBTree<ui64, ui64>;

        TTree tree;

        std::map<ui64, ui64> keys[3];
        TTree::TSnapshot snapshots[3];
        size_t count = NSan::PlainOrUnderSanitizer(1000000u/3, 200000u/3);

        for (int k = 0; k < 3; ++k) {
            if (withClear) {
                tree.Clear();
            } else if (k != 0) {
                keys[k] = keys[k-1];
            }
            for (size_t i = 0; i < count; ++i) {
                ui64 key = RandomNumber<ui64>();
                ui64 value = RandomNumber<ui64>();
                keys[k][key] = value;
                tree[key] = value;
            }
            snapshots[k] = tree.Snapshot();
            UNIT_ASSERT_VALUES_EQUAL(snapshots[k].Size(), tree.Size());
            UNIT_ASSERT_VALUES_EQUAL(snapshots[k].Height(), tree.Height());
            UNIT_ASSERT_VALUES_EQUAL(tree.Size(), keys[k].size());
        }

        for (int k = 0; k < 3; ++k) {
            auto& snapshot = snapshots[k];
            auto it = snapshot.Iterator();
            UNIT_ASSERT_VALUES_EQUAL(it.Size(), snapshot.Size());
            UNIT_ASSERT_VALUES_EQUAL(it.Height(), snapshot.Height());

            // Verify forward iteration
            it.SeekFirst();
            for (auto p = keys[k].begin(); p != keys[k].end(); ++p) {
                UNIT_ASSERT_C(it.IsValid(), "Invalid iterator, expected key " << p->first);
                UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), p->first);
                UNIT_ASSERT_VALUES_EQUAL(it.GetValue(), p->second);
                it.Next();
            }
            UNIT_ASSERT_C(!it.IsValid(), "Unexpected key " << it.GetKey() << ", expected end of iteration");

            // Verify backward iteration
            it.SeekLast();
            for (auto p = keys[k].rbegin(); p != keys[k].rend(); ++p) {
                UNIT_ASSERT_C(it.IsValid(), "Invalid iterator, expected key " << p->first);
                UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), p->first);
                UNIT_ASSERT_VALUES_EQUAL(it.GetValue(), p->second);
                it.Prev();
            }
            UNIT_ASSERT_C(!it.IsValid(), "Unexpected key " << it.GetKey() << ", expected end of iteration");

            // Verify point queries
            for (auto p = keys[k].begin(); p != keys[k].end(); ++p) {
                const auto* value = it.Find(p->first);
                UNIT_ASSERT_C(value, "Failed to find key " << p->first);
                UNIT_ASSERT_VALUES_EQUAL(*value, p->second);
            }

            if (withGc) {
                it = { };
                snapshot = { };
                tree.CollectGarbage();
            }
        }

        if (!withGc) {
            for (int k = 0; k < 3; ++k) {
                snapshots[k] = { };
            }
            tree.CollectGarbage();
        }

        UNIT_ASSERT_VALUES_EQUAL(tree.DroppedPages(), 0u);
    }

    Y_UNIT_TEST(MultipleSnapshots) {
        DoMultipleSnapshots(false, false);
    }

    Y_UNIT_TEST(MultipleSnapshotsWithGc) {
        DoMultipleSnapshots(false, true);
    }

    Y_UNIT_TEST(MultipleSnapshotsWithClear) {
        DoMultipleSnapshots(true, false);
    }

    Y_UNIT_TEST(MultipleSnapshotsWithClearWithGc) {
        DoMultipleSnapshots(true, true);
    }

    void DoDuplicateKeys(bool threadSafe) {
        using TTree = TCowBTree<ui64, ui64>;

        TTree tree;

        TTree::TSnapshot snapshot;

        // When dups is 100 each key spans multiple leaf pages
        const size_t dups = NSan::PlainOrUnderSanitizer(100u, 10u);

        for (size_t i = 0; i < 10000; ++i) {
            // Duplicate values are inserted at the front
            for (size_t j = 1; j <= dups; ++j) {
                if (threadSafe) {
                    snapshot = tree.Snapshot(); // force copy-on-write
                }
                tree.Find(i);
                tree.EmplaceUnsafe(i, j);
            }
        }

        auto it = threadSafe ? tree.Snapshot().Iterator() : tree.UnsafeSnapshot().Iterator();
        it.SeekFirst();
        for (size_t i = 0; i < 10000; ++i) {
            for (size_t j = 1; j <= dups; ++j) {
                UNIT_ASSERT_C(it.IsValid(), "Unexpected EOF, expecting " << i << "/" << j);
                UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), i);
                UNIT_ASSERT_VALUES_EQUAL(it.GetValue(), j);
                it.Next();
            }
        }
        UNIT_ASSERT_C(!it.IsValid(), "Expecting EOF, found " << it.GetKey() << "/" << it.GetValue());

        for (size_t i = 0; i < 10000; ++i) {
            it.SeekExactFirst(i);
            UNIT_ASSERT_C(it.IsValid(), "Unexpected EOF, seeking to " << i);
            UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), i);
            UNIT_ASSERT_VALUES_EQUAL(it.GetValue(), 1u);
            it.SeekExact(i);
            UNIT_ASSERT_C(it.IsValid(), "Unexpected EOF, seeking (backwards) to " << i);
            UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), i);
            UNIT_ASSERT_VALUES_EQUAL(it.GetValue(), dups);
        }
    }

    Y_UNIT_TEST(DuplicateKeysInplace) {
        DoDuplicateKeys(false);
    }

    Y_UNIT_TEST(DuplicateKeysThreadSafe) {
        DoDuplicateKeys(true);
    }

    Y_UNIT_TEST(SnapshotCascade) {
        using TTree = TCowBTree<ui64, ui64>;

        TTree tree;

        TTree::TSnapshot snapshots[3];
        tree[42] = 51;
        snapshots[0] = tree.Snapshot();
        snapshots[1] = tree.Snapshot();
        tree[42] = 150;
        tree[42] = 151;
        tree[43] = 152;
        snapshots[2] = tree.Snapshot();

        auto* value1 = snapshots[2].Iterator().Find(42);
        UNIT_ASSERT(value1);
        UNIT_ASSERT_VALUES_EQUAL(*value1, 151u);
        auto* value2 = snapshots[2].Iterator().Find(43);
        UNIT_ASSERT(value2);
        UNIT_ASSERT_VALUES_EQUAL(*value2, 152u);

        snapshots[2] = { };
        tree.CollectGarbage();

        value1 = snapshots[1].Iterator().Find(42);
        UNIT_ASSERT(value1);
        UNIT_ASSERT_VALUES_EQUAL(*value1, 51u);
        value2 = snapshots[1].Iterator().Find(43);
        UNIT_ASSERT(!value2);

        snapshots[1] = { };
        tree.CollectGarbage();

        value1 = snapshots[0].Iterator().Find(42);
        UNIT_ASSERT(value1);
        UNIT_ASSERT_VALUES_EQUAL(*value1, 51u);
        value2 = snapshots[0].Iterator().Find(43);
        UNIT_ASSERT(!value2);

        snapshots[0] = { };
        tree.CollectGarbage();

        UNIT_ASSERT_VALUES_EQUAL(tree.DroppedPages(), 0u);
    }

    void DoSnapshotRollback(bool earlyErase) {
        using TTree = TCowBTree<ui64, ui64>;

        TTree tree;
        std::map<ui64, ui64> keys;

        TVector<std::pair<TTree::TSnapshot, std::map<ui64, ui64>>> snapshots;
        snapshots.emplace_back(tree.Snapshot(), keys);
        TVector<std::pair<TTree::TSnapshot, std::map<ui64, ui64>>> snapshotsOld;

        size_t count = NSan::PlainOrUnderSanitizer(1000000u, 200000u);
        size_t snapshotEvery = count / 20;
        size_t rollbackEvery = count / 10;

        auto checkSnapshot = [&](const TTree::TSnapshot& snapshot, const std::map<ui64, ui64>& expected) {
            auto itTree = snapshot.Iterator();
            itTree.SeekFirst();
            auto itMap = expected.begin();
            while (itTree.IsValid()) {
                ui64 key = itTree.GetKey();
                ui64 value = itTree.GetValue();
                UNIT_ASSERT_C(itMap != expected.end(), "Key " << key << " present in tree, but not in expected map");
                UNIT_ASSERT_C(key == itMap->first && value == itMap->second, "Found " << key << " value " << value
                    << ", but expected map has " << itMap->first << " value " << itMap->second);
                itTree.Next();
                ++itMap;
            }
            UNIT_ASSERT_C(itMap == expected.end(), "Expected " << itMap->first << " value " << itMap->second
                << ", but not found in the tree");
        };

        for (size_t i = 0; i < count; ++i) {
            ui64 key = RandomNumber<ui64>();
            ui64 value = RandomNumber<ui64>();
            if (keys.emplace(key, value).second) {
                UNIT_ASSERT_C(
                    tree.Emplace(key, value),
                    "Unexpected failure to insert key " << key);
            }
            if (((i + 1) % snapshotEvery) == 0) {
                // Create a new snapshot periodically
                snapshots.emplace_back(tree.Snapshot(), keys);
            }
            if (((i + 1) % rollbackEvery) == 0) {
                // Rollback to a random previous snapshot periodically
                size_t index = RandomNumber<ui64>() % snapshots.size();
                tree.RollbackTo(snapshots[index].first);
                keys = snapshots[index].second;
                for (size_t it = index + 1; it < snapshots.size(); ++it) {
                    snapshotsOld.emplace_back(std::move(snapshots[it]));
                }
                snapshots.erase(snapshots.begin() + (index + 1), snapshots.end());
                while (earlyErase && snapshotsOld.size() > 0) {
                    checkSnapshot(snapshotsOld.back().first, snapshotsOld.back().second);
                    snapshotsOld.pop_back();
                    tree.CollectGarbage();
                }
            }
        }

        while (snapshotsOld.size() > 0) {
            checkSnapshot(snapshotsOld.back().first, snapshotsOld.back().second);
            snapshotsOld.pop_back();
            tree.CollectGarbage();
        }

        while (snapshots.size() > 0) {
            checkSnapshot(snapshots.back().first, snapshots.back().second);
            snapshots.pop_back();
            tree.CollectGarbage();
        }

        checkSnapshot(tree.UnsafeSnapshot(), keys);
    }

    Y_UNIT_TEST(SnapshotRollback) {
        DoSnapshotRollback(false);
    }

    Y_UNIT_TEST(SnapshotRollbackEarlyErase) {
        DoSnapshotRollback(true);
    }

    Y_UNIT_TEST(IteratorDestructor) {
        using TTree = TCowBTree<ui64, ui64>;

        for (int i = 0; i < 5; ++i) {
            TTree::TIterator it;
            {
                TTree tree;
                tree.Emplace(1, 42);
                tree.Emplace(2, 51);
                it = tree.Snapshot().Iterator();
                it.SeekFirst();
                UNIT_ASSERT(it.IsValid());
                UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), 1u);
            }
            // N.B. it isn't safe to use 'it', but destructor must be safe
        }
    }

    struct TTestKey {
        static size_t Count;
        const ui64 Key;

        TTestKey(ui64 key)
            : Key(key)
        {
            ++Count;
        }

        TTestKey(const TTestKey& rhs)
            : Key(rhs.Key)
        {
            ++Count;
        }

        ~TTestKey() {
            --Count;
        }

        friend bool operator<(const TTestKey& a, const TTestKey& b) {
            return a.Key < b.Key;
        }
    };

    size_t TTestKey::Count = 0;

    struct TTestValue {
        static size_t Count;
        const ui64 Value;

        TTestValue(ui64 value)
            : Value(value)
        {
            ++Count;
        }

        TTestValue(const TTestValue& rhs)
            : Value(rhs.Value)
        {
            ++Count;
        }

        ~TTestValue() {
            --Count;
        }
    };

    size_t TTestValue::Count = 0;

    void DoShouldCallDtors(bool threadSafe) {
        using TTree = TCowBTree<TTestKey, TTestValue>;

        UNIT_ASSERT_VALUES_EQUAL(TTestKey::Count, 0u);
        UNIT_ASSERT_VALUES_EQUAL(TTestValue::Count, 0u);

        {
            TTree tree;

            TTree::TSnapshot snapshot;

            for (ui64 i = 0; i < 10000; ++i) {
                if (threadSafe) {
                    snapshot = tree.Snapshot(); // force copy-on-write
                }
                ui64 key = RandomNumber<ui64>();
                ui64 value = RandomNumber<ui64>();
                while (!tree.Emplace(TTestKey(key), value)) {
                    key = RandomNumber<ui64>();
                }
            }

            // Multiple key copies are expected on different levels
            UNIT_ASSERT_C(TTestKey::Count > 10000u,
                "Unexpected " << TTestKey::Count << " live keys");

            if (threadSafe) {
                // Active snapshot would force at least one leaf page copy
                UNIT_ASSERT_C(TTestValue::Count > 10000u,
                    "Unexpected " << TTestValue::Count << " live values");
                // Collecting said snapshot must free extra page copies
                snapshot = { };
                tree.CollectGarbage();
            }

            UNIT_ASSERT_C(TTestValue::Count == 10000u,
                "Unexpected " << TTestValue::Count << " live values");
        }

        UNIT_ASSERT_VALUES_EQUAL(TTestKey::Count, 0u);
        UNIT_ASSERT_VALUES_EQUAL(TTestValue::Count, 0u);
    }

    Y_UNIT_TEST(ShouldCallDtorsInplace) {
        DoShouldCallDtors(false);
    }

    Y_UNIT_TEST(ShouldCallDtorsThreadSafe) {
        DoShouldCallDtors(true);
    }

    class TConcurrentFixture : public NUnitTest::TBaseFixture {
    protected:
        using TTree = TCowBTree<ui64, ui64>;

        static constexpr size_t NumWriters = 2;
        static constexpr size_t NumReaders = 4;
        static constexpr size_t WriteIterations = NSan::PlainOrUnderSanitizer(200000u, 50000u);
        static constexpr size_t ReadIterations = NSan::PlainOrUnderSanitizer(3000000u, 300000u);
        static constexpr size_t ReadSpawnAfter = WriteIterations * 2 / (NumReaders + 1);

        TTree Tree;
        TMutex WriteMutex;
        TManualEvent ReadReady[NumReaders];
        TTree::TSnapshot Snapshots[NumReaders];

        size_t TotalWritesFinished = 0;
        size_t NextReaderIndex = 0;

        size_t SnapshotSizes[NumReaders];
        std::atomic<size_t> ReadSuccessCount{ 0 };

    public:
        TConcurrentFixture() { }

    protected:
        void OnWriteFinished() {
            if ((++TotalWritesFinished % ReadSpawnAfter) == 0 &&
                NextReaderIndex < NumReaders)
            {
                size_t index = NextReaderIndex++;
                Snapshots[index] = Tree.Snapshot();
                ReadReady[index].Signal();
            }
        }

        void OnReadSuccess() {
            ReadSuccessCount.fetch_add(1, std::memory_order_relaxed);
        }
    };

    Y_UNIT_TEST_F(Concurrent, TConcurrentFixture) {
        TVector<THolder<TWorkerThread>> producers(NumWriters);
        for (size_t i = 0; i < NumWriters; ++i) {
            producers[i] = TWorkerThread::Spawn([this] {
                for (size_t k = 0; k < WriteIterations; ++k) {
                    with_lock (WriteMutex) {
                        ui64 key = RandomNumber<ui64>();
                        while (!Tree.Emplace(key, ~key)) {
                            key = RandomNumber<ui64>();
                        }
                        OnWriteFinished();
                    }
                }
            });
        }

        TVector<THolder<TWorkerThread>> consumers(NumReaders);
        for (size_t i = 0; i < NumReaders; ++i) {
            consumers[i] = TWorkerThread::Spawn([this, i] {
                ReadReady[i].WaitI();
                auto& snapshot = Snapshots[i];
                auto it = snapshot.Iterator();
                SnapshotSizes[i] = it.Size();
                for (size_t k = 0; k < ReadIterations; ++k) {
                    ui64 needle = RandomNumber<ui64>();
                    it.SeekLowerBound(needle);
                    if (it.IsValid()) {
                        ui64 key = it.GetKey();
                        ui64 value = it.GetValue();
                        Y_ABORT_UNLESS(key >= needle && value == ~key,
                            "Found unexpected key %" PRIu64 " and value %" PRIu64 " while seeking to %" PRIu64,
                            key, value, needle);
                        OnReadSuccess();
                    }
                }
                // Release snapshot concurrently
                it = { };
                snapshot = { };
            });
        }

        for (size_t i = 0; i < NumWriters; ++i) {
            producers[i]->Join();
            Cerr << "Producer " << i << " worked for " << producers[i]->GetTime() << " seconds" << Endl;
        }

        for (size_t i = 0; i < NumReaders; ++i) {
            consumers[i]->Join();
            Cerr << "Consumer " << i << " worked for " << consumers[i]->GetTime() << " seconds"
                << " on a snapshot of size " << SnapshotSizes[i] << Endl;
        }

        Cerr << "Consumers had " << ReadSuccessCount.load() << " successful seeks" << Endl;
    }

    Y_UNIT_TEST(Alignment) {
        struct alignas(32) TSpecialKey {
            ui8 Key;

            bool operator<(const TSpecialKey& rhs) {
                return Key < rhs.Key;
            }
        };

        struct alignas(128) TSpecialValue {
            ui8 Value;
        };

        using TSpecialTree = TCowBTree<TSpecialKey, TSpecialValue>;

        UNIT_ASSERT_VALUES_EQUAL(size_t(TSpecialTree::InnerPageAlignment), 32u);
        UNIT_ASSERT_VALUES_EQUAL(size_t(TSpecialTree::LeafPageAlignment), 128u);

        using TSmallTree = TCowBTree<ui16, ui16>;

        UNIT_ASSERT_VALUES_EQUAL(size_t(TSmallTree::InnerPageAlignment), 8u);
        UNIT_ASSERT_VALUES_EQUAL(size_t(TSmallTree::LeafPageAlignment), 8u);
    }

}

}   // namespace NKikimr
