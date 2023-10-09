#include "btree.h"

#include "ut_common.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>
#include <util/random/shuffle.h>
#include <util/system/mutex.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TBTreeTest) {
    Y_UNIT_TEST(Basics) {
        using TTree = TBTree<int, int>;

        TMemoryPool pool(8192);
        TTree tree(pool);

        UNIT_ASSERT(tree.FindPtr(42) == nullptr);
        tree.EmplaceUnsafe(42, 123);
        UNIT_ASSERT(tree.FindPtr(42) != nullptr);

        // Tree now has a single item, it should be visible to iteration
        auto it = tree.SafeAccess().Iterator();
        it.SeekFirst();
        UNIT_ASSERT(it.IsValid());
        UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), 42);
        UNIT_ASSERT_VALUES_EQUAL(it.GetValue(), 123);
        it.Next();
        UNIT_ASSERT(!it.IsValid());

        // New keys added to the page tail may or may not be visible
        it.SeekFirst();
        UNIT_ASSERT(it.IsValid());
        UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), 42);
        UNIT_ASSERT_VALUES_EQUAL(it.GetValue(), 123);
        UNIT_ASSERT(tree.FindPtr(51) == nullptr);
        tree.EmplaceUnsafe(51, 555);
        it.Next();
        if (it.IsValid()) {
            UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), 51);
            UNIT_ASSERT_VALUES_EQUAL(it.GetValue(), 555);
            it.Next();
        }
        UNIT_ASSERT(!it.IsValid());

        // New keys added in the middle should use copy-on-write, old page should still be valid
        it.SeekFirst();
        UNIT_ASSERT(it.IsValid());
        UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), 42);
        UNIT_ASSERT_VALUES_EQUAL(it.GetValue(), 123);
        UNIT_ASSERT(tree.FindPtr(45) == nullptr);
        tree.EmplaceUnsafe(45, 444);
        it.Next();
        UNIT_ASSERT(it.IsValid());
        UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), 51);
        UNIT_ASSERT_VALUES_EQUAL(it.GetValue(), 555);
        it.Next();
        UNIT_ASSERT(!it.IsValid());

        // Restarting iteration should grab a new page version that has the new key
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

        // We currently have keys 42, 45 and 51, add more keys until the first split
        ui64 lastKey = -1;
        for (ui64 key = 100; tree.GetHeight() < 2; key += 2) {
            UNIT_ASSERT_C(tree.Emplace(key, ~key), "Failed to insert " << key);
            lastKey = key;
        }

        it.SeekFirst();
        UNIT_ASSERT(it.IsValid());
        UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), 42);
        UNIT_ASSERT_VALUES_EQUAL(it.GetValue(), 123);

        // We should see new keys added to non-current page
        UNIT_ASSERT(tree.Emplace(lastKey - 1, 12345));
        UNIT_ASSERT(tree.Emplace(lastKey + 1, 54321));

        TMap<ui64, ui64> seen;
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
        using TTree = TBTree<int, int>;

        TMemoryPool pool(8192);
        TTree tree(pool);

        auto it = tree.SafeAccess().Iterator();

        for (size_t i = 0; i < 10000; ++i) {
            tree.Emplace(i, i);
        }
        UNIT_ASSERT_VALUES_EQUAL(tree.Size(), 10000u);

        it.SeekFirst();
        for (size_t i = 0; i < 10000; ++i) {
            UNIT_ASSERT_C(it.IsValid(), "Expected " << i);
            UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), i);
            it.Next();
        }
        UNIT_ASSERT_C(!it.IsValid(), "Expected EOF");

        tree.Clear();
        UNIT_ASSERT_VALUES_EQUAL(tree.Size(), 0u);
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

        it.SeekFirst();
        for (size_t i = 100000; i < 110000; ++i) {
            UNIT_ASSERT_C(it.IsValid(), "Expected " << i);
            UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), i);
            it.Next();
        }
        UNIT_ASSERT_C(!it.IsValid(), "Expected EOF");
    }

    void DoSeekForwardPermutations(bool threadSafe) {
        using TTree = TBTree<ui64, ui64>;

        TMemoryPool pool(8192);
        TTree tree(pool);

        TTree::TSafeAccess safeAccess;
        if (threadSafe) {
            safeAccess = tree.SafeAccess(); // force thread-safe operations
        }

        for (ui64 key = 1u; key <= 10001u; key += 2) {
            UNIT_ASSERT_C(
                tree.Emplace(key, ~key),
                "Failed to insert key " << key);
        }

        // ~5000 keys with ~32 keys per page should produce height 3 tree
        UNIT_ASSERT_VALUES_EQUAL(tree.GetHeight(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(tree.Size(), 5001u);

        auto it = tree.SafeAccess().Iterator();
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
        using TTree = TBTree<ui64, ui64>;

        TMemoryPool pool(8192);
        TTree tree(pool);

        TTree::TSafeAccess safeAccess;
        if (threadSafe) {
            safeAccess = tree.SafeAccess(); // force thread-safe operations
        }

        for (ui64 key = 1u; key <= 10001u; key += 2) {
            UNIT_ASSERT_C(
                tree.Emplace(key, ~key),
                "Failed to insert key " << key);
        }

        // ~5000 keys with ~32 keys per page should produce height 3 tree
        UNIT_ASSERT_VALUES_EQUAL(tree.GetHeight(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(tree.Size(), 5001u);

        auto it = tree.SafeAccess().Iterator();
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
        using TTree = TBTree<ui64, ui64>;

        TMemoryPool pool(8192);
        TTree tree(pool);

        TTree::TSafeAccess safeAccess;
        if (threadSafe) {
            safeAccess = tree.SafeAccess(); // force thread-safe operations
        }

        std::map<ui64, ui64> keys;
        size_t count = NSan::PlainOrUnderSanitizer(1000000u, 200000u);

        for (size_t i = 0; i < count; ++i) {
            ui64 key = RandomNumber<ui64>();
            ui64 value = RandomNumber<ui64>();
            if (keys.emplace(key, value).second) {
                UNIT_ASSERT_C(
                    tree.Emplace(key, value),
                    "Unexpected failure to insert key " << key);
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(tree.Size(), keys.size());

        for (const auto& kv : keys) {
            ui64* value = tree.FindPtr(kv.first);
            UNIT_ASSERT_C(value, "Unexpected failure to find key " << kv.first);
            UNIT_ASSERT_VALUES_EQUAL(*value, kv.second);
        }

        // Verify forward iteration
        auto it = tree.SafeAccess().Iterator();
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

    void DoDuplicateKeys(bool threadSafe) {
        using TTree = TBTree<ui64, ui64>;

        TMemoryPool pool(8192);
        TTree tree(pool);

        TTree::TSafeAccess safeAccess;
        if (threadSafe) {
            safeAccess = tree.SafeAccess(); // force thread-safe operations
        }

        // When dups is 100 each key spans multiple leaf pages
        const size_t dups = NSan::PlainOrUnderSanitizer(100u, 10u);

        for (size_t i = 0; i < 10000; ++i) {
            // Duplicate values are inserted at the front
            for (size_t j = 1; j <= dups; ++j) {
                tree.FindPtr(i);
                tree.EmplaceUnsafe(i, j);
            }
        }

        auto it = tree.SafeAccess().Iterator();
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
        using TTree = TBTree<TTestKey, TTestValue>;

        UNIT_ASSERT_VALUES_EQUAL(TTestKey::Count, 0u);
        UNIT_ASSERT_VALUES_EQUAL(TTestValue::Count, 0u);

        {
            TMemoryPool pool(8192);
            TTree tree(pool);

            TTree::TSafeAccess safeAccess;
            if (threadSafe) {
                safeAccess = tree.SafeAccess(); // force thread-safe operations
            }

            for (ui64 i = 0; i < 10000; ++i) {
                ui64 key = RandomNumber<ui64>();
                ui64 value = RandomNumber<ui64>();
                while (!tree.Emplace(TTestKey(key), value)) {
                    key = RandomNumber<ui64>();
                }
            }

            UNIT_ASSERT_C(TTestKey::Count > 10000u,
                "Unexpected " << TTestKey::Count << " live keys");
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
        using TTree = TBTree<ui64, ui64>;

        static constexpr size_t NumWriters = 2;
        static constexpr size_t NumReaders = 4;
        static constexpr size_t WriteIterations = NSan::PlainOrUnderSanitizer(200000u, 50000u);
        static constexpr size_t ReadIterations = NSan::PlainOrUnderSanitizer(3000000u, 300000u);

        TMemoryPool MemoryPool{8192};
        TTree Tree{MemoryPool};
        TTree::TSafeAccess SafeAccess[NumReaders];
        TMutex WriteMutex;

    public:
        TConcurrentFixture() {
            for (size_t i = 0; i < NumReaders; ++i) {
                SafeAccess[i] = Tree.SafeAccess();
            }
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
                    }
                }
            });
        }

        TVector<THolder<TWorkerThread>> consumers(NumReaders);
        for (size_t i = 0; i < NumReaders; ++i) {
            consumers[i] = TWorkerThread::Spawn([this, i] {
                auto it = SafeAccess[i].Iterator();
                for (size_t k = 0; k < ReadIterations; ++k) {
                    ui64 needle = RandomNumber<ui64>();
                    it.SeekLowerBound(needle);
                    if (it.IsValid()) {
                        ui64 key = it.GetKey();
                        ui64 value = it.GetValue();
                        Y_ABORT_UNLESS(key >= needle && value == ~key,
                            "Found unexpected key %" PRIu64 " and value %" PRIu64 " while seeking to %" PRIu64,
                            key, value, needle);
                    }
                }
            });
        }

        for (size_t i = 0; i < NumWriters; ++i) {
            producers[i]->Join();
            Cerr << "Producer " << i << " worked for " << producers[i]->GetTime() << " seconds" << Endl;
        }

        for (size_t i = 0; i < NumReaders; ++i) {
            consumers[i]->Join();
            Cerr << "Consumer " << i << " worked for " << consumers[i]->GetTime() << " seconds" << Endl;
        }
    }

    Y_UNIT_TEST(IteratorDestructor) {
        using TTree = TBTree<ui64, ui64>;

        for (int i = 0; i < 5; ++i) {
            TTree::TSafeIterator it;
            {
                TMemoryPool pool(8192);
                TTree tree(pool);
                tree.Emplace(1, 42);
                tree.Emplace(2, 51);
                it = tree.SafeAccess().Iterator();
                it.SeekFirst();
                UNIT_ASSERT(it.IsValid());
                UNIT_ASSERT_VALUES_EQUAL(it.GetKey(), 1u);
            }
            // N.B. it isn't safe to use 'it', but destructor must be safe
        }
    }
}

}
