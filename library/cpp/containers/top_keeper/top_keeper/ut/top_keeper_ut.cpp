#include <library/cpp/containers/limited_heap/limited_heap.h>
#include <library/cpp/containers/top_keeper/top_keeper.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>

static ui32 seed = 3;
ui32 Rnd() {
    seed = seed * 5 + 1;
    return seed;
}

/*
 * Tests for TTopKeeper
 */
Y_UNIT_TEST_SUITE(TTopKeeperTest) {
    // Tests correctness on usual examples
    Y_UNIT_TEST(CorrectnessTest) {
        int m = 20000;

        TLimitedHeap<std::pair<int, int>> h1(m);
        TTopKeeper<std::pair<int, int>> h2(m);

        int n = 20000000;
        while (n--) {
            int r = int(Rnd());

            h1.Insert({r, -r});
            h2.Emplace(r, -r);
        }

        h2.Finalize();

        UNIT_ASSERT_EQUAL(h1.GetSize(), h2.GetSize());

        while (!h1.IsEmpty()) {
            UNIT_ASSERT_EQUAL(h1.GetMin(), h2.GetNext());
            h1.PopMin();
            h2.Pop();
        }
    }

    // Tests on zero-size correctness
    Y_UNIT_TEST(ZeroSizeCorrectnes) {
        TTopKeeper<int> h(0);
        for (int i = 0; i < 100; ++i) {
            h.Insert(i % 10 + i / 10);
        }
        h.Finalize();
        UNIT_ASSERT(h.IsEmpty());
    }

    // Tests SetMaxSize behaviour
    Y_UNIT_TEST(SetMaxSizeTest) {
        int m = 20000;
        TLimitedHeap<int> h1(m);
        TTopKeeper<int> h2(m);

        int n = 20000000;
        while (n--) {
            int r = int(Rnd());

            h1.Insert(r);
            h2.Insert(r);
        }

        h1.SetMaxSize(m / 3);
        h2.SetMaxSize(m / 3);
        h2.Finalize();

        UNIT_ASSERT_EQUAL(h1.GetSize(), h2.GetSize());

        while (!h1.IsEmpty()) {
            UNIT_ASSERT_EQUAL(h1.GetMin(), h2.GetNext());
            h1.PopMin();
            h2.Pop();
        }
    }

    // Tests reuse behavior
    Y_UNIT_TEST(ReuseTest) {
        int m = 20000;
        TLimitedHeap<int> h1(m);
        TTopKeeper<int> h2(m);

        int n = 20000000;
        while (n--) {
            int r = int(Rnd());

            h1.Insert(r);
            h2.Insert(r);
        }

        UNIT_ASSERT_EQUAL(h1.GetSize(), h2.GetSize());

        while (!h1.IsEmpty()) {
            UNIT_ASSERT_EQUAL(h1.GetMin(), h2.GetNext());
            h1.PopMin();
            h2.Pop();
        }

        n = 20000000;
        while (n--) {
            int r = int(Rnd());

            h1.Insert(r);
            h2.Insert(r);
        }

        UNIT_ASSERT_EQUAL(h1.GetSize(), h2.GetSize());

        while (!h1.IsEmpty()) {
            UNIT_ASSERT_EQUAL(h1.GetMin(), h2.GetNext());
            h1.PopMin();
            h2.Pop();
        }
    }

    // Tests reset behavior
    Y_UNIT_TEST(ResetTest) {
        int m = 20000;
        TLimitedHeap<int> h1(m);
        TTopKeeper<int> h2(m);

        int n = 20000000;
        while (n--) {
            int r = int(Rnd());

            h1.Insert(r);
            h2.Insert(r);
        }

        UNIT_ASSERT_EQUAL(h1.GetSize(), h2.GetSize());

        for (int i = 0; i < m / 2; ++i) {
            UNIT_ASSERT_EQUAL(h1.GetMin(), h2.GetNext());
            h1.PopMin();
            h2.Pop();
        }

        h2.Reset();
        while (!h1.IsEmpty()) {
            h1.PopMin();
        }

        n = 20000000;
        while (n--) {
            int r = int(Rnd());

            h1.Insert(r);
            h2.Insert(r);
        }

        UNIT_ASSERT_EQUAL(h1.GetSize(), h2.GetSize());

        while (!h1.IsEmpty()) {
            UNIT_ASSERT_EQUAL(h1.GetMin(), h2.GetNext());
            h1.PopMin();
            h2.Pop();
        }
    }

    Y_UNIT_TEST(PreRegressionTest) {
        typedef std::pair<float, unsigned int> TElementType;

        const size_t randomTriesCount = 128;
        for (size_t i1 = 0; i1 < randomTriesCount; ++i1) {
            const size_t desiredElementsCount = RandomNumber<size_t>(5) + 1;
            TLimitedHeap<TElementType> h1(desiredElementsCount);
            TTopKeeper<TElementType> h2(desiredElementsCount);

            const size_t elementsToInsert = RandomNumber<size_t>(10) + desiredElementsCount;
            UNIT_ASSERT_C(desiredElementsCount <= elementsToInsert, "Test internal invariant is broken");
            for (size_t i2 = 0; i2 < elementsToInsert; ++i2) {
                const auto f = RandomNumber<float>();
                const auto id = RandomNumber<unsigned int>();

                h1.Insert(TElementType(f, id));
                h2.Insert(TElementType(f, id));
            }

            h2.Finalize();

            //we inserted enough elements to guarantee this outcome
            UNIT_ASSERT_EQUAL(h1.GetSize(), desiredElementsCount);
            UNIT_ASSERT_EQUAL(h2.GetSize(), desiredElementsCount);

            const auto n = h2.GetSize();
            for (size_t i3 = 0; i3 < n; ++i3) {
                UNIT_ASSERT_EQUAL(h1.GetMin(), h2.GetNext());
                h1.PopMin();
                h2.Pop();
            }
        }
    }

    Y_UNIT_TEST(CopyKeeperRegressionCase) {
        using TKeeper = TTopKeeper<float>;
        TVector<TKeeper> v(2, TKeeper(200));
        auto& k = v[1];
        for (size_t i = 0; i < 100; ++i) {
            k.Insert(RandomNumber<float>());
        }
        k.Finalize();
    }

    Y_UNIT_TEST(ExtractTest) {
        TTopKeeper<size_t> keeper(100);
        for (size_t i = 0; i < 100; ++i) {
            keeper.Insert(i);
        }

        auto values = keeper.Extract();
        UNIT_ASSERT_EQUAL(values.size(), 100);
        Sort(values);

        for (size_t i = 0; i < 100; ++i) {
            UNIT_ASSERT_EQUAL(values[i], i);
        }

        UNIT_ASSERT(keeper.IsEmpty());
    }
}
