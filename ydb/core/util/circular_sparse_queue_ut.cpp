#include "circular_sparse_queue.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/random/fast.h>

namespace NKikimr {

using TIntQueue = TCircularSparseQueue<int>;

Y_UNIT_TEST_SUITE(TCircularSparseQueueTest) {

    Y_UNIT_TEST(EmptyOnConstruction) {
        TIntQueue queue(4);
        UNIT_ASSERT(queue.Empty());
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.Capacity(), 4UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.FrontIndex(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.NextIndex(), 0UL);
    }

    Y_UNIT_TEST(MonotonicIndices) {
        TIntQueue queue(4);
        auto i0 = queue.Push(10);
        auto i1 = queue.Push(11);
        auto i2 = queue.Push(12);
        UNIT_ASSERT(i0.has_value());
        UNIT_ASSERT(i1.has_value());
        UNIT_ASSERT(i2.has_value());
        UNIT_ASSERT_VALUES_EQUAL(i0.value(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(i1.value(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(i2.value(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
    }

    Y_UNIT_TEST(PushAndEmplace) {
        TCircularSparseQueue<std::pair<int, int>> queue(4);

        auto i0 = queue.Push(std::make_pair(1, 2));
        UNIT_ASSERT(i0.has_value());
        auto* v0 = queue.Find(i0.value());
        UNIT_ASSERT(v0 != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(v0->first, 1);
        UNIT_ASSERT_VALUES_EQUAL(v0->second, 2);

        auto i1 = queue.Emplace(3, 4);
        UNIT_ASSERT(i1.has_value());
        auto* v1 = queue.Find(i1.value());
        UNIT_ASSERT(v1 != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(v1->first, 3);
        UNIT_ASSERT_VALUES_EQUAL(v1->second, 4);
    }

    Y_UNIT_TEST(FullThenEraseAllowsPush) {
        TIntQueue queue(3);
        UNIT_ASSERT(queue.Push(0).has_value());
        UNIT_ASSERT(queue.Push(1).has_value());
        UNIT_ASSERT(queue.Push(2).has_value());
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);

        auto full = queue.Push(3);
        UNIT_ASSERT(!full.has_value());
        UNIT_ASSERT(!full.error().empty());

        UNIT_ASSERT(queue.Erase(1));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2UL);

        auto again = queue.Push(3);
        UNIT_ASSERT(again.has_value());
        UNIT_ASSERT_VALUES_EQUAL(again.value(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
    }

    Y_UNIT_TEST(FindStoredAndMissing) {
        TIntQueue queue(4);
        auto i0 = queue.Push(100);
        auto i1 = queue.Push(200);
        UNIT_ASSERT_VALUES_EQUAL(*queue.Find(i0.value()), 100);
        UNIT_ASSERT_VALUES_EQUAL(*queue.Find(i1.value()), 200);

        // out of range (>= End)
        UNIT_ASSERT(queue.Find(2) == nullptr);
        // erased
        UNIT_ASSERT(queue.Erase(i0.value()));
        UNIT_ASSERT(queue.Find(i0.value()) == nullptr);
    }

    Y_UNIT_TEST(FindBelowBegin) {
        TIntQueue queue(4);
        UNIT_ASSERT(queue.Push(1).has_value());
        UNIT_ASSERT(queue.Push(2).has_value());
        UNIT_ASSERT(queue.Erase(0));
        // Begin advanced to 1, index 0 now below Begin.
        UNIT_ASSERT_VALUES_EQUAL(queue.FrontIndex(), 1UL);
        UNIT_ASSERT(queue.Find(0) == nullptr);
        UNIT_ASSERT(queue.Find(1) != nullptr);
    }

    Y_UNIT_TEST(EraseMiddleKeepsBegin) {
        TIntQueue queue(4);
        UNIT_ASSERT(queue.Push(1).has_value()); // 0
        UNIT_ASSERT(queue.Push(2).has_value()); // 1
        UNIT_ASSERT(queue.Push(3).has_value()); // 2
        UNIT_ASSERT(queue.Erase(1));
        UNIT_ASSERT_VALUES_EQUAL(queue.FrontIndex(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2UL);
        UNIT_ASSERT(queue.Find(1) == nullptr);
        UNIT_ASSERT(queue.Find(0) != nullptr);
        UNIT_ASSERT(queue.Find(2) != nullptr);
    }

    Y_UNIT_TEST(EraseFrontAdvancesOverHoles) {
        TIntQueue queue(5);
        for (int i = 0; i < 5; ++i) {
            UNIT_ASSERT(queue.Push(i).has_value());
        }
        // Erase 1, 2 first (middle holes), then 0 should jump to 3.
        UNIT_ASSERT(queue.Erase(1));
        UNIT_ASSERT(queue.Erase(2));
        UNIT_ASSERT_VALUES_EQUAL(queue.FrontIndex(), 0UL);
        UNIT_ASSERT(queue.Erase(0));
        UNIT_ASSERT_VALUES_EQUAL(queue.FrontIndex(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2UL);
    }

    Y_UNIT_TEST(EraseInvalid) {
        TIntQueue queue(4);
        UNIT_ASSERT(queue.Push(1).has_value()); // 0
        UNIT_ASSERT(queue.Push(2).has_value()); // 1
        // already erased
        UNIT_ASSERT(queue.Erase(0));
        UNIT_ASSERT(!queue.Erase(0));
        // out of range
        UNIT_ASSERT(!queue.Erase(2));
        UNIT_ASSERT(!queue.Erase(100));
        // indices keep increasing
        auto next = queue.Push(3);
        UNIT_ASSERT_VALUES_EQUAL(next.value(), 2UL);
    }

    Y_UNIT_TEST(EraseLastBecomesEmpty) {
        TIntQueue queue(4);
        auto i0 = queue.Push(1);
        UNIT_ASSERT(queue.Erase(i0.value()));
        UNIT_ASSERT(queue.Empty());
        UNIT_ASSERT_VALUES_EQUAL(queue.FrontIndex(), queue.NextIndex());
    }

    Y_UNIT_TEST(FifoSlidingWindowWrapAround) {
        const size_t n = 4;
        TIntQueue queue(n);
        // Prime with n items.
        for (int i = 0; i < (int)n; ++i) {
            auto r = queue.Push(i);
            UNIT_ASSERT(r.has_value());
        }
        ui64 front = 0;
        int value = (int)n;
        // Slide far beyond 2N to exercise wrap-around.
        for (int step = 0; step < 100; ++step) {
            // erase oldest
            UNIT_ASSERT(queue.Erase(front));
            ++front;
            // push new
            auto r = queue.Push(value);
            UNIT_ASSERT(r.has_value());
            // verify all live items
            for (ui64 idx = front; idx < queue.NextIndex(); ++idx) {
                auto* v = queue.Find(idx);
                UNIT_ASSERT(v != nullptr);
                UNIT_ASSERT_VALUES_EQUAL((ui64)*v, idx);
            }
            UNIT_ASSERT_VALUES_EQUAL(queue.Size(), n);
            ++value;
        }
    }

    // LIFO churn: push N items, erase all N newest-first, repeat.
    // Begin advances only after the front is erased (last in the LIFO sequence),
    // so End-Begin peaks at 2N-1 just before that final erase — exactly within
    // the 2N buffer. Wrap-around is verified across multiple full cycles.
    Y_UNIT_TEST(LifoChurnSlotReuse) {
        const size_t n = 4;
        TIntQueue queue(n);

        int next = 0;
        for (int cycle = 0; cycle < 10; ++cycle) {
            // Fill to capacity.
            const ui64 cycleBegin = queue.NextIndex();
            for (size_t i = 0; i < n; ++i) {
                auto r = queue.Push(next++);
                UNIT_ASSERT(r.has_value());
            }
            UNIT_ASSERT_VALUES_EQUAL(queue.Size(), n);

            // Erase in LIFO order (newest → oldest).
            for (ui64 idx = queue.NextIndex() - 1; idx >= cycleBegin; --idx) {
                UNIT_ASSERT(queue.Erase(idx));
                if (idx == cycleBegin) break; // avoid unsigned underflow
            }
            UNIT_ASSERT(queue.Empty());
            UNIT_ASSERT_VALUES_EQUAL(queue.FrontIndex(), queue.NextIndex());
        }
    }

    Y_UNIT_TEST(Clear) {
        TIntQueue queue(4);
        UNIT_ASSERT(queue.Push(1).has_value());
        UNIT_ASSERT(queue.Push(2).has_value());
        queue.Clear();
        UNIT_ASSERT(queue.Empty());
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.FrontIndex(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.NextIndex(), 0UL);
        // usable after clear
        auto r = queue.Push(5);
        UNIT_ASSERT(r.has_value());
        UNIT_ASSERT_VALUES_EQUAL(r.value(), 0UL);
    }

    // N=4 → BufferSize=8. Push items 0–3, erase 1,2,3 (keep 0 pinned at Begin).
    // Churn (push + immediately erase the pushed item) until End-Begin reaches
    // BufferSize. At that point the next Emplace must fail with a slot-occupied
    // error while Size() < Capacity(). Item 0 must still be findable. After
    // erasing item 0, Begin jumps and pushes succeed again.
    Y_UNIT_TEST(PinnedFrontFillsQueue) {
        const size_t N = 4;
        TIntQueue queue(N);
        // BufferSize = 2*4 = 8, SlotMask = 7.

        // Push 0,1,2,3 then erase 1,2,3 — item 0 is pinned at Begin=0.
        const auto i0 = queue.Push(42);
        UNIT_ASSERT(i0.has_value());
        const auto i1 = queue.Push(1);
        UNIT_ASSERT(i1.has_value());
        const auto i2 = queue.Push(2);
        UNIT_ASSERT(i2.has_value());
        const auto i3 = queue.Push(3);
        UNIT_ASSERT(i3.has_value());
        UNIT_ASSERT(queue.Erase(i1.value()));
        UNIT_ASSERT(queue.Erase(i2.value()));
        UNIT_ASSERT(queue.Erase(i3.value()));
        // Now Begin=0, End=4, LiveCount=1 (only item 0 alive).

        // Churn: push-then-erase until End-Begin == BufferSize (8).
        // Each iteration: push → End becomes 5,6,7 → erase immediately.
        // End-Begin goes from 4 to 7 after 3 churn steps. The 4th push would
        // try to write to slot (8 & 7)=0, which is still occupied by item 0.
        while (queue.NextIndex() - queue.FrontIndex() < queue.Capacity() * 2 - 1) {
            auto r = queue.Push(99);
            UNIT_ASSERT(r.has_value());
            UNIT_ASSERT(queue.Erase(r.value()));
        }
        // End-Begin == BufferSize - 1 = 7 so far; one more push succeeds (slot != 0).
        // We need End-Begin == BufferSize for the collision: churn one more step.
        {
            auto r = queue.Push(99);
            UNIT_ASSERT_C(r.has_value(), r.error());
            UNIT_ASSERT(queue.Erase(r.value()));
        }
        UNIT_ASSERT_VALUES_EQUAL(queue.NextIndex() - queue.FrontIndex(),
                                 queue.Capacity() * 2); // End-Begin == 8

        // Now the next push must fail: End & SlotMask == 0, slot occupied by item 0.
        auto failing = queue.Push(77);
        UNIT_ASSERT_C(!failing.has_value(), "expected failure but got index " << failing.value());
        UNIT_ASSERT(!failing.error().empty());

        // Size must not have changed.
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.NextIndex() - queue.FrontIndex(), queue.Capacity() * 2);

        // Item 0 must still be findable and intact.
        auto* p = queue.Find(i0.value());
        UNIT_ASSERT(p != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(*p, 42);

        // After erasing item 0, Begin advances and pushes work again.
        UNIT_ASSERT(queue.Erase(i0.value()));
        UNIT_ASSERT(queue.Empty());
        auto recovered = queue.Push(55);
        UNIT_ASSERT_C(recovered.has_value(), recovered.error());
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1UL);
    }

    // After reaching the collision-induced "full" state, repeated failed pushes
    // must not corrupt the queue: Size/NextIndex stay consistent, the stuck item
    // remains findable, and erasing it recovers the queue.
    Y_UNIT_TEST(PinnedFrontRecovery) {
        const size_t N = 4;
        TIntQueue queue(N);

        const auto i0 = queue.Push(100);
        UNIT_ASSERT(i0.has_value());
        const auto i1 = queue.Push(1);
        UNIT_ASSERT(i1.has_value());
        UNIT_ASSERT(queue.Erase(i1.value()));

        // Churn until End-Begin == BufferSize.
        while (queue.NextIndex() - queue.FrontIndex() < queue.Capacity() * 2) {
            auto r = queue.Push(0);
            if (!r.has_value()) {
                break;  // collision reached
            }
            UNIT_ASSERT(queue.Erase(r.value()));
        }

        // Make sure we are in the failure state.
        {
            auto r = queue.Push(0);
            // If it succeeded, churn one more.
            if (r.has_value()) {
                queue.Erase(r.value());
                r = queue.Push(0);
            }
            UNIT_ASSERT(!r.has_value());
        }

        const ui64 frozenEnd = queue.NextIndex();
        const ui64 frozenFront = queue.FrontIndex();
        const size_t frozenSize = queue.Size();

        // Hammer with 20 more failing pushes — nothing must change.
        for (int k = 0; k < 20; ++k) {
            auto r = queue.Push(k);
            UNIT_ASSERT(!r.has_value());
            UNIT_ASSERT_VALUES_EQUAL(queue.NextIndex(), frozenEnd);
            UNIT_ASSERT_VALUES_EQUAL(queue.FrontIndex(), frozenFront);
            UNIT_ASSERT_VALUES_EQUAL(queue.Size(), frozenSize);
            UNIT_ASSERT(queue.Find(i0.value()) != nullptr);
        }

        // Erase the stuck item → queue recovers.
        UNIT_ASSERT(queue.Erase(i0.value()));
        auto r = queue.Push(200);
        UNIT_ASSERT_C(r.has_value(), r.error());
    }

    // Fuzz TCircularSparseQueue against a THashMap reference model:
    // push with probability p, erase a random live index otherwise.
    // Occasionally mark an index "sticky" so it survives longer.
    // After every operation verify Size() and all live indices vs the model.
    Y_UNIT_TEST(FuzzAgainstHashMap) {
        const size_t N = 16;
        TCircularSparseQueue<int> queue(N);
        THashMap<ui64, int> model; // index → value
        TFastRng64 rng(12345678ULL);

        constexpr int kIterations = 100'000;
        constexpr double kPushProb = 0.55; // slightly more pushes than erases
        constexpr ui64 kStickyMask = 0xFF; // 1/256 chance to mark an index sticky

        TVector<ui64> stickyIndices; // these survive until explicitly freed
        ui64 nextValue = 0;

        for (int iter = 0; iter < kIterations; ++iter) {
            const double r = static_cast<double>(rng.GenRand()) / static_cast<double>(~0ULL);
            if (r < kPushProb || model.empty()) {
                int val = static_cast<int>(nextValue++);
                auto res = queue.Push(val);
                if (res.has_value()) {
                    const ui64 idx = res.value();
                    model[idx] = val;
                    // Occasionally make this index sticky.
                    if ((rng.GenRand() & kStickyMask) == 0) {
                        stickyIndices.push_back(idx);
                    }
                }
                // A failed push (full or slot occupied) is a no-op in the model.
            } else {
                // Erase a random live index from the model (but not a sticky one).
                // Build list of erasable keys.
                TVector<ui64> keys;
                keys.reserve(model.size());
                for (auto& [k, _] : model) {
                    bool sticky = false;
                    for (ui64 s : stickyIndices) {
                        if (s == k) { sticky = true; break; }
                    }
                    if (!sticky) {
                        keys.push_back(k);
                    }
                }
                if (!keys.empty()) {
                    const ui64 idx = keys[rng.GenRand() % keys.size()];
                    const bool erased = queue.Erase(idx);
                    UNIT_ASSERT_C(erased, "Erase failed for live index " << idx
                        << " at iter " << iter);
                    model.erase(idx);
                }
            }

            // Periodically release sticky indices.
            if ((iter & 0x3FF) == 0 && !stickyIndices.empty()) {
                for (ui64 idx : stickyIndices) {
                    if (model.count(idx)) {
                        queue.Erase(idx);
                        model.erase(idx);
                    }
                }
                stickyIndices.clear();
            }

            // Invariant: Size() == model.size().
            UNIT_ASSERT_VALUES_EQUAL_C(queue.Size(), model.size(),
                "Size mismatch at iter " << iter);

            // Every live model index must be findable in the queue with the right value.
            for (auto& [idx, val] : model) {
                auto* p = queue.Find(idx);
                UNIT_ASSERT_C(p != nullptr,
                    "Find returned null for live index " << idx << " at iter " << iter);
                UNIT_ASSERT_VALUES_EQUAL_C(*p, val,
                    "Value mismatch for index " << idx << " at iter " << iter);
            }
        }
    }
};

} // namespace NKikimr
