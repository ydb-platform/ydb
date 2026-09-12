#include "block_range_field_set.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/memory/arena_allocator.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>
#include <util/random/random.h>

namespace NYdb::NBS::NBlockStore {

namespace {

TBlockRangeFieldSet::TRange R(ui16 start, ui16 end)
{
    return TBlockRangeFieldSet::TRange::MakeClosedInterval(start, end);
}

}   // namespace

Y_UNIT_TEST_SUITE(TBlockRangeFieldSetFuzz)
{
    // Randomized Add/Remove sequence verified against a naive interval-set
    // model after every operation: segment count, block count, Enumerate
    // order (ascending by Start) and contents must match exactly.
    Y_UNIT_TEST(EnumerateSortedAfterRandomOps)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldSet f{allocator, 256 * 1024};

        TVector<TBlockRangeFieldSet::TRange> model;
        for (ui32 iter = 0; iter < 5000; ++iter) {
            // Occasionally hit boundaries: 0 and 65535.
            ui16 start;
            const ui32 mode = RandomNumber<ui32>(10);
            if (mode == 0) {
                start = 0;
            } else if (mode == 1) {
                start = 65535;
            } else {
                start = RandomNumber<ui16>(65535);
            }
            const ui16 len = 1 + RandomNumber<ui16>(100);
            const ui32 end32 = Min<ui32>(65535u, start + len - 1);
            auto r = R(start, static_cast<ui16>(end32));

            if (RandomNumber<ui32>(2) == 0) {
                bool changed = false;
                UNIT_ASSERT(f.TryAdd(r, &changed));
                // TryAdd returns true even when fully covered
                // (changed=false) — update the model only on real change.
                if (changed) {
                    TVector<TBlockRangeFieldSet::TRange> next;
                    TBlockRangeFieldSet::TRange cur = r;
                    for (const auto& x: model) {
                        if (static_cast<ui32>(x.End) + 1 < cur.Start ||
                            static_cast<ui32>(cur.End) + 1 < x.Start)
                        {
                            next.push_back(x);
                        } else {
                            cur.Start = Min(cur.Start, x.Start);
                            cur.End = Max(cur.End, x.End);
                        }
                    }
                    next.push_back(cur);
                    model = std::move(next);
                }
            } else {
                bool changed = false;
                UNIT_ASSERT(f.TryRemove(r, &changed));
                // TryRemove returns true even when nothing overlaps
                // (changed=false) — update the model only on real change.
                if (changed) {
                    TVector<TBlockRangeFieldSet::TRange> next;
                    for (const auto& x: model) {
                        if (static_cast<ui32>(x.End) < r.Start ||
                            static_cast<ui32>(r.End) < x.Start)
                        {
                            next.push_back(x);
                            continue;
                        }
                        if (x.Start < r.Start) {
                            next.push_back(R(x.Start, r.Start - 1));
                        }
                        if (x.End > r.End) {
                            next.push_back(R(r.End + 1, x.End));
                        }
                    }
                    model = std::move(next);
                }
            }

            Sort(model, [](auto& a, auto& b) { return a.Start < b.Start; });

            UNIT_ASSERT_VALUES_EQUAL_C(
                model.size(),
                f.GetSegmentCount(),
                "iter " << iter << " segment count mismatch");
            size_t blockCount = 0;
            for (const auto& x: model) {
                blockCount += x.Size();
            }
            UNIT_ASSERT_VALUES_EQUAL_C(
                blockCount,
                f.GetBlockCount(),
                "iter " << iter << " block count mismatch");

            TVector<TBlockRangeFieldSet::TRange> seen;
            f.Enumerate(
                [&](TBlockRangeFieldSet::TRange x)
                {
                    seen.push_back(x);
                    return TBlockRangeFieldSet::EEnumerateContinuation::
                        Continue;
                });
            UNIT_ASSERT_VALUES_EQUAL_C(
                model.size(),
                seen.size(),
                "iter " << iter << " enumerate count mismatch");
            for (size_t i = 0; i < model.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    model[i].Start,
                    seen[i].Start,
                    "iter " << iter << " idx " << i << " (order/contents)");
                UNIT_ASSERT_VALUES_EQUAL_C(
                    model[i].End,
                    seen[i].End,
                    "iter " << iter << " idx " << i << " (order/contents)");
            }
        }
    }
}

}   // namespace NYdb::NBS::NBlockStore
