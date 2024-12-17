#pragma once

#include "test_part.h"
#include "wrap_part.h"

#include <ydb/core/tablet_flat/test/libs/rows/rows.h>
#include <ydb/core/tablet_flat/flat_part_screen.h>
#include <ydb/core/tablet_flat/flat_part_iter.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    struct TCurtain {
        struct TRes {
            TRowsHeap::TIter Begin;
            TRowsHeap::TIter End;

            TIntrusiveConstPtr<TScreen> Screen;
        };

        TCurtain(const TPartEggs &eggs): Wrap(eggs, { new TTestEnv, 0 }) { }

        TRes Make(const TRowsHeap &heap, TMersenne<ui64> &rnd, size_t max)
        {
            auto len = rnd.Uniform(17, max);

            return Make(heap, rnd.Uniform(heap.Size()), len);
        }

        TRes Make(const TRowsHeap &heap, size_t offset, size_t len)
        {
            Y_ABORT_UNLESS(offset < heap.Size(), "Hole offset is out of the heap");

            const auto on = heap.begin() + offset;

            auto end = on + Min(len, size_t(std::distance(on, heap.end())));

            TRowId h2, h1 = Wrap.Seek(*on, ESeek::Lower)->GetRowId();

            if (end == heap.end()) {
                h2 = Wrap.Seek({}, ESeek::Upper)->GetRowId();
            } else {
                h2 = Wrap.Seek(*end, ESeek::Lower)->GetRowId();
            }

            if (h1 == Max<TRowId>() || h1 >= h2) {
                ythrow yexception() << "Cannot make screen hole for mass part";
            }

            TScreen::TVec holes = { { h1, h2 } };

            return TRes{ on, end, new TScreen(std::move(holes)) };
        }

    private:
        TCheckIter Wrap;
    };


    struct TSlicer {
        TSlicer(const TRowScheme &scheme) : Scheme(scheme) { }

        TIntrusiveConstPtr<TSlices> Cut(const TPartStore &partStore, const TScreen &screen) noexcept
        {
            TTestEnv env;
            TPartIter first(&partStore, { }, Scheme.Keys, &env);
            TPartIter last(&partStore, { }, Scheme.Keys, &env);
            TVector<TSlice> slices;

            TRowId lastEnd = 0;

            for (const auto &hole : screen) {
                Y_ABORT_UNLESS(lastEnd <= hole.Begin, "Screen is not sorted correctly");
                Y_ABORT_UNLESS(first.Seek(hole.Begin) != EReady::Page);
                Y_ABORT_UNLESS(last.Seek(hole.End) != EReady::Page);
                if (first.GetRowId() < last.GetRowId()) {
                    TArrayRef<const TCell> firstKey;
                    if (first.IsValid()) {
                        firstKey = first.GetRawKey();
                    }

                    TArrayRef<const TCell> lastKey;
                    if (last.IsValid()) {
                        lastKey = last.GetRawKey();
                    }

                    auto cut = TSlices::Cut(
                        partStore.Slices,
                        first.GetRowId(),
                        last.GetRowId(),
                        firstKey,
                        lastKey);

                    for (const auto& slice : *cut) {
                        slices.emplace_back(slice);
                    }
                }
                lastEnd = hole.End;
            }

            TIntrusiveConstPtr<TSlices> result = new TSlices(std::move(slices));
            result->Validate();
            return result;
        }

        const TRowScheme Scheme;
    };
}
}
}
