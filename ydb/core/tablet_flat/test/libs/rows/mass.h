#pragma once

#include "rows.h"
#include "heap.h"

#include <ydb/core/tablet_flat/flat_row_scheme.h>
#include <ydb/core/tablet_flat/util_basics.h>

#include <util/random/mersenne.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    class IModel {
    public:
        IModel(TIntrusiveConstPtr<TRowScheme> scheme) : Scheme(scheme) { }

        virtual ~IModel() = default;
        virtual TRow Make(ui64 seq, bool hole) noexcept = 0;
        virtual ui64 Base(const TRow &row) const noexcept = 0;
        virtual void Check(TArrayRef<const ui64>) const = 0;
        virtual void Describe(IOutputStream&) const noexcept = 0;

        const TIntrusiveConstPtr<TRowScheme> Scheme;
    };

    class TMass {
    public:
        TMass(TAutoPtr<IModel> model, ui64 caps, ui64 seed = 0, float holes = 0.1)
            : Model(model)
            , Heap(new TGrowHeap(64 * 1024))
            , Saved(Heap)
            , Holes(Heap)
        {
            TMersenne<ui64> rnd(seed);
            ui64 overrun = 0;   /* omitted holes, should write  */
            bool last = false;  /* last written row is a hole   */

            for (ui64 it = 0, rows = 0; rows < caps; it++) {
                bool hole = rnd.GenRandReal4() <= holes;

                if (last) {
                    overrun += ui64(std::exchange(hole, false));
                } else if (overrun > 0) {
                    overrun -= ui64(!std::exchange(hole, true));
                }

                auto row = Model->Make(it, hole);

                (hole ? Holes : Saved).Put(std::move(row));

                rows += (last = hole) ? 0 : 1;
            }
        }

        void Describe(IOutputStream &out) const noexcept
        {
            out
                << "Mass{"
                << Saved.Size() << " ~" << Holes.Size()
                << " rows, " << Heap->Used() << "b}";
        }

        const TRow* SnapBy(const TRow &row, bool next, bool hole) const noexcept
        {
            auto it = Model->Base(row);

            if (it > Saved.Size()) {
                Y_ABORT("Last saved TMass row slot is out of store range");
            } else if (next) {
                return it >= Saved.Size() ? nullptr : &Saved[it];
            } else if (hole) {
                return it == 0 ? nullptr : &Saved[it - 1];
            } else {
                return it <= 1 ? nullptr : &Saved[it - 2];
            }
        }

        const TAutoPtr<IModel> Model;
        TIntrusivePtr<TGrowHeap> Heap;
        TRowsHeap Saved;
        TRowsHeap Holes;
    };

}
}
}
