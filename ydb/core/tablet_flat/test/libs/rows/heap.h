#pragma once

#include "rows.h"

#include <util/generic/deque.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    class TRowsHeap {
    public:
        using TIter = TDeque<TRow>::const_iterator;

        template<typename Fn, typename ...TArgs>
        TRowsHeap(size_t heap, Fn fn, size_t num, TArgs&& ... args)
            : TRowsHeap(heap)
        {
            for (size_t it = 0; it < num; it++) {
                Put(fn(it, std::forward<TArgs>(args)...));
            }
        }

        TRowsHeap(size_t heap) : Heap(new TGrowHeap(heap)) { }

        TRowsHeap(TIntrusivePtr<TGrowHeap> heap) : Heap(std::move(heap)) { }

        template<typename TRand>
        TIter Any(TRand &rnd) const noexcept
        {
            return begin() + rnd.Uniform(Rows.size());
        }

        template<typename TRand>
        TIter AnyIn(TRand &rnd, TIter it, const TIter end) const noexcept
        {
            return it + rnd.Uniform(std::distance(it, end));
        }

        template<typename TRand>
        TIter AnyOff(TRand &rnd, TIter it, const TIter end) const noexcept
        {
            size_t on = rnd.Uniform(Size() - std::distance(it, end));

            auto left = ui64(std::distance(begin(), it));

            return on < left ? begin() + on : end + (on - left);
        }

        TIter begin() const noexcept
        {
            return Rows.begin();
        }

        TIter end() const noexcept
        {
            return Rows.end();
        }

        size_t Size() const noexcept
        {
            return Rows.size();
        }

        const TRow& operator[](size_t on) const noexcept
        {
            Y_ABORT_UNLESS(on < Rows.size(), "Row index is out of hole scope");

            return Rows[on];
        }

        const TRow& Put(const TRow &row)
        {
            Rows.emplace_back(Heap.Get());
            Rows.back().Add(row);

            return Rows.back();
        }

    private:
        TIntrusivePtr<TGrowHeap> Heap;
        TDeque<TRow>    Rows;
    };

}
}
}
