#pragma once

#include "flat_row_eggs.h"
#include "util_basics.h"
#include "util_fmt_abort.h"
#include <util/generic/deque.h>
#include <util/generic/vector.h>
#include <util/generic/ptr.h>

#include <numeric>

namespace NKikimr {
namespace NTable {

    class TScreen : public TAtomicRefCount<TScreen> {
    public:
        struct THole {
            explicit THole(bool all)
                : Begin(all ? 0 : Max<TRowId>())
            {

            }

            THole(TRowId begin, TRowId end)
                : Begin(begin)
                , End(end)
            {

            }

            explicit operator bool() const
            {
                return Begin < End;
            }

            bool operator==(const THole &hole) const
            {
                return Begin == hole.Begin && End == hole.End;
            }

            bool Has(TRowId rowId) const
            {
                return Begin <= rowId && rowId < End;
            }

            THole Cut(THole hole) const
            {
                auto begin = Max(Begin, hole.Begin);
                auto end = Min(End, hole.End);

                return begin > end ? THole(false) : THole(begin, end);
            }

            TRowId Begin = 0;
            TRowId End = Max<TRowId>();
        };

        struct TCook {
            TDeque<THole> Unwrap()
            {
                Pass(Max<TRowId>()); /* will flush current hole */

                return std::exchange(Holes, TDeque<THole>{ });
            }

            void Pass(TRowId ref)
            {
                Y_ENSURE(Tail <= ref, "Got page ref from the past");

                if (Open != Max<TRowId>() && Tail != ref) {
                    auto begin = std::exchange(Open, Max<TRowId>());

                    Holes.emplace_back(begin, Tail);
                }

                Open = Min(Open, ref);
                Tail = ref + 1;
            }

        private:
            TRowId Open = Max<TRowId>();
            TRowId Tail = 0;
            TDeque<THole> Holes;
        };

        template<typename TVec>
        static TRowId Sum(const TVec &vec) noexcept
        {
            return
                std::accumulate(vec.begin(), vec.end(), TRowId(0),
                    [](TRowId rows, const THole &hole) {
                        return rows + (hole.End - hole.Begin);
                    });
        }

        using TVec = TVector<THole>;

        TScreen(TVec holes) : Holes(std::move(holes)) { }

        static THole Iter(const TIntrusiveConstPtr<TScreen> &ref, size_t &on, TRowId rowId, int dir)
        {
            on = ref ? ref->Lookup(rowId, dir) : Max<size_t>();

            return ref ? ref->Hole(on) : THole(true);
        }

        static THole Next(const TIntrusiveConstPtr<TScreen> &ref, size_t &on, int dir)
        {
             return ref ? ref->Hole(on += dir) : THole(false);
        }

        static bool Intersects(const TScreen *scr, THole hole)
        {
            return bool(hole.Cut(scr ? scr->Bounds() : THole(false)));
        }

        size_t Size() const noexcept
        {
            return Holes.size();
        }

        TVec::const_iterator begin() const
        {
            return Holes.begin();
        }

        TVec::const_iterator end() const
        {
            return Holes.end();
        }

        bool Cmp(const TScreen &screen) const noexcept
        {
            return
                screen.Size() == Size()
                && std::equal(begin(), end(), screen.begin());
        }

        size_t Lookup(TRowId rowId, int dir) const
        {
            Y_ENSURE(dir == +1, "Only forward direction supported");

            auto less = [](TRowId rowId, const THole &hole) {
                return rowId < hole.End;
            };

            auto it = std::upper_bound(Holes.begin(), Holes.end(), rowId, less);

            return it == Holes.end() ? Max<size_t>() : (it - Holes.begin());
        }

        THole Hole(size_t on) const noexcept
        {
            return on < Holes.size() ? Holes[on] : THole(false);
        }

        THole Bounds() const noexcept
        {
            return
                THole(
                    Holes ? Holes[0].Begin : Max<TRowId>(),
                    Holes ? Holes.back().End : Max<TRowId>() );
        }

        void Validate() const
        {
            TRowId last = 0;
            for (const auto &hole : Holes) {
                Y_ENSURE(std::exchange(last, hole.End) <= hole.Begin,
                    "Screen not sorted or has intersections");
            }
        }

        static TIntrusiveConstPtr<TScreen> Cut(TIntrusiveConstPtr<TScreen> scr, THole hole)
        {
            if (hole == THole(true)) {
                return scr;
            } else if (scr == nullptr || !hole) {
                return hole ? new TScreen({ hole }) : new TScreen({});
            }

            auto first = scr->Lookup(hole.Begin, +1);

            if (hole.Cut(scr->Hole(first))) {
                auto last = scr->Lookup(hole.End, +1);

                last = Min(last, scr->Size() - 1);

                if (hole.End <= scr->Hole(last).Begin) {
                    last = Max(first, last - 1);
                }

                TVec sub(last - first + 1, THole(false));

                std::copy(
                    scr->begin() + first, scr->begin() + last + 1, sub.begin());

                sub.front() = hole.Cut(sub.front());
                sub.back() = hole.Cut(sub.back());

                if (!sub.front() || !sub.back()) {
                    Y_TABLET_ERROR("Produced trival edges on screen cutting");
                }

                return new TScreen(std::move(sub));

            } else {
                return new TScreen({ /* opaque screen */});
            }
        }

        static TIntrusiveConstPtr<TScreen> Join(TIntrusiveConstPtr<TScreen> one, TIntrusiveConstPtr<TScreen> two)
        {
            if (one == nullptr || one->Size() == 0) {
                return two;
            } else if (two == nullptr || two->Size() == 0) {
                return one;
            } else if (one->Bounds().Cut(two->Bounds())) {
                Y_TABLET_ERROR("Cannot join two intersecting screens");
            } else if (one->Bounds().End > two->Bounds().Begin) {
                std::swap(one, two);
            }

            size_t glide = (one->Bounds().End == two->Bounds().Begin ? 1 : 0);

            TVec sub(one->Size() + two->Size() - glide, THole(false));

            std::copy(one->begin(), one->end(), sub.begin());

            if (glide) {
                sub[one->Size() - 1].End = two->begin()->End;
            }

            std::copy(
                two->begin() + glide, two->end(), sub.begin() + one->Size());

            if (sub.size() != 1 || !(sub[0] == THole(true))) {
                return new TScreen(std::move(sub));
            } else {
                return nullptr; /* transparent screen */
            }
        }

    private:
        const TVector<THole> Holes;
    };

}
}
