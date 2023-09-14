#pragma once

#include "flat_fwd_iface.h"
#include "flat_fwd_page.h"
#include "flat_fwd_misc.h"
#include "flat_page_blobs.h"
#include "flat_part_screen.h"
#include "flat_part_slice.h"

namespace NKikimr {
namespace NTable {
namespace NFwd {

    class TBlobs : public IPageLoadingLogic {
        using THoles = TScreen::TCook;

    public:
        using TEdges = TVector<ui32>;

        TBlobs(TIntrusiveConstPtr<NPage::TFrames> frames, TIntrusiveConstPtr<TSlices> slices, TEdges edge, bool trace)
            : Edge(std::move(edge))
            , Frames(std::move(frames))
            , Filter(std::move(slices))
            , Trace(trace ? new THoles{ } : nullptr)
        {
            Tags.resize(Frames->Stats().Tags.size(), 0);

            Y_VERIFY(Edge.size() == Tags.size(), "Invalid edges vector");
        }

        ~TBlobs()
        {
            for (auto &it: Pages) it.Release();
        }

        TResult Handle(IPageLoadingQueue *head, ui32 ref, ui64 lower) noexcept override
        {
            Y_VERIFY(ref >= Lower, "Cannot handle backward blob reads");

            auto again = (std::exchange(Tags.at(FrameTo(ref)), 1) == 0);

            Grow = again ? Lower : Max(Lower, Grow);

            Rewind(Lower).Shrink(false); /* points Offset to current frame */

            bool more = Grow < Max<TPageId>() && (OnHold + OnFetch < lower);

            auto &page = Preload(head, 0).Lookup(ref);

            return { page.Touch(ref, Stat), more, page.Size < Edge[page.Tag] };
        }

        void Forward(IPageLoadingQueue *head, ui64 upper) noexcept override
        {
            Preload(head, upper);
        }

        void Apply(TArrayRef<NPageCollection::TLoadedPage> loaded) noexcept override
        {
            for (auto &one: loaded) {
                if (!Pages || one.PageId < Pages.front().PageId) {
                    Y_FAIL("Blobs fwd cache got page below queue");
                } else if (one.PageId > Pages.back().PageId) {
                    Y_FAIL("Blobs fwd cache got page above queue");
                } else if (one.Data.size() > OnFetch) {
                    Y_FAIL("Blobs fwd cache ahead counters is out of sync");
                }

                Stat.Saved += one.Data.size();
                OnFetch -= one.Data.size();
                OnHold += Lookup(one.PageId).Settle(one);
            }

            Shrink(false /* do not drop loading pages */);
        }

        TDeque<TScreen::THole> Traced() noexcept
        {
            Rewind(Max<TPageId>()).Shrink(true /* complete trace */);

            return Trace ? Trace->Unwrap() : TDeque<TScreen::THole>{ };
        }

        TIntrusiveConstPtr<NPage::TFrames> GetFrames() const noexcept
        {
            return Frames;
        }

        TIntrusiveConstPtr<TSlices> GetSlices() const noexcept
        {
            return Filter.GetSlices();
        }

    private:
        TPage& Lookup(ui32 ref) noexcept
        {
            const auto end = Pages.begin() + Offset;

            if (ref >= end->PageId) {
                return Pages.at(Offset + (ref - end->PageId));
            } else {
                auto it = std::lower_bound(Pages.begin(), end, ref);

                Y_VERIFY(it != end && it->PageId == ref);

                return *it;
            }
        }

        ui32 FrameTo(TPageId ref) noexcept
        {
            if (ref >= Lower && ref < Upper) {
                return Lookup(ref).Tag;
            } else if (!Pages || Pages.back().PageId < ref) {
                return FrameTo(ref, Frames->Relation(ref));
            } else {
                const auto &page = Lookup(ref);
                Y_VERIFY(page.Size < Max<ui32>(), "Unexpected huge page");

                i16 refer = ref - page.Refer; /* back to relative refer */

                return FrameTo(ref, { 0, page.Tag, refer, static_cast<ui32>(page.Size) });
            }
        }

        ui32 FrameTo(TPageId ref, NPage::TFrames::TEntry rel) noexcept
        {
            Lower = Min(ref, rel.AbsRef(ref));
            Upper = ref + 1; /* will be extended eventually */

            return rel.Tag;
        }

        TBlobs& Preload(IPageLoadingQueue *head, ui64 upper) noexcept
        {
            auto until = [this, upper]() { return OnHold + OnFetch < upper; };

            while (Grow != Max<TPageId>() && (Grow < Upper || until())) {
                const auto next = Propagate(Grow);

                Y_VERIFY(Grow < next, "Unexpected frame upper boundary");

                Grow = (next < Max<TPageId>() ? Grow : next);

                for ( ; Grow < next; Grow++) {
                    auto &page = Lookup(Grow);
                    const auto rel = Frames->Relation(Grow);

                    if (!Tags.at(page.Tag) || page.Size >= Edge.at(page.Tag) || !Filter.Has(rel.Row)) {
                        /* Page doesn't fits to load criteria   */
                    } else if (page.Fetch == EFetch::None) {
                        auto size = head->AddToQueue(Grow, EPage::Opaque);

                        Y_VERIFY(size == page.Size, "Inconsistent page sizez");

                        page.Fetch = EFetch::Wait;
                        Stat.Fetch += page.Size;
                        OnFetch += page.Size;
                    }
                }
            }

            return *this;
        }

        TPageId Propagate(const TPageId base) noexcept
        {
            if (Pages && base <= Pages.back().PageId) {
                return Lookup(base).Refer;
            } else if (Pages && base != Lower && base - Pages.back().PageId != 1) {
                Y_FAIL("Cannot do so long jumps around of frames");
            } else {
                const auto end = Frames->Relation(base).AbsRef(base);

                Upper = (base == Lower ? end : Lower);

                for (auto page = base; page < end; page++) {
                    const auto rel = Frames->Relation(page);
                    const auto ref = rel.AbsRef(page);

                    Pages.emplace_back(page, rel.Size, rel.Tag, ref);
                }

                return end == base ? Max<TPageId>() : end;
            }
        }

        TBlobs& Rewind(TPageId until) noexcept
        {
            for (; Offset < Pages.size(); Offset++) {
                auto &page = Pages.at(Offset);

                if (page.PageId >= until) {
                    break;
                } else if (page.Size == 0) {
                    Y_FAIL("Dropping page that hasn't been propagated");
                } else if (auto size = page.Release().size()) {
                    OnHold -= size;

                    if (page.Usage == EUsage::None)
                        *(page.Ready() ? &Stat.After : &Stat.Before) += size;
                }
            }

            return *this;
        }

        TBlobs& Shrink(bool force = false) noexcept
        {
            for (; Offset && (Pages[0].Ready() || force); Offset--) {

                if (Trace && Pages.front().Usage == EUsage::Seen) {
                    /* Trace mode is used to track entities that was used by
                        reference and, thus, has not been materialized into
                        another storage unit. Later this set may be used for
                        resource lifetime prolongation.
                     */

                    Trace->Pass(Pages.front().PageId);
                }

                Pages.pop_front();
            }

            return *this;
        }

    private:
        const TVector<ui32> Edge;       /* Desired bytes limit of blobs */
        const TIntrusiveConstPtr<NPage::TFrames> Frames;
        const TSlicesRowFilter Filter;
        TVector<ui8> Tags;              /* Ever used col tags on env    */
        TPageId Lower = 0;              /* Pinned frame lower bound ref */
        TPageId Upper = 0;              /* Pinned frame upper bound ref */
        TPageId Grow = Max<TPageId>();  /* Edge page of loading process */

        TAutoPtr<THoles> Trace;

        /*_ Forward cache line state */

        ui64 OnHold = 0;
        ui64 OnFetch = 0;
        ui32 Offset = 0;
        TDeque<TPage> Pages;
    };

}
}
}
