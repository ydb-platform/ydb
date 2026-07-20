#pragma once

#include "flat_fwd_iface.h"
#include "flat_fwd_page.h"
#include "flat_fwd_misc.h"
#include "flat_part_screen.h"
#include "flat_part_slice.h"
#include "flat_sausage_gut.h"
#include "util_fmt_abort.h"

namespace NKikimr {
namespace NTable {
namespace NFwd {

    using TPageOffset = NPage::TPageOffset;

    class TBlobs : public IPageLoadingLogic {
        using THoles = TScreen::TCook;

    public:
        using TEdges = TVector<ui32>;

        TBlobs(TIntrusiveConstPtr<NPage::TFrames> frames, TIntrusiveConstPtr<TSlices> slices, TEdges edge, bool trace,
                TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection)
            : Edge(std::move(edge))
            , Frames(std::move(frames))
            , PageCollection(std::move(pageCollection))
            , Filter(std::move(slices))
            , Trace(trace ? new THoles{ } : nullptr)
        {
            Tags.resize(Frames->Stats().Tags.size(), 0);

            Y_ENSURE(Edge.size() == Tags.size(), "Invalid edges vector");
        }

        ~TBlobs()
        {
        }

        TResult Get(IPageLoadingQueue *head, TPageOffset offset, EPage, ui64 lower) override
        {
            auto pageId = offset.AsPageIndex();

            Y_ENSURE(pageId >= Lower, "Cannot handle backward blob reads");

            auto again = (std::exchange(Tags.at(FrameTo(pageId)), 1) == 0);

            Grow = again ? Lower : Max(Lower, Grow);

            Rewind(Lower).Shrink(false); /* points Position to current frame */

            bool more = Grow < Max<TPageId>() && (OnHold + OnFetch < lower);

            auto &page = Preload(head, 0).Lookup(pageId);

            return { page.Touch(offset, Stat), more, page.Size < Edge[page.Tag] };
        }

        void Forward(IPageLoadingQueue *head, ui64 upper) override
        {
            Preload(head, upper);
        }

        void Fill(NPageCollection::TLoadedPage& page, NSharedCache::TSharedPageRef sharedPageRef, EPage) override
        {
            if (!Pages || page.Location.Offset < Pages.front().Offset) {
                Y_TABLET_ERROR("Blobs fwd cache got page below queue");
            } else if (page.Location.Offset > Pages.back().Offset) {
                Y_TABLET_ERROR("Blobs fwd cache got page above queue");
            } else if (page.Data.size() > OnFetch) {
                Y_TABLET_ERROR("Blobs fwd cache ahead counters is out of sync");
            }

            Stat.Saved += page.Data.size();
            OnFetch -= page.Data.size();
            OnHold += Lookup(page.Location.Offset.AsPageIndex()).Settle(page, std::move(sharedPageRef));

            Shrink(false /* do not drop loading pages */);
        }

        TDeque<TScreen::THole> Traced()
        {
            Rewind(Max<TPageId>()).Shrink(true /* complete trace */);

            return Trace ? Trace->Unwrap() : TDeque<TScreen::THole>{ };
        }

        TIntrusiveConstPtr<NPage::TFrames> GetFrames() const
        {
            return Frames;
        }

        TIntrusiveConstPtr<TSlices> GetSlices() const
        {
            return Filter.GetSlices();
        }

    private:
        TPage& Lookup(TPageId id)
        {
            const auto end = Pages.begin() + Position;
            auto offset = TPageOffset::FromPageIndex(id);

            if (offset >= end->Offset) {
                return Pages.at(Position + (id - end->Offset.AsPageIndex()));
            } else {
                auto it = std::lower_bound(Pages.begin(), end, offset);

                Y_ENSURE(it != end && it->Offset == offset);

                return *it;
            }
        }

        ui32 FrameTo(TPageId ref)
        {
            if (ref >= Lower && ref < Upper) {
                return Lookup(ref).Tag;
            } else if (!Pages || Pages.back().Offset < TPageOffset::FromPageIndex(ref)) {
                return FrameTo(ref, Frames->Relation(ref));
            } else {
                const auto &page = Lookup(ref);
                Y_ENSURE(page.Size < Max<ui32>(), "Unexpected huge page");

                i16 refer = ref - page.Refer; /* back to relative refer */

                return FrameTo(ref, { 0, page.Tag, refer, static_cast<ui32>(page.Size) });
            }
        }

        ui32 FrameTo(TPageId ref, NPage::TFrames::TEntry rel)
        {
            Lower = Min(ref, rel.AbsRef(ref));
            Upper = ref + 1; /* will be extended eventually */

            return rel.Tag;
        }

        TBlobs& Preload(IPageLoadingQueue *head, ui64 upper)
        {
            auto until = [this, upper]() { return OnHold + OnFetch < upper; };

            while (Grow != Max<TPageId>() && (Grow < Upper || until())) {
                const auto next = Propagate(Grow);

                Y_ENSURE(Grow < next, "Unexpected frame upper boundary");

                Grow = (next < Max<TPageId>() ? Grow : next);

                for ( ; Grow < next; Grow++) {
                    auto &page = Lookup(Grow);
                    const auto rel = Frames->Relation(Grow);

                    if (!Tags.at(page.Tag) || page.Size >= Edge.at(page.Tag) || !Filter.Has(rel.Row)) {
                        /* Page doesn't fits to load criteria   */
                    } else if (page.Fetch == EFetch::None) {
                         auto size = head->AddToQueue(TPageOffset::FromPageIndex(Grow), EPage::Opaque, page.Size, page.Crc32);
                        Y_ENSURE(size == page.Size, "Inconsistent page sizes");

                        page.Fetch = EFetch::Wait;
                        Stat.Fetch += page.Size;
                        OnFetch += page.Size;
                    }
                }
            }

            return *this;
        }

        TPageId Propagate(const TPageId base)
        {
            if (Pages && TPageOffset::FromPageIndex(base) <= Pages.back().Offset) {
                return Lookup(base).Refer;
            } else if (Pages && base != Lower && base - Pages.back().Offset.AsPageIndex() != 1) {
                Y_TABLET_ERROR("Cannot do so long jumps around of frames");
            } else {
                const auto end = Frames->Relation(base).AbsRef(base);

                Upper = (base == Lower ? end : Lower);

                for (auto page = base; page < end; page++) {
                    const auto rel = Frames->Relation(page);
                    const auto ref = rel.AbsRef(page);
                    const auto crc32 = PageCollection
                        ? PageCollection->GetLocation(page).Crc32
                        : ui32(0);

                    Pages.emplace_back(TPageOffset::FromPageIndex(page), rel.Size, rel.Tag, ref, crc32);
                }

                return end == base ? Max<TPageId>() : end;
            }
        }

        TBlobs& Rewind(TPageId until)
        {
            for (; Position < Pages.size(); Position++) {
                auto &page = Pages.at(Position);

                if (page.Offset >= TPageOffset::FromPageIndex(until)) {
                    break;
                } else if (page.Size == 0) {
                    Y_TABLET_ERROR("Dropping page that hasn't been propagated");
                } else if (auto size = page.Release().size()) {
                    OnHold -= size;

                    if (page.Usage == EUsage::None)
                        *(page.Ready() ? &Stat.After : &Stat.Before) += size;
                }
            }

            return *this;
        }

        TBlobs& Shrink(bool force = false)
        {
            for (; Position && (Pages[0].Ready() || force); Position--) {

                if (Trace && Pages.front().Usage == EUsage::Seen) {
                    /* Trace mode is used to track entities that was used by
                        reference and, thus, has not been materialized into
                        another storage unit. Later this set may be used for
                        resource lifetime prolongation.
                     */

                    Trace->Pass(Pages.front().Offset.AsPageIndex());
                }

                Y_ENSURE(Pages.front().Released(), "Forward cache page still holds data");
                Pages.pop_front();
            }

            return *this;
        }

    private:
        const TVector<ui32> Edge;       /* Desired bytes limit of blobs */
        const TIntrusiveConstPtr<NPage::TFrames> Frames;
        const TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
        const TSlicesRowFilter Filter;
        TVector<ui8> Tags;              /* Ever used col tags on env    */
        TPageId Lower = 0;              /* Pinned frame lower bound ref */
        TPageId Upper = 0;              /* Pinned frame upper bound ref */
        TPageId Grow = Max<TPageId>();  /* Edge page of loading process */
        TAutoPtr<THoles> Trace;

        /*_ Forward cache line state */

        ui64 OnHold = 0;
        ui64 OnFetch = 0;
        ui32 Position = 0;
        TDeque<TPage> Pages;
    };

}
}
}
