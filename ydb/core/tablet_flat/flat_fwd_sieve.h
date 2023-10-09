#pragma once

#include "flat_page_blobs.h"
#include "flat_part_screen.h"
#include "flat_part_slice.h"
#include <util/generic/deque.h>

namespace NKikimr {
namespace NTable {
namespace NFwd {

    struct TSieve {
        using THoles = TDeque<TScreen::THole>;

        TSieve() { };

        TSieve(
                TIntrusiveConstPtr<NPage::TExtBlobs> blobs,
                TIntrusiveConstPtr<NPage::TFrames> frames,
                TIntrusiveConstPtr<TSlices> slices,
                THoles holes)
            : Blobs(std::move(blobs))
            , Frames(std::move(frames))
            , Filter(std::move(slices))
            , Holes(std::move(holes))
        {

        }

        ui32 Total() const noexcept
        {
            if (!Blobs) {
                return 0;
            }

            if (!Frames) {
                return Blobs->Total();
            }

            ui32 total = 0;
            for (ui32 page = 0; page < Blobs->Total(); ++page) {
                if (Filter.Has(Frames->Relation(page).Row)) {
                    ++total;
                }
            }

            return total;
        }

        ui32 Removed() const noexcept
        {
            return TScreen::Sum(Holes);
        }

        void MaterializeTo(TVector<TLogoBlobID> &vec) const noexcept
        {
            const auto limit = Blobs ? Blobs->Total() : 0;

            for (ui64 seq = 0, off = 0; off <= Holes.size(); off++) {
                Y_ABORT_UNLESS(off == Holes.size() || Holes.at(off).End <= limit);

                auto end = off < Holes.size() ? Holes[off].Begin : limit;

                for (; seq < end; seq++) {
                    if (!Frames || Filter.Has(Frames->Relation(seq).Row)) {
                        vec.emplace_back(Blobs->Glob(seq).Logo);
                    }
                }

                seq = Max(seq, off < Holes.size() ? Holes[off].End : limit);
            }
        }

        TIntrusiveConstPtr<NPage::TExtBlobs> Blobs;
        TIntrusiveConstPtr<NPage::TFrames> Frames;
        TSlicesRowFilter Filter;
        TDeque<TScreen::THole> Holes;
    };

    struct TSeen {
        const ui64 Total;
        const ui64 Seen;
        TDeque<TSieve> Sieve;
    };

}
}
}
