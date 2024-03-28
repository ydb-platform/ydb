#pragma once

#include "flat_fwd_sieve.h"
#include "flat_mem_snapshot.h"
#include "flat_mem_warm.h"
#include "flat_part_screen.h"
#include "flat_part_iface.h"
#include "flat_page_blobs.h"

#include <util/generic/bitmap.h>
#include <util/generic/xrange.h>

namespace NKikimr {
namespace NTable {
namespace NFwd {

    class TMemTableHandler /* handler for TMemTable table */ {
    public:
        using TResult = IPages::TResult;
        using TRace = TVector<TMemTableSnapshot>;

        TMemTableHandler(TArrayRef<const ui32> tags, ui32 edge, const TRace *trace)
            : Tags(tags)
            , Edge(edge)
        {
            if (trace) {
                for (auto &one: *trace)
                    if (auto *blobs = one->GetBlobs())
                        Offset = Min(Offset, blobs->Head);

                Blobs = TMemTable::MakeBlobsPage(*trace);
                Touches.Reserve(Blobs->Total());
            }
        }

        TResult Locate(const TMemTable *memTable, ui64 ref, ui32 tag) noexcept
        {
            const auto &glob = memTable->GetBlobs()->Get(ref);

            Y_ABORT_UNLESS(glob.Data, "External blob in TMemTable with no data");
            Y_ABORT_UNLESS(!Blobs || ref >= Offset, "Unexpected ELargeObj reference");

            bool omit = glob.Bytes() >= Edge && !TRowScheme::HasTag(Tags, tag);

            if (omit && Blobs) Touches.Set(ref - Offset);

            return { !omit, omit ? nullptr : &glob.Data };
        }

        TSieve Traced() noexcept
        {
            /* Blobs in TMemTable catalog isn't sorted in order of appearance
                in rows cells. That is way bitmap is used instead of just
                trace cooker as it done for alredy compacted TPart.
            */

            TScreen::TCook trace;

            for (auto seq: xrange(Blobs->Total()))
                if (Touches[seq])
                    trace.Pass(seq);

            return { std::move(Blobs), nullptr, nullptr, trace.Unwrap() };
        }

    private:
        const TArrayRef<const ui32> Tags;
        const ui32 Edge = Max<ui32>();
        ui64 Offset = Max<ui64>();
        TIntrusiveConstPtr<NPage::TExtBlobs> Blobs;
        TDynBitMap Touches;
    };
}
}
}
