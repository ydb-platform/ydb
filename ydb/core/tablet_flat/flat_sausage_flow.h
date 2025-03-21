#pragma once

#include "flat_sausage_misc.h"
#include "flat_sausage_solid.h"
#include "util_fmt_abort.h"
#include <util/generic/deque.h>

namespace NKikimr {
namespace NPageCollection {

    template<typename TPageCollectionClass>
    class TPagesToBlobsConverter {
    public:
        struct TReadPortion {

            void Describe(IOutputStream &out) const
            {
                out
                    << "{" << Slot << "p +" << Skip << "b " << Size << "b}";
            }

            bool operator==(const TReadPortion &br) const noexcept
            {
                return
                    Slot == br.Slot && Skip == br.Skip
                    && Size == br.Size && Blob == br.Blob;
            }

            ui32 Slot;  /* Page where to put data       */
            ui32 Size;  /* Bytes, may be more that page */
            ui32 Blob;  /* Blobs storage group number   */
            ui32 Skip;  /* Request data offset in blob  */
        };

        struct TReadPortionRange {
            constexpr explicit operator bool() const
            {
                return +*this > 0;
            }

            constexpr ui32 operator+() const
            {
                return To - From;
            }

            ui32 From, To;
        };

        TPagesToBlobsConverter(const TPageCollectionClass &pageCollection, TArrayRef<const ui32> pages)
            : PageCollection(pageCollection)
            , Slice(pages)
        {}

        bool Complete() const noexcept
        {
            return Tail >= Slice.size();
        }

        TReadPortionRange Grow(ui64 bytes)
        {
            const ui32 from = Queue.size();
            const ui64 limit = OnHold + Min(Max<ui64>() - OnHold, bytes);

            for (ui32 group = Glob.Group; Tail < Slice.size(); Tail++) {
                const auto bound = PageCollection.Bounds(Slice[Tail]);

                if (OnHold + bound.Bytes > limit && from < Queue.size())
                    break;

                ui64 bucket = bound.Bytes; /* total bounded data size */

                for (ui32 it = bound.Lo.Blob; it <= bound.Up.Blob; it++) {
                    (std::exchange(Blob, it) != it) && (Glob = PageCollection.Glob(it));

                    auto was = std::exchange(group, Glob.Group);

                    if (was == Glob.Group || was == TLargeGlobId::InvalidGroup) {

                    } else if (it != bound.Lo.Blob) {
                        Y_TABLET_ERROR("Page placed over different groups");
                    } else if (from < Queue.size()) {
                        /* Have to do each grow over the same group */

                        return { from, ui32(Queue.size()) };
                    }

                    ui32 skip = it > bound.Lo.Blob ? 0 : bound.Lo.Skip;
                    ui32 left = Glob.Logo.BlobSize() - skip;
                    ui32 chunk = bucket <= left ? bucket : left;

                    Queue.emplace_back(TReadPortion{ Tail, chunk, it, skip });

                    (OnHold += chunk) && (bucket -= chunk);
                }
            }

            return { from, ui32(Queue.size()) };
        }

        const TPageCollectionClass &PageCollection;
        const TArrayRef<const ui32> Slice;

        ui32 Head = 0;      /* Slot Offset of the first entry   */
        ui32 Tail = 0;      /* Next page slot to process        */
        ui64 OnHold = 0;    /* Total bytes prepared in the flow */

        TDeque<TReadPortion> Queue;

    private:
        /*_ Basic size lookup cache, omits frequent lookups */

        ui32 Blob   = Max<ui32>();
        TGlobId Glob;
    };

}
}
