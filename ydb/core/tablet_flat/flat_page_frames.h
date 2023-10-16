#pragma once

#include "flat_row_eggs.h"
#include "flat_page_label.h"
#include "util_deref.h"
#include <algorithm>

namespace NKikimr {
namespace NTable {
namespace NPage {

    class TFrames : public TAtomicRefCount<TFrames> {
    public:
        /* Frames index describes how a set of materialized entities relates
            to a set of rows and its columns. Usually this index is used for
            relation revealing between external blobs and rows in caches.

             * For the first entity in a frame Rerfer sets relative offset to
                the next frame with negative frame length. Inner entities has
                positive offsets relative to the frame head. Thus, Refer
                cannot be equal to zero.

             * Tag just points to physical number of column where entity is
                reffered to. It is required for tag values to be dense along
                of entrire index as they could be used in basic bitmaps for
                speeding filtering. Tag have to be less than (Max<i16>() + 1).
         */

        struct TEntry {
            explicit operator bool() const noexcept
            {
                return Refer != 0 && Row != Max<ui64>();
            }

            bool IsHead() const noexcept
            {
                return Refer < 0;
            }

            ui32 AbsRef(const ui32 page) const noexcept
            {
                Y_ABORT_UNLESS(Refer < 0 || page >= ui32(Refer));

                return page - Refer;
            }

            ui64 Row;   /* TRowId, the same along the same frame    */
            ui16 Tag;
            i16 Refer;  /* Negative values sets frame size on head  */
            ui32 Size;
        };

        struct TStats {
            ui32 Items  = 0;    /* Total records in this index      */
            ui32 Rows   = 0;    /* Total unique frames in relations */
            ui64 Size  = 0;     /* Sum of all TEntry::Size fields   */
            TArrayRef<const ui32> Tags; /* Tags frequency stats */
        };

        struct THeader {
            ui32 Skip   = 0;    /* Skip bytes from header to frames */
            ui32 Rows   = 0;
            ui64 Size   = 0;
            ui32 Pad0_  = 0;
            ui32 Tags   = 0;
        };

        static_assert(sizeof(TEntry) == 16, "Invalid TFrames record unit");
        static_assert(sizeof(THeader) == 24, "Invalid TFrames header unit");

        TFrames(TSharedData raw)
            : Raw(std::move(raw))
            , End({ Max<TRowId>(), Max<ui16>(), 0, 0 })
        {
            Y_ABORT_UNLESS(uintptr_t(Raw.data()) % alignof(TEntry) == 0);

            auto got = NPage::TLabelWrapper().Read(Raw, EPage::Frames);

            Y_ABORT_UNLESS(got == ECodec::Plain && got.Version == 0);
            Y_ABORT_UNLESS(got.Page.size() > sizeof(THeader), "Damaged page");

            auto * const ptr = got.Page.data();
            auto hdr = TDeref<THeader>::At(ptr, 0);

            if (hdr->Skip > got.Page.size())
                Y_ABORT("NPage::TFrame header is out of its blob");

            Stats_.Rows = hdr->Rows;
            Stats_.Size = hdr->Size;
            Stats_.Tags = { TDeref<const ui32>::At(ptr, 24), hdr->Tags  };
            Stats_.Items = (got.Page.size() - hdr->Skip) / sizeof(TEntry);

            if (hdr->Skip < sizeof(THeader) + Stats_.Tags.size() * sizeof(ui32))
                Y_ABORT("Invalid NPage::TFrame meta info blob header");

            Records = { TDeref<TEntry>::At(ptr, hdr->Skip) , Stats_.Items };
        }

        const TStats& Stats() const noexcept
        {
            return Stats_;
        }

        const TEntry& Relation(ui32 page) const noexcept
        {
            return page < +Records.size() ? Records[page] : End;
        }

        ui32 Lower(ui64 row, ui32 begin, ui32 end) const noexcept
        {
            begin = Min(begin, Stats_.Items);
            end = Min(end, Stats_.Items);

            for (int hope = 0; hope < 4 && begin < end;) {
                auto &rel = Records[begin];

                if (rel.Row >= row) return begin;

                hope += rel.IsHead();
                begin = rel.AbsRef(begin);
            }

            auto on = Records.begin() + begin;

            on = std::lower_bound(on, Records.begin() + end, row,
                [](const TEntry &left, ui64 row){ return left.Row < row; });

            return std::distance(Records.begin(), on);
        }

    public:
        const TSharedData Raw;

    private:
        const TEntry End;
        TStats Stats_;
        TArrayRef<const TEntry> Records;
    };

}
}
}
