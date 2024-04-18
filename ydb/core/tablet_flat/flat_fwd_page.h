#pragma once

#include "flat_part_iface.h"
#include "flat_sausage_fetch.h"
#include "flat_fwd_misc.h"

namespace NKikimr {
namespace NTable {
namespace NFwd {

    enum class EUsage : ui8 {
        None    = 0,
        Seen    = 1,    /* Page has been used by reference  */
        Keep    = 2,    /* Data has been used at least once */
    };

    enum class EFetch : ui8 {
        Wait    = 0,    /* Page has been queued for load    */
        Drop    = 1,    /* Queued page page won't be used   */
        None    = 2,
        Done    = 3,    /* Page has been settled with data  */
    };

    struct TPage {
        TPage(TPageId pageId, ui64 size, ui16 tag, TPageId refer)
            : Size(size), PageId(pageId), Refer(refer), Tag(tag)
        {

        }

        ~TPage()
        {
            Y_ABORT_UNLESS(!Data, "Forward cache page is still holds data");
        }

        explicit operator bool() const
        {
            return bool(Data) && PageId != Max<ui32>();
        }

        bool Ready() const noexcept
        {
            return Fetch == EFetch::None || Fetch == EFetch::Done;
        }

        bool operator<(TPageId pageId) const
        {
            return PageId < pageId;
        }

        const TSharedData* Plain() const noexcept
        {
            return Data ? &Data : nullptr;
        }

        ui32 Settle(NPageCollection::TLoadedPage &page) noexcept
        {
            const auto was = std::exchange(Fetch, EFetch::Done);

            if (PageId != page.PageId) {
                Y_ABORT("Settling page with different reference number");
            } else if (Size != page.Data.size()) {
                Y_ABORT("Requested and obtained page sizes are not the same");
            } else if (was == EFetch::Drop) {
                std::exchange(page.Data, { });
            } else if (was != EFetch::Wait) {
                Y_ABORT("Settling page that is not waiting for any data");
            } else {
                Data = std::move(page.Data);
            }

            return Data.size();
        }

        const TSharedData* Touch(TPageId pageId, TStat &stat) noexcept
        {
            if (PageId != pageId || (!Data && Fetch == EFetch::Done)) {
                Y_ABORT("Touching page that doesn't fit to this action");
            } else {
                auto to = Fetch == EFetch::None ? EUsage::Seen : EUsage::Keep;

                if (std::exchange(Usage, to) != to && to == EUsage::Keep)
                    stat.Usage += Size;
            }

            return Plain();
        }

        TSharedData Release() noexcept
        {
            Fetch = Max(Fetch, EFetch::Drop);

            return std::exchange(Data, { });
        }

        const ui64 Size = 0;
        const ui32 PageId = Max<ui32>();
        const ui32 Refer = 0;
        const ui16 Tag  = Max<ui16>();
        EUsage Usage    = EUsage::None;
        EFetch Fetch    = EFetch::None;
        TSharedData Data;
    };

}
}
}
