#pragma once

#include "flat_sausage_grind.h"
#include "flat_writer_conf.h"
#include "flat_boot_cookie.h"

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NWriter {

    class ICone {
    public:
        virtual NPageCollection::TCookieAllocator& CookieRange(ui32 cookieRange) = 0;
        virtual void Put(NPageCollection::TGlob&&) = 0;
        virtual NPageCollection::TLargeGlobId Put(ui32 cookieRange, ui8 channel, TArrayRef<const char> body, ui32 block) = 0;
    };

    class TBanks {
    public:
        using TCookie = NBoot::TCookie;
        using EIdx = NBoot::TCookie::EIdx;

        TBanks(const TLogoBlobID &base, TArrayRef<const TConf::TSlot> row)
            : Meta(base.TabletID(), Stamp(base), TCookie::CookieRange(EIdx::Pack), row)
            , Data(base.TabletID(), Stamp(base), TCookie::CookieRangeRaw(), row)
        {

        }

        static ui64 Stamp(const TLogoBlobID &logo) noexcept
        {
            return (ui64(logo.Generation()) << 32) | logo.Step();
        }

        NPageCollection::TCookieAllocator Meta;  /* PageCollections meta blobs      */
        NPageCollection::TCookieAllocator Data;  /* PageCollections body and blobs  */
    };
}
}
}
