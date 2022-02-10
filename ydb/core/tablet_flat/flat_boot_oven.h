#pragma once

#include "defs.h"
#include "util_fmt_abort.h"
#include "flat_sausage_grind.h"
#include "flat_boot_cookie.h"
#include <ydb/core/base/blobstorage.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBoot {

    struct TSteppedCookieAllocatorFactory {
        static_assert(unsigned(TCookie::EIdx::Raw) < sizeof(ui64) * 8, "");

        using TSteppedCookieAllocator = NPageCollection::TSteppedCookieAllocator;

        TSteppedCookieAllocatorFactory() = delete;

        TSteppedCookieAllocatorFactory(const TTabletStorageInfo &info, ui32 gen)
            : Tablet(info.TabletID)
            , Gen(gen)
            , Sys1{ 1, info.GroupFor(1, Gen) }
            , Info(info)
        {

        }

        TAutoPtr<TSteppedCookieAllocator> Sys(TCookie::EIdx idx) noexcept
        {
            Acquire(idx);

            const NTable::TTxStamp stamp(Gen, 0);

            return new TSteppedCookieAllocator(Tablet, stamp, TCookie::CookieRange(idx), { Sys1 });
        }

        TAutoPtr<TSteppedCookieAllocator> Data() noexcept
        {
            Acquire(TCookie::EIdx::Raw);

            TVector<NPageCollection::TSlot> slots;

            for (auto &one: Info.Channels) {
                const auto group = one.GroupForGeneration(Gen);

                if (one.Channel == 0) {
                    /* Zero channel is reserved for system tablet activity */
                } else if (group == NPageCollection::TLargeGlobId::InvalidGroup) {
                    Y_Fail(
                        "Leader{" << Tablet << ":" << Gen << "} got reserved"
                        << " InvalidGroup value for channel " << one.Channel);

                } else if (group == Max<ui32>()) {
                    /* Channel is disabled for given group on this Gen */
                } else {
                    slots.emplace_back(one.Channel, group);
                }
            }

            const auto cookieRange = TCookie::CookieRangeRaw();

            return
                new TSteppedCookieAllocator(Tablet, NTable::TTxStamp{ Gen, 0 }, cookieRange, slots);
        }

    private:
        void Acquire(TCookie::EIdx idx) noexcept
        {
            const auto mask = ui64(1) << unsigned(idx);

            if (std::exchange(Issued, Issued | mask) & mask) {
                Y_Fail("Cookies cookieRange EIdx" << ui32(idx) << " is resued");
            }
        }

    public:
        const ui64 Tablet = Max<ui64>();
        const ui32 Gen = Max<ui32>();
        const NPageCollection::TSlot Sys1;

    private:
        const TTabletStorageInfo &Info;
        ui64 Issued = 0;
    };
}
}
}
