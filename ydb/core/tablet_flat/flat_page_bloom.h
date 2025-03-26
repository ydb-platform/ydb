#pragma once

#include "flat_page_label.h"
#include "flat_bloom_hash.h"
#include "util_deref.h"
#include "util_fmt_abort.h"

namespace NKikimr {
namespace NTable {
namespace NPage {

    struct TBloom : public virtual TThrRefBase {
    public:
        struct THeader {
            ui16 Type = 0;
            ui16 Hashes = 0;
            ui32 Pad0_ = 0;
            ui64 Items = 0;
        };

        struct TStats {
            ui32 Items;
            ui32 Hashes;
        };

        static_assert(sizeof(THeader) == 16, "Invalid TBloom page header");

        TBloom(TSharedData page)
            : Raw(std::move(page))
        {
            const auto got = NPage::TLabelWrapper().Read(Raw, EPage::Bloom);

            Y_ENSURE(got == ECodec::Plain && got.Version == 0);

            auto *header = TDeref<THeader>::At(got.Page.data(), 0);

            if (sizeof(THeader) > got.Page.size()) {
                Y_TABLET_ERROR("NPage::TBloom header is out of its blob");
            }

            auto *ptr = TDeref<ui64>::At(got.Page.data(), sizeof(THeader));

            Hashes = header->Hashes;
            Items = header->Items;
            Array = { ptr, (got.Page.size() - sizeof(THeader)) / sizeof(ui64) };

            if (Items == 0) {
                Y_TABLET_ERROR("NPage::TBloom page has zero items in index");
            } else if (Hashes == 0) {
                Y_TABLET_ERROR("NPage::TBloom page has zero hash count");
            } else if (ui64(Array.size()) << 6 != header->Items) {
                Y_TABLET_ERROR("Items in TBloom header isn't match with array");
            } else if (header->Type != 0) {
                Y_TABLET_ERROR("NPage::TBloom page made with unknown hash type");
            }
        }

        TStats Stats() const noexcept
        {
            return { Items, Hashes };
        }

        bool MightHave(TArrayRef<const char> raw) const
        {
            NBloom::THash hash(raw);

            for (ui32 seq = 0; seq++ < Hashes; ) {
                const ui64 num = hash.Next() % Items;

                if (!(Array[num >> 6] & (ui64(1) << (num & 0x3f))))
                    return false;
            }

            return true;
        }

    public:
        const TSharedData Raw;

    private:
        TArrayRef<const ui64> Array;
        ui32 Hashes = 0;
        ui32 Items = 0;
    };

}
}
}
