#pragma once

#include "flat_page_label.h"
#include "flat_sausage_gut.h"
#include "flat_sausage_solid.h"
#include "util_deref.h"
#include <array>

namespace NKikimr {
namespace NTable {
namespace NPage {

    class TExtBlobs : public NPageCollection::IPageCollection {
    public:
        using TEntry = NPageCollection::TGlobId;

        struct THeader {
            ui32 Skip   = 0;    /* Skip bytes from header to frames */
            ui32 Pad0_  = 0;
            ui64 Pad1_  = 0;
            ui64 Pad2_  = 0;
            ui64 Bytes  = 0;    /* Total blob bytes referenced here */
            ui64 Pad3_  = 0;
        };

        struct TStats {
            ui32 Items;
            ui64 Bytes;
        };

        static_assert(sizeof(TEntry) == 32, "Invalid TExtBlobs entry unit");
        static_assert(sizeof(THeader) == 40, "Invalid TExtBlobs header unit");

        TExtBlobs(TSharedData raw, const TLogoBlobID &label)
            : Raw(std::move(raw))
            , Label_(label)
        {
            Y_ABORT_UNLESS(uintptr_t(Raw.data()) % alignof(TEntry) == 0);

            auto got = NPage::TLabelWrapper().Read(Raw, EPage::Globs);

            Y_ABORT_UNLESS(got == ECodec::Plain && got.Version == 1);

            Header = TDeref<THeader>::At(got.Page.data(), 0);

            if (Header->Skip > got.Page.size())
                Y_ABORT("NPage::TExtBlobs header is out of its blob");

            auto *ptr = TDeref<TEntry>::At(got.Page.data(), Header->Skip);

            Array = { ptr, (got.Page.size() - Header->Skip) / sizeof(TEntry) };
        }

        TStats Stats() const noexcept
        {
            return { ui32(Array.size()), Header->Bytes };
        }

        TArrayRef<const TEntry> operator*() const noexcept
        {
            return Array;
        }

        const TArrayRef<const TEntry>* operator->() const noexcept
        {
            return &Array;
        }

        const TLogoBlobID& Label() const noexcept override
        {
            return Label_;
        }

        ui32 Total() const noexcept override
        {
            return Array.size();
        }

        NPageCollection::TInfo Page(ui32 page) const noexcept override
        {
            return { Glob(page).Logo.BlobSize(), ui32(EPage::Opaque) };
        }

        NPageCollection::TBorder Bounds(ui32 page) const noexcept override
        {
            const auto size = Glob(page).Logo.BlobSize();

            return { size, { page, 0 }, { page, size } };
        }

        NPageCollection::TGlobId Glob(ui32 page) const noexcept override
        {
            return page < Array.size() ? Array[page] : Empty;
        }

        bool Verify(ui32 page, TArrayRef<const char> data) const noexcept override
        {
            return data && data.size() == Array.at(page).Bytes();
        }

        size_t BackingSize() const noexcept override
        {
            return Raw.size();
        }

    public:
        const TSharedData Raw;

    private:
        const TLogoBlobID Label_;
        const TEntry Empty;
        const THeader * Header = nullptr;
        TArrayRef<const TEntry> Array;
    };

}
}
}
