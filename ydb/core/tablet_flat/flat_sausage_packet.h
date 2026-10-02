#pragma once

#include "flat_sausage_meta.h"
#include "flat_sausage_solid.h"
#include "flat_sausage_gut.h"
#include "util_fmt_abort.h"

#include <atomic>

namespace NKikimr {
namespace NPageCollection {

    class TPageCollection: public IPageCollection {
    public:
        TPageCollection() = delete;

        TPageCollection(TLargeGlobId largeGlobId, TSharedData raw)
            : LargeGlobId(largeGlobId)
            , Meta(std::move(raw), LargeGlobId.Group)
            , SkippedInMeta(Meta.SkippedPages())
        {
            if (!Meta.Raw || LargeGlobId.Bytes != Meta.Raw.size() || LargeGlobId.Group == TLargeGlobId::InvalidGroup) {
                Y_TABLET_ERROR("Invalid TLargeGlobId of page collection meta blob");
            }
        }

        const TLogoBlobID& Label() const noexcept override
        {
            return LargeGlobId.Lead;
        }

        ui32 Total() const noexcept override
        {
            return MetaPages() + SkippedInMeta;
        }

        /* Structural pages addressable by TPageId (enumerated in TMeta).
           Total() may exceed MetaPages() for shrunk v2 collections. SkippedInMeta
           carries the number of extra pages beyond MetaPages. */
        ui32 MetaPages() const noexcept override
        {
            return Meta.TotalPages();
        }

        TBorder Bounds(ui32 page) const override
        {
            return Meta.Bounds(page);
        }

        TGlobId Glob(ui32 blob) const override
        {
            return Meta.Glob(blob);
        }

        TInfo Page(ui32 page) const override
        {
            return Meta.Page(page);
        }

        bool Verify(ui32 page, TArrayRef<const char> body) const override
        {
            return
                Meta.Page(page).Size == body.size()
                && Meta.GetPageChecksum(page) == Checksum(body);
        }

        TBorder Bounds(const TPageLocation& location) const override
        {
            return Meta.Bounds(location);
        }

        bool Verify(const TPageLocation& location, TArrayRef<const char> data) const override
        {
            return data.size() == location.Size && Checksum(data) == location.Crc32;
        }

        TPageLocation GetLocation(ui32 pageId) const override
        {
            return Meta.GetLocation(pageId);
        }

        size_t BackingSize() const noexcept override
        {
            return Meta.BackingSize();
        }

        bool SkipBTreeIndexV1Shadow() const noexcept override
        {
            return SkipBTreeIndexV1Shadow_.load(std::memory_order_relaxed);
        }

        void SetSkipBTreeIndexV1Shadow(bool v) const noexcept override
        {
            SkipBTreeIndexV1Shadow_.store(v, std::memory_order_relaxed);
        }

        template<typename TContainer>
        void SaveAllBlobIdsTo(TContainer &vec) const
        {
            LargeGlobId.MaterializeTo(vec);

            {
                auto blobs = Meta.Blobs();
                vec.insert(vec.end(), blobs.begin(), blobs.end());
            }
        }

        const TLargeGlobId LargeGlobId;
        const TMeta Meta;
        const ui32 SkippedInMeta;
        // Set before the collection is handed to the shared cache; read afterwards by cache actors
        mutable std::atomic<bool> SkipBTreeIndexV1Shadow_ = false;
    };

    /// Page-index TPageOffset to satisfy forward cache
    class TOuterPageCollection : public TPageCollection {
    public:
        TOuterPageCollection(TLargeGlobId largeGlobId, TSharedData raw)
            : TPageCollection(std::move(largeGlobId), std::move(raw))
        {}

        NTable::NPage::TPageLocation GetLocation(ui32 pageId) const override
        {
            auto info = Meta.Page(pageId);
            return NTable::NPage::TPageLocation::FromPageIndex(
                pageId, info.Size,
                static_cast<NTable::NPage::EPage>(info.Type),
                Meta.GetPageChecksum(pageId));
        }
    };
}
}
