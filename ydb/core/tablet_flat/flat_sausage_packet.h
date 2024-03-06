#pragma once

#include "flat_sausage_meta.h"
#include "flat_sausage_solid.h"
#include "flat_sausage_gut.h"

namespace NKikimr {
namespace NPageCollection {

    class TPageCollection: public IPageCollection {
    public:
        TPageCollection() = delete;

        TPageCollection(TLargeGlobId largeGlobId, TSharedData raw)
            : LargeGlobId(largeGlobId)
            , Meta(std::move(raw), LargeGlobId.Group)
        {
            if (!Meta.Raw || LargeGlobId.Bytes != Meta.Raw.size() || LargeGlobId.Group == TLargeGlobId::InvalidGroup)
                Y_ABORT("Invalid TLargeGlobId of page collection meta blob");
        }

        const TLogoBlobID& Label() const noexcept override
        {
            return LargeGlobId.Lead;
        }

        ui32 Total() const noexcept override
        {
            return Meta.TotalPages();
        }

        TBorder Bounds(ui32 page) const noexcept override
        {
            return Meta.Bounds(page);
        }

        TGlobId Glob(ui32 blob) const noexcept override
        {
            return Meta.Glob(blob);
        }

        TInfo Page(ui32 page) const noexcept override
        {
            return Meta.Page(page);
        }

        bool Verify(ui32 page, TArrayRef<const char> body) const noexcept override
        {
            return
                Meta.Page(page).Size == body.size()
                && Meta.GetPageChecksum(page) == Checksum(body);
        }

        size_t BackingSize() const noexcept override
        {
            return Meta.BackingSize();
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
    };
}
}
