#pragma once

#include "flat_sausage_misc.h"
#include "flat_sausage_layout.h"
#include "flat_sausage_solid.h"

#include <ydb/library/actors/util/shared_data.h>

namespace NKikimr {
namespace NPageCollection {

    class TMeta {
    public:
        TMeta(TSharedData blob, ui32 group);
        ~TMeta();

        ui32 TotalPages() const { return Header->Pages; }

        inline TArrayRef<const TLogoBlobID> Blobs() const noexcept
        {
            auto *blobs = reinterpret_cast<const TLogoBlobID *>(Header + 1);

            return TArrayRef<const TLogoBlobID>(blobs,  Header->Blobs);
        }

        inline TBorder Bounds(ui32 page) const noexcept
        {
            return Bounds(page, page);
        }

        inline TGlobId Glob(ui32 blob) const noexcept
        {
            return { Blobs()[blob], Group };
        }

        size_t BackingSize() const noexcept;
        TBorder Bounds(ui32 begin, ui32 end) const noexcept;
        TInfo Page(ui32 page) const noexcept;
        ui32 GetPageType(ui32 pageId) const noexcept;
        ui32 GetPageChecksum(ui32 pageId) const noexcept;
        ui64 GetPageSize(ui32 pageId) const noexcept;
        TStringBuf GetPageInplaceData(ui32 pageId) const noexcept;

    public:
        const TSharedData Raw;  /* Page collection serialized meta blob */
        const ui32 Group = TLargeGlobId::InvalidGroup;

    private:
        const THeader *Header = nullptr;
        const TEntry *Index = nullptr;
        const TExtra *Extra = nullptr;
        const char *InboundData = nullptr;
        TVector<ui64> Steps;    /* Pages boundaries vector  */
    };

}
}
