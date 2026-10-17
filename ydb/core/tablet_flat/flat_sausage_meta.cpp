#include "flat_sausage_align.h"
#include "flat_sausage_meta.h"
#include <library/cpp/digest/crc32c/crc32c.h>

namespace NKikimr {
namespace NPageCollection {

TMeta::TMeta(TSharedData raw, ui32 group)
    : Raw(std::move(raw))
    , Group(group)
{
    Y_ENSURE(Raw.size() >= sizeof(NPageCollection::THeader));
    Header = (const NPageCollection::THeader *)Raw.data();
    Y_ENSURE(Header->Magic == NPageCollection::Magic);

    if (Header->Pages == 0)
        return;

    auto * const blobs = (const TLogoBlobID *)(Header + 1);

    Index = (const NPageCollection::TEntry*)(blobs + Header->Blobs);
    Extra = (const NPageCollection::TExtra*)(Index + Header->Pages);
    InboundData = (const char *)(Extra + Header->Pages);

    if (const auto blobs = Blobs()) {
        ui64 offset = 0;

        Steps.reserve(blobs.size());
        for (auto &one: blobs)
            Steps.push_back(offset += one.BlobSize());
    }

    /* Crc32 stores pages - 1 per skip entry (not the raw count, see TRecord::PushSkip);
       Total = MetaPages + SkippedPages gives the correct full page count. */
    SkippedPages_ = (Extra[0].Type == ui32(NTable::NPage::EPage::Skip) ? Extra[0].Crc32 : 0);
}

TMeta::~TMeta()
{

}

size_t TMeta::BackingSize() const noexcept
{
    return Steps ? Steps.back() : 0;
}

TBorder TMeta::Bounds(ui32 begin, ui32 end) const
{
    Y_ENSURE(begin <= end && Max(begin, end) < Header->Pages);

    const ui64 offset = (begin == 0) ? 0 : Index[begin - 1].Page;

    return TAlign(Steps).Lookup(offset, Index[end].Page - offset);
}

TInfo TMeta::Page(ui32 page) const
{
    Y_ENSURE(page < Header->Pages,
            "Requested page " << page << " out of " << Header->Pages << " total pages");

    return { GetPageSize(page), Extra[page].Type };
}

ui32 TMeta::GetPageType(ui32 pageId) const
{
    Y_DEBUG_ABORT_UNLESS(pageId < Header->Pages);
    return Extra[pageId].Type;
}

ui32 TMeta::GetPageChecksum(ui32 pageId) const
{
    Y_DEBUG_ABORT_UNLESS(pageId < Header->Pages);
    return Extra[pageId].Crc32;
}

ui64 TMeta::GetPageSize(ui32 pageId) const
{
    Y_DEBUG_ABORT_UNLESS(pageId < Header->Pages);

    const ui64 begin = (pageId == 0) ? 0 : Index[pageId - 1].Page;
    return Index[pageId].Page - begin;
}

NTable::NPage::TPageLocation TMeta::GetLocation(ui32 pageId) const
{
    Y_ENSURE(pageId < Header->Pages);
    Y_ENSURE(Extra[pageId].Type != ui32(NTable::NPage::EPage::Skip),
        "Cannot get location for skip page entry by pageId");

    const ui64 offset = (pageId == 0) ? 0 : Index[pageId - 1].Page;
    const ui64 size = Index[pageId].Page - offset;

    return NTable::NPage::TPageLocation::FromByteOffset(offset, size, static_cast<NTable::NPage::EPage>(Extra[pageId].Type), Extra[pageId].Crc32);
}

TBorder TMeta::Bounds(const NTable::NPage::TPageLocation& location) const
{
    Y_ENSURE(!location.Offset.IsMax());
    if (!location.Offset.IsByteOffset()) {
        // Page-index path (blob forward cache, outer collections):
        //   resolve byte offset from Index, validate size against metadata.
        const auto pageId = location.Offset.AsPageIndex();
        Y_ENSURE(pageId < Header->Pages,
            "Requested page " << pageId << " out of " << Header->Pages << " total pages");
        const ui64 offset = pageId ? Index[pageId - 1].Page : 0;
        const ui64 size = Index[pageId].Page - offset;
        Y_DEBUG_ABORT_UNLESS(location.Size == size,
            "Size mismatch at page %" PRIu32 ": location claims %" PRIu64 " but meta has %" PRIu64,
            pageId, location.Size, size);
        return TAlign(Steps).Lookup(offset, size);
    }
    return TAlign(Steps).Lookup(location.GetByteOffset(), location.Size);
}

TStringBuf TMeta::GetPageInplaceData(ui32 pageId) const
{
    Y_DEBUG_ABORT_UNLESS(pageId < Header->Pages);

    const ui64 end = Index[pageId].Inplace;
    const ui64 begin = (pageId == 0) ? 0 : Index[pageId - 1].Inplace;

    return TStringBuf(InboundData + begin, InboundData + end);
}

ui32 Checksum(TArrayRef<const char> body) noexcept
{
    return Crc32c(body.data(), body.size());
}

}
}
