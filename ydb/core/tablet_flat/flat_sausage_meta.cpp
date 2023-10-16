#include "flat_sausage_align.h"
#include "flat_sausage_meta.h"
#include <library/cpp/digest/crc32c/crc32c.h>

namespace NKikimr {
namespace NPageCollection {

TMeta::TMeta(TSharedData raw, ui32 group)
    : Raw(std::move(raw))
    , Group(group)
{
    Y_ABORT_UNLESS(Raw.size() >= sizeof(NPageCollection::THeader));
    Header = (const NPageCollection::THeader *)Raw.data();
    Y_ABORT_UNLESS(Header->Magic == NPageCollection::Magic);

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
}

TMeta::~TMeta()
{

}

size_t TMeta::BackingSize() const noexcept
{
    return Steps ? Steps.back() : 0;
}

TBorder TMeta::Bounds(ui32 begin, ui32 end) const noexcept
{
    Y_ABORT_UNLESS(begin <= end && Max(begin, end) < Header->Pages);

    const ui64 offset = (begin == 0) ? 0 : Index[begin - 1].Page;

    return TAlign(Steps).Lookup(offset, Index[end].Page - offset);
}

TInfo TMeta::Page(ui32 page) const noexcept
{
    Y_ABORT_UNLESS(page < Header->Pages,
            "Requested page %" PRIu32 " out of %" PRIu32 " total pages",
            page, Header->Pages);

    return { GetPageSize(page), Extra[page].Type };
}

ui32 TMeta::GetPageType(ui32 pageId) const noexcept
{
    Y_DEBUG_ABORT_UNLESS(pageId < Header->Pages);
    return Extra[pageId].Type;
}

ui32 TMeta::GetPageChecksum(ui32 pageId) const noexcept
{
    Y_DEBUG_ABORT_UNLESS(pageId < Header->Pages);
    return Extra[pageId].Crc32;
}

ui64 TMeta::GetPageSize(ui32 pageId) const noexcept
{
    Y_DEBUG_ABORT_UNLESS(pageId < Header->Pages);

    const ui64 begin = (pageId == 0) ? 0 : Index[pageId - 1].Page;
    return Index[pageId].Page - begin;
}

TStringBuf TMeta::GetPageInplaceData(ui32 pageId) const noexcept
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
