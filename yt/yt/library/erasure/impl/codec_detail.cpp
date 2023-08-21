#include "codec_detail.h"

namespace NYT::NErasure::NDetail {

////////////////////////////////////////////////////////////////////////////////

struct TJerasureBlobTag
{ };

struct TLrcBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

TCodecTraits::TMutableBlobType TCodecTraits::AllocateBlob(size_t size)
{
    return TMutableBlobType::Allocate<TJerasureBlobTag>(size, {.InitializeStorage = false});
}

TCodecTraits::TBufferType TCodecTraits::AllocateBuffer(size_t size)
{
    // Only LRC now uses buffer allocation.
    return TBufferType(GetRefCountedTypeCookie<TLrcBufferTag>(), size);
}

TCodecTraits::TBlobType TCodecTraits::FromBufferToBlob(TBufferType&& blob)
{
    return TBlobType::FromBlob(std::move(blob));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NErasure::NDetail
